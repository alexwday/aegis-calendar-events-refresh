#!/usr/bin/env python3
"""
Stage 2: Data Processing
========================
Reads raw data from Stage 1, cleanses and transforms it.

Key Features:
- Field mapping at the top for easy adaptation to different data sources
- All transformations: enrichment, timezone conversion, deduplication
- Output matches PostgreSQL schema exactly

Input:  ../stage_1_data_acquisition/output/raw_calendar_events.csv
Output: output/processed_calendar_events.csv
"""

import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import yaml
import pytz
from dateutil.parser import parse as dateutil_parse

# =============================================================================
# CONFIGURATION (hardcoded - change here if needed)
# =============================================================================

LOCAL_TIMEZONE = "America/Toronto"

# Earnings event priority (first = highest, kept when duplicates exist)
EARNINGS_PRIORITY = ["Earnings", "ConfirmedEarningsRelease", "ProjectedEarningsRelease"]

# =============================================================================
# FIELD MAPPING - Update this when switching data sources (API → Snowflake)
# =============================================================================
# Maps from SOURCE field names (in raw CSV) to INTERNAL field names
# When switching to Snowflake, just update the source field names on the right

FIELD_MAPPING = {
    # Internal Name         : Source Name (from raw CSV)
    "event_id":             "event_id",
    "ticker":               "ticker",
    "event_type":           "event_type",
    "event_datetime_utc":   "event_date_time",      # API: event_date_time
    "description":          "description",          # API: description (becomes event_headline)
    "webcast_link":         "webcast_link",
    "contact_name":         "contact_name",
    "contact_phone":        "contact_phone",
    "contact_email":        "contact_email",
    "fiscal_year":          "fiscal_year",
    "fiscal_period":        "fiscal_period",
}

# =============================================================================
# OUTPUT SCHEMA - Matches PostgreSQL table aegis_calendar_events
# =============================================================================
# These are the exact field names expected by the database
# Do NOT change these unless you also update the database schema

OUTPUT_SCHEMA = [
    "event_id",
    "ticker",
    "institution_name",
    "institution_id",
    "institution_type",
    "event_type",
    "event_headline",
    "event_date_time_utc",
    "event_date_time_local",
    "event_date",
    "event_time_local",
    "webcast_link",
    "contact_info",
    "fiscal_year",
    "fiscal_period",
    "data_fetched_timestamp",
]

# =============================================================================
# PROCESSING FUNCTIONS
# =============================================================================

def load_raw_data(input_path: Path) -> list:
    """Load raw CSV data from Stage 1."""
    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def load_monitored_institutions() -> dict:
    """Load monitored institutions from project root."""
    institutions_path = Path(__file__).parent.parent / "monitored_institutions.yaml"
    with open(institutions_path, "r") as f:
        return yaml.safe_load(f)


def get_mapped_value(raw_event: dict, internal_field: str, default=""):
    """
    Get a value from raw event using the field mapping.
    This abstraction makes it easy to switch data sources.
    """
    source_field = FIELD_MAPPING.get(internal_field, internal_field)
    return raw_event.get(source_field, default) or default


def convert_to_local_time(utc_datetime_str: str) -> tuple:
    """
    Convert UTC datetime string to local time.
    Returns: (utc_iso, local_iso, date_str, time_with_tz)
    """
    if not utc_datetime_str:
        return ("", "", "", "")

    try:
        # Parse the datetime
        dt = dateutil_parse(str(utc_datetime_str))

        # Ensure it's UTC
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)

        # Convert to local timezone
        local_tz = pytz.timezone(LOCAL_TIMEZONE)
        dt_local = dt.astimezone(local_tz)

        # Format outputs
        utc_iso = dt.isoformat()
        local_iso = dt_local.isoformat()
        date_str = dt_local.strftime("%Y-%m-%d")
        tz_abbr = dt_local.strftime("%Z")  # EST or EDT
        time_with_tz = dt_local.strftime(f"%H:%M {tz_abbr}")

        return (utc_iso, local_iso, date_str, time_with_tz)

    except Exception as e:
        print(f"    WARNING: Could not parse datetime '{utc_datetime_str}': {e}")
        return ("", "", "", "")


def build_contact_info(raw_event: dict) -> str:
    """Build contact info string from separate fields."""
    parts = []

    contact_name = get_mapped_value(raw_event, "contact_name")
    contact_phone = get_mapped_value(raw_event, "contact_phone")
    contact_email = get_mapped_value(raw_event, "contact_email")

    if contact_name:
        parts.append(f"Contact: {contact_name}")
    if contact_phone:
        parts.append(f"Phone: {contact_phone}")
    if contact_email:
        parts.append(f"Email: {contact_email}")

    return " | ".join(parts)


def process_event(raw_event: dict, institutions: dict, timestamp: str) -> dict:
    """
    Process a single raw event into the output schema format.
    """
    # Get mapped values
    ticker = get_mapped_value(raw_event, "ticker")
    event_datetime_utc = get_mapped_value(raw_event, "event_datetime_utc")

    # Get institution metadata
    institution = institutions.get(ticker, {})

    # Convert timezone
    utc_iso, local_iso, date_str, time_with_tz = convert_to_local_time(event_datetime_utc)

    # Build processed event matching OUTPUT_SCHEMA
    return {
        "event_id": get_mapped_value(raw_event, "event_id"),
        "ticker": ticker,
        "institution_name": institution.get("name", "Unknown"),
        "institution_id": institution.get("id", ""),
        "institution_type": institution.get("type", "Unknown"),
        "event_type": get_mapped_value(raw_event, "event_type"),
        "event_headline": get_mapped_value(raw_event, "description"),
        "event_date_time_utc": utc_iso,
        "event_date_time_local": local_iso,
        "event_date": date_str,
        "event_time_local": time_with_tz,
        "webcast_link": get_mapped_value(raw_event, "webcast_link"),
        "contact_info": build_contact_info(raw_event),
        "fiscal_year": get_mapped_value(raw_event, "fiscal_year"),
        "fiscal_period": get_mapped_value(raw_event, "fiscal_period"),
        "data_fetched_timestamp": timestamp,
    }


def deduplicate_earnings_events(events: list) -> list:
    """
    Deduplicate earnings-related events for the same institution and fiscal period.
    Priority order determined by EARNINGS_PRIORITY constant (first = highest priority).
    """
    # Build priority lookup
    priority_lookup = {et: i for i, et in enumerate(EARNINGS_PRIORITY)}

    # Group events by ticker + fiscal period
    event_groups = defaultdict(list)

    for event in events:
        ticker = event.get("ticker", "")
        fiscal_year = event.get("fiscal_year", "")
        fiscal_period = event.get("fiscal_period", "")
        event_type = event.get("event_type", "")

        # Determine grouping key
        if event_type in priority_lookup:
            # Earnings events: group by fiscal period
            if fiscal_year and fiscal_period:
                key = f"{ticker}|{fiscal_year}|{fiscal_period}"
            else:
                # Fallback to date if no fiscal period
                event_date = event.get("event_date", "")
                key = f"{ticker}|date|{event_date}"
        else:
            # Non-earnings events: unique key (no deduplication)
            event_id = event.get("event_id", "")
            key = f"unique|{event_id}"

        event_groups[key].append(event)

    # Deduplicate each group
    deduplicated = []

    for key, group_events in event_groups.items():
        # Separate earnings from non-earnings
        earnings_events = [e for e in group_events if e.get("event_type") in priority_lookup]
        other_events = [e for e in group_events if e.get("event_type") not in priority_lookup]

        if earnings_events:
            # Sort by priority and keep highest
            earnings_events.sort(key=lambda e: priority_lookup.get(e.get("event_type"), 999))
            deduplicated.append(earnings_events[0])

        deduplicated.extend(other_events)

    return deduplicated


def save_processed_data(events: list, output_path: Path) -> bool:
    """Save processed events to CSV matching OUTPUT_SCHEMA."""
    if not events:
        print("  WARNING: No events to save")
        return False

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_SCHEMA)
        writer.writeheader()
        writer.writerows(events)

    return True


def main():
    """Main execution function for Stage 2: Data Processing."""
    print("=" * 60)
    print("STAGE 2: DATA PROCESSING")
    print("=" * 60)
    print()

    # Step 1: Load raw data from Stage 1
    print("[1/5] Loading raw data from Stage 1...")
    input_path = Path(__file__).parent.parent / "stage_1_data_acquisition" / "output" / "raw_calendar_events.csv"

    if not input_path.exists():
        print(f"  ERROR: Input file not found: {input_path}")
        print("  Please run Stage 1 first.")
        return

    raw_events = load_raw_data(input_path)
    print(f"  Loaded {len(raw_events)} raw events")

    # Step 2: Load institution metadata
    print("\n[2/5] Loading institution metadata...")
    institutions = load_monitored_institutions()
    print(f"  Loaded {len(institutions)} institutions")

    # Step 3: Process events
    print("\n[3/5] Processing events...")
    print(f"  Timezone: {LOCAL_TIMEZONE}")
    print(f"  Field mapping:")
    for internal, source in FIELD_MAPPING.items():
        print(f"    {internal} <- {source}")

    timestamp = datetime.now(pytz.UTC).isoformat()
    processed_events = []

    for raw_event in raw_events:
        processed = process_event(raw_event, institutions, timestamp)
        processed_events.append(processed)

    print(f"  Processed {len(processed_events)} events")

    # Step 4: Deduplicate earnings events
    print("\n[4/5] Deduplicating earnings events...")
    print(f"  Priority: {EARNINGS_PRIORITY}")
    before_count = len(processed_events)
    processed_events = deduplicate_earnings_events(processed_events)
    after_count = len(processed_events)
    removed = before_count - after_count
    print(f"  Before: {before_count}, After: {after_count}, Removed: {removed}")

    # Sort by event date
    processed_events.sort(key=lambda x: x.get("event_date_time_utc", ""))

    # Step 5: Save processed data
    print("\n[5/5] Saving processed data...")
    output_path = Path(__file__).parent / "output" / "processed_calendar_events.csv"
    save_processed_data(processed_events, output_path)
    print(f"  Saved to: {output_path}")
    print(f"  Output schema: {', '.join(OUTPUT_SCHEMA)}")

    # Summary
    print()
    print("=" * 60)
    print("STAGE 2 COMPLETE")
    print(f"  Events processed: {len(processed_events)}")
    print(f"  Duplicates removed: {removed}")
    print(f"  Output: {output_path}")
    print("=" * 60)

    # Stats
    event_types = defaultdict(int)
    institution_types = defaultdict(int)
    for event in processed_events:
        event_types[event.get("event_type", "Unknown")] += 1
        institution_types[event.get("institution_type", "Unknown")] += 1

    print("\nEvent Types:")
    for et, count in sorted(event_types.items(), key=lambda x: -x[1]):
        print(f"  {et}: {count}")

    print("\nInstitution Types:")
    for it, count in sorted(institution_types.items(), key=lambda x: -x[1]):
        print(f"  {it}: {count}")


if __name__ == "__main__":
    main()
