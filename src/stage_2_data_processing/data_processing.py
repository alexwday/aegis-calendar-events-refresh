#!/usr/bin/env python3
"""
Stage 2: Data Processing - Transforms raw calendar events from Stage 1 into a clean,
deduplicated dataset ready for database upload. Key transformations include: enriching
events with institution metadata, converting UTC timestamps to local Toronto time,
filtering to approved event types, consolidating duplicate earnings events (keeping highest
priority), and renaming event types for consistency. The output schema matches the
PostgreSQL table structure exactly. Field mapping at the top allows easy adaptation when
switching data sources from API to Snowflake.
"""

import csv
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import yaml
import pytz
from dateutil.parser import parse as dateutil_parse

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
INPUT_PATH = (
    Path(__file__).parent.parent
    / "stage_1_data_acquisition/output/raw_calendar_events.csv"
)
OUTPUT_PATH = Path(__file__).parent / "output" / "processed_calendar_events.csv"

# Timezone for local time conversion
LOCAL_TIMEZONE = "America/Toronto"

# Canadian bank tickers that should always be -CA, never -US
# (FactSet sometimes returns these with -US suffix)
# Note: LB is excluded - LB-US is a different company, not Laurentian Bank
CANADIAN_BANK_BASES = ["RY", "TD", "BMO", "BNS", "CM", "NA"]

# Event types to include in final output
INCLUDED_EVENT_TYPES = [
    "Earnings",
    "SalesRevenue",
    "Dividend",
    "Conference",
    "ShareholdersMeeting",
    "AnalystsInvestorsMeeting",
    "SpecialSituation",
]

# Event types to EXCLUDE completely (filtered out before processing)
EXCLUDED_EVENT_TYPES = [
    "ProjectedEarningsRelease",  # Projections are often inaccurate
    "ConfirmedEarningsRelease",  # Only keep earnings calls, not release dates
]

# Deduplication: keep one event per ticker+fiscal_period, priority order (first=highest)
DEDUP_RULES = {}

# Same-datetime deduplication: when events have IDENTICAL ticker + datetime,
# keep only the highest priority type (first in list = highest priority)
DATETIME_DEDUP_PRIORITY = []

# Simple renames: source type -> target type (applied AFTER datetime dedup)
RENAME_RULES = {
    "SalesRevenueRelease": "SalesRevenue",
    "SalesRevenueCall": "SalesRevenue",
}

# Field mapping: internal name -> source CSV column name
FIELD_MAPPING = {
    "event_id": "event_id",
    "ticker": "ticker",
    "event_type": "event_type",
    "event_datetime_utc": "event_date_time",
    "description": "description",
    "webcast_link": "webcast_link",
    "contact_name": "contact_name",
    "contact_phone": "contact_phone",
    "contact_email": "contact_email",
    "fiscal_year": "fiscal_year",
    "fiscal_period": "fiscal_period",
    "market_time_code": "market_time_code",
    "last_modified_date": "last_modified_date",
}

# Output CSV columns matching PostgreSQL schema
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


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


def load_raw_events(path):
    """Load raw CSV events from Stage 1."""
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_institutions():
    """Load institution metadata from YAML config."""
    with open(PROJECT_ROOT / "monitored_institutions.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_field(event, field, default=""):
    """Get mapped field value from raw event."""
    return event.get(FIELD_MAPPING.get(field, field), default) or default


def normalize_canadian_ticker(ticker):
    """
    Normalize Canadian bank tickers: -US -> -CA.

    FactSet sometimes returns Canadian bank events with -US suffix.
    This ensures institution lookup works correctly.
    """
    ticker = (ticker or "").strip()
    for base in CANADIAN_BANK_BASES:
        if ticker == f"{base}-US":
            return f"{base}-CA"
    return ticker


def convert_timezone(utc_str, time_unconfirmed=False):
    """Convert UTC datetime string to local time.

    Returns (utc_iso, local_iso, date, time).

    Args:
        utc_str: UTC datetime string to convert
        time_unconfirmed: If True (from market_time_code="Unspecified"), preserve
            the UTC date and set local time to midnight. This prevents unconfirmed
            times (which FactSet sets to midnight UTC) from shifting to the previous
            day during timezone conversion.
    """
    if not utc_str:
        return ("", "", "", "")
    try:
        dt = dateutil_parse(str(utc_str))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)

        # Handle unconfirmed times: keep the date intact instead of shifting
        if time_unconfirmed:
            date_str = dt.strftime("%Y-%m-%d")
            local_tz = pytz.timezone(LOCAL_TIMEZONE)
            # Create midnight in local timezone for that same date
            dt_local = local_tz.localize(
                datetime(dt.year, dt.month, dt.day, 0, 0, 0)
            )
            tz_abbr = dt_local.strftime("%Z")
            return (
                dt.isoformat(),
                dt_local.isoformat(),
                date_str,
                f"00:00 {tz_abbr}",
            )

        dt_local = dt.astimezone(pytz.timezone(LOCAL_TIMEZONE))
        tz_abbr = dt_local.strftime("%Z")
        if tz_abbr not in ("EST", "EDT"):
            log.warning("Unexpected timezone: %s", tz_abbr)
        return (
            dt.isoformat(),
            dt_local.isoformat(),
            dt_local.strftime("%Y-%m-%d"),
            dt_local.strftime(f"%H:%M {tz_abbr}"),
        )
    except (ValueError, TypeError, AttributeError) as e:
        log.warning("Failed to parse datetime '%s': %s", utc_str, e)
        return ("", "", "", "")


def build_contact_info(event):
    """Combine contact fields into single string."""
    parts = []
    if name := get_field(event, "contact_name"):
        parts.append(f"Contact: {name}")
    if phone := get_field(event, "contact_phone"):
        parts.append(f"Phone: {phone}")
    if email := get_field(event, "contact_email"):
        parts.append(f"Email: {email}")
    return " | ".join(parts)


def normalize_fiscal_year(value):
    """Convert fiscal_year 0 to blank."""
    if value in ("0", 0, "0.0"):
        return ""
    return str(value) if value else ""


def transform_event(raw, institutions, timestamp):
    """Transform raw event to output schema format."""
    # Normalize Canadian bank tickers (-US -> -CA) for correct institution lookup
    raw_ticker = get_field(raw, "ticker")
    ticker = normalize_canadian_ticker(raw_ticker)

    # Also normalize description if it contains -US ticker
    description = get_field(raw, "description")
    if raw_ticker != ticker and raw_ticker in description:
        description = description.replace(raw_ticker, ticker)

    inst = institutions.get(ticker, {})

    # Check if time is unconfirmed (FactSet sets market_time_code to "Unspecified")
    time_unconfirmed = get_field(raw, "market_time_code") == "Unspecified"

    utc, local, date, time = convert_timezone(
        get_field(raw, "event_datetime_utc"),
        time_unconfirmed=time_unconfirmed,
    )

    # Mark headline when time is unconfirmed
    headline = description
    if time_unconfirmed:
        headline = f"{description} (Time TBD)"

    return {
        "event_id": get_field(raw, "event_id"),
        "ticker": ticker,
        "institution_name": inst.get("name", "Unknown"),
        "institution_id": inst.get("id", ""),
        "institution_type": inst.get("type", "Unknown"),
        "event_type": get_field(raw, "event_type"),
        "event_headline": headline,
        "event_date_time_utc": utc,
        "event_date_time_local": local,
        "event_date": date,
        "event_time_local": time,
        "webcast_link": get_field(raw, "webcast_link"),
        "contact_info": build_contact_info(raw),
        "fiscal_year": normalize_fiscal_year(get_field(raw, "fiscal_year")),
        "fiscal_period": get_field(raw, "fiscal_period"),
        "data_fetched_timestamp": timestamp,
        # Keep last_modified_date for earnings dedup (not in OUTPUT_SCHEMA)
        "_last_modified_date": get_field(raw, "last_modified_date"),
    }


def get_allowed_types():
    """Build set of all allowed source event types."""
    allowed = set(INCLUDED_EVENT_TYPES)
    for types in DEDUP_RULES.values():
        allowed.update(types)
    allowed.update(RENAME_RULES.keys())
    # Remove explicitly excluded types
    allowed -= set(EXCLUDED_EVENT_TYPES)
    return allowed


def filter_events(events):
    """Filter to approved event types, excluding banned types."""
    allowed = get_allowed_types()
    filtered = []
    excluded_counts = defaultdict(int)
    explicitly_excluded_counts = defaultdict(int)

    for event in events:
        etype = event.get("event_type", "")
        if etype in EXCLUDED_EVENT_TYPES:
            explicitly_excluded_counts[etype] += 1
        elif etype in allowed:
            filtered.append(event)
        else:
            excluded_counts[etype] += 1

    if explicitly_excluded_counts:
        log.info("Filtered out excluded types: %s", dict(explicitly_excluded_counts))
    if excluded_counts:
        log.warning("Filtered unknown types: %s", dict(excluded_counts))
    return filtered


def dedupe_same_datetime(events):
    """
    Deduplicate events with identical ticker + datetime.

    When multiple events have the exact same ticker and datetime, keep only the
    highest priority one based on DATETIME_DEDUP_PRIORITY. This handles cases
    where an earnings call and release date are scheduled at the exact same time.
    """
    if not DATETIME_DEDUP_PRIORITY:
        return events

    # Build priority lookup (lower number = higher priority)
    priority = {t: i for i, t in enumerate(DATETIME_DEDUP_PRIORITY)}

    # Group events by (ticker, datetime)
    groups = defaultdict(list)
    other = []

    for event in events:
        etype = event.get("event_type", "")
        if etype in priority:
            key = (event.get("ticker", ""), event.get("event_date_time_utc", ""))
            groups[key].append(event)
        else:
            other.append(event)

    # For each group, keep highest priority event
    result = []
    merged_count = 0

    for key, group in groups.items():
        if len(group) == 1:
            result.append(group[0])
        else:
            # Sort by priority (lowest index = highest priority)
            group.sort(key=lambda e: priority.get(e.get("event_type", ""), 999))
            result.append(group[0])
            merged_count += len(group) - 1
            log.debug(
                "Same datetime %s %s: kept %s, dropped %s",
                key[0], key[1][:10] if key[1] else "",
                group[0].get("event_type"),
                [e.get("event_type") for e in group[1:]]
            )

    if merged_count:
        log.info("Merged %d events with identical ticker+datetime", merged_count)

    result.extend(other)
    return result


def _get_event_month(event):
    """Extract YYYY-MM from event_date, or empty string if unavailable."""
    date = event.get("event_date", "") or ""
    return date[:7] if len(date) >= 7 else ""


def _pick_by_peer_comparison(group, fiscal_year, fiscal_period, peer_index):
    """
    Pick the best event from a duplicate group using peer comparison.

    When duplicate earnings events have dates in different months, compare against
    other Canadian banks' earnings for the same fiscal period to find the consensus.
    """
    # Get months from peer events for this fiscal period
    peer_key = (fiscal_year, fiscal_period)
    peer_months = peer_index.get(peer_key, {})

    if not peer_months:
        # No peer data - fall back to last_modified
        return None

    # Find the consensus month (most common among peers)
    consensus_month = max(peer_months, key=peer_months.get)
    consensus_count = peer_months[consensus_month]

    # Look for an event in our group that matches the consensus month
    for event in group:
        if _get_event_month(event) == consensus_month:
            log.debug(
                "Peer comparison: picked %s (%d peers in %s)",
                event.get("event_date", ""),
                consensus_count,
                consensus_month,
            )
            return event

    # No event matches consensus - fall back to last_modified
    return None


def _pick_by_last_modified(group):
    """Pick the most recently modified event from a group."""
    group_sorted = sorted(
        group,
        key=lambda e: e.get("_last_modified_date", "") or "",
        reverse=True,
    )
    return group_sorted[0]


def dedupe_earnings_by_fiscal_period(events):
    """
    Deduplicate Earnings events by ticker + fiscal_year + fiscal_period.

    When we query both -CA and -US tickers for Canadian banks, we may get duplicate
    earnings events for the same fiscal period. After ticker normalization (both
    become -CA), we resolve duplicates using:

    1. If dates are in the same month: keep the most recently modified event
    2. If dates are in different months: use peer comparison - look at other Canadian
       banks' earnings for that fiscal period and pick the date that matches the
       consensus month (Canadian banks report earnings in the same week typically)
    3. Fall back to most recently modified if peer comparison is inconclusive
    """
    earnings = []
    other = []

    for event in events:
        if event.get("event_type") == "Earnings":
            earnings.append(event)
        else:
            other.append(event)

    if not earnings:
        return events

    # Group earnings by (ticker, fiscal_year, fiscal_period)
    groups = defaultdict(list)
    for event in earnings:
        fy = event.get("fiscal_year", "")
        fp = event.get("fiscal_period", "")
        if fy and fp:
            key = (event.get("ticker", ""), fy, fp)
            groups[key].append(event)
        else:
            # No fiscal period info, can't dedupe - keep as-is
            other.append(event)

    # Build peer index: for each (fiscal_year, fiscal_period), count events per month
    # across ALL tickers (used for peer comparison when dates conflict)
    peer_index = defaultdict(lambda: defaultdict(int))
    for event in earnings:
        fy = event.get("fiscal_year", "")
        fp = event.get("fiscal_period", "")
        month = _get_event_month(event)
        if fy and fp and month:
            peer_index[(fy, fp)][month] += 1

    # Process each group
    result = []
    deduped_count = 0
    peer_resolved_count = 0

    for key, group in groups.items():
        ticker, fy, fp = key

        if len(group) == 1:
            result.append(group[0])
            continue

        # Multiple events for same ticker + fiscal period
        # Check if dates are in different months
        months = set(_get_event_month(e) for e in group)
        months.discard("")  # Remove empty strings

        if len(months) <= 1:
            # Same month (or no dates) - use last_modified
            winner = _pick_by_last_modified(group)
            method = "last_modified"
        else:
            # Different months - try peer comparison
            # Build peer index excluding this ticker's events
            filtered_peer_index = defaultdict(lambda: defaultdict(int))
            for event in earnings:
                if event.get("ticker") != ticker:
                    e_fy = event.get("fiscal_year", "")
                    e_fp = event.get("fiscal_period", "")
                    month = _get_event_month(event)
                    if e_fy and e_fp and month:
                        filtered_peer_index[(e_fy, e_fp)][month] += 1

            winner = _pick_by_peer_comparison(group, fy, fp, filtered_peer_index)
            if winner:
                method = "peer_comparison"
                peer_resolved_count += 1
            else:
                # Peer comparison inconclusive - fall back to last_modified
                winner = _pick_by_last_modified(group)
                method = "last_modified (peer inconclusive)"

        result.append(winner)
        deduped_count += len(group) - 1
        log.debug(
            "Earnings dedup %s FY%s Q%s: kept %s via %s, dropped %d",
            ticker, fy, fp,
            winner.get("event_date", "unknown"),
            method,
            len(group) - 1,
        )

    if deduped_count:
        log.info(
            "Deduplicated %d earnings events by fiscal period (%d via peer comparison)",
            deduped_count,
            peer_resolved_count,
        )

    result.extend(other)
    return result


def build_dedup_lookup():
    """Build lookup mapping source types to (target, priority)."""
    lookup = {}
    for target, sources in DEDUP_RULES.items():
        for priority, source in enumerate(sources):
            lookup[source] = (target, priority)
    return lookup


def get_dedup_key(event, target):
    """Get grouping key for deduplication."""
    ticker = event.get("ticker", "")
    fy, fp = event.get("fiscal_year", ""), event.get("fiscal_period", "")
    if fy and fp:
        return (target, ticker, fy, fp)
    return (target, ticker, "date", event.get("event_date", ""))


def consolidate_events(events):
    """Apply deduplication and rename rules."""
    dedup_lookup = build_dedup_lookup()
    groups, other = defaultdict(list), []

    for event in events:
        etype = event.get("event_type", "")
        if etype in dedup_lookup:
            target, _ = dedup_lookup[etype]
            groups[get_dedup_key(event, target)].append(event)
        else:
            other.append(event)

    result = []
    for key, group in groups.items():
        group.sort(key=lambda e: dedup_lookup.get(e.get("event_type"), ("", 999))[1])
        winner = group[0].copy()
        winner["event_type"] = key[0]
        result.append(winner)

    # Apply rename rules to remaining events
    for event in other:
        if event.get("event_type", "") in RENAME_RULES:
            event = event.copy()
            event["event_type"] = RENAME_RULES[event["event_type"]]
        result.append(event)

    return result


def save_events(events, path):
    """Save processed events to CSV."""
    if not events:
        log.warning("No events to save")
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        # extrasaction='ignore' drops internal fields like _last_modified_date
        writer = csv.DictWriter(f, fieldnames=OUTPUT_SCHEMA, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(events)
    return True


def log_summary(events):
    """Log event type and institution type counts."""
    etypes, itypes = defaultdict(int), defaultdict(int)
    for e in events:
        etypes[e.get("event_type", "Unknown")] += 1
        itypes[e.get("institution_type", "Unknown")] += 1
    log.info("Event types: %s", dict(sorted(etypes.items(), key=lambda x: -x[1])))
    log.info("Institution types: %s", dict(sorted(itypes.items(), key=lambda x: -x[1])))


def main():
    """Execute Stage 2 data processing pipeline."""
    log.info("STAGE 2: DATA PROCESSING")

    if not INPUT_PATH.exists():
        log.error("Input not found: %s - run Stage 1 first", INPUT_PATH)
        return

    raw_events = load_raw_events(INPUT_PATH)
    institutions = load_institutions()
    log.info(
        "Loaded %d raw events, %d institutions", len(raw_events), len(institutions)
    )

    timestamp = datetime.now(pytz.UTC).isoformat()
    events = [transform_event(raw, institutions, timestamp) for raw in raw_events]

    events = filter_events(events)
    log.info("After filtering: %d events", len(events))

    # Dedupe events with identical ticker+datetime (before rename)
    before_datetime_dedup = len(events)
    events = dedupe_same_datetime(events)
    if before_datetime_dedup != len(events):
        log.info("After datetime dedup: %d events", len(events))

    # Dedupe earnings events by fiscal period (keep most recently modified)
    before_earnings_dedup = len(events)
    events = dedupe_earnings_by_fiscal_period(events)
    if before_earnings_dedup != len(events):
        log.info("After earnings dedup: %d events", len(events))

    # Apply fiscal period dedup and renames
    before = len(events)
    events = consolidate_events(events)
    if before != len(events):
        log.info(
            "After consolidation: %d events (removed %d duplicates)",
            len(events),
            before - len(events),
        )

    events.sort(key=lambda x: x.get("event_date_time_utc", ""))
    save_events(events, OUTPUT_PATH)
    log.info("Saved to %s", OUTPUT_PATH)
    log_summary(events)


if __name__ == "__main__":
    main()
