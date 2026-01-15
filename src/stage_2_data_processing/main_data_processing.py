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

# Deduplication: keep one event per ticker+fiscal_period, priority order (first=highest)
DEDUP_RULES = {
    "Earnings": ["Earnings", "ConfirmedEarningsRelease", "ProjectedEarningsRelease"],
}

# Simple renames: source type -> target type (no deduplication)
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


# Logging configuration with colors
class ColorFormatter(logging.Formatter):
    """Custom formatter with ANSI colors."""

    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[31;1m",
    }
    RESET = "\033[0m"
    BOLD = "\033[1m"

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        record.levelname = f"{color}{record.levelname:7}{self.RESET}"
        record.msg = f"{self.BOLD}{record.msg}{self.RESET}"
        return super().format(record)


handler = logging.StreamHandler()
handler.setFormatter(
    ColorFormatter("%(asctime)s │ %(levelname)s │ %(message)s", "%H:%M:%S")
)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(handler)


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


def convert_timezone(utc_str):
    """Convert UTC datetime string to local time, returns (utc_iso, local_iso, date, time)."""
    if not utc_str:
        return ("", "", "", "")
    try:
        dt = dateutil_parse(str(utc_str))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
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


def transform_event(raw, institutions, timestamp):
    """Transform raw event to output schema format."""
    ticker = get_field(raw, "ticker")
    inst = institutions.get(ticker, {})
    utc, local, date, time = convert_timezone(get_field(raw, "event_datetime_utc"))
    return {
        "event_id": get_field(raw, "event_id"),
        "ticker": ticker,
        "institution_name": inst.get("name", "Unknown"),
        "institution_id": inst.get("id", ""),
        "institution_type": inst.get("type", "Unknown"),
        "event_type": get_field(raw, "event_type"),
        "event_headline": get_field(raw, "description"),
        "event_date_time_utc": utc,
        "event_date_time_local": local,
        "event_date": date,
        "event_time_local": time,
        "webcast_link": get_field(raw, "webcast_link"),
        "contact_info": build_contact_info(raw),
        "fiscal_year": get_field(raw, "fiscal_year"),
        "fiscal_period": get_field(raw, "fiscal_period"),
        "data_fetched_timestamp": timestamp,
    }


def get_allowed_types():
    """Build set of all allowed source event types."""
    allowed = set(INCLUDED_EVENT_TYPES)
    for types in DEDUP_RULES.values():
        allowed.update(types)
    allowed.update(RENAME_RULES.keys())
    return allowed


def filter_events(events):
    """Filter to approved event types, log unknown types."""
    allowed = get_allowed_types()
    filtered, excluded = [], defaultdict(int)
    for event in events:
        etype = event.get("event_type", "")
        if etype in allowed:
            filtered.append(event)
        else:
            excluded[etype] += 1
    if excluded:
        log.warning("Filtered unknown types: %s", dict(excluded))
    return filtered


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
        writer = csv.DictWriter(f, fieldnames=OUTPUT_SCHEMA)
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

    before = len(events)
    events = consolidate_events(events)
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
