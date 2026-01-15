#!/usr/bin/env python3
"""
Stage 3: Database Upload - Validates processed calendar events from Stage 2 and uploads
them to PostgreSQL. Currently implements schema validation and data quality checks as a
placeholder while the PostgreSQL connection is pending IT infrastructure setup. Once
configured, this stage will clear existing data and insert fresh events into the
aegis_calendar_events table using a full replace strategy.
"""

import csv
import logging
from pathlib import Path
from collections import defaultdict

# Paths
INPUT_PATH = (
    Path(__file__).parent.parent
    / "stage_2_data_processing/output/processed_calendar_events.csv"
)

# Schema matching PostgreSQL table
EXPECTED_SCHEMA = [
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

# NOT NULL fields in PostgreSQL
REQUIRED_FIELDS = [
    "event_id",
    "ticker",
    "institution_name",
    "institution_id",
    "institution_type",
    "event_type",
    "event_date_time_utc",
    "event_date",
    "data_fetched_timestamp",
]

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def load_events(path):
    """Load processed CSV events from Stage 2."""
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def validate_schema(events):
    """Validate events match expected schema, returns (missing, extra) field lists."""
    if not events:
        return [], []
    actual = set(events[0].keys())
    expected = set(EXPECTED_SCHEMA)
    return list(expected - actual), list(actual - expected)


def validate_required(events):
    """Check for missing required field values, returns list of errors."""
    errors = []
    for i, event in enumerate(events):
        for field in REQUIRED_FIELDS:
            if not event.get(field, "").strip():
                errors.append(
                    f"Row {i+1}: Missing '{field}' for event_id={event.get('event_id')}"
                )
    return errors


def log_summary(events):
    """Log summary statistics about the data."""
    if not events:
        return
    etypes, itypes = defaultdict(int), defaultdict(int)
    for e in events:
        etypes[e.get("event_type", "Unknown")] += 1
        itypes[e.get("institution_type", "Unknown")] += 1
    dates = [e.get("event_date", "") for e in events if e.get("event_date")]
    date_range = f"{min(dates)} to {max(dates)}" if dates else "N/A"
    log.info("Total: %d events, date range: %s", len(events), date_range)
    log.info("Event types: %s", dict(sorted(etypes.items(), key=lambda x: -x[1])))
    log.info("Institution types: %s", dict(sorted(itypes.items(), key=lambda x: -x[1])))


def upload_to_postgres(_events):
    """Upload events to PostgreSQL (placeholder - not yet implemented)."""
    log.warning("PostgreSQL upload not yet implemented")
    return False


def main():
    """Execute Stage 3 database upload pipeline."""
    log.info("STAGE 3: DATABASE UPLOAD")

    if not INPUT_PATH.exists():
        log.error("Input not found: %s - run Stage 2 first", INPUT_PATH)
        return

    events = load_events(INPUT_PATH)
    log.info("Loaded %d events", len(events))

    missing, extra = validate_schema(events)
    if missing:
        log.error("Missing schema fields: %s", missing)
        return
    if extra:
        log.warning("Extra fields (ignored): %s", extra)

    errors = validate_required(events)
    if errors:
        log.warning("Validation errors (%d): %s", len(errors), errors[:5])

    log_summary(events)
    success = upload_to_postgres(events)
    log.info(
        "Stage 3 %s - %d events ready",
        "COMPLETE" if success else "INCOMPLETE",
        len(events),
    )


if __name__ == "__main__":
    main()
