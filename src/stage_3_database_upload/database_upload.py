#!/usr/bin/env python3
"""
Stage 3: Database Upload - Validates processed calendar events from Stage 2 and uploads
them to PostgreSQL using SQLAlchemy. The process connects to the database, validates the
schema matches expectations, deletes all existing records row-by-row (no TRUNCATE access),
then inserts fresh data. Use --dry-run flag to test connectivity and validation without
modifying the database.

Usage:
    python main_database_upload.py              # Full upload (deletes and inserts)
    python main_database_upload.py --dry-run   # Test mode (no database changes)
"""

import argparse
import csv
import logging
import os
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
INPUT_PATH = (
    Path(__file__).parent.parent
    / "stage_2_data_processing/output/processed_calendar_events.csv"
)
load_dotenv(PROJECT_ROOT / ".env")

# PostgreSQL configuration from environment
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DATABASE"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "table": os.getenv("POSTGRES_TABLE"),
}

# Expected schema columns
EXPECTED_COLUMNS = [
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


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)


def load_events(path):
    """Load processed CSV events from Stage 2."""
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def get_db_url():
    """Build PostgreSQL connection URL from config."""
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


def connect_db():
    """Create database engine and test connection."""
    engine = create_engine(get_db_url())
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return engine


def validate_table_schema(engine):
    """Validate table exists and has expected columns, returns (missing, extra)."""
    inspector = inspect(engine)
    table = DB_CONFIG["table"]

    if table not in inspector.get_table_names():
        raise ValueError(f"Table '{table}' not found in database")

    db_columns = {col["name"] for col in inspector.get_columns(table)}
    expected = set(EXPECTED_COLUMNS)

    return list(expected - db_columns), list(db_columns - expected)


def get_row_count(engine):
    """Get current row count in target table."""
    table = DB_CONFIG["table"]
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        return result.scalar()


def delete_all_rows(engine, dry_run=False):
    """Delete all rows from table one by one, returns count deleted."""
    table = DB_CONFIG["table"]

    if dry_run:
        count = get_row_count(engine)
        log.info("[DRY RUN] Would delete %d rows from %s", count, table)
        return count

    with engine.connect() as conn:
        result = conn.execute(text(f"DELETE FROM {table}"))
        conn.commit()
        return result.rowcount


def insert_events(engine, events, dry_run=False):
    """Insert events into table, returns count inserted."""
    table = DB_CONFIG["table"]

    if dry_run:
        log.info("[DRY RUN] Would insert %d rows into %s", len(events), table)
        return len(events)

    if not events:
        return 0

    columns = EXPECTED_COLUMNS
    placeholders = ", ".join([f":{col}" for col in columns])
    col_names = ", ".join(columns)

    with engine.connect() as conn:
        for event in events:
            row = {col: event.get(col, "") for col in columns}
            conn.execute(
                text(f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"),
                row,
            )
        conn.commit()

    return len(events)


def log_summary(events):
    """Log summary statistics about the data."""
    if not events:
        return
    etypes = defaultdict(int)
    for e in events:
        etypes[e.get("event_type", "Unknown")] += 1
    dates = [e.get("event_date", "") for e in events if e.get("event_date")]
    date_range = f"{min(dates)} to {max(dates)}" if dates else "N/A"
    log.info("Data summary: %d events, date range: %s", len(events), date_range)
    log.info("Event types: %s", dict(sorted(etypes.items(), key=lambda x: -x[1])))


def main():
    """Execute Stage 3 database upload pipeline."""
    parser = argparse.ArgumentParser(description="Upload calendar events to PostgreSQL")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test mode: validate and connect but don't modify database",
    )
    args = parser.parse_args()

    if args.dry_run:
        log.info("=" * 60)
        log.info("DRY RUN MODE - No database changes will be made")
        log.info("=" * 60)

    log.info("STAGE 3: DATABASE UPLOAD")

    if not INPUT_PATH.exists():
        log.error("Input not found: %s - run Stage 2 first", INPUT_PATH)
        return

    events = load_events(INPUT_PATH)
    log.info("Loaded %d events from CSV", len(events))
    log_summary(events)

    log.info(
        "Connecting to PostgreSQL: %s@%s:%s/%s",
        DB_CONFIG["user"],
        DB_CONFIG["host"],
        DB_CONFIG["port"],
        DB_CONFIG["database"],
    )

    try:
        engine = connect_db()
        log.info("Database connection successful")
    except Exception as e:
        log.error("Database connection failed: %s", e)
        return

    log.info("Validating table schema: %s", DB_CONFIG["table"])
    try:
        missing, extra = validate_table_schema(engine)
        if missing:
            log.error("Missing columns in database: %s", missing)
            return
        if extra:
            log.warning("Extra columns in database (ignored): %s", extra)
        log.info("Schema validation passed")
    except ValueError as e:
        log.error("Schema validation failed: %s", e)
        return

    current_count = get_row_count(engine)
    log.info("Current rows in table: %d", current_count)

    deleted = delete_all_rows(engine, dry_run=args.dry_run)
    log.info("Deleted %d rows", deleted)

    inserted = insert_events(engine, events, dry_run=args.dry_run)
    log.info("Inserted %d rows", inserted)

    if args.dry_run:
        log.info("=" * 60)
        log.info("DRY RUN COMPLETE - No changes were made")
        log.info("Run without --dry-run to perform actual upload")
        log.info("=" * 60)
    else:
        final_count = get_row_count(engine)
        log.info("Stage 3 COMPLETE - Table now has %d rows", final_count)


if __name__ == "__main__":
    main()
