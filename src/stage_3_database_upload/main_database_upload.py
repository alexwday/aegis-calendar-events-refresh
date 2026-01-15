#!/usr/bin/env python3
"""
Stage 3: Database Upload (PLACEHOLDER)
======================================
Loads processed data from Stage 2 and uploads to PostgreSQL.

This stage is currently a PLACEHOLDER:
- Loads and validates processed data
- Shows summary statistics
- TODO: Implement actual PostgreSQL upload

Input: ../stage_2_data_processing/output/processed_calendar_events.csv
Target: PostgreSQL table 'aegis_calendar_events' (see postgres_schema.sql)
"""

import csv
from pathlib import Path
from collections import defaultdict

# =============================================================================
# EXPECTED SCHEMA - Must match OUTPUT_SCHEMA from Stage 2 and PostgreSQL table
# =============================================================================

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

# Required fields (NOT NULL in PostgreSQL schema)
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


def load_processed_data(input_path: Path) -> list:
    """Load processed CSV data from Stage 2."""
    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def validate_schema(events: list, expected_fields: list) -> tuple:
    """
    Validate that events match expected schema.
    Returns: (is_valid, missing_fields, extra_fields)
    """
    if not events:
        return True, [], []

    actual_fields = set(events[0].keys())
    expected_set = set(expected_fields)

    missing = expected_set - actual_fields
    extra = actual_fields - expected_set

    is_valid = len(missing) == 0

    return is_valid, list(missing), list(extra)


def validate_required_fields(events: list, required_fields: list) -> list:
    """
    Check for events with missing required field values.
    Returns: list of validation errors
    """
    errors = []

    for i, event in enumerate(events):
        for field in required_fields:
            value = event.get(field, "")
            if not value or str(value).strip() == "":
                event_id = event.get("event_id", "UNKNOWN")
                msg = f"Row {i+1}: Missing '{field}' for event_id={event_id}"
                errors.append(msg)

    return errors


def print_data_summary(events: list):
    """Print summary statistics about the data."""
    if not events:
        print("  No events to summarize")
        return

    # Count by event type
    event_types = defaultdict(int)
    for event in events:
        event_types[event.get("event_type", "Unknown")] += 1

    # Count by institution type
    institution_types = defaultdict(int)
    for event in events:
        institution_types[event.get("institution_type", "Unknown")] += 1

    # Date range
    dates = [e.get("event_date", "") for e in events if e.get("event_date")]
    if dates:
        min_date = min(dates)
        max_date = max(dates)
    else:
        min_date = "N/A"
        max_date = "N/A"

    print("\n  Data Summary:")
    print(f"    Total events: {len(events)}")
    print(f"    Date range: {min_date} to {max_date}")

    print("\n    By Event Type:")
    for et, count in sorted(event_types.items(), key=lambda x: -x[1]):
        print(f"      {et}: {count}")

    print("\n    By Institution Type:")
    for it, count in sorted(institution_types.items(), key=lambda x: -x[1]):
        print(f"      {it}: {count}")


def upload_to_postgres(_events: list) -> bool:
    """
    PLACEHOLDER: Upload events to PostgreSQL.

    TODO: Implement this function with:
    1. Connect to PostgreSQL using connection parameters
    2. Clear existing data (or handle upsert logic)
    3. Insert processed events
    4. Commit transaction
    5. Return success/failure

    Example implementation outline:
    ```
    import psycopg2

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    cursor = conn.cursor()

    # Clear existing data (replace strategy)
    cursor.execute("DELETE FROM aegis_calendar_events;")

    # Insert new data
    for event in events:
        cursor.execute(
            '''INSERT INTO aegis_calendar_events
               (event_id, ticker, institution_name, ...)
               VALUES (%s, %s, %s, ...)''',
            (event["event_id"], event["ticker"], ...)
        )

    conn.commit()
    conn.close()
    return True
    ```
    """
    print("\n  PostgreSQL upload is not yet implemented.")
    print("  This is a placeholder for future development.")
    print("\n  To implement:")
    print("    - Add PostgreSQL connection code to upload_to_postgres()")
    print("    - Add psycopg2 to requirements.txt")

    return False


def main():
    """Main execution function for Stage 3: Database Upload."""
    print("=" * 60)
    print("STAGE 3: DATABASE UPLOAD")
    print("=" * 60)
    print()

    # Step 1: Load processed data from Stage 2
    print("[1/4] Loading processed data from Stage 2...")
    input_path = (
        Path(__file__).parent.parent
        / "stage_2_data_processing"
        / "output"
        / "processed_calendar_events.csv"
    )

    if not input_path.exists():
        print(f"  ERROR: Input file not found: {input_path}")
        print("  Please run Stage 2 first.")
        return

    events = load_processed_data(input_path)
    print(f"  Loaded {len(events)} events")

    # Step 2: Validate schema
    print("\n[2/4] Validating schema...")
    _, missing, extra = validate_schema(events, EXPECTED_SCHEMA)

    if missing:
        print(f"  ERROR: Missing expected fields: {missing}")
        print("  Schema validation FAILED")
        return

    print("  Schema matches expected structure")

    if extra:
        print(f"  NOTE: Extra fields found (will be ignored): {extra}")

    # Step 3: Validate required fields
    print("\n[3/4] Validating required fields...")
    errors = validate_required_fields(events, REQUIRED_FIELDS)

    if errors:
        print(f"  WARNING: Found {len(errors)} validation errors:")
        for error in errors[:10]:  # Show first 10
            print(f"    {error}")
        if len(errors) > 10:
            print(f"    ... and {len(errors) - 10} more")
    else:
        print("  All required fields present")

    # Print data summary
    print_data_summary(events)

    # Step 4: Upload to PostgreSQL (PLACEHOLDER)
    print("\n[4/4] Uploading to PostgreSQL...")
    success = upload_to_postgres(events)

    # Summary
    print()
    print("=" * 60)
    if success:
        print("STAGE 3 COMPLETE - Data uploaded successfully")
    else:
        print("STAGE 3 INCOMPLETE - Upload not implemented yet")
    print(f"  Events ready for upload: {len(events)}")
    print("  Target table: aegis_calendar_events")
    print("=" * 60)


if __name__ == "__main__":
    main()
