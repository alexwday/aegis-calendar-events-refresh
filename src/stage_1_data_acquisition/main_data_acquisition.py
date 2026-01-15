#!/usr/bin/env python3
"""
Stage 1: Data Acquisition
=========================
Queries the FactSet Calendar Events API and dumps raw data to CSV.

This stage is designed to be SWAPPABLE:
- Currently uses FactSet API
- Can be replaced with Snowflake query in the future
- Outputs raw data with NO transformations

Output: output/raw_calendar_events.csv
"""

import os
import csv
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv
from dateutil.parser import parse as dateutil_parse
import yaml

import fds.sdk.EventsandTranscripts
from fds.sdk.EventsandTranscripts.api import calendar_events_api
from fds.sdk.EventsandTranscripts.models import (
    CompanyEventRequest,
    CompanyEventRequestData,
    CompanyEventRequestDataDateTime,
    CompanyEventRequestDataUniverse,
)

# RBC SSL certificates (only available in RBC environment)
try:
    import rbc_security
    _RBC_SECURITY_AVAILABLE = True
except ImportError:
    _RBC_SECURITY_AVAILABLE = False

# Project root (for loading .env and config files)
PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# Configuration (hardcoded - change here if needed)
PAST_MONTHS = 6
FUTURE_MONTHS = 6


def load_monitored_institutions():
    """Load monitored institutions from project root."""
    with open(PROJECT_ROOT / "monitored_institutions.yaml", "r") as f:
        return yaml.safe_load(f)


def configure_api_client():
    """Configure the FactSet API client with proxy and authentication."""
    # Build proxy URL with domain authentication
    proxy_username = os.getenv("PROXY_USERNAME")
    proxy_password = quote(os.getenv("PROXY_PASSWORD", ""))
    proxy_domain = os.getenv("PROXY_DOMAIN", "MAPLE")
    proxy_host = os.getenv("PROXY_HOST")

    escaped_domain = quote(proxy_domain + "\\" + proxy_username)
    proxy_url = f"http://{escaped_domain}:{proxy_password}@{proxy_host}"

    configuration = fds.sdk.EventsandTranscripts.Configuration(
        username=os.getenv("FACTSET_USERNAME"),
        password=os.getenv("FACTSET_PASSWORD"),
        proxy=proxy_url,
    )
    configuration.get_basic_auth_token()

    return configuration


def calculate_date_range(past_months: int = 6, future_months: int = 6):
    """Calculate date range for API query."""
    today = datetime.now().date()
    start_date = today - timedelta(days=past_months * 30)
    end_date = today + timedelta(days=future_months * 30)
    return start_date, end_date


def query_calendar_events(api_instance, tickers: list, start_date, end_date) -> list:
    """
    Query FactSet Calendar Events API.
    Returns RAW event data as list of dictionaries.
    """
    # FactSet API has a 90-day maximum date range limit
    MAX_DAYS_PER_QUERY = 89

    date_ranges = []
    current_start = start_date

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=MAX_DAYS_PER_QUERY), end_date)
        date_ranges.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Split into {len(date_ranges)} chunks (API limit: 90 days each)")

    all_events = []

    for range_num, (chunk_start, chunk_end) in enumerate(date_ranges, 1):
        print(f"  Querying chunk {range_num}/{len(date_ranges)}: {chunk_start} to {chunk_end}...")

        start_datetime = dateutil_parse(f"{chunk_start}T00:00:00Z")
        end_datetime = dateutil_parse(f"{chunk_end}T23:59:59Z")

        company_event_request = CompanyEventRequest(
            data=CompanyEventRequestData(
                date_time=CompanyEventRequestDataDateTime(
                    start=start_datetime,
                    end=end_datetime,
                ),
                universe=CompanyEventRequestDataUniverse(
                    symbols=tickers,
                    type="Tickers",
                ),
            ),
        )

        try:
            response = api_instance.get_company_event(company_event_request)

            if response and hasattr(response, "data") and response.data:
                # Convert to raw dictionaries - NO transformations
                chunk_events = [event.to_dict() for event in response.data]
                # Filter to only monitored tickers
                chunk_events = [e for e in chunk_events if e.get("ticker") in tickers]
                print(f"    Found {len(chunk_events)} events")
                all_events.extend(chunk_events)
            else:
                print(f"    No events found")

        except Exception as e:
            print(f"    ERROR: {str(e)}")
            continue

    return all_events


def save_raw_data(events: list, output_path: Path):
    """
    Save raw event data to CSV.
    Preserves ALL fields from the API response.
    """
    if not events:
        print("  WARNING: No events to save")
        return False

    # Get all unique field names from events
    all_fields = set()
    for event in events:
        all_fields.update(event.keys())

    # Sort fields for consistent output
    fieldnames = sorted(list(all_fields))

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)

    print(f"  Saved {len(events)} events to {output_path}")
    print(f"  Fields: {', '.join(fieldnames)}")

    return True


def main():
    """Main execution function for Stage 1: Data Acquisition."""
    print("=" * 60)
    print("STAGE 1: DATA ACQUISITION")
    print("=" * 60)
    print()

    # Step 1: Load monitored institutions
    print("[1/4] Loading monitored institutions...")
    institutions = load_monitored_institutions()
    tickers = list(institutions.keys())
    print(f"  Loaded {len(tickers)} tickers")

    # Step 2: Configure SSL and API client
    print("\n[2/4] Configuring API client...")
    if _RBC_SECURITY_AVAILABLE:
        rbc_security.enable_certs()
        print("  RBC SSL certificates enabled")
    else:
        print("  rbc_security not available, skipping SSL setup")
    api_config = configure_api_client()
    print("  API client configured")

    # Step 3: Query API
    print("\n[3/4] Querying FactSet Calendar Events API...")
    print(f"  Date range: {PAST_MONTHS} months back, {FUTURE_MONTHS} months forward")
    start_date, end_date = calculate_date_range(PAST_MONTHS, FUTURE_MONTHS)

    with fds.sdk.EventsandTranscripts.ApiClient(api_config) as api_client:
        api_instance = calendar_events_api.CalendarEventsApi(api_client)
        events = query_calendar_events(api_instance, tickers, start_date, end_date)

    print(f"\n  Total events retrieved: {len(events)}")

    # Step 4: Save raw data
    print("\n[4/4] Saving raw data...")
    output_path = Path(__file__).parent / "output" / "raw_calendar_events.csv"
    save_raw_data(events, output_path)

    # Summary
    print()
    print("=" * 60)
    print("STAGE 1 COMPLETE")
    print(f"  Events: {len(events)}")
    print(f"  Output: {output_path}")
    print("=" * 60)

    # Event type breakdown - useful for knowing what types are available
    if events:
        event_type_counts = defaultdict(int)
        for event in events:
            et = event.get("event_type", "Unknown")
            event_type_counts[et] += 1

        print("\nEvent Types Found (for reference - add to config if needed):")
        for et, count in sorted(event_type_counts.items(), key=lambda x: -x[1]):
            print(f"  {et}: {count}")


if __name__ == "__main__":
    main()
