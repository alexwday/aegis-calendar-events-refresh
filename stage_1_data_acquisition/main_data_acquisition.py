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

import yaml
from dotenv import load_dotenv
from dateutil.parser import parse as dateutil_parse

import fds.sdk.EventsandTranscripts
from fds.sdk.EventsandTranscripts.api import calendar_events_api
from fds.sdk.EventsandTranscripts.models import (
    CompanyEventRequest,
    CompanyEventRequestData,
    CompanyEventRequestDataDateTime,
    CompanyEventRequestDataUniverse,
)

# Load environment variables from project root
load_dotenv(Path(__file__).parent.parent / ".env")


def load_config():
    """Load configuration from local config file."""
    config_path = Path(__file__).parent.parent / "config" / "calendar_config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def load_monitored_institutions():
    """Load monitored institutions from local config file."""
    institutions_path = Path(__file__).parent.parent / "config" / "monitored_institutions.yaml"
    with open(institutions_path, "r") as f:
        return yaml.safe_load(f)


def setup_ssl_certificate(ssl_cert_path: str) -> str:
    """Set up SSL certificate for API calls."""
    # If path is absolute and exists, use it directly
    if os.path.isabs(ssl_cert_path) and os.path.exists(ssl_cert_path):
        cert_path = ssl_cert_path
    else:
        # Try relative to project root
        cert_path = str(Path(__file__).parent.parent / ssl_cert_path)

    if os.path.exists(cert_path):
        os.environ["REQUESTS_CA_BUNDLE"] = cert_path
        os.environ["SSL_CERT_FILE"] = cert_path
        print(f"  SSL certificate configured: {cert_path}")
        return cert_path
    else:
        print(f"  WARNING: SSL certificate not found at {cert_path}")
        print(f"  API calls may fail without proper SSL configuration")
        return None


def configure_api_client(ssl_cert_path: str = None):
    """Configure the FactSet API client with proxy and authentication."""
    # Build proxy URL with domain authentication
    proxy_user = os.getenv("PROXY_USER")
    proxy_password = quote(os.getenv("PROXY_PASSWORD", ""))
    proxy_domain = os.getenv("PROXY_DOMAIN", "MAPLE")
    proxy_url_base = os.getenv("PROXY_URL")

    escaped_domain = quote(proxy_domain + "\\" + proxy_user)
    proxy_url = f"http://{escaped_domain}:{proxy_password}@{proxy_url_base}"

    configuration = fds.sdk.EventsandTranscripts.Configuration(
        username=os.getenv("API_USERNAME"),
        password=os.getenv("API_PASSWORD"),
        proxy=proxy_url,
        ssl_ca_cert=ssl_cert_path,
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

    # Step 1: Load configuration
    print("[1/6] Loading configuration...")
    config = load_config()
    date_range = config.get("date_range", {})
    past_months = date_range.get("past_months", 6)
    future_months = date_range.get("future_months", 6)
    print(f"  Date range: {past_months} months back, {future_months} months forward")

    # Step 2: Load monitored institutions
    print("\n[2/6] Loading monitored institutions...")
    institutions = load_monitored_institutions()
    tickers = list(institutions.keys())
    print(f"  Loaded {len(tickers)} tickers")

    # Step 3: Setup SSL certificate
    print("\n[3/6] Setting up SSL certificate...")
    ssl_cert_path = config.get("ssl_cert_path")
    cert_path = None
    if ssl_cert_path:
        cert_path = setup_ssl_certificate(ssl_cert_path)
    else:
        print("  No SSL certificate path configured")

    # Step 4: Configure API client
    print("\n[4/6] Configuring API client...")
    api_config = configure_api_client(cert_path)
    print("  API client configured with proxy authentication")

    # Step 5: Query API
    print("\n[5/6] Querying FactSet Calendar Events API...")
    start_date, end_date = calculate_date_range(past_months, future_months)

    with fds.sdk.EventsandTranscripts.ApiClient(api_config) as api_client:
        api_instance = calendar_events_api.CalendarEventsApi(api_client)
        events = query_calendar_events(api_instance, tickers, start_date, end_date)

    print(f"\n  Total events retrieved: {len(events)}")

    # Step 6: Save raw data
    print("\n[6/6] Saving raw data...")
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
