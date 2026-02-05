#!/usr/bin/env python3
"""
Temporary diagnostic script to check what FactSet returns for Canadian bank
US vs CA ticker variants. Shows event counts for all 7 Canadian banks.
"""

import calendar
import os
import time
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
from urllib.parse import quote

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

try:
    import rbc_security
    RBC_SECURITY_AVAILABLE = True
except ImportError:
    RBC_SECURITY_AVAILABLE = False

PROJECT_ROOT = Path(__file__).parent
load_dotenv(PROJECT_ROOT / ".env")

# All 7 Canadian banks - CA tickers
CANADIAN_BANKS_CA = [
    "RY-CA",   # Royal Bank of Canada
    "TD-CA",   # Toronto-Dominion Bank
    "BMO-CA",  # Bank of Montreal
    "BNS-CA",  # Bank of Nova Scotia
    "CM-CA",   # Canadian Imperial Bank of Commerce
    "NA-CA",   # National Bank of Canada
    "LB-CA",   # Laurentian Bank
]

# US variants (no LB-US - it's a different company)
CANADIAN_BANKS_US = [
    "RY-US",
    "TD-US",
    "BMO-US",
    "BNS-US",
    "CM-US",
    "NA-US",
]

TEST_TICKERS = CANADIAN_BANKS_CA + CANADIAN_BANKS_US

PAST_MONTHS = 6
FUTURE_MONTHS = 6


def build_proxy_url():
    proxy_user = os.getenv("PROXY_USER")
    proxy_password = os.getenv("PROXY_PASSWORD")
    proxy_url_base = os.getenv("PROXY_URL")

    if not all([proxy_user, proxy_password, proxy_url_base]):
        return None

    proxy_domain = os.getenv("PROXY_DOMAIN", "MAPLE")
    escaped_domain = quote(proxy_domain + "\\" + proxy_user)
    escaped_password = quote(proxy_password)
    return f"http://{escaped_domain}:{escaped_password}@{proxy_url_base}"


def create_api_client():
    proxy_url = build_proxy_url()

    if proxy_url:
        print("Proxy configured")
        config = fds.sdk.EventsandTranscripts.Configuration(
            username=os.getenv("FACTSET_USERNAME"),
            password=os.getenv("FACTSET_PASSWORD"),
            proxy=proxy_url,
        )
    else:
        print("No proxy configured (direct connection)")
        config = fds.sdk.EventsandTranscripts.Configuration(
            username=os.getenv("FACTSET_USERNAME"),
            password=os.getenv("FACTSET_PASSWORD"),
        )

    config.get_basic_auth_token()
    return config


def add_months(d, months):
    month = d.month + months
    year = d.year + (month - 1) // 12
    month = ((month - 1) % 12) + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def get_date_range():
    today = datetime.now().date()
    start = add_months(today, -PAST_MONTHS).replace(day=1)
    end_month = add_months(today, FUTURE_MONTHS)
    end = date(end_month.year, end_month.month, calendar.monthrange(end_month.year, end_month.month)[1])
    return (start, end)


def split_date_range(start_date, end_date):
    """Split date range into monthly chunks (first to last day of each month)."""
    ranges = []
    current = start_date

    while current <= end_date:
        last_day = calendar.monthrange(current.year, current.month)[1]
        month_end = date(current.year, current.month, last_day)
        chunk_end = min(month_end, end_date)
        ranges.append((current, chunk_end))

        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    return ranges


def query_chunk(api, tickers, start, end, max_retries=5):
    """Query API for a single date range chunk with retry logic."""
    request = CompanyEventRequest(
        data=CompanyEventRequestData(
            date_time=CompanyEventRequestDataDateTime(
                start=dateutil_parse(f"{start}T00:00:00Z"),
                end=dateutil_parse(f"{end}T23:59:59Z"),
            ),
            universe=CompanyEventRequestDataUniverse(symbols=tickers, type="Tickers"),
        ),
    )

    for attempt in range(max_retries):
        try:
            response = api.get_company_event(request)
            if response and hasattr(response, "data") and response.data:
                return [e.to_dict() for e in response.data if e.ticker in tickers]
            return []
        except Exception as err:
            error_msg = str(err)
            is_last_attempt = attempt == max_retries - 1

            is_retryable = any(x in error_msg.lower() for x in [
                "parameter", "timeout", "connection", "temporary", "503", "429"
            ])

            if is_retryable and not is_last_attempt:
                wait_time = 2 ** attempt
                print(f"  API error (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {error_msg[:100]}")
                time.sleep(wait_time)
            else:
                print(f"  API error (attempt {attempt + 1}/{max_retries}): {err}")
                return []

    return []


def fetch_events(api, tickers, start_date, end_date):
    """Fetch all events across date range chunks."""
    chunks = split_date_range(start_date, end_date)
    print(f"Querying {len(tickers)} tickers across {len(chunks)} monthly chunks")

    events = []
    for i, (start, end) in enumerate(chunks, 1):
        print(f"  Chunk {i}/{len(chunks)}: {start} to {end}")
        events.extend(query_chunk(api, tickers, start, end))
        if i < len(chunks):
            time.sleep(1)  # Delay between chunks

    return events


def main():
    print("=" * 70)
    print("Canadian Banks: CA vs US Ticker Diagnostic")
    print("=" * 70)

    if RBC_SECURITY_AVAILABLE:
        rbc_security.enable_certs()
        print("RBC SSL certificates enabled")

    api_config = create_api_client()
    start_date, end_date = get_date_range()

    print(f"\nDate range: {start_date} to {end_date}")
    print()

    with fds.sdk.EventsandTranscripts.ApiClient(api_config) as client:
        api = calendar_events_api.CalendarEventsApi(client)
        events = fetch_events(api, TEST_TICKERS, start_date, end_date)

    print(f"Total events returned: {len(events)}")

    # Group by ticker
    by_ticker = defaultdict(list)
    for e in events:
        by_ticker[e.get("ticker", "Unknown")].append(e)

    # Summary table comparing CA vs US for each bank
    print("\n" + "=" * 70)
    print("SUMMARY: Event counts by ticker")
    print("=" * 70)
    print(f"{'Bank':<8} {'CA Ticker':<10} {'CA Count':>10} {'US Ticker':<10} {'US Count':>10}")
    print("-" * 70)

    bank_names = ["RY", "TD", "BMO", "BNS", "CM", "NA", "LB"]
    for bank in bank_names:
        ca_ticker = f"{bank}-CA"
        us_ticker = f"{bank}-US"
        ca_count = len(by_ticker.get(ca_ticker, []))
        us_count = len(by_ticker.get(us_ticker, []))
        us_display = us_ticker if bank != "LB" else "(n/a)"
        us_count_display = str(us_count) if bank != "LB" else "-"
        print(f"{bank:<8} {ca_ticker:<10} {ca_count:>10} {us_display:<10} {us_count_display:>10}")

    print("-" * 70)

    # Show any other tickers that came back
    other_tickers = set(by_ticker.keys()) - set(TEST_TICKERS)
    if other_tickers:
        print(f"\nUnexpected tickers in response: {other_tickers}")

    # Detailed breakdown for tickers with events
    print("\n" + "=" * 70)
    print("DETAILS: Event types per ticker")
    print("=" * 70)

    for ticker in TEST_TICKERS:
        events_for_ticker = by_ticker.get(ticker, [])
        if not events_for_ticker:
            continue

        print(f"\n{ticker} ({len(events_for_ticker)} events)")
        print("-" * 40)

        # Event type counts
        type_counts = defaultdict(int)
        for e in events_for_ticker:
            type_counts[e.get("event_type", "Unknown")] += 1

        for etype, count in sorted(type_counts.items(), key=lambda x: -x[1]):
            print(f"  {etype}: {count}")

    print("\n" + "=" * 70)
    print("Done.")
    print("=" * 70)


if __name__ == "__main__":
    main()
