#!/usr/bin/env python3
"""
Temporary diagnostic script to check what FactSet returns for RY-US vs RY-CA.
Run this to see if FactSet has any events under the RY-US ticker.
"""

import calendar
import os
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

# Test these specific tickers
TEST_TICKERS = ["RY-CA", "RY-US"]

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


def query_events(api, tickers, start, end):
    request = CompanyEventRequest(
        data=CompanyEventRequestData(
            date_time=CompanyEventRequestDataDateTime(
                start=dateutil_parse(f"{start}T00:00:00Z"),
                end=dateutil_parse(f"{end}T23:59:59Z"),
            ),
            universe=CompanyEventRequestDataUniverse(symbols=tickers, type="Tickers"),
        ),
    )

    try:
        response = api.get_company_event(request)
        if response and hasattr(response, "data") and response.data:
            return [e.to_dict() for e in response.data]
        return []
    except Exception as err:
        print(f"API error: {err}")
        return []


def main():
    print("=" * 60)
    print("RY-US vs RY-CA Diagnostic Test")
    print("=" * 60)

    if RBC_SECURITY_AVAILABLE:
        rbc_security.enable_certs()
        print("RBC SSL certificates enabled")

    api_config = create_api_client()
    start_date, end_date = get_date_range()

    print(f"\nQuerying tickers: {TEST_TICKERS}")
    print(f"Date range: {start_date} to {end_date}")
    print()

    with fds.sdk.EventsandTranscripts.ApiClient(api_config) as client:
        api = calendar_events_api.CalendarEventsApi(client)
        events = query_events(api, TEST_TICKERS, start_date, end_date)

    print(f"Total events returned: {len(events)}")
    print()

    # Group by ticker
    by_ticker = defaultdict(list)
    for e in events:
        by_ticker[e.get("ticker", "Unknown")].append(e)

    # Show counts
    print("Events by ticker:")
    print("-" * 40)
    for ticker in TEST_TICKERS:
        count = len(by_ticker.get(ticker, []))
        print(f"  {ticker}: {count} events")

    # Show any other tickers that came back
    other_tickers = set(by_ticker.keys()) - set(TEST_TICKERS)
    if other_tickers:
        print(f"\nOther tickers in response: {other_tickers}")

    # Show sample events for each ticker
    print()
    for ticker in TEST_TICKERS:
        events_for_ticker = by_ticker.get(ticker, [])
        print(f"\n{'=' * 60}")
        print(f"{ticker} - {len(events_for_ticker)} events")
        print("=" * 60)

        if not events_for_ticker:
            print("  (no events)")
        else:
            # Show event types summary
            type_counts = defaultdict(int)
            for e in events_for_ticker:
                type_counts[e.get("event_type", "Unknown")] += 1
            print(f"Event types: {dict(type_counts)}")
            print()

            # Show first 5 events
            for i, e in enumerate(events_for_ticker[:5]):
                print(f"  [{i+1}] {e.get('event_type')} on {e.get('event_datetime_utc', 'N/A')}")
                print(f"      Title: {e.get('event_title', 'N/A')[:60]}")

            if len(events_for_ticker) > 5:
                print(f"  ... and {len(events_for_ticker) - 5} more")


if __name__ == "__main__":
    main()
