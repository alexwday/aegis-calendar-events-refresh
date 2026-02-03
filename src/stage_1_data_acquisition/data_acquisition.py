#!/usr/bin/env python3
"""
Stage 1: Data Acquisition - Fetches calendar events from the FactSet API for monitored
financial institutions and saves raw data to CSV. This stage is designed to be swappable
with other data sources (e.g., Snowflake) by replacing the query functions while keeping
the same output format. The output contains all fields from the API response without any
transformations, serving as the input for Stage 2 processing.
"""

import calendar
import csv
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, date
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

try:
    import rbc_security

    RBC_SECURITY_AVAILABLE = True
except ImportError:
    RBC_SECURITY_AVAILABLE = False

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
OUTPUT_PATH = Path(__file__).parent / "output" / "raw_calendar_events.csv"
load_dotenv(PROJECT_ROOT / ".env")

# Date range configuration
PAST_MONTHS = 6
FUTURE_MONTHS = 6
DELAY_BETWEEN_CHUNKS = 1  # Seconds to wait between API calls

# =============================================================================
# CANADIAN BANK US TICKER VARIANTS
# =============================================================================
# FactSet sometimes stores Canadian bank events under US tickers.
# We query these additional US variants to capture all events.
# Stage 2 will normalize -US back to -CA during processing.

CANADIAN_BANK_US_VARIANTS = [
    "RY-US",   # Royal Bank of Canada
    "TD-US",   # Toronto-Dominion Bank
    "BMO-US",  # Bank of Montreal
    "BNS-US",  # Bank of Nova Scotia
    "CM-US",   # Canadian Imperial Bank of Commerce
    "NA-US",   # National Bank of Canada
    "LB-US",   # Laurentian Bank
]
# =============================================================================


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


def get_query_tickers(tickers):
    """
    Add the 7 Canadian bank US variants to the query list.

    We query both -CA (from YAML) and -US (hardcoded) because FactSet
    sometimes stores events under US tickers for Canadian banks.
    Stage 2 will normalize -US back to -CA during processing.
    """
    query_tickers = list(tickers) + CANADIAN_BANK_US_VARIANTS
    log.info("Query expansion: Added %d Canadian bank US variants: %s",
             len(CANADIAN_BANK_US_VARIANTS), ", ".join(CANADIAN_BANK_US_VARIANTS))
    return query_tickers


def load_institutions():
    """Load monitored institutions from YAML config."""
    with open(PROJECT_ROOT / "monitored_institutions.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_proxy_url():
    """Build proxy URL with NTLM domain authentication if proxy is configured."""
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
    """Create and configure FactSet API client."""
    proxy_url = build_proxy_url()

    if proxy_url:
        log.info("Proxy configured")
        config = fds.sdk.EventsandTranscripts.Configuration(
            username=os.getenv("FACTSET_USERNAME"),
            password=os.getenv("FACTSET_PASSWORD"),
            proxy=proxy_url,
        )
    else:
        log.info("No proxy configured (direct connection)")
        config = fds.sdk.EventsandTranscripts.Configuration(
            username=os.getenv("FACTSET_USERNAME"),
            password=os.getenv("FACTSET_PASSWORD"),
        )

    config.get_basic_auth_token()
    return config


def add_months(d, months):
    """Add (or subtract) months to a date, handling year boundaries."""
    month = d.month + months
    year = d.year + (month - 1) // 12
    month = ((month - 1) % 12) + 1
    # Clamp day to valid range for the new month
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def get_date_range():
    """Calculate query date range based on configured months."""
    today = datetime.now().date()
    start = add_months(today, -PAST_MONTHS).replace(day=1)  # First day of start month
    end_month = add_months(today, FUTURE_MONTHS)
    end = date(end_month.year, end_month.month, calendar.monthrange(end_month.year, end_month.month)[1])
    return (start, end)


def split_date_range(start_date, end_date):
    """Split date range into monthly chunks (first to last day of each month)."""
    ranges = []
    current = start_date

    while current <= end_date:
        # Get last day of current month
        last_day = calendar.monthrange(current.year, current.month)[1]
        month_end = date(current.year, current.month, last_day)

        # Don't go past the end date
        chunk_end = min(month_end, end_date)
        ranges.append((current, chunk_end))

        # Move to first day of next month
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

            # Check if it's a retryable error
            is_retryable = any(x in error_msg.lower() for x in [
                "parameter", "timeout", "connection", "temporary", "503", "429"
            ])

            if is_retryable and not is_last_attempt:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                log.warning(
                    "API error (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1, max_retries, wait_time, error_msg[:100]
                )
                time.sleep(wait_time)
            else:
                log.error("API error (attempt %d/%d): %s", attempt + 1, max_retries, err)
                return []

    return []


def fetch_events(api, tickers, start_date, end_date):
    """Fetch all events across date range chunks."""
    # Expand tickers to include US variants for Canadian banks
    query_tickers = get_query_tickers(tickers)

    chunks = split_date_range(start_date, end_date)
    log.info("Querying %d tickers across %d monthly chunks from %s to %s",
             len(query_tickers), len(chunks), start_date, end_date)

    events = []
    for i, (start, end) in enumerate(chunks, 1):
        log.info("  Chunk %d/%d: %s to %s", i, len(chunks), start, end)
        events.extend(query_chunk(api, query_tickers, start, end))
        # Delay between chunks to avoid rate limiting
        if i < len(chunks) and DELAY_BETWEEN_CHUNKS > 0:
            time.sleep(DELAY_BETWEEN_CHUNKS)

    # Raw events returned as-is. Ticker normalization (-US -> -CA) happens in Stage 2.
    return events


def save_events(events, output_path):
    """Save events to CSV preserving all fields - NO transformations."""
    if not events:
        log.warning("No events to save")
        return False

    # Save raw data as-is. Ticker normalization (-US -> -CA) happens in Stage 2.
    fields = sorted({key for event in events for key in event.keys()})
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(events)
    return True


def log_event_types(events):
    """Log summary of event types retrieved."""
    counts = defaultdict(int)
    for event in events:
        counts[event.get("event_type", "Unknown")] += 1
    log.info("Event types: %s", dict(sorted(counts.items(), key=lambda x: -x[1])))


def main():
    """Execute Stage 1 data acquisition pipeline."""
    log.info("STAGE 1: DATA ACQUISITION")

    institutions = load_institutions()
    tickers = list(institutions.keys())
    log.info("Loaded %d tickers", len(tickers))

    if RBC_SECURITY_AVAILABLE:
        rbc_security.enable_certs()
        log.info("RBC SSL certificates enabled")

    api_config = create_api_client()
    start_date, end_date = get_date_range()

    with fds.sdk.EventsandTranscripts.ApiClient(api_config) as client:
        api = calendar_events_api.CalendarEventsApi(client)
        events = fetch_events(api, tickers, start_date, end_date)

    log.info("Retrieved %d events", len(events))
    save_events(events, OUTPUT_PATH)
    log.info("Saved to %s", OUTPUT_PATH)
    log_event_types(events)


if __name__ == "__main__":
    main()
