#!/usr/bin/env python3
"""
Stage 1: Data Acquisition - Fetches calendar events from the FactSet API for monitored
financial institutions and saves raw data to CSV. This stage is designed to be swappable
with other data sources (e.g., Snowflake) by replacing the query functions while keeping
the same output format. The output contains all fields from the API response without any
transformations, serving as the input for Stage 2 processing.
"""

import os
import csv
import logging
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
MAX_DAYS_PER_QUERY = 89


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


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


def get_date_range():
    """Calculate query date range based on configured months."""
    today = datetime.now().date()
    return (
        today - timedelta(days=PAST_MONTHS * 30),
        today + timedelta(days=FUTURE_MONTHS * 30),
    )


def split_date_range(start_date, end_date):
    """Split date range into API-compliant chunks."""
    ranges = []
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=MAX_DAYS_PER_QUERY), end_date)
        ranges.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return ranges


def query_chunk(api, tickers, start, end):
    """Query API for a single date range chunk."""
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
            return [e.to_dict() for e in response.data if e.ticker in tickers]
        return []
    except (ConnectionError, TimeoutError, ValueError, RuntimeError) as err:
        log.error("API error: %s", err)
        return []


def fetch_events(api, tickers, start_date, end_date):
    """Fetch all events across date range chunks."""
    chunks = split_date_range(start_date, end_date)
    log.info("Querying %d chunks from %s to %s", len(chunks), start_date, end_date)

    events = []
    for i, (start, end) in enumerate(chunks, 1):
        log.info("  Chunk %d/%d: %s to %s", i, len(chunks), start, end)
        events.extend(query_chunk(api, tickers, start, end))
    return events


def save_events(events, output_path):
    """Save events to CSV preserving all fields."""
    if not events:
        log.warning("No events to save")
        return False

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
