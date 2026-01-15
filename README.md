# AEGIS Calendar Events Refresh

A 3-stage ETL pipeline to acquire, process, and load calendar events data for monitored financial institutions.

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│  Stage 1            │     │  Stage 2            │     │  Stage 3            │
│  DATA ACQUISITION   │ ──► │  DATA PROCESSING    │ ──► │  DATABASE UPLOAD    │
│                     │     │                     │     │                     │
│  - Query API        │     │  - Field mapping    │     │  - Validate schema  │
│  - Raw data dump    │     │  - Enrich data      │     │  - Load to Postgres │
│  - No transforms    │     │  - Deduplicate      │     │                     │
│                     │     │  - Timezone convert │     │                     │
│  [SWAPPABLE]        │     │  [STABLE]           │     │  [PLACEHOLDER]      │
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘
         │                           │                           │
         ▼                           ▼                           ▼
  raw_calendar_events.csv    processed_calendar_events.csv    PostgreSQL
```

### Design Principles

1. **Stage 1 is SWAPPABLE** - Currently queries FactSet API, can be replaced with Snowflake script
2. **Stage 2 has FIELD MAPPING** - Easy to adapt when source field names change
3. **All data is LOCAL** - No NAS dependencies, everything in project folders
4. **Schema-driven output** - Stage 2 output matches PostgreSQL schema exactly

## Quick Start

```bash
# 1. Clone and setup
git clone https://github.com/alexwday/aegis-calendar-events-refresh.git
cd aegis-calendar-events-refresh
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Edit .env with your API credentials

# 3. Run the pipeline
python stage_1_data_acquisition/main_data_acquisition.py
python stage_2_data_processing/main_data_processing.py
python stage_3_database_upload/main_database_upload.py
```

## Project Structure

```
aegis-calendar-events-refresh/
├── stage_1_data_acquisition/
│   ├── main_data_acquisition.py    # Query API, dump raw data
│   └── output/
│       └── raw_calendar_events.csv # Raw API response (generated)
│
├── stage_2_data_processing/
│   ├── main_data_processing.py     # Transform and cleanse data
│   └── output/
│       └── processed_calendar_events.csv # Ready for database (generated)
│
├── stage_3_database_upload/
│   └── main_database_upload.py     # Load to PostgreSQL (placeholder)
│
├── config/
│   ├── calendar_config.yaml        # Pipeline configuration
│   └── monitored_institutions.yaml # 91 monitored institutions
│
├── visualization/                  # Interactive HTML calendar (utility)
│   ├── generate_calendar.py
│   ├── sample_data/
│   └── output/
│
├── postgres_schema.sql             # Database schema
├── requirements.txt
├── .env.example
└── README.md
```

## Stage Details

### Stage 1: Data Acquisition

**Purpose**: Query FactSet Calendar Events API and dump raw data to CSV.

**Key Points**:
- Outputs RAW data with NO transformations
- Designed to be swappable (API → Snowflake)
- All API response fields preserved

**Output**: `stage_1_data_acquisition/output/raw_calendar_events.csv`

```bash
python stage_1_data_acquisition/main_data_acquisition.py
```

### Stage 2: Data Processing

**Purpose**: Transform raw data into database-ready format.

**Key Points**:
- **Field mapping at the top** - Update when switching data sources
- Enriches with institution metadata
- Converts UTC → Toronto timezone
- Deduplicates earnings events
- Output matches PostgreSQL schema exactly

**Field Mapping** (in `main_data_processing.py`):
```python
FIELD_MAPPING = {
    "event_id":             "event_id",
    "ticker":               "ticker",
    "event_type":           "event_type",
    "event_datetime_utc":   "event_date_time",      # API field name
    "description":          "description",          # Becomes event_headline
    "webcast_link":         "webcast_link",
    # ... update these when switching to Snowflake
}
```

**Output**: `stage_2_data_processing/output/processed_calendar_events.csv`

```bash
python stage_2_data_processing/main_data_processing.py
```

### Stage 3: Database Upload (Placeholder)

**Purpose**: Load processed data into PostgreSQL.

**Current Status**: Placeholder - loads and validates data, upload not yet implemented.

```bash
python stage_3_database_upload/main_database_upload.py
```

## Output Schema

The processed CSV (and PostgreSQL table) has 16 fields:

| Field | Description |
|-------|-------------|
| `event_id` | Unique event identifier |
| `ticker` | Institution ticker symbol |
| `institution_name` | Full institution name |
| `institution_id` | Internal institution ID |
| `institution_type` | Category (Canadian_Banks, etc.) |
| `event_type` | Type of event |
| `event_headline` | Event description |
| `event_date_time_utc` | Event datetime in UTC |
| `event_date_time_local` | Event datetime in Toronto time |
| `event_date` | Event date (YYYY-MM-DD) |
| `event_time_local` | Event time with timezone (HH:MM EST) |
| `webcast_link` | Webcast URL if available |
| `contact_info` | Contact details if available |
| `fiscal_year` | Fiscal year if applicable |
| `fiscal_period` | Fiscal period (Q1, Q2, etc.) |
| `data_fetched_timestamp` | When data was processed |

## Configuration

### Environment Variables (.env)

```bash
# Stage 1: FactSet API
API_USERNAME=your_factset_username
API_PASSWORD=your_factset_api_password

# Proxy (Corporate network)
PROXY_USER=your_proxy_username
PROXY_PASSWORD=your_proxy_password
PROXY_URL=proxy.company.com:8080
PROXY_DOMAIN=MAPLE

# Stage 3: PostgreSQL (future)
# POSTGRES_HOST=localhost
# POSTGRES_PORT=5432
# POSTGRES_DB=aegis
# POSTGRES_USER=your_user
# POSTGRES_PASSWORD=your_password
```

### Calendar Config (config/calendar_config.yaml)

```yaml
# Date range for data acquisition
data_acquisition:
  date_range:
    past_months: 6
    future_months: 6

# Processing settings
data_processing:
  local_timezone: "America/Toronto"
```

## Switching to Snowflake

When ready to switch from FactSet API to Snowflake:

1. **Create new Stage 1 script** that queries Snowflake and outputs CSV with same structure
2. **Update Field Mapping** in Stage 2 if Snowflake field names differ:
   ```python
   FIELD_MAPPING = {
       "event_datetime_utc":   "EVENT_DATETIME",  # Snowflake field name
       "description":          "EVENT_DESC",       # Snowflake field name
       # ...
   }
   ```
3. Stage 2 and Stage 3 remain unchanged

## Monitored Institutions

91 financial institutions across 12 categories:

- Canadian Banks (7)
- US Banks (7)
- European Banks (14)
- US Boutiques (9)
- Canadian Asset Managers (4)
- US Regionals (15)
- US Wealth & Asset Managers (10)
- UK Wealth & Asset Managers (3)
- Nordic Banks (4)
- Canadian Insurers (6)
- Canadian Monoline Lenders (4)
- Australian Banks (5)
- Trusts (3)

## Visualization (Optional)

Generate an interactive HTML calendar from processed data:

```bash
cd visualization
python generate_calendar.py
open output/calendar.html
```

## Dependencies

```
fds.sdk.EventsandTranscripts==1.1.1
python-dotenv
pyyaml
pytz
python-dateutil
```

## License

Internal use only.
