-- =====================================================
-- AEGIS Calendar Events Database Schema
-- =====================================================

-- Main Table: aegis_calendar_events (16 fields from CSV)
-- =====================================================

CREATE TABLE aegis_calendar_events (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Unique Business Key (from FactSet API)
    event_id VARCHAR(100) NOT NULL,
    ticker VARCHAR(20) NOT NULL,

    -- Institution Information
    institution_name VARCHAR(255) NOT NULL,
    institution_id INTEGER NOT NULL,
    institution_type VARCHAR(100) NOT NULL,

    -- Event Details
    event_type VARCHAR(50) NOT NULL,
    event_headline VARCHAR(500),

    -- Event Timing (UTC and Toronto local time)
    event_date_time_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    event_date_time_local TIMESTAMP WITH TIME ZONE,
    event_date DATE NOT NULL,
    event_time_local VARCHAR(20),  -- Format: "HH:MM EST" or "HH:MM EDT"

    -- Webcast Information
    webcast_link TEXT,
    contact_info TEXT,

    -- Fiscal Period Information
    fiscal_year VARCHAR(10),       -- Can be empty string
    fiscal_period VARCHAR(10),     -- Can be empty string (Q1, Q2, Q3, Q4)

    -- Audit Field
    data_fetched_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Constraints
    CONSTRAINT unique_event UNIQUE (ticker, event_id)
);

-- =====================================================
-- Indexes for Performance
-- =====================================================

-- Date-based queries (most common)
CREATE INDEX idx_aegis_calendar_events_date ON aegis_calendar_events(event_date);
CREATE INDEX idx_aegis_calendar_events_datetime_utc ON aegis_calendar_events(event_date_time_utc);

-- Filter queries
CREATE INDEX idx_aegis_calendar_events_ticker ON aegis_calendar_events(ticker);
CREATE INDEX idx_aegis_calendar_events_institution_type ON aegis_calendar_events(institution_type);
CREATE INDEX idx_aegis_calendar_events_event_type ON aegis_calendar_events(event_type);

-- Fiscal period queries (for earnings)
CREATE INDEX idx_aegis_calendar_events_fiscal_period ON aegis_calendar_events(fiscal_year, fiscal_period)
WHERE fiscal_year IS NOT NULL AND fiscal_year != '';

