#!/usr/bin/env python3
"""
Interactive Calendar Visualization Generator
============================================
Reads PROCESSED calendar events CSV (from Stage 2) and generates an interactive
HTML calendar. This script does NO data processing - it only handles display/UI.

Data processing (deduplication, enrichment, etc.) is handled by Stage 2.

Input:  ../stage_2_data_processing/output/processed_calendar_events.csv
Output: output/calendar.html
"""

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
import base64


def read_csv_data(csv_path):
    """Read calendar events from CSV file."""
    events = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            events.append(row)
    return events


def convert_to_fullcalendar_format(events):
    """Convert CSV events to FullCalendar format."""
    calendar_events = []

    # Define color mapping for event types
    event_type_colors = {
        "Earnings": "#2563eb",  # Blue
        "ConfirmedEarningsRelease": "#1d4ed8",  # Dark blue
        "ProjectedEarningsRelease": "#60a5fa",  # Light blue
        "Dividend": "#059669",  # Green
        "Conference": "#d97706",  # Orange
        "ShareholdersMeeting": "#7c3aed",  # Purple
        "SalesRevenueCall": "#dc2626",  # Red
        "SalesRevenueMeeting": "#e11d48",  # Pink-red
        "SalesRevenueRelease": "#be185d",  # Pink
        "AnalystsInvestorsMeeting": "#0891b2",  # Cyan
        "SpecialSituation": "#ea580c",  # Dark orange
    }

    for event in events:
        # Parse datetime
        event_datetime = event.get("event_date_time_local", "")

        calendar_event = {
            "id": event.get("event_id", ""),
            "title": f"{event.get('ticker', '')} - {event.get('event_type', '')}",
            "start": (
                event_datetime.split("+")[0] if event_datetime else ""
            ),  # Remove timezone offset
            "description": event.get("event_headline", ""),
            "institution": event.get("institution_name", ""),
            "ticker": event.get("ticker", ""),
            "institutionType": event.get("institution_type", ""),
            "eventType": event.get("event_type", ""),
            "webcastLink": event.get("webcast_link", ""),
            "contactInfo": event.get("contact_info", ""),
            "fiscalYear": event.get("fiscal_year", ""),
            "fiscalPeriod": event.get("fiscal_period", ""),
            "eventTimeLocal": event.get("event_time_local", ""),
            "backgroundColor": event_type_colors.get(
                event.get("event_type", ""), "#6b7280"
            ),
            "borderColor": event_type_colors.get(
                event.get("event_type", ""), "#6b7280"
            ),
        }
        calendar_events.append(calendar_event)

    return calendar_events


def get_unique_values(events, field):
    """Get unique values for a field from events."""
    values = set()
    for event in events:
        value = event.get(field, "")
        if value:
            values.add(value)
    return sorted(list(values))


def get_grouped_event_types(events):
    """
    Get unique event types, grouping earnings-related types into one for UI display.
    This is a UI concern (filter dropdown), not data processing.
    """
    # Earnings types that should be grouped together in the UI
    earnings_types = {
        "Earnings",
        "ConfirmedEarningsRelease",
        "ProjectedEarningsRelease",
    }

    # Collect all unique event types from the data
    all_types = set()
    has_earnings = False

    for event in events:
        event_type = event.get("event_type", "")
        if event_type in earnings_types:
            has_earnings = True
        elif event_type:
            all_types.add(event_type)

    # Build the final list for display
    result = []
    if has_earnings:
        result.append("Earnings")  # Single option for all earnings types

    result.extend(sorted(all_types))
    return result


def generate_ics_content(event):
    """Generate iCalendar (.ics) content for an event."""
    # Parse the datetime
    event_datetime_str = event.get("event_date_time_utc", "")
    try:
        event_dt = datetime.fromisoformat(event_datetime_str.replace("Z", "+00:00"))
        dtstart = event_dt.strftime("%Y%m%dT%H%M%SZ")
        # Assume 1 hour duration
        dtend = event_dt.replace(hour=event_dt.hour + 1).strftime("%Y%m%dT%H%M%SZ")
    except (ValueError, AttributeError):
        dtstart = "20251201T120000Z"
        dtend = "20251201T130000Z"

    # Build description
    description_parts = [event.get("event_headline", "")]
    if event.get("webcast_link"):
        description_parts.append(f"Webcast: {event['webcast_link']}")
    if event.get("contact_info"):
        description_parts.append(f"Contact: {event['contact_info']}")
    if event.get("fiscal_period"):
        description_parts.append(
            f"Fiscal Period: {event['fiscal_period']} {event.get('fiscal_year', '')}"
        )

    description = "\\n".join(description_parts)

    ics_content = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//AEGIS Calendar Events//EN
BEGIN:VEVENT
UID:{event.get('event_id', 'EVENT')}@aegis-calendar
DTSTAMP:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}
DTSTART:{dtstart}
DTEND:{dtend}
SUMMARY:{event.get('ticker', '')} - {event.get('event_type', '')}
DESCRIPTION:{description}
LOCATION:{event.get('institution_name', '')}
STATUS:CONFIRMED
SEQUENCE:0
END:VEVENT
END:VCALENDAR"""

    return ics_content


def generate_inst_options(institution_types):
    """Generate HTML options for institution type filter."""
    options = []
    for inst_type in institution_types:
        display_name = inst_type.replace("_", " ")
        opt = f"""                        <div class="multiselect-option">
                            <input type="checkbox" id="inst_{inst_type}" \
value="{inst_type}">
                            <label for="inst_{inst_type}">{display_name}</label>
                        </div>"""
        options.append(opt)
    return "\n".join(options)


def generate_evt_options(event_types):
    """Generate HTML options for event type filter."""
    options = []
    for evt_type in event_types:
        opt = f"""                        <div class="multiselect-option">
                            <input type="checkbox" id="evt_{evt_type}" \
value="{evt_type}">
                            <label for="evt_{evt_type}">{evt_type}</label>
                        </div>"""
        options.append(opt)
    return "\n".join(options)


def generate_html(calendar_events, institution_types, event_types, csv_events):
    """Generate the HTML page with embedded calendar and filters."""

    # Create a mapping for ICS downloads
    ics_data = {}
    for csv_event in csv_events:
        event_id = csv_event.get("event_id", "")
        ics_content = generate_ics_content(csv_event)
        # Base64 encode for data URL
        ics_base64 = base64.b64encode(ics_content.encode("utf-8")).decode("utf-8")
        ics_data[event_id] = ics_base64

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Financial Institutions Calendar Events</title>

    <!-- FullCalendar CSS -->
    <link rel="stylesheet"
        href="https://cdn.jsdelivr.net/npm/fullcalendar@6.1.10/index.global.min.css">

    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
                Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f9fafb;
            padding: 20px;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            padding: 30px;
        }}

        header {{
            margin-bottom: 30px;
            border-bottom: 2px solid #e5e7eb;
            padding-bottom: 20px;
        }}

        h1 {{
            font-size: 28px;
            color: #111827;
            margin-bottom: 8px;
        }}

        .subtitle {{
            color: #6b7280;
            font-size: 14px;
        }}

        .filters {{
            display: flex;
            gap: 15px;
            margin-bottom: 25px;
            flex-wrap: wrap;
            align-items: center;
        }}

        .filter-group {{
            display: flex;
            flex-direction: column;
            gap: 6px;
        }}

        .filter-group label {{
            font-size: 13px;
            font-weight: 600;
            color: #374151;
        }}

        .multiselect-dropdown {{
            position: relative;
            min-width: 220px;
        }}

        .multiselect-button {{
            padding: 8px 12px;
            border: 1px solid #d1d5db;
            border-radius: 6px;
            font-size: 14px;
            background: white;
            cursor: pointer;
            width: 100%;
            text-align: left;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        .multiselect-button:hover {{
            border-color: #9ca3af;
        }}

        .multiselect-button:focus {{
            outline: none;
            border-color: #2563eb;
            box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1);
        }}

        .multiselect-arrow {{
            margin-left: 8px;
            transition: transform 0.2s;
        }}

        .multiselect-dropdown.open .multiselect-arrow {{
            transform: rotate(180deg);
        }}

        .multiselect-options {{
            position: absolute;
            top: 100%;
            left: 0;
            right: 0;
            margin-top: 4px;
            background: white;
            border: 1px solid #d1d5db;
            border-radius: 6px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
            max-height: 300px;
            overflow-y: auto;
            z-index: 1000;
            display: none;
        }}

        .multiselect-dropdown.open .multiselect-options {{
            display: block;
        }}

        .multiselect-option {{
            padding: 8px 12px;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 8px;
        }}

        .multiselect-option:hover {{
            background: #f3f4f6;
        }}

        .multiselect-option input[type="checkbox"] {{
            cursor: pointer;
        }}

        .multiselect-option label {{
            cursor: pointer;
            flex: 1;
            margin: 0;
        }}

        .reset-btn {{
            padding: 8px 16px;
            background: #ef4444;
            color: white;
            border: none;
            border-radius: 6px;
            font-size: 14px;
            cursor: pointer;
            font-weight: 500;
            margin-top: 20px;
        }}

        .reset-btn:hover {{
            background: #dc2626;
        }}

        #calendar {{
            margin-top: 20px;
        }}

        .legend {{
            margin-top: 25px;
            padding: 20px;
            background: #f9fafb;
            border-radius: 8px;
            border: 1px solid #e5e7eb;
        }}

        .legend-title {{
            font-weight: 600;
            color: #111827;
            margin-bottom: 12px;
            font-size: 14px;
        }}

        .legend-items {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 10px;
        }}

        .legend-item {{
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 13px;
        }}

        .legend-color {{
            width: 16px;
            height: 16px;
            border-radius: 3px;
        }}

        .event-modal {{
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.5);
            z-index: 1000;
            align-items: center;
            justify-content: center;
        }}

        .event-modal.active {{
            display: flex;
        }}

        .modal-content {{
            background: white;
            border-radius: 12px;
            padding: 30px;
            max-width: 600px;
            width: 90%;
            max-height: 80vh;
            overflow-y: auto;
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.1);
        }}

        .modal-header {{
            display: flex;
            justify-content: space-between;
            align-items: start;
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 2px solid #e5e7eb;
        }}

        .modal-title {{
            font-size: 20px;
            font-weight: 700;
            color: #111827;
        }}

        .close-btn {{
            background: none;
            border: none;
            font-size: 24px;
            cursor: pointer;
            color: #6b7280;
            padding: 0;
            width: 30px;
            height: 30px;
            display: flex;
            align-items: center;
            justify-content: center;
            border-radius: 6px;
        }}

        .close-btn:hover {{
            background: #f3f4f6;
            color: #111827;
        }}

        .event-detail {{
            margin-bottom: 15px;
        }}

        .event-detail-label {{
            font-weight: 600;
            color: #374151;
            font-size: 13px;
            margin-bottom: 4px;
        }}

        .event-detail-value {{
            color: #111827;
            font-size: 14px;
        }}

        .event-detail-value a {{
            color: #2563eb;
            text-decoration: none;
        }}

        .event-detail-value a:hover {{
            text-decoration: underline;
        }}

        .download-ics-btn {{
            width: 100%;
            padding: 12px;
            background: #2563eb;
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            margin-top: 20px;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
        }}

        .download-ics-btn:hover {{
            background: #1d4ed8;
        }}

        .fc-event {{
            cursor: pointer;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Financial Institutions Calendar Events</h1>
            <p class="subtitle">Interactive calendar showing earnings, dividends,
                conferences, and other corporate events</p>
        </header>

        <div class="filters">
            <div class="filter-group">
                <label>Institution Type</label>
                <div class="multiselect-dropdown" id="institutionTypeDropdown">
                    <button class="multiselect-button" type="button">
                        <span id="institutionTypeLabel">Select Institution Types</span>
                        <span class="multiselect-arrow">▼</span>
                    </button>
                    <div class="multiselect-options" id="institutionTypeOptions">
{generate_inst_options(institution_types)}
                    </div>
                </div>
            </div>

            <div class="filter-group">
                <label>Event Type</label>
                <div class="multiselect-dropdown" id="eventTypeDropdown">
                    <button class="multiselect-button" type="button">
                        <span id="eventTypeLabel">Select Event Types</span>
                        <span class="multiselect-arrow">▼</span>
                    </button>
                    <div class="multiselect-options" id="eventTypeOptions">
{generate_evt_options(event_types)}
                    </div>
                </div>
            </div>

            <button class="reset-btn" onclick="resetFilters()">
                Reset All Filters</button>
        </div>

        <div id="calendar"></div>

        <div class="legend">
            <div class="legend-title">Event Type Legend</div>
            <div class="legend-items">
                <div class="legend-item">
                    <div class="legend-color" style="background: #2563eb;"></div>
                    <span>Earnings</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #1d4ed8;"></div>
                    <span>Confirmed Earnings Release</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #60a5fa;"></div>
                    <span>Projected Earnings Release</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #059669;"></div>
                    <span>Dividend</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #d97706;"></div>
                    <span>Conference</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #7c3aed;"></div>
                    <span>Shareholders Meeting</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #dc2626;"></div>
                    <span>Sales Revenue Call</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #e11d48;"></div>
                    <span>Sales Revenue Meeting</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #be185d;"></div>
                    <span>Sales Revenue Release</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #0891b2;"></div>
                    <span>Analysts/Investors Meeting</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: #ea580c;"></div>
                    <span>Special Situation</span>
                </div>
            </div>
        </div>
    </div>

    <!-- Event Detail Modal -->
    <div class="event-modal" id="eventModal">
        <div class="modal-content">
            <div class="modal-header">
                <div class="modal-title" id="modalTitle"></div>
                <button class="close-btn" onclick="closeModal()">&times;</button>
            </div>
            <div id="modalBody"></div>
        </div>
    </div>

    <!-- FullCalendar JS -->
    <script
        src="https://cdn.jsdelivr.net/npm/fullcalendar@6.1.10/index.global.min.js">
    </script>

    <script>
        // Event data
        const allEvents = {json.dumps(calendar_events, indent=8)};

        // ICS data for downloads
        const icsData = {json.dumps(ics_data, indent=8)};

        // Earnings type grouping - maps "Earnings" filter to actual types
        const EARNINGS_TYPES = [
            'Earnings', 'ConfirmedEarningsRelease', 'ProjectedEarningsRelease'
        ];

        let calendar;
        let currentEvents = [...allEvents];

        // Initialize calendar
        document.addEventListener('DOMContentLoaded', function() {{
            const calendarEl = document.getElementById('calendar');

            calendar = new FullCalendar.Calendar(calendarEl, {{
                initialView: 'dayGridMonth',
                headerToolbar: {{
                    left: 'prev,next today',
                    center: 'title',
                    right: 'dayGridMonth,listMonth'
                }},
                events: currentEvents,
                eventClick: function(info) {{
                    showEventDetails(info.event);
                }},
                height: 'auto',
                eventTimeFormat: {{
                    hour: '2-digit',
                    minute: '2-digit',
                    meridiem: 'short'
                }}
            }});

            calendar.render();

            // Setup multiselect dropdowns
            setupMultiselect();

            // Set default selections: Canadian_Banks, US_Banks, Earnings
            setCheckbox('inst_Canadian_Banks', true);
            setCheckbox('inst_US_Banks', true);
            setCheckbox('evt_Earnings', true);
            updateDropdownLabels();
            applyFilters();
        }});

        function setupMultiselect() {{
            // Institution Type dropdown
            const instDropdown = document.getElementById('institutionTypeDropdown');
            const instButton = instDropdown.querySelector('.multiselect-button');
            const instOptions = document.getElementById('institutionTypeOptions');

            instButton.addEventListener('click', (e) => {{
                e.stopPropagation();
                instDropdown.classList.toggle('open');
                const evtDrop = document.getElementById('eventTypeDropdown');
                evtDrop.classList.remove('open');
            }});

            const instChk = 'input[type="checkbox"]';
            instOptions.querySelectorAll(instChk).forEach(checkbox => {{
                checkbox.addEventListener('change', () => {{
                    updateDropdownLabels();
                    applyFilters();
                }});
            }});

            // Event Type dropdown
            const evtDropdown = document.getElementById('eventTypeDropdown');
            const evtButton = evtDropdown.querySelector('.multiselect-button');
            const evtOptions = document.getElementById('eventTypeOptions');

            evtButton.addEventListener('click', (e) => {{
                e.stopPropagation();
                evtDropdown.classList.toggle('open');
                const instDrop = document.getElementById('institutionTypeDropdown');
                instDrop.classList.remove('open');
            }});

            const evtChk = 'input[type="checkbox"]';
            evtOptions.querySelectorAll(evtChk).forEach(checkbox => {{
                checkbox.addEventListener('change', () => {{
                    updateDropdownLabels();
                    applyFilters();
                }});
            }});

            // Close dropdowns when clicking outside
            document.addEventListener('click', () => {{
                const instEl = document.getElementById('institutionTypeDropdown');
                const evtEl = document.getElementById('eventTypeDropdown');
                instEl.classList.remove('open');
                evtEl.classList.remove('open');
            }});

            // Prevent closing when clicking inside options
            instOptions.addEventListener('click', (e) => e.stopPropagation());
            evtOptions.addEventListener('click', (e) => e.stopPropagation());
        }}

        function setCheckbox(id, checked) {{
            const checkbox = document.getElementById(id);
            if (checkbox) checkbox.checked = checked;
        }}

        function updateDropdownLabels() {{
            // Update institution type label
            const instSel = '#institutionTypeOptions input:checked';
            const instCheckboxes = document.querySelectorAll(instSel);
            const instLabel = document.getElementById('institutionTypeLabel');
            if (instCheckboxes.length === 0) {{
                instLabel.textContent = 'Select Institution Types';
            }} else if (instCheckboxes.length === 1) {{
                const txt = instCheckboxes[0].nextElementSibling.textContent;
                instLabel.textContent = txt;
            }} else {{
                instLabel.textContent = `${{instCheckboxes.length}} selected`;
            }}

            // Update event type label
            const evtSel = '#eventTypeOptions input:checked';
            const evtCheckboxes = document.querySelectorAll(evtSel);
            const evtLabel = document.getElementById('eventTypeLabel');
            if (evtCheckboxes.length === 0) {{
                evtLabel.textContent = 'Select Event Types';
            }} else if (evtCheckboxes.length === 1) {{
                const txt = evtCheckboxes[0].nextElementSibling.textContent;
                evtLabel.textContent = txt;
            }} else {{
                evtLabel.textContent = `${{evtCheckboxes.length}} selected`;
            }}
        }}

        function applyFilters() {{
            // Get selected institution types
            const instSel = '#institutionTypeOptions input:checked';
            const selectedInstitutions = Array.from(
                document.querySelectorAll(instSel)
            ).map(cb => cb.value);

            // Get selected event types
            const evtSel = '#eventTypeOptions input:checked';
            const selectedEvents = Array.from(
                document.querySelectorAll(evtSel)
            ).map(cb => cb.value);

            currentEvents = allEvents.filter(event => {{
                // Institution filter
                const matchesInstitution = selectedInstitutions.length === 0 ||
                                         selectedInstitutions.includes(event.institutionType);

                // Event type filter - handle "Earnings" group
                let matchesEvent;
                if (selectedEvents.length === 0) {{
                    matchesEvent = true;
                }} else {{
                    matchesEvent = selectedEvents.some(selectedType => {{
                        if (selectedType === 'Earnings') {{
                            return EARNINGS_TYPES.includes(event.eventType);
                        }} else {{
                            return event.eventType === selectedType;
                        }}
                    }});
                }}

                return matchesInstitution && matchesEvent;
            }});

            calendar.removeAllEvents();
            calendar.addEventSource(currentEvents);
        }}

        function resetFilters() {{
            // Uncheck all checkboxes
            const instBoxes = '#institutionTypeOptions input';
            const evtBoxes = '#eventTypeOptions input';
            document.querySelectorAll(instBoxes).forEach(cb => {{
                cb.checked = false;
            }});
            document.querySelectorAll(evtBoxes).forEach(cb => {{
                cb.checked = false;
            }});

            updateDropdownLabels();
            applyFilters();
        }}

        function showEventDetails(event) {{
            const modal = document.getElementById('eventModal');
            const modalTitle = document.getElementById('modalTitle');
            const modalBody = document.getElementById('modalBody');
            const props = event.extendedProps;

            modalTitle.textContent = event.title;

            let bodyHTML = `
                <div class="event-detail">
                    <div class="event-detail-label">Institution</div>
                    <div class="event-detail-value">${{props.institution}}</div>
                </div>
                <div class="event-detail">
                    <div class="event-detail-label">Event Description</div>
                    <div class="event-detail-value">${{props.description}}</div>
                </div>
                <div class="event-detail">
                    <div class="event-detail-label">Date & Time</div>
                    <div class="event-detail-value">${{props.eventTimeLocal}}</div>
                </div>
            `;

            if (props.fiscalPeriod) {{
                bodyHTML += `
                    <div class="event-detail">
                        <div class="event-detail-label">Fiscal Period</div>
                        <div class="event-detail-value">
                            ${{props.fiscalPeriod}} ${{props.fiscalYear}}</div>
                    </div>
                `;
            }}

            if (props.webcastLink) {{
                bodyHTML += `
                    <div class="event-detail">
                        <div class="event-detail-label">Webcast</div>
                        <div class="event-detail-value">
                            <a href="${{props.webcastLink}}" target="_blank">
                                ${{props.webcastLink}}</a></div>
                    </div>
                `;
            }}

            if (props.contactInfo) {{
                bodyHTML += `
                    <div class="event-detail">
                        <div class="event-detail-label">Contact Information</div>
                        <div class="event-detail-value">${{props.contactInfo}}</div>
                    </div>
                `;
            }}

            const evtId = event.id;
            const evtTitle = event.title;
            bodyHTML += `
                <button class="download-ics-btn"
                    onclick="downloadICS('${{evtId}}', '${{evtTitle}}')">
                    Add to Calendar (.ics)
                </button>
            `;

            modalBody.innerHTML = bodyHTML;
            modal.classList.add('active');
        }}

        function closeModal() {{
            document.getElementById('eventModal').classList.remove('active');
        }}

        function downloadICS(eventId, eventTitle) {{
            const icsContent = icsData[eventId];
            if (!icsContent) {{
                alert('Calendar file not available for this event');
                return;
            }}

            // Decode base64 and create blob
            const binaryString = atob(icsContent);
            const bytes = new Uint8Array(binaryString.length);
            for (let i = 0; i < binaryString.length; i++) {{
                bytes[i] = binaryString.charCodeAt(i);
            }}
            const blob = new Blob([bytes], {{ type: 'text/calendar' }});

            // Create download link
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `${{eventId}}.ics`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        }}

        // Close modal when clicking outside
        document.getElementById('eventModal').addEventListener('click', function(e) {{
            if (e.target === this) {{
                closeModal();
            }}
        }});
    </script>
</body>
</html>"""

    return html


def main():
    """Main execution function."""
    print("=" * 60)
    print("CALENDAR VISUALIZATION GENERATOR")
    print("=" * 60)

    # Setup paths - default to Stage 2 output
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    # Try Stage 2 output first, fall back to sample data
    stage_2_csv = (
        project_root
        / "stage_2_data_processing"
        / "output"
        / "processed_calendar_events.csv"
    )
    sample_csv = script_dir / "sample_data" / "example_calendar_events.csv"

    if stage_2_csv.exists():
        csv_path = stage_2_csv
        print(f"\nUsing Stage 2 processed data: {csv_path.name}")
    elif sample_csv.exists():
        csv_path = sample_csv
        print(f"\nUsing sample data (Stage 2 output not found): {csv_path.name}")
    else:
        print("\nERROR: No data found!")
        print(f"  Expected: {stage_2_csv}")
        print(f"  Or: {sample_csv}")
        print("\nPlease run Stage 2 first, or provide sample data.")
        return

    output_dir = script_dir / "output"
    output_html = output_dir / "calendar.html"

    # Ensure output directory exists
    output_dir.mkdir(exist_ok=True)

    # Load data (already processed by Stage 2 - no additional processing needed)
    print("\nLoading data...")
    csv_events = read_csv_data(csv_path)
    print(f"  Loaded {len(csv_events)} events (pre-processed by Stage 2)")

    # Convert to calendar format (UI transformation only)
    print("\nConverting to calendar format...")
    calendar_events = convert_to_fullcalendar_format(csv_events)

    # Extract filter options (UI concern)
    print("Extracting filter options...")
    institution_types = get_unique_values(csv_events, "institution_type")
    event_types = get_grouped_event_types(csv_events)
    print(f"  {len(institution_types)} institution types")
    print(f"  {len(event_types)} event type groups")

    # Generate HTML
    print("\nGenerating HTML calendar...")
    html_content = generate_html(
        calendar_events, institution_types, event_types, csv_events
    )

    # Save
    print(f"Saving to: {output_html}")
    with open(output_html, "w", encoding="utf-8") as f:
        f.write(html_content)

    print()
    print("=" * 60)
    print("SUCCESS!")
    print(f"  Output: {output_html}")
    print("  Open in browser to view calendar")
    print("=" * 60)


if __name__ == "__main__":
    main()
