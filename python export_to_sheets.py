import webbrowser
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from db import fetch_all_events, fetch_all_organizers

# Google Sheets API setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SHEET_ID = None  # keep in memory only, no file


def authenticate_google_sheets():
    """Authenticate Google Sheets API."""
    creds = None
    try:
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    except Exception:
        pass

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds


def create_google_sheet_if_missing(sheet_title="Event Catalog Export"):
    """Create a new Google Sheet with Events, Organizers, and Combined tabs."""
    service = build('sheets', 'v4', credentials=authenticate_google_sheets())
    spreadsheet = {
        'properties': {'title': sheet_title},
        'sheets': [
            {'properties': {'title': 'Events'}},
            {'properties': {'title': 'Organizers'}},
            {'properties': {'title': 'Events+Organizers'}}
        ]
    }
    sheet = service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
    sheet_id = sheet.get('spreadsheetId')
    print(f"✅ Created new Google Sheet: {sheet_title} → ID {sheet_id}")
    return sheet_id


def get_sheet_id():
    """Return the sheet ID (create if missing)."""
    global SHEET_ID
    if SHEET_ID:
        return SHEET_ID
    SHEET_ID = create_google_sheet_if_missing()
    return SHEET_ID


def export_to_google_sheets(sheet_data, sheet_name, sheet_id):
    """Export data to Google Sheets."""
    service = build('sheets', 'v4', credentials=authenticate_google_sheets())
    sheet = service.spreadsheets()

    # Clear old values first
    sheet.values().clear(spreadsheetId=sheet_id, range=f'{sheet_name}!A:Z').execute()

    # Write new values
    sheet.values().update(
        spreadsheetId=sheet_id,
        range=f'{sheet_name}!A1',
        valueInputOption="RAW",
        body={'values': sheet_data}
    ).execute()
    print(f"✅ {sheet_name} sheet updated successfully.")


def style_sheet(sheet_id, sheet_name):
    """Apply formatting: bold headers, freeze first row, auto-resize, filter."""
    service = build('sheets', 'v4', credentials=authenticate_google_sheets())

    # Get sheetId (numeric ID, not the name)
    sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheets = sheet_metadata.get("sheets", [])
    sheet_id_num = None
    for s in sheets:
        if s["properties"]["title"] == sheet_name:
            sheet_id_num = s["properties"]["sheetId"]

    if sheet_id_num is None:
        return

    requests = [
        # Bold header row
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id_num, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold"
            }
        },
        # Freeze first row
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id_num, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }
        },
        # Auto-resize all columns
        {
            "autoResizeDimensions": {
                "dimensions": {"sheetId": sheet_id_num, "dimension": "COLUMNS", "startIndex": 0}
            }
        },
        # Add filter row
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id_num,
                        "startRowIndex": 0,
                        "startColumnIndex": 0
                    }
                }
            }
        }
    ]

    service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id, body={"requests": requests}
    ).execute()
    print(f"✨ Styled {sheet_name} sheet (filters enabled)")


def prepare_event_data(events):
    """Prepare event data."""
    headers = [
        "Event Name", "Event Date", "Event Type", "Description",
        "Venue Name", "Venue Address", "Venue City", "Venue State", "Venue Zip",
        "Venue Parking", "Venue Website",
        "Registration URL", "Sponsorship URL", "Sponsorship Tiers", "Sponsorship Contact",
        "Past Sponsors", "Dress Code", "Organizer ID"
    ]
    data = [headers]
    for e in events:
        data.append([
            e.get("name"), str(e.get("date")), e.get("event_type"), e.get("description"),
            e.get("venue_name"), e.get("venue_address"), e.get("venue_city"), e.get("venue_state"), e.get("venue_zip"),
            e.get("venue_parking"), e.get("venue_website"),
            e.get("registration_url"), e.get("sponsorship_url"), e.get("sponsorship_tiers"), e.get("sponsorship_contact"),
            e.get("past_sponsors"), e.get("dress_code"), e.get("organizer_id")
        ])
    return data


def prepare_organizer_data(orgs):
    """Prepare organizer data."""
    headers = [
        "Organizer Name", "EIN", "Website", "Email", "Phone",
        "Contact Name", "Contact Title", "Contact Email", "Facebook", "Instagram"
    ]
    data = [headers]
    for o in orgs:
        data.append([
            o.get("name"), o.get("ein"), o.get("website"), o.get("email"), o.get("phone"),
            o.get("contact_name"), o.get("contact_title"), o.get("contact_email"),
            o.get("facebook"), o.get("instagram")
        ])
    return data


def prepare_combined_data(events, orgs):
    """Prepare joined event+organizer data."""
    org_map = {o.get("id"): o for o in orgs}
    headers = [
        "Event Name", "Event Date", "Event Type", "Venue City", "Venue State",
        "Registration URL", "Sponsorship URL", "Dress Code",
        "Organizer Name", "EIN", "Organizer Email", "Organizer Phone", "Organizer Website"
    ]
    data = [headers]
    for e in events:
        org = org_map.get(e.get("organizer_id"), {})
        data.append([
            e.get("name"), str(e.get("date")), e.get("event_type"), e.get("venue_city"), e.get("venue_state"),
            e.get("registration_url"), e.get("sponsorship_url"), e.get("dress_code"),
            org.get("name"), org.get("ein"), org.get("email"), org.get("phone"), org.get("website")
        ])
    return data


def export_data():
    sheet_id = get_sheet_id()
    events = fetch_all_events()
    orgs = fetch_all_organizers()

    # Export + style Events
    export_to_google_sheets(prepare_event_data(events), "Events", sheet_id)
    style_sheet(sheet_id, "Events")

    # Export + style Organizers
    export_to_google_sheets(prepare_organizer_data(orgs), "Organizers", sheet_id)
    style_sheet(sheet_id, "Organizers")

    # Export + style Combined
    export_to_google_sheets(prepare_combined_data(events, orgs), "Events+Organizers", sheet_id)
    style_sheet(sheet_id, "Events+Organizers")

    # 🔥 Auto-open the Google Sheet in your browser
    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    print(f"🌍 Opening Google Sheet: {sheet_url}")
    webbrowser.open(sheet_url)


if __name__ == "__main__":
    export_data()
