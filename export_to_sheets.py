import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

# Конфигурация Grafana
GRAFANA_URL = "https://grafana.payda.online"
DASHBOARD_UID = "cenbdzt50mps0a"
PANEL_ID = 1
GRAFANA_API_KEY = os.getenv("GRAFANA_API_KEY")

# Конфигурация Google Sheets
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "grafana_export")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")

def fetch_grafana_data():
    headers = {
        "Authorization": f"Bearer {GRAFANA_API_KEY}"
    }
    url = f"{GRAFANA_URL}/api/ds/query"
    payload = {
        "queries": [
            {
                "refId": "A",
                "datasource": {
                    "uid": "fdk6lqw39jgn4f",
                    "type": "grafana-postgresql-datasource"
                },
                "rawSql": """select bin_iin,
       full_name,
       phone_number,
       created
from users_client
where client_category_id is not null
and created >= '2025-05-01'
order by created asc""",
                "format": "table"
            }
        ],
        "from": "now-6h",
        "to": "now"
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def export_to_sheets(headers, rows):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open(GOOGLE_SHEET_NAME).sheet1
    sheet.clear()
    sheet.append_row(headers)
    sheet.append_rows(rows)

if __name__ == "__main__":
    data = fetch_grafana_data()
    table = data["results"]["A"]["frames"][0]
    fields = [field["name"] for field in table["schema"]["fields"]]
    values = list(zip(*[field["values"] for field in table["data"]["values"]]))
    export_to_sheets(fields, values)
    print(f"✅ Exported {len(values)} rows to Google Sheet '{GOOGLE_SHEET_NAME}'")
