import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime

# 🔧 Конфигурация Grafana
GRAFANA_URL = "https://grafana.payda.online"
GRAFANA_API_KEY = os.getenv("GRAFANA_API_KEY")
GRAFANA_DATASOURCE_UID = "fdk6lqw39jgn4f"

# 🔧 Конфигурация Google Sheets
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "grafana_export")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")

def fetch_grafana_data(from_ts):
    headers = {
        "Authorization": f"Bearer {GRAFANA_API_KEY}"
    }
    url = f"{GRAFANA_URL}/api/ds/query"
    raw_sql = f"""
        SELECT bin_iin,
               full_name,
               phone_number,
               created
        FROM users_client
        WHERE client_category_id IS NOT NULL
          AND created > '{from_ts}'
        ORDER BY created ASC
    """
    payload = {
        "queries": [
            {
                "refId": "A",
                "datasource": {
                    "uid": GRAFANA_DATASOURCE_UID,
                    "type": "grafana-postgresql-datasource"
                },
                "rawSql": raw_sql,
                "format": "table"
            }
        ],
        "from": "now-7d",
        "to": "now"
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def get_last_created(sheet):
    values = sheet.get_all_values()
    if len(values) <= 1:
        return "2025-05-01 00:00:00"
    last_row = values[-1]
    try:
        last_timestamp = int(last_row[-1])  # Последний столбец — created
        dt = datetime.fromtimestamp(last_timestamp / 1000)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return "2025-05-01 00:00:00"

def export_to_sheets(sheet, rows):
    sheet.append_rows(rows, value_input_option="RAW")

if __name__ == "__main__":
    # Авторизация в Google Sheets
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open(GOOGLE_SHEET_NAME).sheet1

    # Получаем последнюю дату
    from_ts = get_last_created(sheet)

    # Забираем данные из Grafana
    data = fetch_grafana_data(from_ts)
    table = data["results"]["A"]["frames"][0]
    raw_values = table["data"]["values"]
    rows = list(zip(*raw_values))  # Транспонирование
    rows_clean = [list(row) for row in rows]  # Без индексов и сдвигов

    # Экспорт в Google Sheets
    if rows_clean:
        export_to_sheets(sheet, rows_clean)
        print(f"✅ Exported {len(rows_clean)} rows to Google Sheet '{GOOGLE_SHEET_NAME}'")
    else:
        print("⚠️ No new rows to export.")
