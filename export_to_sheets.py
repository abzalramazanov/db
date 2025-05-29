import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime, timezone

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
          AND created >= '{from_ts}'
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
        last_timestamp = int(last_row[3])  # index of `created`
        dt = datetime.fromtimestamp(last_timestamp / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return "2025-05-01 00:00:00"

def export_to_sheets(sheet, rows):
    sheet.append_rows(rows, value_input_option="RAW")

if __name__ == "__main__":
    # Авторизация Google Sheets
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open(GOOGLE_SHEET_NAME).sheet1

    # Проверка заголовков
    all_values = sheet.get_all_values()
    header_ok = False
    if all_values and isinstance(all_values[0], list):
        first_row = all_values[0]
        if first_row[:4] == ["bin_iin", "full_name", "phone_number", "created"]:
            header_ok = True

    if not header_ok:
        sheet.insert_row(["bin_iin", "full_name", "phone_number", "created", "created_local"], 1)
        all_values = sheet.get_all_values()

    # Последний created
    from_ts = get_last_created(sheet)

    # Получаем данные из Grafana
    data = fetch_grafana_data(from_ts)
    table = data["results"]["A"]["frames"][0]
    raw_values = table["data"]["values"]
    rows = list(zip(*raw_values))  # транспонируем

    # Уникализируем по bin_iin и добавляем created_local
    existing_bin_iins = set(row[0] for row in all_values[1:] if len(row) > 0)
    rows_clean = []

    for row in rows:
        bin_iin, full_name, phone_number, created = row
        if bin_iin in existing_bin_iins:
            continue
        try:
            created = int(created)
            dt = datetime.fromtimestamp(created / 1000, tz=timezone.utc)
            created_local = dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            created_local = ''
        rows_clean.append([bin_iin, full_name, phone_number, created, created_local])

    # Экспорт
    if rows_clean:
        export_to_sheets(sheet, rows_clean)
        print(f"✅ Exported {len(rows_clean)} new rows to Google Sheet '{GOOGLE_SHEET_NAME}'")
    else:
        print("⚠️ No new unique rows to export.")
