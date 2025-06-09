import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
import json
import base64

# 🧩 Восстановление credentials.json из ENV (если отсутствует)
if not os.path.exists("credentials.json"):
    raw = os.getenv("CREDENTIALS_JSON")
    if raw:
        creds = json.loads(base64.b64decode(raw))
        with open("credentials.json", "w") as f:
            json.dump(creds, f)
    else:
        raise ValueError("❌ CREDENTIALS_JSON not найден в окружении!")

# 🔧 Конфигурация Grafana
GRAFANA_URL = "https://grafana.payda.online/api/ds/query"
GRAFANA_API_KEY = os.getenv("GRAFANA_API_KEY")
DATASOURCE_UID = "fdk6lqw39jgn4f"

# 🔧 Конфигурация Google Sheets
GOOGLE_SHEET_ID = "1JeYJqv5q_S3CfC855Tl5xjP7nD5Fkw9jQXrVyvEXK1Y"
SHEET_NAME = "uniqe drivers main"
CREDENTIALS_FILE = "credentials.json"

# 📡 Авторизация в Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
client = gspread.authorize(credentials)
sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_NAME)

# 🧹 Очистка таблицы
sheet.clear()

# 📡 Получение данных с Grafana
headers = {
    "Authorization": f"Bearer {GRAFANA_API_KEY}",
    "Content-Type": "application/json"
}

# ⚠️ ВАЖНО: Замени rawSql на твой актуальный запрос (если не работает)
payload = {
    "queries": [
        {
            "refId": "A",
            "datasource": {
                "uid": DATASOURCE_UID,
                "type": "postgres"
            },
            "rawSql": "SELECT name, tin, phone, park_full_name, COALESCE(avr_status, 'n/a') as avr_status, COALESCE(esf_status, 'n/a') as esf_status FROM drivers_data WHERE document_date >= CURRENT_DATE - INTERVAL '30 days'",
            "format": "table"
        }
    ],
    "range": {
        "from": "now-30d",
        "to": "now"
    }
}

response = requests.post(GRAFANA_URL, headers=headers, json=payload)
data = response.json()

# 📤 Парсинг и загрузка в таблицу
rows = data['results']['A']['frames'][0]['data']
fields = rows['fields']
headers_row = [field['name'] for field in fields]
sheet.append_row(headers_row)

values = list(zip(*[f['values'] for f in fields]))
for row in values:
    sheet.append_row(list(row))

print(f"[{datetime.now()}] ✅ Данные успешно загружены в Google Таблицу.")
