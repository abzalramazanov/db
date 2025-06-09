import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta, timezone
import os
import json
import base64

# 🧩 Восстановление credentials.json из ENV
if not os.path.exists("credentials.json"):
    raw = os.getenv("CREDENTIALS_JSON")
    if raw:
        creds = json.loads(base64.b64decode(raw))
        with open("credentials.json", "w") as f:
            json.dump(creds, f)
        print("🔐 credentials.json восстановлен из переменной окружения.")
    else:
        raise ValueError("❌ CREDENTIALS_JSON не найден в окружении!")

# 🔧 Конфигурация
GRAFANA_URL = "https://grafana.payda.online/api/ds/query"
GRAFANA_API_KEY = os.getenv("GRAFANA_API_KEY")
DATASOURCE_UID = "ce37vo70kfcaob"
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "grafana_export")
META_SHEET_NAME = "Meta"

# 🔐 Авторизация
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(credentials)
doc = client.open(GOOGLE_SHEET_NAME)

# 🔹 Листы
try:
    main_sheet = doc.worksheet(GOOGLE_SHEET_NAME)
except gspread.exceptions.WorksheetNotFound:
    main_sheet = doc.add_worksheet(title=GOOGLE_SHEET_NAME, rows="100", cols="20")

try:
    meta_sheet = doc.worksheet(META_SHEET_NAME)
except gspread.exceptions.WorksheetNotFound:
    meta_sheet = doc.add_worksheet(title=META_SHEET_NAME, rows="10", cols="2")

# 📅 Очистка таблицы
main_sheet.clear()
print("🧼 Главный лист очищен")

# 📈 Запрос к Grafana
headers = {
    "Authorization": f"Bearer {GRAFANA_API_KEY}",
    "Content-Type": "application/json"
}

payload = {
    "queries": [
        {
            "refId": "A",
            "datasource": {
                "uid": DATASOURCE_UID,
                "type": "postgres"
            },
            "rawSql": """
                SELECT DISTINCT ON (sub.tin)
                       sub.name AS \"Имя\",
                       sub.phone AS \"Телефон\",
                       sub.tin AS \"ИНН\"
                FROM (
                  SELECT a.seller_name AS name,
                         a.seller_tin  AS tin,
                         a.seller_phone AS phone
                  FROM awps a
                  WHERE a.document_date >= '2025-05-01'
                    AND a.buyer_name = 'ТОО \"Яндекс.Такси Корп\"'

                  UNION ALL

                  SELECT a.buyer_name,
                         a.buyer_tin,
                         a.buyer_phone
                  FROM awps a
                  WHERE a.document_date >= '2025-05-01'
                    AND a.seller_name = 'ТОО \"Яндекс.Такси Корп\"'
                ) sub
            """,
            "format": "table"
        }
    ],
    "range": {
        "from": "now-30d",
        "to": "now"
    }
}

response = requests.post(GRAFANA_URL, headers=headers, json=payload)
response.raise_for_status()
data = response.json()

# 📄 Парсинг ответа
try:
    rows = data['results']['A']['frames'][0]['data']
    fields = rows['fields']
    headers_row = [field['name'] for field in fields]
    values = list(zip(*[f['values'] for f in fields]))
    table_data = [headers_row] + list(values)
except Exception as e:
    print(f"❌ Ошибка парсинга данных из Grafana: {e}")
    print(json.dumps(data, indent=2))
    raise

# ⬆️ Загрузка в таблицу
main_sheet.update("A1", table_data)
print(f"📄 Загружено строк: {len(table_data) - 1}")

# ⏰ Обновление мета-листа
now_kz = datetime.now(timezone.utc) + timedelta(hours=5)
now_str = now_kz.strftime("%Y-%m-%d %H:%M:%S")
meta_sheet.update("A1", [[now_str]])
print(f"🕒 Время обновления: {now_str}")
