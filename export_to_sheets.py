import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

# 🔐 Данные подключения к БД (из переменных окружения для безопасности)
db_config = {
    'host': os.getenv('DB_HOST', '172.16.88.11'),
    'port': os.getenv('DB_PORT', 5432),
    'database': os.getenv('DB_NAME', 'production_payda'),
    'user': os.getenv('DB_USER', 'tech_read_user'),
    'password': os.getenv('DB_PASSWORD', 'AqIXe52814BvKIWIBbpM')
}

# 🔐 Название Google Таблицы и путь к credentials
GOOGLE_SHEET_NAME = os.getenv('GOOGLE_SHEET_NAME', 'grafana_export')
CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE', 'credentials.json')

# SQL-запрос для экспорта
SQL_QUERY = """
SELECT phone_number, bin_iin FROM users_client WHERE client_category_id IS NOT NULL LIMIT 100;
"""

def fetch_data():
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute(SQL_QUERY)
    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return headers, rows

def export_to_sheets(headers, rows):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)

    sheet = client.open(GOOGLE_SHEET_NAME).sheet1
    sheet.clear()
    sheet.append_row(headers)
    sheet.append_rows(rows)

if __name__ == '__main__':
    headers, rows = fetch_data()
    export_to_sheets(headers, rows)
    print(f"✅ Exported {len(rows)} rows to Google Sheet '{GOOGLE_SHEET_NAME}'")
