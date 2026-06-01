import sqlite3
conn = sqlite3.connect('/usr/local/airflow/market_data.db')
cursor = conn.cursor()
cursor.execute("SELECT * FROM market_data")
rows = cursor.fetchall()

for row in rows:
    print(row)
conn.close()
