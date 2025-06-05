import psycopg2
from statcan.tools.config import DB_CONFIG

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS cube.test_table (id INT);")
conn.commit()
cur.close()
conn.close()

