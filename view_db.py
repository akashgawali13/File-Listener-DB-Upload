import sqlite3
import pandas as pd

# Connect to your database
conn = sqlite3.connect(r"E:\Project\File listner & Database upload\db.sqlite3")

# List all table names
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
print("📄 Tables:", tables)

# View the content of one table (replace with actual table name if needed)
df = pd.read_sql_query("SELECT * FROM customer_data", conn)
print(df.head())

conn.close()
