import time
import os
import sqlite3
import pandas as pd

# Folder to monitor for new files
WATCH_FOLDER = "watch_folder"
# SQLite database filename
DB_NAME = "db.sqlite3"

# Connect to SQLite database
def connect_db():
    return sqlite3.connect(DB_NAME)

# Create table based on DataFrame columns if it doesn't exist
def create_table_if_not_exists(cursor, table_name, df):
    columns = ', '.join([f'"{col}" TEXT' for col in df.columns])
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {columns}
        )
    ''')

# Insert all data from the DataFrame into the specified table
def insert_data(cursor, table_name, df):
    placeholders = ', '.join(['?'] * len(df.columns))
    columns = ', '.join(df.columns)
    cursor.executemany(
        f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})',
        df.values.tolist()
    )

# Process a single file and upload its data into the SQLite database
def process_file(file_path):
    try:
        print(f"📂 Processing: {file_path}")
        ext = os.path.splitext(file_path)[1]

        # Read file into DataFrame
        if ext.lower() == '.csv':
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='latin1')
        elif ext.lower() in ['.xls', '.xlsx']:
            df = pd.read_excel(file_path)
        else:
            print("❌ Unsupported file type. Skipping.")
            return

        table_name = os.path.splitext(os.path.basename(file_path))[0].replace(" ", "_")
        conn = connect_db()
        cursor = conn.cursor()

        # Ensure table exists
        create_table_if_not_exists(cursor, table_name, df)

        # 🔄 Clear previous data
        cursor.execute(f"DELETE FROM {table_name}")

        # Insert fresh data
        insert_data(cursor, table_name, df)

        conn.commit()
        conn.close()

        print(f"✅ Data from '{file_path}' uploaded successfully to table '{table_name}'.")

    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")

# Continuously watch the folder and process any new files
def watch_folder():
    processed_files = set()
    print("👀 Watching folder for new files...")

    while True:
        try:
            files = os.listdir(WATCH_FOLDER)
            for file in files:
                full_path = os.path.join(WATCH_FOLDER, file)
                if file not in processed_files and os.path.isfile(full_path):
                    process_file(full_path)
                    processed_files.add(file)
        except Exception as e:
            print(f"⚠️ Error watching folder: {e}")
        
        time.sleep(5)

# Entry point
if __name__ == "__main__":
    if not os.path.exists(WATCH_FOLDER):
        os.makedirs(WATCH_FOLDER)
    watch_folder()
