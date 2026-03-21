import pandas as pd
import os
from postgrest import SyncPostgrestClient

# --- SETTINGS ---
URL = "https://your-project-id.supabase.co/rest/v1"
KEY = "your-anon-key"
TABLE_NAME = "mercari_items" # Supabaseのテーブル名
CSV_PATH = r"D:\mercari_github\scripts\your_data.csv" # 読み込むCSVのパス

def upload():
    if not os.path.exists(CSV_PATH):
        print(f"Error: CSV not found at {CSV_PATH}")
        return

    # Load data
    df = pd.read_csv(CSV_PATH)
    data = df.to_dict(orient='records')
    total = len(data)
    
    # Initialize client
    client = SyncPostgrestClient(URL, headers={
        "apikey": KEY,
        "Authorization": f"Bearer {KEY}"
    })

    # Batch upload (50 items each)
    batch_size = 50
    for i in range(0, total, batch_size):
        batch = data[i:i + batch_size]
        try:
            client.table(TABLE_NAME).insert(batch).execute()
            print(f"Progress: {min(i + batch_size, total)} / {total}")
        except Exception as e:
            print(f"Error at {i}: {e}")

if __name__ == "__main__":
    upload()