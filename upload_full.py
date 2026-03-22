import os
import csv
import time
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

supabase = create_client(url, key)

FILE_NAME = "data.csv"

all_data = []

with open(FILE_NAME, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        row["price"] = int(row["price"])
        all_data.append(row)

print("総データ数:", len(all_data))
print("先頭データ:", all_data[:3])

BATCH_SIZE = 500

for i in range(0, len(all_data), BATCH_SIZE):
    batch = all_data[i:i+BATCH_SIZE]

    try:
        supabase.table("items").upsert(
            batch,
            on_conflict="name"
        ).execute()

        print(f"{i}〜{i+len(batch)} 件 完了")

    except Exception as e:
        print("エラー:", e)
        time.sleep(2)

print("=== 完全終了 ===")