import os
import csv
import time
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    print("ENV ERROR")
    exit()

supabase = create_client(url, key)

FILE_NAME = "data.csv"

all_data = []
skip_count = 0

with open(FILE_NAME, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            if not row.get("price"):
                raise ValueError("empty")

            row["price"] = int(row["price"])
            all_data.append(row)

        except Exception as e:
            print("SKIP:", row)
            skip_count += 1

print("総データ数:", len(all_data))
print("スキップ数:", skip_count)
print("先頭データ:", all_data[:3])

if len(all_data) == 0:
    print("NO DATA")
    exit()

print("TEST START")

try:
    supabase.table("items").upsert(
        [all_data[0]],
        on_conflict="name"
    ).execute()

    print("TEST OK")

except Exception as e:
    print("TEST ERROR:", e)
    exit()

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
        print("ERROR:", e)
        time.sleep(2)

print("=== 完全終了 ===")