import os
import csv
import time
from supabase import create_client
from dotenv import load_dotenv

# =====================
# ENV読み込み
# =====================
load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    print("ENV ERROR")
    exit()

supabase = create_client(url, key)

# =====================
# CSV読み込み
# =====================
FILE_NAME = "data.csv"

all_data = []

try:
    with open(FILE_NAME, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                row["price"] = int(row["price"])
                all_data.append(row)
            except:
                print("SKIP:", row)
except:
    print("CSV ERROR")
    exit()

print("TOTAL:", len(all_data))
print("HEAD:", all_data[:3])

if len(all_data) == 0:
    print("NO DATA")
    exit()

# =====================
# Supabase投入（本番）
# =====================
BATCH_SIZE = 500

for i in range(0, len(all_data), BATCH_SIZE):
    batch = all_data[i:i+BATCH_SIZE]

    try:
        supabase.table("items").upsert(
            batch,
            on_conflict="name"
        ).execute()

        print(f"{i} to {i+len(batch)} done")

    except Exception as e:
        print("ERROR:", e)
        time.sleep(2)

print("=== FINISHED ===")