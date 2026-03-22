import os
import csv
import time
from supabase import create_client
from dotenv import load_dotenv

# =====================
# ENV
# =====================
load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    print("ENV ERROR")
    exit()

supabase = create_client(url, key)

# =====================
# CSV LOAD
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
except Exception as e:
    print("CSV ERROR:", e)
    exit()

print("TOTAL:", len(all_data))
print("HEAD:", all_data[:3])

if len(all_data) == 0:
    print("NO DATA")
    exit()

# =====================
# 事前検証（1件）
# =====================
print("TEST START")

try:
    test = supabase.table("items").upsert(
        [all_data[0]],
        on_conflict="name"
    ).execute()

    print("TEST OK")

except Exception as e:
    print("TEST ERROR:", e)
    exit()

print("TEST PASSED")

# =====================
# 本番投入
# =====================
BATCH_SIZE = 500

for i in range(0, len(all_data), BATCH_SIZE):
    batch = all_data[i:i+BATCH_SIZE]

    retry = 0

    while retry < 3:
        try:
            supabase.table("items").upsert(
                batch,
                on_conflict="name"
            ).execute()

            print(f"{i} to {i+len(batch)} done")
            break

        except Exception as e:
            print("ERROR:", e)
            retry += 1
            print(f"RETRY {retry}/3")
            time.sleep(2)

    if retry == 3:
        print("FAILED BATCH:", i)
        exit()

print("=== FINISHED ===")