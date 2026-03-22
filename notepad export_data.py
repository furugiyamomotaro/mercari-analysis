import os
import csv
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    print("ENV ERROR")
    exit()

supabase = create_client(url, key)

OUTPUT_FILE = "export.csv"

all_data = []
page = 0
limit = 1000

while True:
    res = supabase.table("items").select("*").range(page*limit, (page+1)*limit-1).execute()
    data = res.data

    if not data:
        break

    all_data.extend(data)
    page += 1

print("取得件数:", len(all_data))

if len(all_data) == 0:
    print("NO DATA")
    exit()

keys = all_data[0].keys()

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=keys)
    writer.writeheader()
    writer.writerows(all_data)

print("CSV出力完了:", OUTPUT_FILE)