import os
import csv
import time
from supabase import create_client
from dotenv import load_dotenv

# 環境変数読み込み
load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

supabase = create_client(url, key)

# CSV読み込み
all_data = []
with open("data.csv", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        row["price"] = int(row["price"])  # 型変換
        all_data.append(row)

print(f"総データ数: {len(all_data)}")

# バッチ設定
BATCH_SIZE = 500

# 分割してアップロード
for i in range(0, len(all_data), BATCH_SIZE):
    batch = all_data[i:i+BATCH_SIZE]

    try:
        supabase.table("items").upsert(batch).execute()
        print(f"{i}〜{i+len(batch)} 件 完了")
    except Exception as e:
        print(f"エラー発生: {e}")
        print("2秒待機してリトライ")
        time.sleep(2)