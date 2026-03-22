import os, sqlite3, time
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DB_PATH      = r"D:\mercari_data\mercari_full.db"
BATCH_SIZE   = 500

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

con = sqlite3.connect(DB_PATH)
con.row_factory = sqlite3.Row
cur = con.cursor()

cur.execute("""
    SELECT brand, lv1, lv2, lv3, lv4, lv5, lv6,
           price, created_dt, updated_dt
    FROM sold_items
    WHERE price > 0
""")
rows = cur.fetchall()
con.close()

all_data = []
for r in rows:
    all_data.append({
        "brand":      (r["brand"]      or "").strip(),
        "lv1":        (r["lv1"]        or "").strip(),
        "lv2":        (r["lv2"]        or "").strip(),
        "lv3":        (r["lv3"]        or "").strip(),
        "lv4":        (r["lv4"]        or "").strip(),
        "lv5":        (r["lv5"]        or "").strip(),
        "lv6":        (r["lv6"]        or "").strip(),
        "price":      int(r["price"] or 0),
        "created_dt": str(r["created_dt"] or ""),
        "updated_dt": str(r["updated_dt"] or ""),
    })

print(f"総件数: {len(all_data):,}")

supabase.table("items").delete().neq("id", 0).execute()
print("既存データ削除完了")

for i in range(0, len(all_data), BATCH_SIZE):
    batch = all_data[i:i+BATCH_SIZE]
    try:
        supabase.table("items").insert(batch).execute()
        print(f"  {i+len(batch):,} / {len(all_data):,} 件完了")
    except Exception as e:
        print(f"  エラー: {e}")
        time.sleep(2)

print("完了")
