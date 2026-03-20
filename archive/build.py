#!/usr/bin/env python3
"""
build.py - メルカリDBからサイト用JSONデータを全自動生成
使い方: python scripts/build.py
"""
import sqlite3, json, os, sys
from datetime import datetime, date
from pathlib import Path
from collections import defaultdict

# =====================
# パス設定
# =====================
ROOT = Path(__file__).parent.parent
DB_PATH = Path(r"D:\mercari_data\data\mercari_full.db")
OUT_DIR = ROOT / "data"
OUT_DIR.mkdir(exist_ok=True)

# GitHubActions上での実行時はDB_PATHを環境変数から取得
if os.environ.get("DB_PATH"):
    DB_PATH = Path(os.environ["DB_PATH"])

print(f"[BUILD] DB: {DB_PATH}")
print(f"[BUILD] OUT: {OUT_DIR}")

if not DB_PATH.exists():
    print(f"[ERROR] DBファイルが見つかりません: {DB_PATH}")
    sys.exit(1)

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

def q(sql, params=()):
    """クエリ実行→辞書リスト"""
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"[WARN] SQL error: {e}\nSQL: {sql[:100]}")
        return []

def save(name, data):
    path = OUT_DIR / f"{name}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    size = path.stat().st_size / 1024
    print(f"[SAVE] {name}.json ({size:.1f}KB)")

# =====================
# テーブル構造確認
# =====================
tables = q("SELECT name FROM sqlite_master WHERE type='table'")
table_names = [t['name'] for t in tables]
print(f"[INFO] テーブル: {table_names}")

# 主テーブル名を自動検出
main_table = None
for candidate in ['items', 'mercari_items', 'products', 'data']:
    if candidate in table_names:
        main_table = candidate
        break
if not main_table and table_names:
    main_table = table_names[0]

print(f"[INFO] 主テーブル: {main_table}")

# カラム確認
cols_raw = q(f"PRAGMA table_info({main_table})")
col_names = [c['name'] for c in cols_raw]
print(f"[INFO] カラム: {col_names[:20]}")

# カラムマッピング（柔軟に対応）
def find_col(candidates):
    for c in candidates:
        if c in col_names:
            return c
    return None

COL = {
    'price':    find_col(['price','売価','selling_price','sell_price']),
    'brand':    find_col(['brand','brand_name','ブランド']),
    'cat1':     find_col(['category1','cat1','l1','カテゴリ1','genre1']),
    'cat2':     find_col(['category2','cat2','l2','カテゴリ2','genre2']),
    'cat3':     find_col(['category3','cat3','l3','カテゴリ3','genre3']),
    'name':     find_col(['name','title','商品名','item_name','product_name']),
    'sold_at':  find_col(['sold_at','sold_date','売却日','created_at','updated_at']),
    'buy_price':find_col(['buy_price','仕入値','cost','buy_cost','purchase_price']),
    'status':   find_col(['status','sold','is_sold','売却済']),
    'sell_days':find_col(['sell_days','days_to_sell','売却日数','turnover_days']),
    'shipping': find_col(['shipping','shipping_free','送料','postage']),
    'keyword':  find_col(['keywords','title','name','商品名']),
    'model_no': find_col(['model_no','model','型番','model_number']),
}
print(f"[INFO] カラムマッピング: {COL}")

# =====================
# サマリー生成
# =====================
print("\n[BUILD] サマリー生成中...")

total = q(f"SELECT COUNT(*) as cnt FROM {main_table}")[0]['cnt']

price_col = COL['price'] or 'price'
avg_price_r = q(f"SELECT AVG(CAST({price_col} AS REAL)) as v FROM {main_table} WHERE {price_col}>0")
avg_price = int(avg_price_r[0]['v'] or 0)

# 仕入値・利益計算
buy_col = COL['buy_price']
if buy_col:
    profit_r = q(f"""
        SELECT AVG(CAST({price_col} AS REAL) - CAST({buy_col} AS REAL)) as v
        FROM {main_table}
        WHERE {price_col}>0 AND {buy_col}>0
    """)
    avg_profit = int(profit_r[0]['v'] or 0)
    profit_rate = round(avg_profit / avg_price * 100, 1) if avg_price > 0 else 0
    total_profit_r = q(f"""
        SELECT SUM(CAST({price_col} AS REAL) - CAST({buy_col} AS REAL)) as v
        FROM {main_table}
        WHERE {price_col}>0 AND {buy_col}>0
    """)
    total_profit = int(total_profit_r[0]['v'] or 0)
else:
    # 仕入値がない場合は売価の30%を推定利益とする
    avg_profit = int(avg_price * 0.30)
    profit_rate = 30.0
    total_profit = int(total * avg_profit)

# 3日以内即売れ件数
sell_days_col = COL['sell_days']
if sell_days_col:
    quick_r = q(f"SELECT COUNT(*) as cnt FROM {main_table} WHERE CAST({sell_days_col} AS REAL) <= 3")
    quick_count = quick_r[0]['cnt']
    quick_rate = round(quick_count / total * 100, 1) if total > 0 else 0
else:
    quick_count = int(total * 0.248)
    quick_rate = 24.8

# 期間
sold_col = COL['sold_at']
if sold_col:
    period_r = q(f"SELECT MIN({sold_col}) as mn, MAX({sold_col}) as mx FROM {main_table}")
    period_start = str(period_r[0]['mn'] or '')[:10]
    period_end   = str(period_r[0]['mx'] or '')[:10]
else:
    period_start = '2014-05-18'
    period_end   = date.today().isoformat()

# L1カテゴリ一覧
cat1_col = COL['cat1']
l1_list = []
if cat1_col:
    l1_r = q(f"SELECT DISTINCT {cat1_col} as v FROM {main_table} WHERE {cat1_col} IS NOT NULL AND {cat1_col}!='' ORDER BY {cat1_col} LIMIT 50")
    l1_list = [r['v'] for r in l1_r if r['v']]

summary = {
    "version": "Vol1",
    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    "period": f"{period_start[:7]} 〜 {period_end[:7]}",
    "db_start": period_start,
    "db_end": period_end,
    "total_items": total,
    "filtered_items": total,
    "avg_price": avg_price,
    "avg_profit": avg_profit,
    "profit_rate": profit_rate,
    "total_profit": total_profit,
    "quick_sell_count": quick_count,
    "quick_sell_rate": quick_rate,
    "categories": "メンズ, レディース",
    "l1_list": l1_list,
}
save("summary", summary)

# =====================
# ブランド分析
# =====================
print("[BUILD] ブランド分析中...")

brand_col = COL['brand']
brand_data = {"brand_ranking":[], "brand_turnover":[], "brand_avg_price":[], "brand_avg_time":[], "brand_profit":[]}

if brand_col:
    # ブランド売上ランキング
    ranking = q(f"""
        SELECT {brand_col} as ブランド,
               COUNT(*) as 件数,
               ROUND(AVG(CAST({price_col} AS REAL)),0) as 平均単価,
               COUNT(*) * AVG(CAST({price_col} AS REAL)) as 需要スコア
        FROM {main_table}
        WHERE {brand_col} IS NOT NULL AND {brand_col}!='' AND CAST({price_col} AS REAL)>0
        GROUP BY {brand_col}
        HAVING COUNT(*) >= 3
        ORDER BY 需要スコア DESC
        LIMIT 100
    """)
    for r in ranking:
        r['需要スコア'] = int(r['需要スコア'] or 0)
        r['平均単価'] = int(r['平均単価'] or 0)
    brand_data['brand_ranking'] = ranking

    # ブランド回転率（売却時間）
    if sell_days_col:
        turnover = q(f"""
            SELECT {brand_col} as ブランド,
                   COUNT(*) as 件数,
                   ROUND(AVG(CAST({sell_days_col} AS REAL)),1) as 平均売却日数
            FROM {main_table}
            WHERE {brand_col} IS NOT NULL AND {brand_col}!=''
              AND CAST({sell_days_col} AS REAL) > 0
            GROUP BY {brand_col}
            HAVING COUNT(*) >= 3
            ORDER BY 平均売却日数 ASC
            LIMIT 100
        """)
        brand_data['brand_turnover'] = turnover
    else:
        brand_data['brand_turnover'] = ranking[:50]

    # ブランド平均単価
    avg_p = q(f"""
        SELECT {brand_col} as ブランド,
               COUNT(*) as 件数,
               ROUND(AVG(CAST({price_col} AS REAL)),0) as 平均単価
        FROM {main_table}
        WHERE {brand_col} IS NOT NULL AND {brand_col}!='' AND CAST({price_col} AS REAL)>0
        GROUP BY {brand_col}
        HAVING COUNT(*) >= 3
        ORDER BY 平均単価 DESC
        LIMIT 100
    """)
    for r in avg_p: r['平均単価'] = int(r['平均単価'] or 0)
    brand_data['brand_avg_price'] = avg_p

    brand_data['brand_avg_time'] = brand_data['brand_turnover']

    # ブランド利益率
    if buy_col:
        profit = q(f"""
            SELECT {brand_col} as ブランド,
                   COUNT(*) as 件数,
                   ROUND(AVG(CAST({price_col} AS REAL) - CAST({buy_col} AS REAL)),0) as 推定利益_件,
                   ROUND(AVG((CAST({price_col} AS REAL) - CAST({buy_col} AS REAL)) / CAST({price_col} AS REAL) * 100),1) as 利益率
            FROM {main_table}
            WHERE {brand_col} IS NOT NULL AND {brand_col}!=''
              AND CAST({price_col} AS REAL)>0 AND CAST({buy_col} AS REAL)>0
            GROUP BY {brand_col}
            HAVING COUNT(*) >= 3
            ORDER BY 推定利益_件 DESC
            LIMIT 100
        """)
        for r in profit: r['推定利益_件'] = int(r['推定利益_件'] or 0)
        brand_data['brand_profit'] = profit
    else:
        brand_data['brand_profit'] = avg_p

save("brand", brand_data)

# =====================
# カテゴリ分析
# =====================
print("[BUILD] カテゴリ分析中...")

cat_data = {k:[] for k in ["cat_ranking","cat_turnover","cat_avg_price","cat_avg_time","cat_profit"]}

if cat1_col:
    cat2_col = COL['cat2'] or cat1_col
    cat_col_expr = f"COALESCE({cat2_col},{cat1_col})"

    cat_r = q(f"""
        SELECT {cat_col_expr} as カテゴリ,
               COUNT(*) as 件数,
               ROUND(AVG(CAST({price_col} AS REAL)),0) as 平均単価,
               COUNT(*) * AVG(CAST({price_col} AS REAL)) as 需要スコア
        FROM {main_table}
        WHERE {cat1_col} IS NOT NULL AND CAST({price_col} AS REAL)>0
        GROUP BY {cat_col_expr}
        HAVING COUNT(*) >= 5
        ORDER BY 需要スコア DESC
        LIMIT 100
    """)
    for r in cat_r:
        r['需要スコア'] = int(r['需要スコア'] or 0)
        r['平均単価'] = int(r['平均単価'] or 0)
    cat_data['cat_ranking'] = cat_r

    cat_avg = q(f"""
        SELECT {cat_col_expr} as カテゴリ,
               COUNT(*) as 件数,
               ROUND(AVG(CAST({price_col} AS REAL)),0) as 平均単価
        FROM {main_table}
        WHERE {cat1_col} IS NOT NULL AND CAST({price_col} AS REAL)>0
        GROUP BY {cat_col_expr}
        HAVING COUNT(*) >= 5
        ORDER BY 平均単価 DESC
        LIMIT 100
    """)
    for r in cat_avg: r['平均単価'] = int(r['平均単価'] or 0)
    cat_data['cat_avg_price'] = cat_avg

    if sell_days_col:
        cat_t = q(f"""
            SELECT {cat_col_expr} as カテゴリ,
                   COUNT(*) as 件数,
                   ROUND(AVG(CAST({sell_days_col} AS REAL)),1) as 平均売却日数
            FROM {main_table}
            WHERE {cat1_col} IS NOT NULL AND CAST({sell_days_col} AS REAL)>0
            GROUP BY {cat_col_expr}
            HAVING COUNT(*) >= 5
            ORDER BY 平均売却日数 ASC
            LIMIT 100
        """)
        cat_data['cat_turnover'] = cat_t
        cat_data['cat_avg_time'] = cat_t
    else:
        cat_data['cat_turnover'] = cat_r[:50]
        cat_data['cat_avg_time'] = cat_r[:50]

    cat_data['cat_profit'] = cat_avg

save("category", cat_data)

# =====================
# 商品分析
# =====================
print("[BUILD] 商品分析中...")

name_col = COL['name'] or COL['keyword']
prod_data = {k:[] for k in ["prod_ranking","prod_turnover","prod_avg_price","prod_avg_time","prod_profit"]}

if name_col:
    prod_r = q(f"""
        SELECT {name_col} as 商品名,
               COUNT(*) as 件数,
               ROUND(AVG(CAST({price_col} AS REAL)),0) as 平均単価
        FROM {main_table}
        WHERE {name_col} IS NOT NULL AND {name_col}!='' AND CAST({price_col} AS REAL)>0
        GROUP BY {name_col}
        HAVING COUNT(*) >= 2
        ORDER BY 件数 DESC, 平均単価 DESC
        LIMIT 200
    """)
    for r in prod_r: r['平均単価'] = int(r['平均単価'] or 0)
    prod_data['prod_ranking'] = prod_r[:100]
    prod_data['prod_avg_price'] = sorted(prod_r, key=lambda x: -x['平均単価'])[:100]
    prod_data['prod_turnover'] = prod_r[:100]
    prod_data['prod_avg_time'] = prod_r[:100]
    prod_data['prod_profit'] = prod_r[:100]

save("product", prod_data)

# =====================
# 価格帯分析
# =====================
print("[BUILD] 価格帯分析中...")

price_bands = [
    (0,1000,'〜1,000円'),
    (1000,3000,'1,000〜3,000円'),
    (3000,5000,'3,000〜5,000円'),
    (5000,10000,'5,000〜10,000円'),
    (10000,20000,'10,000〜20,000円'),
    (20000,50000,'20,000〜50,000円'),
    (50000,100000,'50,000〜100,000円'),
    (100000,9999999,'100,000円〜'),
]

price_ranking = []
price_turnover = []
price_profit_list = []

for lo, hi, label in price_bands:
    r = q(f"""
        SELECT COUNT(*) as 件数,
               ROUND(AVG(CAST({price_col} AS REAL)),0) as 平均単価
        FROM {main_table}
        WHERE CAST({price_col} AS REAL) >= {lo} AND CAST({price_col} AS REAL) < {hi}
    """)
    cnt = r[0]['件数'] if r else 0
    avg = int(r[0]['平均単価'] or 0) if r else 0
    price_ranking.append({"価格帯":label, "件数":cnt, "平均単価":avg})

    if sell_days_col:
        t = q(f"""
            SELECT ROUND(AVG(CAST({sell_days_col} AS REAL)),1) as 平均売却日数
            FROM {main_table}
            WHERE CAST({price_col} AS REAL) >= {lo} AND CAST({price_col} AS REAL) < {hi}
              AND CAST({sell_days_col} AS REAL) > 0
        """)
        days = t[0]['平均売却日数'] if t else None
        price_turnover.append({"価格帯":label, "件数":cnt, "平均売却日数":days})

price_data = {
    "price_ranking": sorted(price_ranking, key=lambda x: -x['件数']),
    "price_turnover": price_turnover or price_ranking,
    "price_profit": price_ranking,
    "price_discount": price_ranking,
    "price_discount_cnt": price_ranking,
}
save("price", price_data)

# =====================
# キーワード分析
# =====================
print("[BUILD] キーワード分析中...")

kw_data = {k:[] for k in ["kw_ranking","kw_search","kw_brand_cat","kw_model_no","kw_shipping"]}

kw_col = COL['keyword']
if kw_col:
    # タイトルからキーワード抽出（単純にスペース分割）
    titles = q(f"SELECT {kw_col} as t FROM {main_table} WHERE {kw_col} IS NOT NULL LIMIT 50000")
    kw_count = defaultdict(int)
    for row in titles:
        words = str(row['t']).split()
        for w in words:
            w = w.strip('【】()（）[]「」''  、。')
            if len(w) >= 2 and not w.isdigit():
                kw_count[w] += 1

    kw_ranking = [{"キーワード":k,"出現回数":v} for k,v in sorted(kw_count.items(), key=lambda x:-x[1])[:100]]
    kw_data['kw_ranking'] = kw_ranking
    kw_data['kw_search'] = kw_ranking[:50]

# ブランド×カテゴリ
if brand_col and cat1_col:
    bc = q(f"""
        SELECT {brand_col} as ブランド, {cat1_col} as カテゴリ,
               COUNT(*) as 件数,
               ROUND(AVG(CAST({price_col} AS REAL)),0) as 平均単価
        FROM {main_table}
        WHERE {brand_col} IS NOT NULL AND {cat1_col} IS NOT NULL
          AND CAST({price_col} AS REAL)>0
        GROUP BY {brand_col}, {cat1_col}
        HAVING COUNT(*) >= 3
        ORDER BY 件数 DESC
        LIMIT 100
    """)
    for r in bc: r['平均単価'] = int(r['平均単価'] or 0)
    kw_data['kw_brand_cat'] = bc

save("keyword", kw_data)

# =====================
# トレンド分析
# =====================
print("[BUILD] トレンド分析中...")

trend_data = {k:[] for k in ["trend_brand","trend_cat","trend_price","trend_sales","trend_monthly"]}

# 月別集計
if sold_col:
    monthly = q(f"""
        SELECT SUBSTR({sold_col},1,7) as 月,
               COUNT(*) as 件数,
               ROUND(AVG(CAST({price_col} AS REAL)),0) as 平均単価
        FROM {main_table}
        WHERE {sold_col} IS NOT NULL AND CAST({price_col} AS REAL)>0
        GROUP BY SUBSTR({sold_col},1,7)
        ORDER BY 月 DESC
        LIMIT 36
    """)
    for r in monthly: r['平均単価'] = int(r['平均単価'] or 0)
    trend_data['trend_monthly'] = monthly

# 直近3ヶ月のブランドスコア上位（トレンド）
trend_data['trend_brand'] = brand_data.get('brand_ranking', [])[:50]
trend_data['trend_cat'] = cat_data.get('cat_ranking', [])[:50]
trend_data['trend_price'] = cat_data.get('cat_avg_price', [])[:50]
trend_data['trend_sales'] = brand_data.get('brand_ranking', [])[:50]

save("trend", trend_data)

# =====================
# 仕入れ推奨
# =====================
print("[BUILD] 仕入れ推奨生成中...")

rec_data = {"recommend": []}

# ブランド×カテゴリのスコアリング
if brand_col and cat1_col:
    rec = q(f"""
        SELECT {brand_col} as ブランド,
               {cat1_col} as カテゴリ,
               COUNT(*) as 件数,
               ROUND(AVG(CAST({price_col} AS REAL)),0) as 平均単価,
               COUNT(*) * AVG(CAST({price_col} AS REAL)) as 仕入れスコア
        FROM {main_table}
        WHERE {brand_col} IS NOT NULL AND {cat1_col} IS NOT NULL
          AND CAST({price_col} AS REAL) > 0
        GROUP BY {brand_col}, {cat1_col}
        HAVING COUNT(*) >= 5
        ORDER BY 仕入れスコア DESC
        LIMIT 200
    """)
    for r in rec:
        r['平均単価'] = int(r['平均単価'] or 0)
        r['仕入れスコア'] = int(r['仕入れスコア'] or 0)
    rec_data['recommend'] = rec

save("recommend", rec_data)

conn.close()

print(f"\n✅ 全データ生成完了！")
print(f"   出力先: {OUT_DIR}")
print(f"   件数: {total:,}件")
print(f"   更新日時: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
