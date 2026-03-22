#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FULL_AUTO_LOCAL.py
==================
ローカル環境用 完全自動パイプライン
  Step1: mercari_full.db を lv2/lv3 別に分割DB生成
  Step2: 各分割DBを集計してJSON出力
  Step3: 全カテゴリ統合ダッシュボードHTML生成

【実行方法】
  I:\mercari_data> python FULL_AUTO_LOCAL.py
  I:\mercari_data> python FULL_AUTO_LOCAL.py --skip-split   # DB分割済みの場合
  I:\mercari_data> python FULL_AUTO_LOCAL.py --only-html    # JSON集計済みの場合

【出力先】
  I:\mercari_data\output\mercari_dashboard.html
"""

import os, sys, sqlite3, json, time, argparse
from datetime import datetime
from collections import Counter

# ===================== パス設定 =====================
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH  = os.path.join(SCRIPT_DIR, "config", "config.json")

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = json.load(f)

SOURCE_DB    = cfg["paths"]["source_db"]
SPLIT_DB_DIR = cfg["paths"]["split_db_dir"]
OUTPUT_DIR   = cfg["paths"]["output_dir"]
SHIPPING_MAP = cfg["shipping_map"]
DATABASES    = cfg["databases"]

os.makedirs(SPLIT_DB_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR,   exist_ok=True)

# ====================================================
# STEP 1: DB分割
# ====================================================
def step_split():
    print("\n" + "="*60)
    print("  STEP 1: DB分割")
    print("="*60)

    if not os.path.exists(SOURCE_DB):
        print(f"❌ 元DBが見つかりません: {SOURCE_DB}")
        sys.exit(1)

    src_con = sqlite3.connect(SOURCE_DB)
    src_con.row_factory = sqlite3.Row
    src_cur = src_con.cursor()

    src_cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='sold_items'")
    create_sql = src_cur.fetchone()[0]

    src_cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='brands'")
    brands_row = src_cur.fetchone()
    brands_sql = brands_row[0] if brands_row else None

    # 実際のlv3名を確認（スペース差異対策）
    src_cur.execute("SELECT DISTINCT lv2, lv3 FROM sold_items WHERE price > 0")
    actual_cats = {(r[0], r[1]) for r in src_cur.fetchall()}

    results = []
    for db_def in DATABASES:
        lv2      = db_def["lv2"]
        lv3      = db_def["lv3"]
        filename = db_def["file"]
        out_path = os.path.join(SPLIT_DB_DIR, filename)

        # lv3名のスペース差異を自動補正
        matched_lv3 = lv3
        if (lv2, lv3) not in actual_cats:
            # スペースを除去して再マッチ
            for (al2, al3) in actual_cats:
                if al2 == lv2 and al3.replace(" ", "") == lv3.replace(" ", ""):
                    matched_lv3 = al3
                    print(f"  ⚠️  lv3名補正: '{lv3}' → '{matched_lv3}'")
                    break

        t0 = time.time()
        print(f"\n  [{lv2} > {matched_lv3}] → {filename}")

        if os.path.exists(out_path):
            os.remove(out_path)

        dst_con = sqlite3.connect(out_path)
        dst_cur = dst_con.cursor()
        dst_cur.execute(create_sql)
        if brands_sql:
            dst_cur.execute(brands_sql)

        src_cur.execute("""
            SELECT * FROM sold_items
            WHERE lv2 = ? AND lv3 = ? AND price > 0
        """, (lv2, matched_lv3))
        rows = src_cur.fetchall()
        cnt  = len(rows)

        if cnt > 0:
            cols = [d[0] for d in src_cur.description]
            ph   = ",".join(["?"] * len(cols))
            dst_cur.executemany(
                f"INSERT INTO sold_items VALUES ({ph})",
                [tuple(r) for r in rows]
            )

        if brands_sql:
            src_cur.execute("SELECT * FROM brands")
            brand_rows = src_cur.fetchall()
            if brand_rows:
                bh = ",".join(["?"] * len([d[0] for d in src_cur.description]))
                dst_cur.executemany(f"INSERT INTO brands VALUES ({bh})",
                                    [tuple(r) for r in brand_rows])

        dst_cur.execute("CREATE INDEX IF NOT EXISTS idx_brand   ON sold_items(brand)")
        dst_cur.execute("CREATE INDEX IF NOT EXISTS idx_lv2_lv3 ON sold_items(lv2,lv3)")
        dst_cur.execute("CREATE INDEX IF NOT EXISTS idx_updated ON sold_items(updated_dt)")
        dst_con.commit()
        dst_con.close()

        elapsed = time.time() - t0
        size_mb = os.path.getsize(out_path) / 1024 / 1024
        print(f"    → {cnt:,}件 / {size_mb:.1f}MB / {elapsed:.1f}秒")
        results.append({"file": filename, "lv2": lv2, "lv3": matched_lv3, "count": cnt})

        # db_defのlv3を補正済みで更新（後続ステップ用）
        db_def["_lv3_actual"] = matched_lv3

    src_con.close()
    total = sum(r["count"] for r in results)
    print(f"\n✅ DB分割完了 ({total:,}件)")
    return results

# ====================================================
# STEP 2: 集計 → JSON
# ====================================================
def parse_date(s):
    if not s: return None
    try: return datetime.fromtimestamp(float(s))
    except: pass
    try: return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")
    except: pass
    try: return datetime.strptime(str(s)[:10], "%Y-%m-%d")
    except: return None

def get_shipping(lv3):
    for k, v in SHIPPING_MAP.items():
        if k in (lv3 or ""):
            return v
    return 700

def aggregate_db(db_path, db_def):
    lv2 = db_def["lv2"]
    lv3 = db_def.get("_lv3_actual", db_def["lv3"])

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) FROM sold_items")
    db_total = cur.fetchone()[0]

    cur.execute("SELECT MIN(updated_dt), MAX(updated_dt) FROM sold_items WHERE updated_dt IS NOT NULL AND updated_dt != ''")
    row_mm = cur.fetchone()
    db_min = parse_date(row_mm[0])
    db_max = parse_date(row_mm[1])
    db_start     = db_min.strftime("%Y-%m-%d") if db_min else ""
    db_end       = db_max.strftime("%Y-%m-%d") if db_max else ""
    db_start_disp = db_min.strftime("%Y年%m月%d日") if db_min else "-"
    db_end_disp   = db_max.strftime("%Y年%m月%d日") if db_max else "-"

    cur.execute("SELECT brand,lv1,lv2,lv3,lv4,lv5,lv6,price,created_dt,updated_dt FROM sold_items WHERE price>0")
    raw_items = cur.fetchall()
    con.close()

    groups = {}
    for row in raw_items:
        brand = (row["brand"] or "").strip()
        if not brand: continue
        l1,l2,l3 = row["lv1"] or "",row["lv2"] or "",row["lv3"] or ""
        l4,l5,l6 = row["lv4"] or "",row["lv5"] or "",row["lv6"] or ""
        price = float(row["price"] or 0)
        c_dt  = parse_date(row["created_dt"])
        u_dt  = parse_date(row["updated_dt"])
        total_seconds = None
        days = None
        if c_dt and u_dt:
            delta = u_dt - c_dt
            total_seconds = max(0, delta.total_seconds())
            days = max(0, delta.days)
        ym = u_dt.strftime("%Y-%m") if u_dt else "不明"
        key = f"{brand}|{l1}|{l2}|{l3}|{l4}|{l5}|{l6}"
        if key not in groups:
            groups[key] = {"ブランド":brand,"L1":l1,"L2":l2,"L3":l3,"L4":l4,"L5":l5,"L6":l6,
                           "prices":[],"seconds_list":[],"days_list":[],"quick":0,"ym_list":[]}
        g = groups[key]
        g["prices"].append(price)
        g["ym_list"].append(ym)
        if total_seconds is not None:
            g["seconds_list"].append(total_seconds)
            g["days_list"].append(days)
            if days <= 3: g["quick"] += 1

    raw_data = []
    for g in groups.values():
        prices = g["prices"]
        cnt    = len(prices)
        avg_p  = sum(prices)/cnt if cnt else 0
        ship   = get_shipping(g["L3"])
        buy_p  = avg_p * 0.25
        profit = avg_p * 0.65 - ship
        rate   = (profit/avg_p*100) if avg_p > 0 else 0
        sl     = g["seconds_list"]
        dl     = g["days_list"]
        avg_sec = round(sum(sl)/len(sl),0) if sl else 0.0
        qr      = round(g["quick"]/len(dl)*100,1) if dl else 0.0
        ym_s    = sorted([y for y in g["ym_list"] if y!="不明"],reverse=True)
        ym      = ym_s[0] if ym_s else "不明"
        raw_data.append({
            "ブランド":g["ブランド"],"L1":g["L1"],"L2":g["L2"],"L3":g["L3"],
            "L4":g["L4"],"L5":g["L5"],"L6":g["L6"],
            "件数":cnt,"需要スコア":cnt,
            "平均単価":int(round(avg_p)),
            "推定仕入価格":int(round(buy_p)),
            "推定利益/件":round(profit,1),
            "利益率":round(rate,1),
            "平均売却秒数":avg_sec,
            "即売れ率":qr,
            "送料":ship,
            "損益分岐点":int(round(buy_p+ship)),
            "仕入れスコア":int(cnt*avg_p*max(rate,0)/100),
            "年月":ym,
        })
    raw_data.sort(key=lambda r: r["件数"], reverse=True)

    # KPI
    ac   = sum(r["件数"] for r in raw_data)
    ap   = sum(r["平均単価"]*r["件数"] for r in raw_data)/ac if ac else 0
    apf  = sum(r["推定利益/件"]*r["件数"] for r in raw_data)/ac if ac else 0
    ar   = sum(r["利益率"]*r["件数"] for r in raw_data)/ac if ac else 0
    qt   = sum(r["件数"]*r["即売れ率"]/100 for r in raw_data)
    qra  = round(qt/ac*100,1) if ac else 0
    tp   = int(sum(r["推定利益/件"]*r["件数"] for r in raw_data))
    q3d  = int(qt)

    # カテゴリツリー
    tree = {}
    for r in raw_data:
        l1,l2,l3,l4,l5,l6 = r["L1"],r["L2"],r["L3"],r["L4"],r["L5"],r["L6"]
        if not l1: continue
        t = tree.setdefault(l1,{})
        if l2:
            t = t.setdefault(l2,{})
            if l3:
                t = t.setdefault(l3,{})
                if l4:
                    t = t.setdefault(l4,{})
                    if l5:
                        t = t.setdefault(l5,{})
                        if l6: t.setdefault(l6,{})

    all_brands = sorted(set(r["ブランド"] for r in raw_data if r["ブランド"]))
    l1_set     = sorted(set(r["L1"] for r in raw_data if r["L1"]))
    l2_set     = sorted(set(r["L2"] for r in raw_data if r["L2"]))
    default_l1 = l1_set[0] if len(l1_set)==1 else ""

    return {
        "db_def": db_def, "raw_data": raw_data, "tree": tree,
        "all_brands": all_brands, "l2_set": l2_set, "default_l1": default_l1,
        "analysis_count": ac, "avg_price_all": int(ap), "avg_profit_all": int(apf),
        "avg_rate_all": round(ar,1), "quick_rate_all": qra,
        "db_total_count": db_total, "total_profit": tp, "quick_3days": q3d,
        "db_start_disp": db_start_disp, "db_end_disp": db_end_disp,
        "db_start_str": db_start, "db_end_str": db_end,
    }

def step_aggregate():
    print("\n" + "="*60)
    print("  STEP 2: 集計 → JSON")
    print("="*60)
    results = []
    for db_def in DATABASES:
        db_path = os.path.join(SPLIT_DB_DIR, db_def["file"])
        if not os.path.exists(db_path):
            print(f"  ⚠️  スキップ（未生成）: {db_def['file']}")
            continue
        lv2 = db_def["lv2"]
        lv3 = db_def.get("_lv3_actual", db_def["lv3"])
        print(f"\n  集計中: {lv2} > {lv3}")
        data = aggregate_db(db_path, db_def)
        out_name = db_def["file"].replace(".db","_data.json")
        out_path = os.path.join(OUTPUT_DIR, out_name)
        with open(out_path,"w",encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        print(f"  ✅ {data['analysis_count']:,}件 → {out_name}")
        results.append(data)
    return results

# ====================================================
# STEP 3: HTML生成（build_html_v3.py から流用）
# ====================================================
def step_build_html(all_data):
    print("\n" + "="*60)
    print("  STEP 3: ダッシュボードHTML生成")
    print("="*60)

    # FULL_TREEとJSONマップ
    full_tree = {}
    db_map    = {}
    for data in all_data:
        lv2 = data["db_def"]["lv2"]
        lv3 = data["db_def"].get("_lv3_actual", data["db_def"]["lv3"])
        full_tree.setdefault("ファッション",{}).setdefault(lv2,{})[lv3] = \
            data["db_def"]["file"].replace(".db","_data.json")
        db_map[f"{lv2}>{lv3}"] = data["db_def"]["file"].replace(".db","_data.json")

    tree_json = json.dumps(full_tree, ensure_ascii=False)
    first     = all_data[0]
    lv2       = first["db_def"]["lv2"]
    lv3       = first["db_def"].get("_lv3_actual", first["db_def"]["lv3"])
    r         = first
    raw_json  = json.dumps(r["raw_data"],   ensure_ascii=False)
    cat_json  = json.dumps(r["tree"],       ensure_ascii=False)
    brands_json = json.dumps(r["all_brands"], ensure_ascii=False)
    l2_str    = lv2
    default_l1 = r["default_l1"]
    total     = len(r["raw_data"])

    # build_html_v3.pyのbuild_html_str相当（.slice(0,100)修正済み）
    html = build_html_str(r, raw_json, cat_json, brands_json, l2_str,
                          default_l1, lv2, lv3, total, tree_json)

    out_path = os.path.join(OUTPUT_DIR, "mercari_dashboard.html")
    with open(out_path,"w",encoding="utf-8") as f:
        f.write(html)
    size_mb = os.path.getsize(out_path)/1024/1024
    print(f"  ✅ mercari_dashboard.html ({size_mb:.1f}MB)")
    print(f"  📂 {out_path}")
    return out_path

def build_html_str(r, raw_json, cat_json, brands_json, l2_str,
                   default_l1, lv2, lv3, total, tree_json):
    # build_html_v3.pyのbuild_html_strをそのまま移植
    # ※ build_html_v3.py が同フォルダにある場合はそちらを直接呼ぶ
    analytics_dir = os.path.join(SCRIPT_DIR, "analytics")
    build_v3 = os.path.join(analytics_dir, "build_html_v3.py")
    if os.path.exists(build_v3):
        import importlib.util
        spec = importlib.util.spec_from_file_location("build_html_v3", build_v3)
        mod  = importlib.util.load_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.build_html_str(r, raw_json, cat_json, brands_json,
                                  l2_str, default_l1, lv2, lv3, total, tree_json)
    else:
        print("  ⚠️  build_html_v3.py が見つかりません。build_html_v3.py を analytics/ に配置してください。")
        sys.exit(1)

# ====================================================
# メイン
# ====================================================
def main():
    parser = argparse.ArgumentParser(description="メルカリ仕入判断AI 完全自動パイプライン")
    parser.add_argument("--skip-split",   action="store_true", help="DB分割をスキップ")
    parser.add_argument("--skip-agg",     action="store_true", help="集計をスキップ")
    parser.add_argument("--only-html",    action="store_true", help="HTML生成のみ（分割・集計スキップ）")
    args = parser.parse_args()

    t0 = time.time()
    print("\n" + "="*60)
    print("  メルカリ仕入判断AI 完全自動パイプライン")
    print(f"  開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    # Step1: DB分割
    if not args.skip_split and not args.only_html:
        step_split()
    else:
        print("\n  [STEP 1] DB分割: スキップ")

    # Step2: 集計
    if not args.skip_agg and not args.only_html:
        all_data = step_aggregate()
    else:
        print("\n  [STEP 2] 集計: スキップ → JSONを読み込み中...")
        all_data = []
        for db_def in DATABASES:
            json_name = db_def["file"].replace(".db","_data.json")
            json_path = os.path.join(OUTPUT_DIR, json_name)
            if os.path.exists(json_path):
                with open(json_path,"r",encoding="utf-8") as f:
                    all_data.append(json.load(f))
            else:
                print(f"  ⚠️  JSONなし: {json_name}")

    if not all_data:
        print("❌ 集計データが0件です。先にStep1・2を実行してください。")
        sys.exit(1)

    # Step3: HTML生成
    step_build_html(all_data)

    print(f"\n✅ 全処理完了 (合計 {time.time()-t0:.0f}秒)")
    print(f"   ブラウザで開く: {os.path.join(OUTPUT_DIR, 'mercari_dashboard.html')}")

if __name__ == "__main__":
    main()
