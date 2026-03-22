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

def _resolve(p):
    if not p: return p
    if len(p) >= 2 and p[1] == ':':
        return SCRIPT_DIR[0:2] + p[2:]
    return os.path.join(SCRIPT_DIR, p)

SOURCE_DB    = _resolve(cfg["paths"]["source_db"])
SPLIT_DB_DIR = _resolve(cfg["paths"]["split_db_dir"])
OUTPUT_DIR   = _resolve(cfg["paths"]["output_dir"])
SHIPPING_MAP = cfg["shipping_map"]
DATABASES    = cfg["databases"]

print("  [パス確認]")
print("  SCRIPT_DIR   :", SCRIPT_DIR)
print("  SOURCE_DB    :", SOURCE_DB)
print("  SPLIT_DB_DIR :", SPLIT_DB_DIR)
print("  OUTPUT_DIR   :", OUTPUT_DIR)

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
    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>メルカリ仕入判断AI 30分析 {lv2}>{lv3}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{font-family:"Meiryo","MS PGothic",sans-serif;background:#f0f2f7;color:#1a1f36;font-size:13px;}}
header{{background:linear-gradient(135deg,#1a2a6c,#2a3f8f);padding:12px 24px;display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;}}
header h1{{font-size:17px;font-weight:bold;color:#fff;letter-spacing:1px;}}
header h1 span{{color:#74c0fc;}}
.hbadge{{font-size:10px;background:rgba(255,255,255,.2);border-radius:4px;padding:2px 7px;color:#d0e8ff;margin-left:6px;}}
.hstats{{display:flex;gap:10px;flex-wrap:wrap;align-items:center;}}
.hs{{text-align:center;min-width:70px;}}
.hs .n{{font-size:14px;font-weight:bold;color:#74c0fc;}}
.hs .l{{font-size:9px;color:#a5b4d0;margin-top:1px;}}
.hs-sep{{width:1px;background:rgba(255,255,255,.2);height:32px;}}
.infobar{{background:#e7f0ff;border-bottom:1px solid #c5d5f5;padding:5px 16px;font-size:11px;color:#1971c2;display:flex;align-items:center;gap:8px;flex-wrap:wrap;}}
.infobar b{{color:#1558a0;}}
.fbar{{background:#fff;border-bottom:2px solid #3b5bdb;padding:8px 16px;display:flex;flex-direction:column;gap:6px;}}
.frow{{display:flex;gap:6px;align-items:center;flex-wrap:wrap;}}
.frow label{{font-size:11px;color:#4a5568;white-space:nowrap;}}
input[type=date]{{background:#f0f5ff;border:1px solid #c5d5f5;border-radius:4px;padding:4px 7px;color:#1a1f36;font-size:11px;color-scheme:light;min-width:120px;}}
input[type=date]:focus{{outline:none;border-color:#3b5bdb;}}
input[type=text]{{background:#f0f5ff;border:1px solid #c5d5f5;border-radius:4px;padding:4px 7px;color:#1a1f36;font-size:11px;min-width:160px;}}
input[type=text]:focus{{outline:none;border-color:#3b5bdb;}}
select{{background:#f0f5ff;border:1px solid #c5d5f5;border-radius:4px;padding:4px 7px;color:#1a1f36;font-size:11px;cursor:pointer;max-width:140px;}}
select:disabled{{opacity:.35;cursor:default;}}
select:focus{{outline:none;border-color:#3b5bdb;}}
.btn{{border:none;border-radius:4px;padding:5px 12px;font-size:11px;font-family:inherit;cursor:pointer;font-weight:bold;white-space:nowrap;}}
.btn-navy{{background:#3b5bdb;color:#fff;}} .btn-navy:hover{{background:#2f4ac7;}}
.btn-white{{background:#fff;border:1px solid #c5d5f5;color:#4a5568;}} .btn-white:hover{{border-color:#3b5bdb;color:#1971c2;}}
.sbar{{background:#e7f0ff;border-bottom:1px solid #c5d5f5;padding:5px 16px;font-size:11px;color:#1971c2;min-height:24px;display:flex;align-items:center;gap:6px;}}
.sbar.filtered{{background:#dbeafe;}} .sbar b{{color:#1558a0;}}
nav{{background:#fff;border-bottom:2px solid #d0d7e8;display:flex;overflow-x:auto;}}
nav button{{flex:1;min-width:80px;padding:9px 5px;background:none;border:none;border-bottom:3px solid transparent;color:#4a5568;font-size:10px;font-family:inherit;cursor:pointer;line-height:1.4;transition:.15s;white-space:nowrap;}}
nav button:hover{{background:#f0f5ff;color:#1971c2;}}
nav button.on{{color:#1971c2;border-bottom-color:#1971c2;background:#e7f0ff;font-weight:bold;}}
nav button small{{display:block;font-size:9px;color:#888;margin-top:1px;}}
nav button.on small{{color:#1971c2;}}
nav button.gold.on{{color:#e67700;border-bottom-color:#e67700;background:#fff8e1;}}
.tab{{display:none;padding:16px 20px;}} .tab.on{{display:block;}}
.thead{{margin-bottom:14px;padding-bottom:8px;border-bottom:2px solid #3b5bdb;}}
.thead h2{{font-size:15px;color:#1971c2;font-weight:bold;}}
.thead p{{font-size:10px;color:#4a5568;margin-top:3px;}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:8px;}}
.card{{background:#fff;border:1px solid #d0d7e8;border-radius:7px;padding:12px;cursor:pointer;transition:.15s;box-shadow:0 1px 3px rgba(0,0,0,.05);}}
.card:hover{{border-color:#3b5bdb;transform:translateY(-1px);box-shadow:0 3px 12px rgba(59,91,219,.14);}}
.ctop{{display:flex;gap:7px;align-items:flex-start;margin-bottom:5px;}}
.cnum{{background:#1971c2;color:#fff;font-size:9px;font-weight:bold;padding:2px 5px;border-radius:3px;flex-shrink:0;}}
.ctitle{{font-size:12px;font-weight:bold;color:#1a1f36;line-height:1.35;}}
.cdesc{{font-size:10px;color:#4a5568;line-height:1.45;}}
.ov{{display:none;position:fixed;inset:0;background:rgba(20,30,80,.4);z-index:200;align-items:center;justify-content:center;}}
.ov.on{{display:flex;}}
.pop{{background:#fff;border-radius:10px;width:96%;max-width:1200px;max-height:88vh;overflow-y:auto;box-shadow:0 8px 40px rgba(59,91,219,.18);}}
.phead{{position:sticky;top:0;background:#fff;border-bottom:1px solid #d0d7e8;padding:12px 18px;display:flex;align-items:center;justify-content:space-between;z-index:1;}}
.phl{{display:flex;align-items:center;gap:7px;flex-wrap:wrap;}}
.pnum{{background:#1971c2;color:#fff;font-size:10px;padding:2px 7px;border-radius:3px;}}
.ptitle{{font-size:14px;font-weight:bold;color:#1a1f36;}}
.pperiod{{font-size:10px;color:#4a5568;background:#f0f5ff;border:1px solid #c5d5f5;border-radius:3px;padding:2px 6px;}}
.pfbadge{{font-size:10px;background:#dbeafe;color:#1558a0;border:1px solid #c5d5f5;border-radius:3px;padding:2px 7px;}}
.pcls{{background:#e7f0ff;border:none;color:#1971c2;width:28px;height:28px;border-radius:5px;cursor:pointer;font-size:17px;line-height:28px;text-align:center;}}
.pcls:hover{{background:#1971c2;color:#fff;}}
.pbody{{padding:14px 18px;}}
.krow{{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;}}
.kpi{{flex:1;min-width:90px;background:#f0f5ff;border:1px solid #c5d5f5;border-radius:7px;padding:9px;text-align:center;}}
.kv{{font-size:17px;font-weight:bold;color:#1971c2;}} .kl{{font-size:9px;color:#4a5568;margin-top:2px;}}
.ins{{background:#f0f9ff;border:1px solid #bae6fd;border-radius:6px;padding:10px 14px;margin-bottom:12px;font-size:11px;color:#0369a1;}}
.ins h4{{font-size:11px;font-weight:bold;margin-bottom:5px;}}
.ins ul{{padding-left:14px;}} .ins li{{margin-bottom:2px;}}
.sec{{font-size:11px;font-weight:bold;color:#1971c2;margin:10px 0 5px;padding-bottom:4px;border-bottom:1px solid #c5d5f5;}}
.tw{{overflow-x:auto;}}
table{{width:100%;border-collapse:collapse;font-size:11px;}}
thead tr{{background:#e7f0ff;}}
th{{padding:6px 8px;text-align:left;color:#1558a0;font-weight:bold;white-space:nowrap;border-bottom:2px solid #c5d5f5;position:sticky;top:0;background:#e7f0ff;}}
td{{padding:5px 8px;border-bottom:1px solid #f0f4fb;white-space:nowrap;}}
tr:hover td{{background:#f5f8ff;}}
.nr{{text-align:right;}}
.rk{{background:#1971c2;color:#fff;font-size:9px;font-weight:bold;padding:1px 5px;border-radius:3px;}}
.bcsv{{margin:4px 0 8px;border:1px solid #c5d5f5;background:#f0f5ff;color:#1971c2;border-radius:4px;padding:3px 10px;font-size:10px;cursor:pointer;}}
.bcsv:hover{{background:#1971c2;color:#fff;}}
.nodata{{padding:30px;text-align:center;color:#4a5568;font-size:12px;}}
.ac-wrap{{position:relative;display:inline-block;}}
.ac-list{{position:absolute;top:100%;left:0;z-index:300;background:#fff;border:1px solid #c5d5f5;border-radius:4px;max-height:300px;overflow-y:auto;min-width:200px;box-shadow:0 4px 12px rgba(0,0,0,.1);display:none;}}
.ac-list.show{{display:block;}}
.ac-item{{padding:5px 10px;font-size:11px;cursor:pointer;color:#1a1f36;white-space:nowrap;}}
.ac-item:hover,.ac-item.active{{background:#e7f0ff;color:#1971c2;}}
.ac-count{{padding:4px 10px;font-size:10px;color:#4a5568;border-top:1px solid #f0f4fb;background:#f8faff;}}
</style>
</head>
<body>
<header>
  <div><h1>📦 メルカリ仕入判断AI <span>30分析</span><span class="hbadge">{lv2} &gt; {lv3}</span></h1></div>
  <div class="hstats">
    <div class="hs"><div class="n">{r['analysis_count']:,}</div><div class="l">分析対象件数</div></div>
    <div class="hs-sep"></div>
    <div class="hs"><div class="n">¥{r['avg_price_all']:,}</div><div class="l">平均単価</div></div>
    <div class="hs-sep"></div>
    <div class="hs"><div class="n">¥{r['avg_profit_all']:,}</div><div class="l">推定利益/件</div></div>
    <div class="hs-sep"></div>
    <div class="hs"><div class="n">{r['avg_rate_all']:.1f}%</div><div class="l">平均利益率</div></div>
    <div class="hs-sep"></div>
    <div class="hs"><div class="n">{r['quick_rate_all']:.1f}%</div><div class="l">即売れ率</div></div>
    <div class="hs-sep"></div>
    <div class="hs"><div class="n">30</div><div class="l">分析数</div></div>
  </div>
</header>
<div class="infobar">
  🗄️ <b>DB収録期間:</b><span id="ib_period">{r['db_start_disp']}〜{r['db_end_disp']}</span>&nbsp;｜&nbsp;
  <b>総件数:</b>{r['db_total_count']:,}件&nbsp;｜&nbsp;
  <b>推定総利益:</b>¥{r['total_profit']:,}&nbsp;｜&nbsp;
  <b>3日以内即売れ:</b>{r['quick_3days']:,}件&nbsp;｜&nbsp;
  <b>カテゴリ:</b>{l2_str}
</div>
<div class="fbar">
  <div class="frow">
    <label>期間:</label>
    <input type="date" id="dfrom">
    <label>〜</label>
    <input type="date" id="dto">
    <label>L1:</label>
    <select id="sl1"><option value="">全L1</option></select>
    <label>L2:</label>
    <select id="sl2" disabled><option value="">全L2</option></select>
    <label>L3:</label>
    <select id="sl3" disabled><option value="">全L3</option></select>
    <label>L4:</label>
    <select id="sl4" disabled><option value="">全L4</option></select>
    <label>L5:</label>
    <select id="sl5" disabled><option value="">全L5</option></select>
    <label>L6:</label>
    <select id="sl6" disabled><option value="">全L6</option></select>
  </div>
  <div class="frow">
    <label>ブランド:</label>
    <div class="ac-wrap">
      <input type="text" id="brand-input" placeholder="ブランド名を入力..." autocomplete="off"
        oninput="onBrandInput(this.value)" onkeydown="onBrandKey(event)">
      <div class="ac-list" id="ac-list"></div>
    </div>
    <button class="btn btn-navy" onclick="applyFilter()">🔍 絞込</button>
    <button class="btn btn-white" onclick="resetFilter()">✕ リセット</button>
  </div>
</div>
<div class="sbar" id="sbar">全 {total:,} 件表示中</div>
<nav>
  <button class="on" onclick="showTab(1)">🏷️ ブランド<small>分析</small></button>
  <button onclick="showTab(2)">📂 カテゴリ<small>分析</small></button>
  <button onclick="showTab(3)">📦 商品<small>分析</small></button>
  <button onclick="showTab(4)">💰 価格帯<small>分析</small></button>
  <button onclick="showTab(5)">🔑 キーワード<small>分析</small></button>
  <button onclick="showTab(6)">📈 トレンド<small>分析</small></button>
  <button class="gold" onclick="showTab(7)">🏆 仕入れ<small>推奨</small></button>
</nav>
<div class="tab on" id="tab1"><div class="thead"><h2>🏷️ ブランド分析</h2><p>ブランド別の売上・回転率・利益率を分析します</p></div><div class="grid" id="g1"></div></div>
<div class="tab" id="tab2"><div class="thead"><h2>📂 カテゴリ分析</h2><p>カテゴリ別の売上・回転率・利益率を分析します</p></div><div class="grid" id="g2"></div></div>
<div class="tab" id="tab3"><div class="thead"><h2>📦 商品分析</h2><p>商品別の売上・回転率・利益率を分析します</p></div><div class="grid" id="g3"></div></div>
<div class="tab" id="tab4"><div class="thead"><h2>💰 価格帯分析</h2><p>価格帯別の売れ行き・利益率を分析します</p></div><div class="grid" id="g4"></div></div>
<div class="tab" id="tab5"><div class="thead"><h2>🔑 キーワード分析</h2><p>タイトルキーワードの出現頻度を分析します</p></div><div class="grid" id="g5"></div></div>
<div class="tab" id="tab6"><div class="thead"><h2>📈 トレンド分析</h2><p>急上昇ブランド・カテゴリ・月別売上を分析します</p></div><div class="grid" id="g6"></div></div>
<div class="tab" id="tab7"><div class="thead"><h2>🏆 仕入れ推奨</h2><p>仕入れスコアで総合評価した推奨ランキングです</p></div><div class="grid" id="g7"></div></div>
<div class="ov" id="ov" onclick="closePop(event)">
  <div class="pop">
    <div class="phead">
      <div class="phl">
        <span class="pnum" id="pnum">#1</span>
        <span class="ptitle" id="ptitle"></span>
        <span class="pperiod" id="pper"></span>
        <span class="pfbadge" id="pfb" style="display:none"></span>
      </div>
      <button class="pcls" onclick="closePop()">×</button>
    </div>
    <div class="pbody" id="pbody"></div>
  </div>
</div>
<script>
var RAW={raw_json};
var CAT={cat_json};
var ALL_BRANDS={brands_json};
var DEFAULT_L1="{default_l1}";
var DEFAULT_L2="";
var DEFAULT_L3="";
var DB_START="{r['db_start_str']}";
var DB_END="{r['db_end_str']}";
var CUR_FROM="",CUR_TO="";
var CUR_L1="",CUR_L2="",CUR_L3="",CUR_L4="",CUR_L5="",CUR_L6="",CUR_BRAND="";
var AC_IDX=-1;
function time_fmt(sec){{
  if(sec===null||sec===undefined)return"-";
  var s=parseFloat(sec);if(isNaN(s))return"-";
  if(s<86400){{var h=s/3600;return h<1?Math.round(s/60)+"分":h.toFixed(1)+"時間";}}
  return(s/86400).toFixed(1)+"日";
}}
function onBrandInput(val){{
  AC_IDX=-1;var list=document.getElementById("ac-list");
  if(!val){{list.classList.remove("show");list.innerHTML="";return;}}
  var q=val.toLowerCase();
  var matches=ALL_BRANDS.filter(function(b){{return b.toLowerCase().indexOf(q)>=0;}});
  if(!matches.length){{list.classList.remove("show");list.innerHTML="";return;}}
  var html="";
  matches.forEach(function(b){{
    var esc=b.replace(/&/g,"&amp;").replace(/'/g,"&#39;").replace(/"/g,"&quot;");
    var hi=b.replace(new RegExp(val.replace(/[.*+?^${{}}()|[\\]\\\\]/g,"\\\\$&"),"gi"),function(m){{return"<b style='color:#1971c2'>"+m+"</b>";}});
    html+="<div class='ac-item' data-val='"+esc+"' onmousedown='selectBrand(this.dataset.val)'>"+hi+"</div>";
  }});
  html+="<div class='ac-count'>"+matches.length+"件一致</div>";
  list.innerHTML=html;list.classList.add("show");
}}
function onBrandKey(e){{
  var list=document.getElementById("ac-list");var items=list.querySelectorAll(".ac-item");
  if(e.key==="ArrowDown"){{e.preventDefault();AC_IDX=Math.min(AC_IDX+1,items.length-1);items.forEach(function(el,i){{el.classList.toggle("active",i===AC_IDX);}});}}
  else if(e.key==="ArrowUp"){{e.preventDefault();AC_IDX=Math.max(AC_IDX-1,0);items.forEach(function(el,i){{el.classList.toggle("active",i===AC_IDX);}});}}
  else if(e.key==="Enter"){{if(AC_IDX>=0&&items[AC_IDX]){{selectBrand(items[AC_IDX].getAttribute("data-val"));}}else{{applyFilter();}}}}
  else if(e.key==="Escape"){{list.classList.remove("show");}}
}}
function selectBrand(val){{document.getElementById("brand-input").value=val;document.getElementById("ac-list").classList.remove("show");CUR_BRAND=val;applyFilter();}}
document.addEventListener("click",function(e){{if(!e.target.closest(".ac-wrap"))document.getElementById("ac-list").classList.remove("show");}});
function setSel(id,opts,disabled){{
  var labels={{"sl1":"全L1","sl2":"全L2","sl3":"全L3","sl4":"全L4","sl5":"全L5","sl6":"全L6"}};
  var s=document.getElementById(id);
  s.innerHTML="<option value=''>"+labels[id]+"</option>";
  opts.forEach(function(v){{var o=document.createElement("option");o.value=v;o.textContent=v;s.appendChild(o);}});
  s.disabled=disabled;
}}
function initSel(){{
  var src1=FULL_TREE||CAT;
  var l1keys=Object.keys(src1).sort();setSel("sl1",l1keys,false);
  if(DEFAULT_L1){{
    document.getElementById("sl1").value=DEFAULT_L1;CUR_L1=DEFAULT_L1;
    var src2=FULL_TREE?FULL_TREE[DEFAULT_L1]:CAT[DEFAULT_L1];
    var l2keys=src2?Object.keys(src2).sort():[];
    setSel("sl2",l2keys,false);
  }}
  document.getElementById("sl1").addEventListener("change",function(){{
    var l1=this.value;var src=FULL_TREE?FULL_TREE[l1]:CAT[l1];
    setSel("sl2",l1&&src?Object.keys(src).sort():[],!l1);
    setSel("sl3",[],true);setSel("sl4",[],true);setSel("sl5",[],true);setSel("sl6",[],true);
  }});
  document.getElementById("sl2").addEventListener("change",function(){{
    var l1=document.getElementById("sl1").value,l2=this.value;
    var src=FULL_TREE?FULL_TREE[l1]&&FULL_TREE[l1][l2]:CAT[l1]&&CAT[l1][l2];
    setSel("sl3",l1&&l2&&src?Object.keys(src).sort():[],!l2);
    setSel("sl4",[],true);setSel("sl5",[],true);setSel("sl6",[],true);
  }});
  document.getElementById("sl3").addEventListener("change",function(){{
    var l1=document.getElementById("sl1").value,l2=document.getElementById("sl2").value,l3=this.value;
    setSel("sl4",[],true);setSel("sl5",[],true);setSel("sl6",[],true);
    if(!l3)return;
    var node=CAT[l1]&&CAT[l1][l2]&&CAT[l1][l2][l3];
    var l4keys=node?Object.keys(node).sort():[];
    setSel("sl4",l4keys,l4keys.length===0);
  }});
  document.getElementById("sl4").addEventListener("change",function(){{
    var l1=Object.keys(CAT)[0]||"",l2=CUR_L2||document.getElementById("sl2").value;
    var l3=CUR_L3||document.getElementById("sl3").value,l4=this.value;
    var node=l1&&l2&&l3&&l4&&CAT[l1]&&CAT[l1][l2]&&CAT[l1][l2][l3]&&CAT[l1][l2][l3][l4];
    setSel("sl5",node?Object.keys(node).sort():[],!l4||!node);setSel("sl6",[],true);
  }});
  document.getElementById("sl5").addEventListener("change",function(){{
    var l1=Object.keys(CAT)[0]||"",l2=CUR_L2||document.getElementById("sl2").value;
    var l3=CUR_L3||document.getElementById("sl3").value;
    var l4=document.getElementById("sl4").value,l5=this.value;
    var node=l1&&l2&&l3&&l4&&l5&&CAT[l1]&&CAT[l1][l2]&&CAT[l1][l2][l3]&&CAT[l1][l2][l3][l4]&&CAT[l1][l2][l3][l4][l5];
    var opts=node?Object.keys(node).sort():[];setSel("sl6",opts,!l5||opts.length===0);
  }});
}}
function applyFilter(){{
  CUR_FROM=document.getElementById("dfrom").value;CUR_TO=document.getElementById("dto").value;
  CUR_L1=document.getElementById("sl1").value;CUR_L2=document.getElementById("sl2").value;
  CUR_L3=document.getElementById("sl3").value;CUR_L4=document.getElementById("sl4").value;
  CUR_L5=document.getElementById("sl5").value;CUR_L6=document.getElementById("sl6").value;
  CUR_BRAND=document.getElementById("brand-input").value.trim();refreshUI();
}}
function resetFilter(){{
  var today=new Date();
  var ymd=today.getFullYear()+"-"+String(today.getMonth()+1).padStart(2,"0")+"-"+String(today.getDate()).padStart(2,"0");
  document.getElementById("dfrom").value=DB_START;document.getElementById("dto").value=ymd;
  document.getElementById("brand-input").value="";document.getElementById("ac-list").classList.remove("show");
  ["sl2","sl3","sl4","sl5","sl6"].forEach(function(id){{document.getElementById(id).value="";}});
  if(DEFAULT_L1){{document.getElementById("sl1").value=DEFAULT_L1;CUR_L1=DEFAULT_L1;
    setSel("sl2",FULL_TREE&&FULL_TREE[DEFAULT_L1]?Object.keys(FULL_TREE[DEFAULT_L1]).sort():[],false);
  }}else{{document.getElementById("sl1").value="";["sl2","sl3","sl4","sl5","sl6"].forEach(function(id){{document.getElementById(id).disabled=true;}});CUR_L1="";}}
  CUR_FROM=DB_START;CUR_TO=ymd;CUR_L2="";CUR_L3="";CUR_L4="";CUR_L5="";CUR_L6="";CUR_BRAND="";refreshUI();
}}
function getFiltered(){{
  var fromYM=CUR_FROM.slice(0,7),toYM=CUR_TO.slice(0,7);var q=CUR_BRAND.toLowerCase();
  return RAW.filter(function(r){{
    if(r["年月"]&&r["年月"]!=="不明"){{if(r["年月"]<fromYM||r["年月"]>toYM)return false;}}
    if(CUR_L1&&r.L1!==CUR_L1)return false;if(CUR_L2&&r.L2!==CUR_L2)return false;
    if(CUR_L3&&r.L3!==CUR_L3)return false;if(CUR_L4&&r.L4!==CUR_L4)return false;
    if(CUR_L5&&r.L5!==CUR_L5)return false;if(CUR_L6&&r.L6!==CUR_L6)return false;
    if(q&&r["ブランド"].toLowerCase().indexOf(q)<0)return false;return true;
  }});
}}
function fparts(){{
  var p=[];
  if(CUR_L1)p.push(CUR_L1);if(CUR_L2)p.push(CUR_L2);if(CUR_L3)p.push(CUR_L3);
  if(CUR_L4)p.push(CUR_L4);if(CUR_L5)p.push(CUR_L5);if(CUR_L6)p.push(CUR_L6);
  if(CUR_BRAND)p.push("🏷️"+CUR_BRAND);return p;
}}
function refreshUI(){{
  var f=getFiltered(),sb=document.getElementById("sbar"),parts=fparts();
  var fromYM=CUR_FROM.slice(0,7),toYM=CUR_TO.slice(0,7);
  var dbFromYM=DB_START.slice(0,7);
  var today=new Date();var todayYM=today.getFullYear()+"-"+String(today.getMonth()+1).padStart(2,"0");
  var hasFilter=parts.length>0||fromYM!==dbFromYM||toYM!==todayYM;
  if(hasFilter){{sb.className="sbar filtered";var info="📅 "+fromYM+" 〜 "+toYM;
    if(parts.length)info+=" 🔍 "+parts.join(" > ");sb.innerHTML=info+" — <b>"+f.length.toLocaleString()+"</b> 件";
  }}else{{sb.className="sbar";sb.innerHTML="全 <b>"+f.length.toLocaleString()+"</b> 件表示中";}}
}}
function ssum(a,k){{return a.reduce(function(s,r){{return s+(parseFloat(r[k])||0);}},0);}}
function aavg(a,k){{return a.length?ssum(a,k)/a.length:0;}}
function yen(n){{if(n===null||n===undefined)return"-";return"¥"+Math.round(n).toLocaleString("ja-JP");}}
function pct(n){{if(n===null||n===undefined)return"-";return parseFloat(n).toFixed(1)+"%";}}
function srt(a,k,asc){{return a.slice().sort(function(x,y){{var av=parseFloat(x[k])||0,bv=parseFloat(y[k])||0;return asc?av-bv:bv-av;}});}}
var BC=[{{k:"__r",lb:"Rank"}},{{k:"ブランド",lb:"ブランド"}},{{k:"L1",lb:"L1"}},{{k:"L2",lb:"L2"}},{{k:"L3",lb:"L3"}},{{k:"L4",lb:"L4"}},{{k:"L5",lb:"L5"}},{{k:"L6",lb:"L6"}}];
var CC=[{{k:"__r",lb:"Rank"}},{{k:"L1",lb:"L1"}},{{k:"L2",lb:"L2"}},{{k:"L3",lb:"L3"}},{{k:"L4",lb:"L4"}},{{k:"L5",lb:"L5"}},{{k:"L6",lb:"L6"}}];
function grpBrand(rows){{
  var m={{}};
  rows.forEach(function(r){{var k=r["ブランド"]+"|"+r.L1+"|"+r.L2+"|"+r.L3+"|"+r.L4+"|"+r.L5+"|"+r.L6;if(!m[k])m[k]=[];m[k].push(r);}});
  return Object.keys(m).map(function(k){{
    var g=m[k],t=g[0];
    return{{ブランド:t["ブランド"],L1:t.L1,L2:t.L2,L3:t.L3,L4:t.L4,L5:t.L5,L6:t.L6,
      件数:ssum(g,"件数"),需要スコア:ssum(g,"需要スコア"),平均単価:Math.round(aavg(g,"平均単価")),
      推定仕入価格:Math.round(aavg(g,"推定仕入価格")),推定利益:Math.round(aavg(g,"推定利益/件")),
      利益率:parseFloat(aavg(g,"利益率").toFixed(1)),平均売却秒数:Math.round(aavg(g,"平均売却秒数")),
      即売れ率:parseFloat(aavg(g,"即売れ率").toFixed(1)),送料:t["送料"],
      損益分岐点:Math.round(aavg(g,"損益分岐点")),仕入れスコア:ssum(g,"仕入れスコア")}};
  }});
}}
function grpCat(rows){{
  var m={{}};
  rows.forEach(function(r){{var k=r.L1+"|"+r.L2+"|"+r.L3+"|"+r.L4+"|"+r.L5+"|"+r.L6;if(!m[k])m[k]=[];m[k].push(r);}});
  return Object.keys(m).map(function(k){{
    var g=m[k],t=g[0];
    return{{L1:t.L1,L2:t.L2,L3:t.L3,L4:t.L4,L5:t.L5,L6:t.L6,
      件数:ssum(g,"件数"),需要スコア:ssum(g,"需要スコア"),平均単価:Math.round(aavg(g,"平均単価")),
      推定仕入価格:Math.round(aavg(g,"推定仕入価格")),推定利益:Math.round(aavg(g,"推定利益/件")),
      利益率:parseFloat(aavg(g,"利益率").toFixed(1)),平均売却秒数:Math.round(aavg(g,"平均売却秒数")),
      即売れ率:parseFloat(aavg(g,"即売れ率").toFixed(1)),送料:t["送料"]}};
  }});
}}
function tbl(rows,cols){{
  if(!rows||!rows.length)return"<div class='nodata'>該当データなし</div>";
  var h="<div class='tw'><table><thead><tr>";
  cols.forEach(function(c){{h+="<th>"+c.lb+"</th>";}});h+="</tr></thead><tbody>";
  rows.forEach(function(r,i){{
    h+="<tr>";
    cols.forEach(function(c){{
      var v;if(c.k==="__r")v="<span class='rk'>"+(i+1)+"</span>";
      else if(c.f)v=c.f(r[c.k]);
      else v=(r[c.k]!==undefined&&r[c.k]!==null&&r[c.k]!=="")?r[c.k]:"-";
      h+="<td"+(c.cl?" class='"+c.cl+"'":"")+">"+v+"</td>";
    }});h+="</tr>";
  }});return h+"</tbody></table></div>";
}}
function krow(items){{return"<div class='krow'>"+items.map(function(x){{return"<div class='kpi'><div class='kv'>"+x[0]+"</div><div class='kl'>"+x[1]+"</div></div>";}}).join("")+"</div>";}}
function badge(){{
  var p=fparts();var fromYM=CUR_FROM.slice(0,7),toYM=CUR_TO.slice(0,7);
  var dbFromYM=DB_START.slice(0,7);var today=new Date();var todayYM=today.getFullYear()+"-"+String(today.getMonth()+1).padStart(2,"0");
  var out="";
  if(fromYM!==dbFromYM||toYM!==todayYM)out+="<span style='font-size:10px;background:#e7f0ff;color:#1971c2;border:1px solid #c5d5f5;border-radius:4px;padding:2px 7px;margin-right:4px;'>📅 "+fromYM+" 〜 "+toYM+"</span>";
  if(p.length)out+="<span style='font-size:10px;background:#dbeafe;color:#1558a0;border:1px solid #c5d5f5;border-radius:4px;padding:2px 7px;'>🔍 "+p.join(" > ")+"</span>";
  return out?"<div style='margin-bottom:10px;'>"+out+"</div>":"";
}}
var N={{
  件数:{{k:"件数",lb:"件数",cl:"nr"}},需要:{{k:"需要スコア",lb:"需要スコア",cl:"nr"}},
  単価:{{k:"平均単価",lb:"平均単価",f:yen,cl:"nr"}},仕入:{{k:"推定仕入価格",lb:"仕入価格",f:yen,cl:"nr"}},
  利益:{{k:"推定利益",lb:"推定利益/件",f:yen,cl:"nr"}},利益率:{{k:"利益率",lb:"利益率",f:pct,cl:"nr"}},
  売却日:{{k:"平均売却秒数",lb:"売却時間",f:time_fmt,cl:"nr"}},即売:{{k:"即売れ率",lb:"即売れ率",f:pct,cl:"nr"}},
  送料:{{k:"送料",lb:"送料",f:yen,cl:"nr"}},スコア:{{k:"仕入れスコア",lb:"仕入スコア",cl:"nr"}}
}};
var _popCache={{}};
function _saveCache(n,d,cols){{_popCache[n]={{d:d,cols:cols}};}}
function genPop(n){{
  var raw=getFiltered();
  if(!raw.length)return"<div class='nodata'>絞込条件に一致するデータがありません<br><small>期間・カテゴリ・ブランド条件を確認してください</small></div>";
  var bd=grpBrand(raw),cd=grpCat(raw),bk=badge();
  if(n===1){{var cols=BC.concat([N.件数,N.需要,N.単価,N.仕入,N.利益,N.利益率,N.売却日,N.即売,N.送料]);var d=srt(bd,"需要スコア",false),t=d[0]||{{}};_saveCache(1,d,cols);return bk+krow([[ssum(raw,"件数").toLocaleString()+"件","絞込総件数"],["1位:"+(t["ブランド"]||"-"),(t["件数"]||0).toLocaleString()+"件"],[yen(t["平均単価"]),"1位平均単価"],[pct(t["利益率"]),"1位利益率"]])+"<div class='ins'><h4>💡 ポイント</h4><ul><li>需要スコア降順 / ブランド別集計</li><li>売却時間はDBから実計算（0日の場合は時間表示）</li></ul></div><div class='sec'>ブランド売上ランキング 全件</div><button class='bcsv' onclick='dlCSV(1)'>⬇ CSV</button>"+tbl(d,cols);}}
  if(n===2){{var cols=BC.concat([N.売却日,N.即売,N.件数,N.単価,N.利益]);var d=srt(bd,"平均売却秒数",true),t=d[0]||{{}};_saveCache(2,d,cols);return bk+krow([[time_fmt(t["平均売却秒数"]),"最速1位:"+(t["ブランド"]||"-")],[time_fmt(Math.round(aavg(d,"平均売却秒数"))),"平均売却時間"],[pct(parseFloat(aavg(d,"即売れ率").toFixed(1))),"平均即売れ率"],[d.length.toLocaleString()+"件","集計件数"]])+"<div class='sec'>ブランド回転率 全件</div><button class='bcsv' onclick='dlCSV(2)'>⬇ CSV</button>"+tbl(d,cols);}}
  if(n===3){{var cols=BC.concat([N.単価,N.仕入,N.利益,N.利益率,N.件数]);var d=srt(bd,"平均単価",false),t=d[0]||{{}};_saveCache(3,d,cols);return bk+krow([[yen(t["平均単価"]),"1位:"+(t["ブランド"]||"-")],[yen(Math.round(aavg(d,"平均単価"))),"全体平均"],[d.length.toLocaleString()+"件","集計件数"],["-",""]])+"<div class='sec'>ブランド平均売却価格 全件</div><button class='bcsv' onclick='dlCSV(3)'>⬇ CSV</button>"+tbl(d,cols);}}
  if(n===4){{var cols=BC.concat([N.売却日,N.即売,N.件数,N.単価]);var d=srt(bd,"平均売却秒数",true),t=d[0]||{{}};_saveCache(4,d,cols);return bk+krow([[time_fmt(t["平均売却秒数"]),"最速:"+(t["ブランド"]||"-")],[time_fmt(Math.round(aavg(d,"平均売却秒数"))),"平均"],[pct(parseFloat(aavg(d,"即売れ率").toFixed(1))),"即売れ率"],[d.length.toLocaleString()+"件","集計件数"]])+"<div class='sec'>ブランド平均売却時間 全件</div><button class='bcsv' onclick='dlCSV(4)'>⬇ CSV</button>"+tbl(d,cols);}}
  if(n===5){{var cols=BC.concat([N.利益,N.利益率,N.単価,N.件数]);var d=srt(bd,"推定利益",false),t=d[0]||{{}};_saveCache(5,d,cols);return bk+krow([[yen(t["推定利益"]),"1位:"+(t["ブランド"]||"-")],[pct(t["利益率"]),"1位利益率"],[yen(Math.round(aavg(d,"推定利益"))),"平均推定利益/件"],[d.length.toLocaleString()+"件","集計件数"]])+"<div class='sec'>ブランド利益率 全件</div><button class='bcsv' onclick='dlCSV(5)'>⬇ CSV</button>"+tbl(d,cols);}}
  if(n===6){{var cols=CC.concat([N.件数,N.需要,N.単価,N.利益,N.利益率,N.売却日,N.即売]);var d=srt(cd,"需要スコア",false),t=d[0]||{{}},tc=[t.L1,t.L2,t.L3,t.L4,t.L5].filter(Boolean).join(">");_saveCache(6,d,cols);return bk+krow([[(t["件数"]||0).toLocaleString()+"件","1位:"+tc],[yen(Math.round(aavg(d,"平均単価"))),"平均単価"],[pct(parseFloat(aavg(d,"利益率").toFixed(1))),"平均利益率"],[d.length.toLocaleString()+"件","集計件数"]])+"<div class='sec'>カテゴリ売上ランキング 全件</div><button class='bcsv' onclick='dlCSV(6)'>⬇ CSV</button>"+tbl(d,cols);}}
  if(n===7){{var cols=CC.concat([N.売却日,N.即売,N.件数,N.単価]);var d=srt(cd,"平均売却秒数",true),t=d[0]||{{}};_saveCache(7,d,cols);return bk+krow([[time_fmt(t["平均売却秒数"]),"最速:"+[t.L1,t.L2,t.L3,t.L4,t.L5].filter(Boolean).join(">")],[time_fmt(Math.round(aavg(d,"平均売却秒数"))),"平均"],[pct(parseFloat(aavg(d,"即売れ率").toFixed(1))),"即売れ率"],[d.length.toLocaleString()+"件","集計件数"]])+"<div class='sec'>カテゴリ回転率 全件</div><button class='bcsv' onclick='dlCSV(7)'>⬇ CSV</button>"+tbl(d,cols);}}
  if(n===8){{var cols=CC.concat([N.単価,N.仕入,N.利益,N.利益率,N.件数]);var d=srt(cd,"平均単価",false),t=d[0]||{{}};_saveCache(8,d,cols);return bk+krow([[yen(t["平均単価"]),"1位:"+[t.L1,t.L2,t.L3,t.L4,t.L5].filter(Boolean).join(">")],[yen(Math.round(aavg(d,"平均単価"))),"全体平均"],[d.length.toLocaleString()+"件","集計件数"],["-",""]])+"<div class='sec'>カテゴリ平均価格 全件</div><button class='bcsv' onclick='dlCSV(8)'>⬇ CSV</button>"+tbl(d,cols);}}
  if(n===9){{var cols=CC.concat([N.売却日,N.即売,N.件数,N.単価]);var d=srt(cd,"平均売却秒数",true),t=d[0]||{{}};_saveCache(9,d,cols);return bk+krow([[time_fmt(t["平均売却秒数"]),"最速:"+[t.L1,t.L2,t.L3,t.L4,t.L5].filter(Boolean).join(">")],[time_fmt(Math.round(aavg(d,"平均売却秒数"))),"平均"],[d.length.toLocaleString()+"件","集計件数"],["-",""]])+"<div class='sec'>カテゴリ平均売却時間 全件</div><button class='bcsv' onclick='dlCSV(9)'>⬇ CSV</button>"+tbl(d,cols);}}
  if(n===10){{var cols=CC.concat([N.利益,N.利益率,N.単価,N.件数]);var d=srt(cd,"推定利益",false),t=d[0]||{{}};_saveCache(10,d,cols);return bk+krow([[yen(t["推定利益"]),"1位:"+[t.L1,t.L2,t.L3,t.L4,t.L5].filter(Boolean).join(">")],[pct(t["利益率"]),"1位利益率"],[d.length.toLocaleString()+"件","集計件数"],["-",""]])+"<div class='sec'>カテゴリ利益率 全件</div><button class='bcsv' onclick='dlCSV(10)'>⬇ CSV</button>"+tbl(d,cols);}}
  if(n>=11&&n<=15){{var cols=BC.concat([N.件数,N.需要,N.単価,N.利益,N.利益率,N.売却日,N.即売]);var sk={{11:"需要スコア",12:"平均売却秒数",13:"平均単価",14:"平均売却秒数",15:"推定利益"}};var sa={{11:false,12:true,13:false,14:true,15:false}};var st={{11:"売れる商品ランキング",12:"商品回転率",13:"商品平均売却価格",14:"商品平均売却時間",15:"商品利益率"}};var d=srt(bd,sk[n],sa[n]),t=d[0]||{{}};var kv=(n===12||n===14)?time_fmt(t["平均売却秒数"]):(n===13||n===15)?yen(n===13?t["平均単価"]:t["推定利益"]):(t["件数"]||0).toLocaleString()+"件";_saveCache(n,d,cols);return bk+krow([[kv,"1位:"+(t["ブランド"]||"-")],[d.length.toLocaleString()+"件","集計件数"],["-",""],["-",""]])+"<div class='sec'>"+st[n]+" 全件</div><button class='bcsv' onclick='dlCSV("+n+")'>⬇ CSV</button>"+tbl(d,cols);}}
  if(n>=16&&n<=20){{var cols=[{{k:"__r",lb:"Rank"}},{{k:"価格帯",lb:"価格帯"}},N.件数,N.単価,N.仕入,N.利益,N.利益率,N.売却日,N.即売,{{k:"損益分岐点",lb:"損益分岐点",f:yen,cl:"nr"}}];var bands=[["〜3,000円",0,3e3],["3,000〜5,000円",3e3,5e3],["5,000〜10,000円",5e3,1e4],["10,000〜30,000円",1e4,3e4],["30,000円〜",3e4,1e9]];var sk={{16:"件数",17:"平均売却秒数",18:"利益率",19:"件数",20:"件数"}};var sa={{16:false,17:true,18:false,19:false,20:false}};var st={{16:"売れる価格帯",17:"価格帯別回転率",18:"価格帯別利益率",19:"平均値下げ幅",20:"値下げ売却率"}};var d=srt(bands.map(function(b){{var g=raw.filter(function(r){{var p=parseFloat(r["平均単価"])||0;return p>=b[1]&&p<b[2];}});if(!g.length)return null;return{{価格帯:b[0],件数:ssum(g,"件数"),平均単価:Math.round(aavg(g,"平均単価")),推定仕入価格:Math.round(aavg(g,"推定仕入価格")),推定利益:Math.round(aavg(g,"推定利益/件")),利益率:parseFloat(aavg(g,"利益率").toFixed(1)),平均売却秒数:Math.round(aavg(g,"平均売却秒数")),即売れ率:parseFloat(aavg(g,"即売れ率").toFixed(1)),損益分岐点:Math.round(aavg(g,"損益分岐点"))}};}}).filter(Boolean),sk[n],sa[n]);var t=d[0]||{{}};_saveCache(n,d,cols);return krow([[t["価格帯"]||"-","1位価格帯"],[(t["件数"]||0).toLocaleString()+"件","件数"],[pct(t["利益率"]),"利益率"],[pct(t["即売れ率"]),"即売れ率"]])+"<div class='sec'>"+st[n]+" 全件</div><button class='bcsv' onclick='dlCSV("+n+")'>⬇ CSV</button>"+tbl(d,cols);}}
  if(n>=21&&n<=25){{var cols=[{{k:"__r",lb:"Rank"}},{{k:"キーワード",lb:"キーワード"}},{{k:"出現頻度",lb:"出現頻度",cl:"nr"}}];var m={{}};raw.forEach(function(r){{[r["ブランド"],r.L3,r.L4,r.L5].filter(Boolean).forEach(function(kw){{m[kw]=(m[kw]||0)+(parseInt(r["件数"])||1);}});}});var d=srt(Object.keys(m).map(function(k){{return{{キーワード:k,出現頻度:m[k]}};}}),"出現頻度",false);var t=d[0]||{{}};var st={{21:"タイトルKWランキング",22:"検索ヒットKW",23:"ブランド+商品名",24:"型番効果",25:"送料表記効果"}};_saveCache(n,d,cols);return bk+krow([[t["キーワード"]||"-","1位"],[(t["出現頻度"]||0).toLocaleString(),"出現頻度"],[d.length.toLocaleString()+"件","総KW数"],["-",""]])+"<div class='sec'>"+st[n]+" 全件</div><button class='bcsv' onclick='dlCSV("+n+")'>⬇ CSV</button>"+tbl(d,cols);}}
  if(n>=26&&n<=30){{var st={{26:"急上昇ブランド",27:"急上昇カテゴリ",28:"価格上昇商品",29:"売上急増商品",30:"月別売上"}};if(n===30){{var cols=[{{k:"__r",lb:"Rank"}},{{k:"月",lb:"月"}},N.件数,{{k:"推定売上",lb:"推定売上",f:yen,cl:"nr"}},{{k:"推定利益",lb:"推定利益",f:yen,cl:"nr"}},N.単価,{{k:"利益_件",lb:"利益/件",f:yen,cl:"nr"}}];var mm={{}};raw.forEach(function(r){{var mo=r["年月"]||"不明";if(!mm[mo])mm[mo]=[];mm[mo].push(r);}});var d=srt(Object.keys(mm).filter(function(k){{return k!=="不明";}}).map(function(mo){{var g=mm[mo];return{{月:mo,件数:ssum(g,"件数"),推定売上:Math.round(ssum(g,"件数")*aavg(g,"平均単価")),推定利益:Math.round(ssum(g,"件数")*aavg(g,"推定利益/件")),平均単価:Math.round(aavg(g,"平均単価")),利益_件:Math.round(aavg(g,"推定利益/件"))}};}}),"月",false);var t=d[0]||{{}};_saveCache(30,d,cols);return bk+krow([[t["月"]||"-","最新月"],[(t["件数"]||0).toLocaleString()+"件","件数"],[yen(t["推定売上"]),"推定売上"],[yen(t["推定利益"]),"推定利益"]])+"<div class='sec'>月別売上 全件</div><button class='bcsv' onclick='dlCSV(30)'>⬇ CSV</button>"+tbl(d,cols);}}var cols=BC.concat([N.件数,N.需要,N.単価,N.利益,N.利益率]);var d=(n===27)?srt(cd,"需要スコア",false):(n===28)?srt(bd,"平均単価",false):(n===29)?srt(bd,"仕入れスコア",false):srt(bd,"需要スコア",false);var t=d[0]||{{}};var kl0=(n===27)?[t.L1,t.L2,t.L3,t.L4,t.L5].filter(Boolean).join(">"):(t["ブランド"]||"-");var kv0=(n===28)?yen(t["平均単価"]):(t["件数"]||0).toLocaleString()+"件";_saveCache(n,d,cols);return bk+krow([[kv0,"1位:"+kl0],[d.length.toLocaleString()+"件","集計件数"],[yen(t["平均単価"]),"1位平均単価"],[pct(t["利益率"]),"1位利益率"]])+"<div class='sec'>"+st[n]+" 全件</div><button class='bcsv' onclick='dlCSV("+n+")'>⬇ CSV</button>"+tbl(d,cols);}}
  return"<div class='nodata'>分析 #"+n+" 準備中</div>";
}}
function genPurchase(){{
  var raw=getFiltered();if(!raw.length)return"<div class='nodata'>絞込条件に一致するデータがありません</div>";
  var cols=BC.concat([N.送料,N.件数,N.需要,N.単価,N.仕入,N.利益,N.利益率,N.売却日,N.即売,N.スコア]);
  var d=srt(grpBrand(raw),"仕入れスコア",false),t=d[0]||{{}},bk=badge();
  _saveCache("P",d,cols);
  return bk+krow([[d.length.toLocaleString()+"件","対象件数"],[yen(Math.round(aavg(d,"平均単価"))),"平均単価"],[yen(Math.round(aavg(d,"推定利益"))),"平均推定利益/件"],[pct(parseFloat(aavg(d,"利益率").toFixed(1))),"平均利益率"]])+"<div class='ins'><h4>💡 ポイント</h4><ul><li>仕入れスコア=件数×平均単価×利益率</li><li>売却時間はDBから実計算（0日の場合は時間表示）</li></ul></div><div class='sec'>仕入れ推奨 全件（仕入れスコア順）</div><button class='bcsv' onclick='dlCSV(0)'>⬇ CSV</button>"+tbl(d,cols);
}}
var CPOP=0;
function openPop(n){{CPOP=n;var parts=fparts();document.getElementById("pnum").textContent="#"+n;document.getElementById("ptitle").textContent=CARDS[n-1]?CARDS[n-1].t:"";document.getElementById("pper").textContent="📅 "+CUR_FROM+" 〜 "+CUR_TO;var fb=document.getElementById("pfb");if(parts.length){{fb.textContent="🔍 "+parts.join(" > ");fb.style.display="";}}else fb.style.display="none";document.getElementById("pbody").innerHTML="<div style='text-align:center;padding:40px;color:#4a5568;'>⟳ 集計中...</div>";document.getElementById("ov").classList.add("on");setTimeout(function(){{document.getElementById("pbody").innerHTML=genPop(n);}},8);}}
function openPurchase(){{CPOP="P";var parts=fparts();document.getElementById("pnum").textContent="P";document.getElementById("ptitle").textContent="仕入れ推奨ランキング";document.getElementById("pper").textContent="📅 "+CUR_FROM+" 〜 "+CUR_TO;var fb=document.getElementById("pfb");if(parts.length){{fb.textContent="🔍 "+parts.join(" > ");fb.style.display="";}}else fb.style.display="none";document.getElementById("pbody").innerHTML="<div style='text-align:center;padding:40px;color:#4a5568;'>⟳ 集計中...</div>";document.getElementById("ov").classList.add("on");setTimeout(function(){{document.getElementById("pbody").innerHTML=genPurchase();}},8);}}
function closePop(e){{if(!e||e.target===document.getElementById("ov"))document.getElementById("ov").classList.remove("on");}}
document.addEventListener("keydown",function(e){{if(e.key==="Escape")document.getElementById("ov").classList.remove("on");}});
function dlCSV(n){{
  var cacheKey=(n===0)?"P":n;
  var cache=_popCache[cacheKey];
  if(!cache||!cache.d||!cache.d.length){{alert("データなし：先にタイルを開いてください");return;}}
  var d=cache.d,cols=cache.cols;
  var NAMES={{0:"仕入れ推奨ランキング",1:"ブランド売上ランキング",2:"ブランド回転率",3:"ブランド平均売却価格",4:"ブランド平均売却時間",5:"ブランド利益率",6:"カテゴリ売上ランキング",7:"カテゴリ回転率",8:"カテゴリ平均価格",9:"カテゴリ平均売却時間",10:"カテゴリ利益率",11:"売れる商品ランキング",12:"商品回転率",13:"商品平均売却価格",14:"商品平均売却時間",15:"商品利益率",16:"売れる価格帯",17:"価格帯別回転率",18:"価格帯別利益率",19:"平均値下げ幅",20:"値下げ売却率",21:"タイトルKW",22:"検索ヒットKW",23:"ブランド商品名成約率",24:"型番効果",25:"送料表記効果",26:"急上昇ブランド",27:"急上昇カテゴリ",28:"価格上昇商品",29:"売上急増商品",30:"月別売上"}};
  var nm=NAMES[n]||("分析"+n);
  if(!window._dlSeq)window._dlSeq=0;window._dlSeq+=1;
  var fn=nm+"_Vol"+window._dlSeq+".csv";
  var headers=cols.filter(function(c){{return c.k!=="__r";}}).map(function(c){{return c.lb;}});
  var bom="\\uFEFF";
  var csv=bom+[headers].concat(d.map(function(r,i){{
    return cols.filter(function(c){{return c.k!=="__r";}}).map(function(c){{
      var v=r[c.k];
      if(v===undefined||v===null)v="";
      v=String(v);
      if(v.indexOf(",")>=0||v.indexOf('"')>=0)v='"'+v.replace(/"/g,'""')+'"';
      return v;
    }});
  }})).map(function(r){{return r.join(",");}}).join("\\n");
  try{{var blob=new Blob([csv],{{type:"text/csv;charset=utf-8"}});var url=URL.createObjectURL(blob);var a=document.createElement("a");a.href=url;a.download=fn;a.style.display="none";document.body.appendChild(a);a.click();setTimeout(function(){{URL.revokeObjectURL(url);document.body.removeChild(a);}},100);}}
  catch(e2){{var a=document.createElement("a");a.href="data:text/csv;charset=utf-8,"+encodeURIComponent(csv);a.download=fn;a.style.display="none";document.body.appendChild(a);a.click();setTimeout(function(){{document.body.removeChild(a);}},100);}}
}}
function showTab(n){{document.querySelectorAll(".tab").forEach(function(t){{t.classList.remove("on");}});document.querySelectorAll("nav button").forEach(function(b){{b.classList.remove("on");}});document.getElementById("tab"+n).classList.add("on");document.querySelectorAll("nav button")[n-1].classList.add("on");}}
var CARDS=[
  {{n:1,t:"ブランド売上ランキング",tab:1,d:"需要スコア降順"}},{{n:2,t:"ブランド回転率",tab:1,d:"売却時間昇順"}},
  {{n:3,t:"ブランド平均売却価格",tab:1,d:"平均単価降順"}},{{n:4,t:"ブランド平均売却時間",tab:1,d:"売却時間昇順"}},
  {{n:5,t:"ブランド利益率",tab:1,d:"推定利益/件降順"}},{{n:6,t:"カテゴリ売上ランキング",tab:2,d:"需要スコア降順"}},
  {{n:7,t:"カテゴリ回転率",tab:2,d:"売却時間昇順"}},{{n:8,t:"カテゴリ平均価格",tab:2,d:"平均単価降順"}},
  {{n:9,t:"カテゴリ平均売却時間",tab:2,d:"売却時間昇順"}},{{n:10,t:"カテゴリ利益率",tab:2,d:"推定利益降順"}},
  {{n:11,t:"売れる商品ランキング",tab:3,d:"需要スコア降順"}},{{n:12,t:"商品回転率",tab:3,d:"売却時間昇順"}},
  {{n:13,t:"商品平均売却価格",tab:3,d:"平均単価降順"}},{{n:14,t:"商品平均売却時間",tab:3,d:"売却時間昇順"}},
  {{n:15,t:"商品利益率",tab:3,d:"推定利益降順"}},{{n:16,t:"売れる価格帯ランキング",tab:4,d:"件数降順"}},
  {{n:17,t:"価格帯別回転率",tab:4,d:"売却時間昇順"}},{{n:18,t:"価格帯別利益率",tab:4,d:"利益率降順"}},
  {{n:19,t:"平均値下げ幅",tab:4,d:"価格帯別分析"}},{{n:20,t:"値下げ回数と売却率",tab:4,d:"価格帯別分析"}},
  {{n:21,t:"タイトルKWランキング",tab:5,d:"出現頻度降順"}},{{n:22,t:"検索ヒットKW",tab:5,d:"キーワード分析"}},
  {{n:23,t:"ブランド+商品名成約率",tab:5,d:"ブランド×カテゴリ"}},{{n:24,t:"型番あり vs なし",tab:5,d:"型番効果分析"}},
  {{n:25,t:"送料無料表記効果",tab:5,d:"送料別分析"}},{{n:26,t:"急上昇ブランド",tab:6,d:"需要スコア上位"}},
  {{n:27,t:"急上昇カテゴリ",tab:6,d:"カテゴリ需要スコア上位"}},{{n:28,t:"価格上昇商品",tab:6,d:"平均単価上位"}},
  {{n:29,t:"売上急増商品",tab:6,d:"仕入れスコア上位"}},{{n:30,t:"月別売上ランキング",tab:6,d:"月別集計"}}
];
function buildGrids(){{
  for(var tab=1;tab<=7;tab++){{
    var el=document.getElementById("g"+tab);if(!el)continue;var h="";
    if(tab===7)h='<div class="card" onclick="openPurchase()" style="border-color:#f0b429;background:linear-gradient(135deg,#fff9e6,#fff);"><div class="ctop"><span class="cnum" style="background:#e67700;">P</span><span class="ctitle" style="font-size:13px;">🏆 仕入れ推奨ランキング</span></div><div class="cdesc">仕入れスコアで絞込後データから総合評価</div></div>';
    CARDS.filter(function(c){{return c.tab===tab;}}).forEach(function(c){{h+='<div class="card" onclick="openPop('+c.n+')"><div class="ctop"><span class="cnum">#'+c.n+'</span><span class="ctitle">'+c.t+'</span></div><div class="cdesc">'+c.d+'</div></div>';}});
    el.innerHTML=h;
  }}
}}
var FULL_TREE={tree_json};
function loadCategory(l2,l3){{
  if(!FULL_TREE)return;
  var jsonFile=FULL_TREE["ファッション"]&&FULL_TREE["ファッション"][l2]&&FULL_TREE["ファッション"][l2][l3];
  if(!jsonFile)return;
  document.getElementById("sbar").innerHTML="⟳ データ読み込み中...";
  fetch(jsonFile).then(function(res){{return res.json();}}).then(function(data){{
    RAW=data.raw_data;CAT=data.tree;ALL_BRANDS=data.all_brands;
    CUR_L2=l2;CUR_L3=l3;CUR_L4="";CUR_L5="";CUR_L6="";
    var l1k=Object.keys(CAT);var l1=l1k.length?l1k[0]:"";
    if(l1&&CAT[l1]&&CAT[l1][l2]&&CAT[l1][l2][l3]){{
      var l4keys=Object.keys(CAT[l1][l2][l3]).sort();
      setSel("sl4",l4keys,l4keys.length===0);
    }}else{{setSel("sl4",[],true);}}
    setSel("sl5",[],true);setSel("sl6",[],true);
    DB_START=data.db_start_str;DB_END=data.db_end_str;
    document.getElementById("ib_period").textContent=data.db_start_disp+"〜"+data.db_end_disp;
    var today=new Date();
    var ymd=today.getFullYear()+"-"+String(today.getMonth()+1).padStart(2,"0")+"-"+String(today.getDate()).padStart(2,"0");
    document.getElementById("dfrom").value=DB_START;document.getElementById("dto").value=ymd;
    CUR_FROM=DB_START;CUR_TO=ymd;refreshUI();
  }}).catch(function(){{document.getElementById("sbar").innerHTML="❌ データ読み込み失敗";}});
}}
document.addEventListener("DOMContentLoaded",function(){{
  var today=new Date();
  var ymd=today.getFullYear()+"-"+String(today.getMonth()+1).padStart(2,"0")+"-"+String(today.getDate()).padStart(2,"0");
  document.getElementById("dfrom").value=DB_START;document.getElementById("dto").value=ymd;
  CUR_FROM=DB_START;CUR_TO=ymd;
  initSel();buildGrids();refreshUI();
}});
</script>
</body>
</html>"""

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
