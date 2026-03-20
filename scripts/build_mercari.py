#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_mercari.py
================
メルカリDBから 仕入判断AIページ（index_v20.html相当）を全自動生成する。

使い方:
  python build_mercari.py
  python build_mercari.py --db D:/mercari/data/mercari_full.db
  python build_mercari.py --db D:/path/to/db.db --out D:/mercari_github/html/mercari_Vol3.html

環境変数でも指定可:
  DB_PATH=D:/mercari/data/mercari_full.db

出力先省略時は --out-dir (デフォルト: D:\mercari_github\html\) に
mercari_VoLN.html を連番で出力する。
"""
import sqlite3, json, sys, os, re, argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# ============================================================
# 設定
# ============================================================
DEFAULT_DB = Path(r"D:\mercari\data\mercari_full.db")

# 送料マップ（変更禁止）
SHIPPING = {
    "靴":850,"バッグ":800,"時計":380,"アクセサリー":210,
    "帽子":450,"小物":380,"レッグウェア":210,"トップス":600,
    "パンツ":700,"スカート":700,"ジャケット・アウター":900,
    "スーツ":900,"スーツ・フォーマル・ドレス":850,"ワンピース":700,
    "セットアップ":850,
}
DEFAULT_SHIP = 600

def get_shipping(cat3):
    if not cat3: return DEFAULT_SHIP
    for k, v in SHIPPING.items():
        if k in str(cat3):
            return v
    return DEFAULT_SHIP

# ============================================================
# CLI引数
# ============================================================
parser = argparse.ArgumentParser(description="メルカリDB→HTML自動生成")
parser.add_argument("--db",  default=None, help="DBファイルパス")
parser.add_argument("--out", default=None, help="出力HTMLパス（省略時は自動連番）")
parser.add_argument("--out-dir", default=None, help="出力ディレクトリ")
parser.add_argument("--template", default=None, help="テンプレートHTMLパス")
args = parser.parse_args()

# DBパス決定
db_path = Path(args.db or os.environ.get("DB_PATH", "") or DEFAULT_DB)
if not db_path.exists():
    print(f"[ERROR] DBが見つかりません: {db_path}", file=sys.stderr)
    sys.exit(1)

# 出力パス決定
if args.out:
    out_path = Path(args.out)
else:
    out_dir = Path(args.out_dir) if args.out_dir else Path(__file__).parent.parent / "html"
    out_dir.mkdir(parents=True, exist_ok=True)
    # 連番カウンタ
    counter_file = out_dir / "mercari_counter.txt"
    vol = 1
    if counter_file.exists():
        try: vol = int(counter_file.read_text().strip()) + 1
        except: pass
    counter_file.write_text(str(vol))
    out_path = out_dir / f"mercari_Vol{vol}.html"

# テンプレートパス
tmpl_path = Path(args.template) if args.template else Path(__file__).parent.parent / "html" / "index_v20.html"

print(f"[BUILD] DB     : {db_path}")
print(f"[BUILD] 出力   : {out_path}")
print(f"[BUILD] テンプレ: {tmpl_path}")

# ============================================================
# DB接続
# ============================================================
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

def q(sql, params=()):
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"[WARN] SQL: {e} | {sql[:80]}", file=sys.stderr)
        return []

# テーブル・カラム自動検出
tables = [t['name'] for t in q("SELECT name FROM sqlite_master WHERE type='table'")]
main_table = next((t for t in ['items','mercari_items','products','data'] if t in tables), tables[0] if tables else None)
if not main_table:
    print("[ERROR] テーブルが見つかりません", file=sys.stderr)
    sys.exit(1)

cols = [c['name'] for c in q(f"PRAGMA table_info({main_table})")]
print(f"[INFO] テーブル={main_table}  カラム={cols[:15]}")

def fc(candidates):
    for c in candidates:
        if c in cols: return c
    return None

C = {
    'price':    fc(['price','selling_price','sell_price','売価']),
    'brand':    fc(['brand','brand_name','ブランド']),
    'cat1':     fc(['lv1','category1','cat1','l1','カテゴリ1','genre1']),
    'cat2':     fc(['lv2','category2','cat2','l2','カテゴリ2','genre2']),
    'cat3':     fc(['lv3','category3','cat3','l3','カテゴリ3','genre3']),
    'cat4':     fc(['lv4','category4','cat4','l4','カテゴリ4','genre4']),
    'cat5':     fc(['lv5','category5','cat5','l5','カテゴリ5','genre5']),
    'created':  fc(['created_dt','created_at','created','出品日','listed_at']),
    'updated':  fc(['updated_dt','updated_at','sold_at','updated','売却日']),
    'title':    fc(['title','name','item_name','商品名','product_name']),
}
print(f"[INFO] カラムマッピング: {C}")

p  = C['price']  or 'price'
br = C['brand']  or 'brand'
c1 = C['cat1']   or 'category1'
c2 = C['cat2']   or 'category2'
c3 = C['cat3']   or 'category3'
c4 = C['cat4']   or 'category4'
c5 = C['cat5']   or 'category5'
cr = C['created'] or 'created_at'
up = C['updated'] or 'updated_at'
ti = C['title']  or 'title'

# ============================================================
# 計算ヘルパー
# ============================================================
def ship(cat3):
    return get_shipping(cat3)

def buy_price(avg):
    return round(avg * 0.25)

def profit(avg, sh):
    return round(avg * 0.65 - sh)

def margin(avg, sh):
    pr = profit(avg, sh)
    return round(pr / avg * 100, 1) if avg > 0 else 0.0

def score(cnt, avg, sh):
    m = max(margin(avg, sh), 0)
    return int(cnt * avg * m / 100)

def bep(avg, sh):
    return buy_price(avg) + sh

# ============================================================
# DB期間取得
# ============================================================
period_r = q(f"SELECT MIN({cr}) as mn, MAX({up}) as mx FROM {main_table}")
DB_START = str(period_r[0]['mn'] or '')[:10]
DB_END   = str(period_r[0]['mx'] or '')[:10]
TOTAL    = q(f"SELECT COUNT(*) as n FROM {main_table}")[0]['n']
print(f"[INFO] 期間={DB_START}~{DB_END}  総件数={TOTAL:,}")

# バージョン文字列（ユニーク: 生成日時ベース）
_now = datetime.now()
VER = f"{DB_END}_{_now.strftime('%H%M%S')}"

# ============================================================
# 分析関数群
# ============================================================
print("[BUILD] 分析開始...")

SELL_SECONDS = f"(julianday({up}) - julianday({cr})) * 86400"
SELL_DAYS    = f"((julianday({up}) - julianday({cr})))"

def has_col(c):
    return c in cols

# 月ラベル
def month_label(row):
    d = str(row.get('mon',''))
    return d[:7] if d else ''

# ブランド売上ランキング（tmpl-1）
def brand_sales(limit=50):
    rows = q(f"""
        SELECT {br} as b,
               COALESCE({c1},'') as c1, COALESCE({c2},'') as c2,
               COALESCE({c3},'') as c3, COALESCE({c4},'') as c4,
               COALESCE({c5},'') as c5,
               COUNT(*) as cnt,
               ROUND(AVG(CAST({p} AS REAL)),0) as avg_p,
               ROUND(AVG((julianday({up})-julianday({cr}))),1) as avg_days,
               ROUND(100.0*SUM(CASE WHEN (julianday({up})-julianday({cr}))<=3 THEN 1 ELSE 0 END)/COUNT(*),1) as quick
        FROM {main_table}
        WHERE {br}!='' AND {br} IS NOT NULL AND CAST({p} AS REAL)>0
        GROUP BY {br},{c3}
        HAVING COUNT(*)>=3
        ORDER BY cnt*avg_p DESC LIMIT {limit}
    """)
    out = []
    for i,r in enumerate(rows,1):
        avg = int(r['avg_p'] or 0)
        sh  = ship(r['c3'])
        out.append({
            'rank':i,'brand':r['b'],
            'c1':r['c1'],'c2':r['c2'],'c3':r['c3'],'c4':r['c4'],'c5':r['c5'],
            'cnt':r['cnt'],'avg_p':avg,'buy':buy_price(avg),
            'profit':profit(avg,sh),'margin':margin(avg,sh),
            'days':r['avg_days'] or 0,'quick':r['quick'] or 0,
            'ship':sh,'bep':bep(avg,sh),'score':score(r['cnt'],avg,sh)
        })
    return out

# ブランド回転率（tmpl-2）
def brand_turnover(limit=30):
    rows = q(f"""
        SELECT {br} as b,
               COALESCE({c1},'') as c1,COALESCE({c2},'') as c2,
               COALESCE({c3},'') as c3,COALESCE({c4},'') as c4,COALESCE({c5},'') as c5,
               COUNT(*) as cnt,
               ROUND(AVG(CAST({p} AS REAL)),0) as avg_p,
               ROUND(AVG(julianday({up})-julianday({cr})),1) as avg_days,
               ROUND(100.0*SUM(CASE WHEN julianday({up})-julianday({cr})<=3 THEN 1 ELSE 0 END)/COUNT(*),1) as quick
        FROM {main_table}
        WHERE {br}!='' AND {br} IS NOT NULL
          AND julianday({up})>julianday({cr})
        GROUP BY {br},{c3}
        HAVING COUNT(*)>=1
        ORDER BY avg_days ASC LIMIT {limit}
    """)
    out = []
    for i,r in enumerate(rows,1):
        avg = int(r['avg_p'] or 0)
        sh  = ship(r['c3'])
        out.append({'rank':i,'brand':r['b'],
            'c1':r['c1'],'c2':r['c2'],'c3':r['c3'],'c4':r['c4'],'c5':r['c5'],
            'cnt':r['cnt'],'avg_p':avg,'buy':buy_price(avg),
            'profit':profit(avg,sh),'ship':sh,'bep':bep(avg,sh),
            'days':r['avg_days'] or 0,'quick':r['quick'] or 0})
    return out

# ブランド平均売却価格（tmpl-3）
def brand_avg_price(limit=30):
    rows = q(f"""
        SELECT {br} as b,
               COALESCE({c1},'') as c1,COALESCE({c2},'') as c2,
               COALESCE({c3},'') as c3,COALESCE({c4},'') as c4,COALESCE({c5},'') as c5,
               COUNT(*) as cnt,
               ROUND(AVG(CAST({p} AS REAL)),0) as avg_p
        FROM {main_table}
        WHERE {br}!='' AND {br} IS NOT NULL AND CAST({p} AS REAL)>0
        GROUP BY {br},{c3}
        HAVING COUNT(*)>=1
        ORDER BY avg_p DESC LIMIT {limit}
    """)
    out = []
    for i,r in enumerate(rows,1):
        avg = int(r['avg_p'] or 0)
        sh  = ship(r['c3'])
        out.append({'rank':i,'brand':r['b'],
            'c1':r['c1'],'c2':r['c2'],'c3':r['c3'],'c4':r['c4'],'c5':r['c5'],
            'avg_p':avg,'buy':buy_price(avg),'profit':profit(avg,sh),
            'margin':margin(avg,sh),'cnt':r['cnt'],'ship':sh,'bep':bep(avg,sh)})
    return out

# カテゴリ売上（tmpl-6）
def cat_sales(limit=20):
    rows = q(f"""
        SELECT COALESCE({c1},'') as c1,COALESCE({c2},'') as c2,
               COALESCE({c3},'') as c3,COALESCE({c4},'') as c4,COALESCE({c5},'') as c5,
               COUNT(*) as cnt,
               ROUND(AVG(CAST({p} AS REAL)),0) as avg_p,
               ROUND(AVG(julianday({up})-julianday({cr})),1) as avg_days,
               ROUND(100.0*SUM(CASE WHEN julianday({up})-julianday({cr})<=3 THEN 1 ELSE 0 END)/COUNT(*),1) as quick
        FROM {main_table}
        WHERE CAST({p} AS REAL)>0 AND {c3}!='' AND {c3} IS NOT NULL
        GROUP BY {c1},{c2},{c3},{c4},{c5}
        HAVING COUNT(*)>=100
        ORDER BY cnt DESC LIMIT {limit}
    """)
    out = []
    for i,r in enumerate(rows,1):
        avg = int(r['avg_p'] or 0)
        sh  = ship(r['c3'])
        out.append({'rank':i,
            'c1':r['c1'],'c2':r['c2'],'c3':r['c3'],'c4':r['c4'],'c5':r['c5'],
            'ship':sh,'cnt':r['cnt'],'avg_p':avg,'buy':buy_price(avg),
            'profit':profit(avg,sh),'margin':margin(avg,sh),
            'days':r['avg_days'] or 0,'quick':r['quick'] or 0,'bep':bep(avg,sh)})
    return out

# カテゴリ回転率（tmpl-7）
def cat_turnover(limit=20):
    rows = q(f"""
        SELECT COALESCE({c1},'') as c1,COALESCE({c2},'') as c2,
               COALESCE({c3},'') as c3,COALESCE({c4},'') as c4,COALESCE({c5},'') as c5,
               COUNT(*) as cnt,
               ROUND(AVG(CAST({p} AS REAL)),0) as avg_p,
               ROUND(AVG(julianday({up})-julianday({cr})),1) as avg_days,
               ROUND(100.0*SUM(CASE WHEN julianday({up})-julianday({cr})<=3 THEN 1 ELSE 0 END)/COUNT(*),1) as quick
        FROM {main_table}
        WHERE CAST({p} AS REAL)>0 AND julianday({up})>julianday({cr})
        GROUP BY {c1},{c2},{c3},{c4},{c5}
        HAVING COUNT(*)>=50
        ORDER BY avg_days ASC LIMIT {limit}
    """)
    out = []
    for i,r in enumerate(rows,1):
        avg = int(r['avg_p'] or 0)
        sh  = ship(r['c3'])
        out.append({'rank':i,
            'c1':r['c1'],'c2':r['c2'],'c3':r['c3'],'c4':r['c4'],'c5':r['c5'],
            'ship':sh,'cnt':r['cnt'],'avg_p':avg,'buy':buy_price(avg),
            'profit':profit(avg,sh),'margin':margin(avg,sh),
            'days':r['avg_days'] or 0,'quick':r['quick'] or 0,'bep':bep(avg,sh)})
    return out

# 価格帯別（tmpl-16〜20）
def price_bands():
    bands = [('~3,000円','0','3000'),('3,001~6,000円','3001','6000'),
             ('6,001~10,000円','6001','10000'),('10,001~30,000円','10001','30000'),
             ('30,001円~','30001','9999999')]
    out = []
    for label,lo,hi in bands:
        rows = q(f"""
            SELECT COUNT(*) as cnt,
                   ROUND(AVG(CAST({p} AS REAL)),0) as avg_p,
                   ROUND(100.0*COUNT(*)/({TOTAL}),1) as pct,
                   ROUND(AVG(julianday({up})-julianday({cr})),1) as avg_days,
                   ROUND(100.0*SUM(CASE WHEN julianday({up})-julianday({cr})<=3 THEN 1 ELSE 0 END)/COUNT(*),1) as quick
            FROM {main_table}
            WHERE CAST({p} AS REAL)>={lo} AND CAST({p} AS REAL)<={hi}
        """)
        r = rows[0] if rows else {}
        avg = int(r.get('avg_p') or 0)
        sh  = 600
        out.append({'label':label,'cnt':r.get('cnt',0),'pct':r.get('pct',0),
                    'avg_p':avg,'buy':buy_price(avg),'bep':bep(avg,sh),
                    'profit':profit(avg,sh),'margin':margin(avg,sh),
                    'days':r.get('avg_days',0),'quick':r.get('quick',0)})
    return out

# キーワード（tmpl-21/22）
def keywords(limit=50):
    if not C['title']: return []
    rows = q(f"""
        SELECT {ti} as t FROM {main_table}
        WHERE {ti} IS NOT NULL AND {ti}!=''
        LIMIT 50000
    """)
    freq = defaultdict(int)
    stop = {'の','を','に','は','が','で','と','から','まで','も','や','こと','これ','あ','い','う','え','お'}
    for r in rows:
        words = re.split(r'[\s　・/【】「」（）()]+', str(r['t']))
        for w in words:
            w = w.strip()
            if len(w) >= 2 and w not in stop:
                freq[w] += 1
    sorted_kw = sorted(freq.items(), key=lambda x:-x[1])[:limit]
    return [{'rank':i+1,'keyword':k,'freq':v} for i,(k,v) in enumerate(sorted_kw)]

# 月別売上（tmpl-30）
def monthly(limit=15):
    rows = q(f"""
        SELECT strftime('%Y-%m', {up}) as mon,
               COUNT(*) as cnt,
               ROUND(SUM(CAST({p} AS REAL)),0) as total_sales,
               ROUND(AVG(CAST({p} AS REAL)),0) as avg_p
        FROM {main_table}
        WHERE {up} IS NOT NULL AND CAST({p} AS REAL)>0
        GROUP BY mon
        ORDER BY mon DESC LIMIT {limit}
    """)
    out = []
    for r in rows:
        avg = int(r['avg_p'] or 0)
        sh = 600
        pr = profit(avg, sh)
        out.append({'mon':r['mon'],'cnt':r['cnt'],
                    'total':int(r['total_sales'] or 0),
                    'est_profit':int(r['cnt']*pr),
                    'avg_p':avg,'profit_per':pr})
    return out

# 仕入れスコア（purchase）
def purchase_score(limit=30):
    rows = q(f"""
        SELECT {br} as b,
               COALESCE({c3},'') as c3,
               COUNT(*) as cnt,
               ROUND(AVG(CAST({p} AS REAL)),0) as avg_p,
               ROUND(AVG(julianday({up})-julianday({cr})),1) as avg_days,
               ROUND(100.0*SUM(CASE WHEN julianday({up})-julianday({cr})<=3 THEN 1 ELSE 0 END)/COUNT(*),1) as quick
        FROM {main_table}
        WHERE {br}!='' AND {br} IS NOT NULL AND CAST({p} AS REAL)>0
        GROUP BY {br},{c3}
        HAVING COUNT(*)>=3
        ORDER BY cnt*avg_p DESC LIMIT 200
    """)
    scored = []
    for r in rows:
        avg = int(r['avg_p'] or 0)
        sh  = ship(r['c3'])
        sc  = score(r['cnt'], avg, sh)
        scored.append({'brand':r['b'],'cat':r['c3'],'ship':sh,
                       'cnt':r['cnt'],'avg_p':avg,'buy':buy_price(avg),
                       'profit':profit(avg,sh),'margin':margin(avg,sh),'score':sc})
    scored.sort(key=lambda x:-x['score'])
    for i,r in enumerate(scored[:limit],1):
        r['rank'] = i
    return scored[:limit]

# モメンタム（tmpl-m-top20）
def momentum(limit=20):
    # 直近3ヶ月 vs 前3ヶ月
    rows = q(f"""
        SELECT {br} as b,
               COALESCE({c3},'') as c3,
               COUNT(*) as total,
               SUM(CASE WHEN {up} >= date('now','-3 months') THEN 1 ELSE 0 END) as recent,
               SUM(CASE WHEN {up} < date('now','-3 months') AND {up} >= date('now','-6 months') THEN 1 ELSE 0 END) as prev,
               ROUND(AVG(CAST({p} AS REAL)),0) as avg_p,
               ROUND(AVG(julianday({up})-julianday({cr})),1) as avg_days,
               ROUND(100.0*SUM(CASE WHEN julianday({up})-julianday({cr})<=3 THEN 1 ELSE 0 END)/COUNT(*),1) as quick
        FROM {main_table}
        WHERE {br}!='' AND {br} IS NOT NULL AND CAST({p} AS REAL)>0
        GROUP BY {br},{c3}
        HAVING recent>=3 AND (recent+prev)>0
        ORDER BY CAST(recent AS REAL)/(recent+prev) DESC
        LIMIT {limit}
    """)
    out = []
    for i,r in enumerate(rows,1):
        avg  = int(r['avg_p'] or 0)
        sh   = ship(r['c3'])
        rec  = r['recent'] or 0
        prv  = r['prev']   or 0
        mom  = round(rec/(rec+prv),3) if (rec+prv)>0 else 0
        growth = round(rec/prv,1) if prv>0 else float(rec*10)
        vel  = round(rec/90, 2)
        out.append({'rank':i,'brand':r['b'],'cat':r['c3'],
                    'momentum':mom,'recent':rec,'prev':prv,
                    'growth':growth,'avg_p':avg,'profit':profit(avg,sh),
                    'vel':vel,'days':r['avg_days'] or 0,'quick':r['quick'] or 0})
    return out

# ============================================================
# HTML生成ヘルパー
# ============================================================
def h(v):
    """HTML文字エスケープ"""
    return str(v).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;')

def yen(v):
    try: return f"¥{int(v):,}"
    except: return f"¥{v}"

def pct(v):
    try: return f"{float(v):.1f}%"
    except: return f"{v}%"

def days(v):
    try: return f"{float(v):.1f}日"
    except: return f"{v}日"

def nr(v): return f'<td class="nr">{h(v)}</td>'
def nrY(v): return f'<td class="nr">{yen(v)}</td>'
def nrP(v): return f'<td class="nr">{pct(v)}</td>'
def nrD(v): return f'<td class="nr">{days(v)}</td>'

def cat_attrs(r):
    attrs = ''
    for lv, key in [('lv1','c1'),('lv2','c2'),('lv3','c3'),('lv4','c4'),('lv5','c5')]:
        v = r.get(key,'')
        if v: attrs += f' data-{lv}="{h(v)}"'
    return attrs

def kpi_block(items):
    html = '<div class="kr">'
    for val, label in items:
        html += f'<div class="kpi"><div class="kv">{h(val)}</div><div class="kl">{h(label)}</div></div>'
    html += '</div>'
    return html

def table_head(*cols):
    html = '<table class="dt"><thead><tr>'
    for c in cols: html += f'<th>{h(c)}</th>'
    html += '</tr></thead><tbody>'
    return html

# ============================================================
# テンプレート生成（各tmpl）
# ============================================================
print("[BUILD] テンプレートHTML生成中...")

def gen_brand_sales():
    data = brand_sales(50)
    top  = data[0] if data else {}
    html = kpi_block([
        (f"{top.get('cnt',0)}件", f"1位: {top.get('brand','')}"),
        (yen(top.get('avg_p',0)), "平均単価"),
        (yen(top.get('profit',0)), "推定利益/件"),
        (pct(top.get('margin',0)), "利益率"),
    ])
    html += '<div class="sec">ブランド売上ランキング 全件表示</div>'
    html += table_head('Rank','ブランド','カテゴリL1','カテゴリL2','カテゴリL3','カテゴリL4','カテゴリL5','件数','需要スコア','平均単価','推定仕入価格','推定利益/件','利益率','平均売却日数','即売れ率','仕入れスコア','送料','損益分岐点')
    for r in data:
        html += f'<tr{cat_attrs(r)}>'
        html += nr(r['rank'])+nr(r['brand'])+nr(r['c1'])+nr(r['c2'])+nr(r['c3'])+nr(r['c4'])+nr(r['c5'])
        html += nr(r['cnt'])+nr(r['cnt'])+nrY(r['avg_p'])+nrY(r['buy'])+nrY(r['profit'])+nrP(r['margin'])
        html += nrD(r['days'])+nrP(r['quick'])+nr(r['score'])+nrY(r['ship'])+nrY(r['bep'])
        html += '</tr>'
    html += '</tbody></table>'
    return html

def gen_brand_turnover():
    data = brand_turnover(30)
    top  = data[0] if data else {}
    html = kpi_block([
        (days(top.get('days',0)), f"最速: {top.get('brand','')}"),
        (yen(top.get('avg_p',0)), "平均単価"),
        (pct(top.get('quick',0)), "即売れ率"),
        (yen(top.get('ship',0)), "送料"),
    ])
    html += '<div class="sec">ブランド回転率 全件表示</div>'
    html += table_head('Rank','ブランド','カテゴリL1','カテゴリL2','カテゴリL3','カテゴリL4','カテゴリL5','平均売却日数','即売れ率','件数','平均単価','推定仕入価格','推定利益/件','送料','損益分岐点')
    for r in data:
        html += f'<tr{cat_attrs(r)}>'
        html += nr(r['rank'])+nr(r['brand'])+nr(r['c1'])+nr(r['c2'])+nr(r['c3'])+nr(r['c4'])+nr(r['c5'])
        html += nrD(r['days'])+nrP(r['quick'])+nr(r['cnt'])+nrY(r['avg_p'])+nrY(r['buy'])+nrY(r['profit'])+nrY(r['ship'])+nrY(r['bep'])
        html += '</tr>'
    html += '</tbody></table>'
    return html

def gen_brand_avg_price():
    data = brand_avg_price(30)
    top  = data[0] if data else {}
    html = kpi_block([
        (yen(top.get('avg_p',0)), f"1位: {top.get('brand','')}"),
        (yen(top.get('buy',0)), "推定仕入価格"),
        (yen(top.get('profit',0)), "推定利益/件"),
        (pct(top.get('margin',0)), "利益率"),
    ])
    html += '<div class="sec">ブランド平均売却価格 全件表示</div>'
    html += table_head('Rank','ブランド','カテゴリL1','カテゴリL2','カテゴリL3','カテゴリL4','カテゴリL5','平均単価','推定仕入価格','推定利益/件','利益率','件数','送料','損益分岐点')
    for r in data:
        html += f'<tr{cat_attrs(r)}>'
        html += nr(r['rank'])+nr(r['brand'])+nr(r['c1'])+nr(r['c2'])+nr(r['c3'])+nr(r['c4'])+nr(r['c5'])
        html += nrY(r['avg_p'])+nrY(r['buy'])+nrY(r['profit'])+nrP(r['margin'])+nr(r['cnt'])+nrY(r['ship'])+nrY(r['bep'])
        html += '</tr>'
    html += '</tbody></table>'
    return html

def gen_cat_sales():
    data = cat_sales(20)
    top  = data[0] if data else {}
    html = kpi_block([
        (f"{top.get('cnt',0)}件", f"1位: {top.get('c2','')}"),
        (yen(top.get('avg_p',0)), "平均単価"),
        (yen(top.get('profit',0)), "推定利益/件"),
        (pct(top.get('margin',0)), "利益率"),
    ])
    html += '<div class="sec">カテゴリ売上 全件表示</div>'
    html += table_head('Rank','カテゴリL1','カテゴリL2','カテゴリL3','カテゴリL4','カテゴリL5','送料','件数','需要スコア','平均単価','推定仕入価格','推定利益/件','利益率','平均売却日数','即売れ率','損益分岐点')
    for r in data:
        html += f'<tr{cat_attrs(r)}>'
        html += nr(r['rank'])+nr(r['c1'])+nr(r['c2'])+nr(r['c3'])+nr(r['c4'])+nr(r['c5'])
        html += nrY(r['ship'])+nr(r['cnt'])+nr(r['cnt'])+nrY(r['avg_p'])+nrY(r['buy'])
        html += nrY(r['profit'])+nrP(r['margin'])+nrD(r['days'])+nrP(r['quick'])+nrY(r['bep'])
        html += '</tr>'
    html += '</tbody></table>'
    return html

def gen_cat_turnover():
    data = cat_turnover(20)
    top  = data[0] if data else {}
    html = kpi_block([
        (days(top.get('days',0)), f"最速: {top.get('c2','')}"),
        (yen(top.get('avg_p',0)), "平均単価"),
        (pct(top.get('quick',0)), "即売れ率"),
        (yen(top.get('ship',0)), "送料"),
    ])
    html += '<div class="sec">カテゴリ回転率 全件表示</div>'
    html += table_head('Rank','カテゴリL1','カテゴリL2','カテゴリL3','カテゴリL4','カテゴリL5','送料','件数','需要スコア','平均単価','推定仕入価格','推定利益/件','利益率','平均売却日数','即売れ率','損益分岐点')
    for r in data:
        html += f'<tr{cat_attrs(r)}>'
        html += nr(r['rank'])+nr(r['c1'])+nr(r['c2'])+nr(r['c3'])+nr(r['c4'])+nr(r['c5'])
        html += nrY(r['ship'])+nr(r['cnt'])+nr(r['cnt'])+nrY(r['avg_p'])+nrY(r['buy'])
        html += nrY(r['profit'])+nrP(r['margin'])+nrD(r['days'])+nrP(r['quick'])+nrY(r['bep'])
        html += '</tr>'
    html += '</tbody></table>'
    return html

def gen_price_bands():
    data = price_bands()
    top  = data[0] if data else {}
    html = kpi_block([
        (f"{top.get('cnt',0):,}件", f"最多: {top.get('label','')}"),
        (yen(top.get('avg_p',0)), "平均単価"),
        (yen(top.get('profit',0)), "推定利益/件"),
        (pct(top.get('margin',0)), "利益率"),
    ])
    html += '<div class="sec">価格帯別売上</div>'
    html += table_head('価格帯','件数','構成比','平均単価','推定仕入価格','損益分岐点','推定利益/件','利益率','平均売却日数','即売れ率')
    for r in data:
        html += '<tr>'
        html += f'<td>{r["label"]}</td>'+nr(f"{r['cnt']:,}")+nrP(r['pct'])
        html += nrY(r['avg_p'])+nrY(r['buy'])+nrY(r['bep'])+nrY(r['profit'])+nrP(r['margin'])+nrD(r['days'])+nrP(r['quick'])
        html += '</tr>'
    html += '</tbody></table>'
    return html

def gen_keywords():
    data = keywords(50)
    top4 = data[:4] if data else []
    kpi_items = [(str(r['freq']), f"{r['rank']}位: {r['keyword']}") for r in top4]
    if len(kpi_items) < 4:
        kpi_items += [('','')]*( 4-len(kpi_items))
    html = kpi_block(kpi_items)
    html += '<div class="sec">キーワードランキング 全件表示</div>'
    html += table_head('Rank','キーワード','出現頻度')
    for r in data:
        html += f'<tr><td>{r["rank"]}</td><td>{h(r["keyword"])}</td><td class="nr">{r["freq"]:,}</td></tr>'
    html += '</tbody></table>'
    return html

def gen_monthly():
    data = monthly(15)
    top  = data[0] if data else {}
    html = kpi_block([
        (top.get('mon',''), "最新月"),
        (f"{top.get('cnt',0):,}件", "件数"),
        (yen(top.get('avg_p',0)), "平均単価"),
        (yen(top.get('profit_per',0)), "推定利益/件"),
    ])
    html += '<div class="sec">月別売上</div>'
    html += table_head('月','件数','推定売上','推定利益','平均単価','推定利益/件')
    for r in data:
        html += '<tr>'
        html += f'<td>{r["mon"]}</td>'+nr(f"{r['cnt']:,}")+nrY(r['total'])+nrY(r['est_profit'])
        html += nrY(r['avg_p'])+nrY(r['profit_per'])
        html += '</tr>'
    html += '</tbody></table>'
    return html

def gen_purchase():
    data = purchase_score(30)
    top  = data[0] if data else {}
    html = kpi_block([
        (f"{TOTAL:,}件", "分析対象"),
        (yen(top.get('avg_p',0)), "平均単価"),
        (yen(top.get('profit',0)), "平均推定利益/件"),
        (pct(top.get('margin',0)), "平均利益率"),
    ])
    html += '<div class="ins"><h4>💡 インサイト</h4><ul>'
    html += f'<li>DB全件 {TOTAL:,}件から仕入れスコア上位を表示</li>'
    html += '<li>仕入れスコア = 件数 × 平均単価 × 利益率（指示書準拠）</li>'
    html += '</ul></div>'
    html += '<div class="sec">仕入れ推奨 全件表示（実績データ順）</div>'
    html += f'<button class="cb" onclick="dlCSV(\'purchase_score\')">⬇ CSVダウンロード</button>'
    html += table_head('Rank','ブランド','カテゴリ','送料','件数','需要スコア','平均単価','推定仕入価格','推定利益/件','利益率','仕入れスコア')
    for r in data:
        html += '<tr>'
        html += f'<td><span class="rb">{r["rank"]}</span></td>'
        html += nr(r['brand'])+nr(r['cat'])+nrY(r['ship'])+nr(r['cnt'])+nr(r['cnt'])
        html += nrY(r['avg_p'])+nrY(r['buy'])+nrY(r['profit'])+nrP(r['margin'])+nr(f"{r['score']:,}")
        html += '</tr>'
    html += '</tbody></table>'
    return html

def gen_momentum():
    data = momentum(20)
    top  = data[0] if data else {}
    html = kpi_block([
        (f"{TOTAL:,}件", "分析対象"),
        (f"{len(data)}商品", "🔴 爆発需要"),
        (str(top.get('momentum',0)), "平均Momentum"),
        (f"{top.get('growth',0)}倍", "最高成長率"),
    ])
    html += '<div class="sec">モメンタムランキング 全件表示（Momentum順）</div>'
    html += f'<button class="cb" onclick="dlCSV(\'momentum_top20\')">⬇ CSVダウンロード</button>'
    html += table_head('Rank','ブランド','カテゴリ','Momentum','評価','直近3M','前3M','成長率','平均単価','推定利益/件','売れ速度(件/日)','平均売却日数','即売れ率')
    for r in data:
        mom = r['momentum']
        if mom >= 0.7:   eval_label = '<span class="gb-ex">🔴 爆発需要</span>'
        elif mom >= 0.4: eval_label = '<span class="gb-st">🟠 強需要</span>'
        elif mom >= 0.2: eval_label = '<span class="gb-nm">🟡 普通</span>'
        else:            eval_label = '<span class="gb-lw">⚪ 低需要</span>'
        bar_pct = int(mom*100)
        bar_html = f'<div class="mbar"><div class="mfill" style="width:{bar_pct}%;background:#ef4444"></div><span>{mom}</span></div>'
        html += '<tr>'
        html += f'<td><span class="rb">{r["rank"]}</span></td>'
        html += nr(r['brand'])+nr(r['cat'])
        html += f'<td>{bar_html}</td><td>{eval_label}</td>'
        html += nr(r['recent'])+nr(r['prev'])+nr(f"{r['growth']}倍")
        html += nrY(r['avg_p'])+nrY(r['profit'])+nr(r['vel'])+nrD(r['days'])+nrP(r['quick'])
        html += '</tr>'
    html += '</tbody></table>'
    return html

# ============================================================
# テンプレートHTMLへの埋め込み
# ============================================================
print(f"[BUILD] テンプレート読み込み: {tmpl_path}")

if tmpl_path.exists():
    with open(tmpl_path, encoding='utf-8') as f:
        base_html = f.read()
else:
    print(f"[WARN] テンプレートなし。スタンドアロンHTMLを生成します。", file=sys.stderr)
    base_html = None

# テンプレートがある場合: tmpl-N の中身を差し替え
# テンプレートがない場合: 最小スタンドアロンを生成

TMPL_MAP = {
    1:  gen_brand_sales,
    2:  gen_brand_turnover,
    3:  gen_brand_avg_price,
    # 4,5はブランド平均売却日数/利益率（3,2と同構造）
    6:  gen_cat_sales,
    7:  gen_cat_turnover,
    # 8-10はcat_avg_price/days/margin（6と同構造）
    16: gen_price_bands,
    17: gen_price_bands,
    18: gen_price_bands,
    19: gen_price_bands,
    20: gen_price_bands,
    21: gen_keywords,
    22: gen_keywords,
    30: gen_monthly,
}

if base_html:
    # ①  <script type="text/x-template" id="tmpl-N"> ... </script> を差し替え
    for tmpl_id, gen_fn in TMPL_MAP.items():
        try:
            content = gen_fn()
            pattern = rf'(<script type="text/x-template" id="tmpl-{tmpl_id}">)(.*?)(</script>)'
            replacement = rf'\g<1>{content}\g<3>'
            base_html = re.sub(pattern, replacement, base_html, flags=re.DOTALL)
        except Exception as e:
            print(f"[WARN] tmpl-{tmpl_id} 生成失敗: {e}", file=sys.stderr)

    # ② purchase / momentum テンプレートも差し替え
    try:
        content = gen_purchase()
        pattern = r'(<script type="text/x-template" id="tmpl-purchase">)(.*?)(</script>)'
        base_html = re.sub(pattern, rf'\g<1>{content}\g<3>', base_html, flags=re.DOTALL)
    except Exception as e:
        print(f"[WARN] tmpl-purchase 生成失敗: {e}", file=sys.stderr)

    try:
        content = gen_momentum()
        pattern = r'(<script type="text/x-template" id="tmpl-m-top20">)(.*?)(</script>)'
        base_html = re.sub(pattern, rf'\g<1>{content}\g<3>', base_html, flags=re.DOTALL)
    except Exception as e:
        print(f"[WARN] tmpl-m-top20 生成失敗: {e}", file=sys.stderr)

    # ③ ヘッダー変数更新
    base_html = re.sub(r'var DB_END\s*=\s*"[^"]*"', f'var DB_END   ="{DB_END}"', base_html)
    base_html = re.sub(r'var CUR_FROM\s*=\s*"[^"]*"', f'var CUR_FROM="{DB_START}"', base_html)
    base_html = re.sub(r'var CUR_TO\s*=\s*"[^"]*"', f'var CUR_TO  ="{DB_END}"', base_html)

    # ④ タイトル更新
    base_html = re.sub(
        r'<title>[^<]*</title>',
        f'<title>メルカリ 仕入判断AI {VER}</title>',
        base_html
    )

    out_html = base_html

else:
    # スタンドアロン最小HTML
    out_html = f"""<!DOCTYPE html>
<html lang="ja"><head>
<meta charset="UTF-8">
<title>メルカリ 仕入判断AI {VER}</title>
<style>body{{font-family:sans-serif;background:#0b0e18;color:#e0e4f0;padding:20px;}}
h2{{color:#e74c3c;}} table{{border-collapse:collapse;width:100%;font-size:12px;}}
th{{background:#1c2038;color:#7880a0;padding:8px;text-align:left;border-bottom:1px solid #2a2f4a;}}
td{{padding:7px;border-bottom:1px solid #1c2038;}}
.nr{{text-align:right;color:#ff6b6b;font-weight:bold;}}
</style></head><body>
<h1>メルカリ 仕入判断AI {VER}</h1>
<p>DB: {DB_START} 〜 {DB_END} ｜ 総件数: {TOTAL:,}</p>
<h2>仕入れスコアランキング</h2>{gen_purchase()}
<h2>モメンタム</h2>{gen_momentum()}
<h2>月別売上</h2>{gen_monthly()}
</body></html>"""

# ============================================================
# 出力
# ============================================================
out_path.parent.mkdir(parents=True, exist_ok=True)
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(out_html)

size_mb = out_path.stat().st_size / 1024 / 1024
print(f"\n[DONE] 出力完了: {out_path}")
print(f"[DONE] ファイルサイズ: {size_mb:.2f} MB")
print(f"[DONE] DB期間: {DB_START} 〜 {DB_END}  総件数: {TOTAL:,}")
