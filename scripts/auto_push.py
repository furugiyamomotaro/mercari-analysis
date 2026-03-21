#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess, sys, os, logging, argparse, json, sqlite3
import urllib.request
from pathlib import Path
from datetime import datetime

# ============================================================
# 1. 基本設定（ここだけ確認してください）
# ============================================================
SUPABASE_URL = "https://tikqwmpclunseegqksnk.supabase.co"
SUPABASE_KEY = "ここにあなたのanon_keyを貼り付けてください"
TABLE_NAME   = "商品"  # Supabase側のテーブル名

ROOT      = Path(r"D:\mercari_github")
DB_PATH   = Path(r"D:\mercari\data\mercari_full.db")
HTML_OUT  = ROOT / "html" / "index.html"
BUILD_PY  = ROOT / "scripts" / "build_mercari.py"
LOG_FILE  = ROOT / "logs" / "auto_push.log"

# ============================================================
# 2. ログ設定
# ============================================================
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

def run(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=str(ROOT))

# ============================================================
# Step 1: HTML生成
# ============================================================
def step_build(limit):
    log.info(f"─── Step 1: HTML生成 ({limit}件) ───")
    cmd = f'"{sys.executable}" "{BUILD_PY}" --db "{DB_PATH}" --out "{HTML_OUT}" --limit {limit}'
    r = run(cmd)
    if r.returncode != 0:
        log.error(f"HTML生成失敗: {r.stderr[:200]}")
        return False
    log.info("  ✅ 完了")
    return True

# ============================================================
# Step 2: Supabase自動同期 (テーブル名自動検知)
# ============================================================
def step_supabase_sync(limit):
    log.info("─── Step 2: Supabase自動同期 ───")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 【自動化】DB内のテーブル名を自動で取得する
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = cursor.fetchall()
        if not tables:
            log.error("  ❌ DB内にテーブルが見つかりません")
            return False
        
        real_table_name = tables[0]['name'] # 最初に見つかったテーブルを使用
        log.info(f"  検出されたテーブル名: {real_table_name}")

        # データ取得
        cursor.execute(f"SELECT * FROM {real_table_name} LIMIT {limit}")
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()

        if not rows:
            log.warning("  送るデータがありません")
            return True

        # API送信
        url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }
        
        req = urllib.request.Request(url, data=json.dumps(rows).encode("utf-8"), headers=headers, method="POST")
        with urllib.request.urlopen(req) as res:
            if res.status in [200, 201, 204]:
                log.info(f"  ✅ {len(rows)}件をSupabaseへ同期完了")
                return True
    except Exception as e:
        log.error(f"  ❌ 同期エラー: {e}")
        return False

# ============================================================
# Step 3: GitHub反映
# ============================================================
def step_git():
    log.info("─── Step 3: GitHub保存 ───")
    run("git add -A")
    run(f'git commit -m "Auto Update {datetime.now().strftime("%H:%M")}"')
    if run("git push").returncode == 0:
        log.info("  ✅ GitHub完了")
        return True
    log.error("  ❌ Push失敗")
    return False

# ============================================================
# メイン
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    log.info("🚀 完全自動更新開始")
    if step_build(args.limit):
        step_supabase_sync(args.limit)
        step_git()
    log.info("🏁 すべて完了")

if __name__ == "__main__":
    main()