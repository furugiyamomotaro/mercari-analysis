#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess, sys, os, logging, argparse, json, sqlite3
import urllib.request
from pathlib import Path
from datetime import datetime

# ============================================================
# 1. 基本設定
# ============================================================
SUPABASE_URL = "https://tikqwmpclunseegqksnk.supabase.co"
SUPABASE_KEY = "あなたのanon_keyをここに貼り付け"
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
# Step 2: Supabase同期（日本語エラー対策済み）
# ============================================================
def step_supabase_sync(limit):
    log.info("─── Step 2: Supabase自動同期 ───")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 先ほど検出された 'sold_items' からデータを取得
        real_table_name = "sold_items"
        log.info(f"  DBテーブル '{real_table_name}' から読み込み中...")

        cursor.execute(f"SELECT * FROM {real_table_name} LIMIT {limit}")
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()

        if not rows:
            log.warning("  送るデータがありません")
            return True

        # 日本語を送れるように json.dumps で ensure_ascii=False を指定
        json_data = json.dumps(rows, ensure_ascii=False).encode("utf-8")

        # URLも日本語（「商品」など）が含まれる場合はエンコードが必要
        safe_table_name = urllib.parse.quote(TABLE_NAME)
        url = f"{SUPABASE_URL}/rest/v1/{safe_table_name}"

        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }
        
        req = urllib.request.Request(url, data=json_data, headers=headers, method="POST")
        with urllib.request.urlopen(req) as res:
            if res.status in [200, 201, 204]:
                log.info(f"  ✅ {len(rows)}件を同期完了")
                return True
    except Exception as e:
        log.error(f"  ❌ 同期エラー: {e}")
        return False

# ============================================================
# メイン
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    log.info("🚀 更新プロセス開始")
    # Step 1: HTML作成
    cmd_build = f'"{sys.executable}" "{BUILD_PY}" --db "{DB_PATH}" --out "{HTML_OUT}" --limit {args.limit}'
    if run(cmd_build).returncode == 0:
        log.info("  ✅ Step 1: HTML完了")
        # Step 2: Supabase同期
        step_supabase_sync(args.limit)
        # Step 3: GitHub保存
        run("git add -A")
        run(f'git commit -m "Auto Update {datetime.now().strftime("%H:%M")}"')
        if run("git push").returncode == 0:
            log.info("  ✅ Step 3: GitHub完了")
    
    log.info("🏁 すべての工程が終了しました")

if __name__ == "__main__":
    main()