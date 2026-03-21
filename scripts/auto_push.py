#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess
import sys
import os
import logging
import argparse
import json
import sqlite3
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime

# ============================================================
# 1. SETTINGS
# ============================================================
SUPABASE_URL = "https://tikqwmpclunseegqksnk.supabase.co"
# [IMPORTANT] Paste your actual Supabase anon_key here
SUPABASE_KEY = "あなたのanon_keyをここに貼り付け"
TABLE_NAME   = "商品"

ROOT      = Path(r"D:\mercari_github")
DB_PATH   = Path(r"D:\mercari\data\mercari_full.db")
HTML_OUT  = ROOT / "html" / "index.html"
BUILD_PY  = ROOT / "scripts" / "build_mercari.py"
LOG_FILE  = ROOT / "logs" / "auto_push.log"

# ============================================================
# 2. LOGGING (Ensuring UTF-8)
# ============================================================
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# ============================================================
# 3. SYNC PROCESS
# ============================================================
def step_supabase_sync(limit):
    log.info(f"--- Step 2: Syncing to Supabase (Limit: {limit}) ---")
    try:
        if not DB_PATH.exists():
            log.error(f"DB not found: {DB_PATH}")
            return False

        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        source_table = "sold_items"
        cursor.execute(f"SELECT * FROM {source_table} LIMIT ?", (limit,))
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()

        if not rows:
            return True

        json_payload = json.dumps(rows, ensure_ascii=False).encode("utf-8")
        encoded_table = urllib.parse.quote(TABLE_NAME)
        url = f"{SUPABASE_URL}/rest/v1/{encoded_table}"

        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }
        
        req = urllib.request.Request(url, data=json_payload, headers=headers, method="POST")
        with urllib.request.urlopen(req) as res:
            if res.getcode() in [200, 201, 204]:
                log.info(f"Sync Success: {len(rows)} rows.")
                return True
    except Exception as e:
        log.error(f"Sync Error: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    log.info("=== Start Auto Process ===")
    
    # --- Step 1: HTML Build ---
    # パス問題を回避するため、直接 'python' 命令を使用し、各パスを引用符で囲みます
    # これが Windows 環境において最も干渉を受けにくい実行方法です
    cmd = f'python "{str(BUILD_PY)}" --db "{str(DB_PATH)}" --out "{str(HTML_OUT)}" --limit {args.limit}'
    
    log.info("Step 1: HTML build starting...")
    # shell=True で環境変数の python を直接呼び出します
    res_step1 = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=str(ROOT))
    
    if res_step1.returncode == 0:
        log.info("Step 1 (HTML): OK")
        
        # --- Step 2: Supabase Sync ---
        step_supabase_sync(args.limit)
        
        # --- Step 3: Git Operations ---
        log.info("--- Step 3: Git Operations ---")
        subprocess.run("git add -A", shell=True, cwd=str(ROOT))
        
        tag = datetime.now().strftime('%m%d_%H%M')
        commit_msg = f"Update_{tag}"
        subprocess.run(f'git commit -m "{commit_msg}"', shell=True, cwd=str(ROOT))
        
        push_res = subprocess.run("git push", shell=True, capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=str(ROOT))
        if push_res.returncode == 0:
            log.info("Step 3 (Git): Push Success")
        else:
            log.info("Step 3 (Git): Finished (Check logs for details).")
            
    else:
        log.error("--- STEP 1 FAILED ---")
        log.error(f"Error detail: {res_step1.stderr}")
    
    log.info("=== Process Completed ===")

if __name__ == "__main__":
    main()