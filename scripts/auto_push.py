#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess, sys, os, logging, argparse, json, sqlite3
import urllib.request
from pathlib import Path
from datetime import datetime

# ============================================================
# 1. 設定エリア（ここを自分の情報に書き換えてください）
# ============================================================
# Supabaseの情報（URLの末尾に / は入れないでください）
SUPABASE_URL = "https://tikqwmpclunseegqksnk.supabase.co"
SUPABASE_KEY = "ここにあなたのanon_keyを貼り付けてください"
TABLE_NAME   = "商品"  # Supabase側のテーブル名（「商品」か「items」か確認）

# フォルダの場所（r をつけることでWindows特有のパスエラーを防ぎます）
ROOT      = Path(r"D:\mercari_github")
DB_PATH   = Path(r"D:\mercari\data\mercari_full.db")
HTML_OUT  = ROOT / "html" / "index.html"
BUILD_PY  = ROOT / "scripts" / "build_mercari.py"
LOG_FILE  = ROOT / "logs" / "auto_push.log"

# ============================================================
# 2. 準備
# ============================================================
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

def run(cmd, cwd=None):
    """コマンド実行ヘルパー"""
    return subprocess.run(
        cmd, shell=True, capture_output=True, text=True, 
        encoding="utf-8", errors="replace", cwd=str(cwd or ROOT)
    )

# ============================================================
# Step 1: HTML生成
# ============================================================
def step_build(limit):
    log.info(f"─── Step 1: HTML生成 (制限:{limit}件) ───")
    if not BUILD_PY.exists():
        log.error(f"プログラムが見つかりません: {BUILD_PY}")
        return False
    
    # build_mercari.py を呼び出し
    cmd = f'"{sys.executable}" "{BUILD_PY}" --db "{DB_PATH}" --out "{HTML_OUT}" --limit {limit}'
    r = run(cmd)
    
    if r.returncode != 0:
        log.error(f"HTML生成失敗: {r.stderr[:200]}")
        return False
    
    log.info("  ✅ HTML生成完了")
    return True

# ============================================================
# Step 2: Supabaseへ直接同期（ライブラリ不要版）
# ============================================================
def step_supabase_sync(limit):
    log.info("─── Step 2: Supabaseクラウド同期 ───")
    try:
        # ローカルのSQLite DBからデータを読み出す
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        # itemsテーブルから最新データを取得
        cursor.execute(f"SELECT * FROM items LIMIT {limit}")
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()

        if not rows:
            log.warning("  送るデータがDB内に見つかりませんでした")
            return True

        # Supabase APIへ送信
        url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates" # 重複データは上書き(upsert)
        }
        
        # 標準のurllibを使って送信（requestsすら不要）
        req = urllib.request.Request(
            url, 
            data=json.dumps(rows).encode("utf-8"), 
            headers=headers, 
            method="POST"
        )
        
        with urllib.request.urlopen(req) as res:
            if res.status in [200, 201, 204]:
                log.info(f"  ✅ {len(rows)}件のデータをSupabaseに同期しました")
                return True
            else:
                log.error(f"  ❌ 同期失敗: ステータス {res.status}")
                return False
                
    except Exception as e:
        log.error(f"  ❌ 同期エラー発生: {e}")
        return False

# ============================================================
# Step 3: GitHubへプッシュ
# ============================================================
def step_git():
    log.info("─── Step 3: GitHubへ保存 ───")
    run("git add -A")
    commit_msg = f"Auto Update: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    run(f'git commit -m "{commit_msg}"')
    
    log.info("  GitHubへプッシュ中...")
    r = run("git push")
    
    if r.returncode == 0:
        log.info("  ✅ GitHubへのプッシュ完了")
        return True
    else:
        log.error(f"  ❌ プッシュ失敗: {r.stderr[:200]}")
        return False

# ============================================================
# メイン処理
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    log.info("="*50)
    log.info("🚀 メルカリ在庫管理システム 自動更新スタート")
    log.info("="*50)

    # 1. HTML作成
    if not step_build(args.limit):
        sys.exit(1)

    # 2. Supabase同期
    step_supabase_sync(args.limit)

    # 3. GitHub反映
    step_git()

    log.info("="*50)
    log.info("✅ すべての工程が正常に終了しました")
    log.info("="*50)

if __name__ == "__main__":
    main()