#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
auto_push.py (50件制限対応版)
"""
import subprocess, sys, os, logging, argparse
from pathlib import Path
from datetime import datetime

# ============================================================
# パス設定
# ============================================================
ROOT      = Path(__file__).parent.parent
DB_PATH   = Path(r"D:\mercari\data\mercari_full.db")
HTML_OUT  = ROOT / "html" / "index.html"
BUILD_PY  = ROOT / "scripts" / "build_mercari.py"
LOG_FILE  = ROOT / "logs" / "auto_push.log"
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

def run(cmd, cwd=None, check=False):
    r = subprocess.run(
        cmd, shell=True, capture_output=True,
        text=True, encoding="utf-8", errors="replace",
        cwd=str(cwd or ROOT)
    )
    return r

# ============================================================
# Step 1: HTML生成（ここで --limit 50 を渡すようにしました）
# ============================================================
def step_build(limit=None):
    log.info("─── Step 1: HTML生成 ───────────────────────────")
    db = Path(os.environ.get("DB_PATH", str(DB_PATH)))
    
    # 命令を作る（--limit があれば追加する）
    limit_cmd = f"--limit {limit}" if limit else ""
    cmd = (
        f'"{sys.executable}" "{BUILD_PY}" '
        f'--db "{db}" '
        f'--out "{HTML_OUT}" '
        f'{limit_cmd}'
    )
    
    log.info(f"実行コマンド: {cmd}")
    r = run(cmd)

    if r.returncode != 0:
        log.error("HTML生成失敗")
        return False
    
    log.info("  ✅ HTML生成完了")
    return True

# ============================================================
# Step 2: Git push (ここは元のまま)
# ============================================================
def step_git():
    log.info("─── Step 2: Git コミット & プッシュ ────────────")
    run("git add -A")
    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    run(f'git commit -m "自動更新: {today}"')
    log.info("  GitHubへプッシュ中...")
    r = run("git push")
    if r.returncode != 0:
        log.error("プッシュ失敗")
        return False
    log.info("  ✅ GitHubへのプッシュ完了")
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50, help="件数制限 (デフォルト50)")
    args = parser.parse_args()

    log.info(f"開始: 制限 {args.limit}件")
    if step_build(limit=args.limit):
        step_git()

if __name__ == "__main__":
    main()