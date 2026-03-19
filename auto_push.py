#!/usr/bin/env python3
"""
auto_push.py - 毎日自動実行: データ生成→GitHub自動プッシュ
Windowsタスクスケジューラに登録して使う
"""
import subprocess, sys, os, logging
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent
LOG_FILE = ROOT / 'logs' / 'auto_push.log'
LOG_FILE.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

def run(cmd, cwd=None):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                      cwd=cwd or ROOT)
    if r.returncode != 0 and r.stderr:
        log.warning(f"CMD: {cmd} | {r.stderr.strip()[:200]}")
    return r

def main():
    log.info("=" * 50)
    log.info("自動更新開始")

    # Step 1: データ生成
    log.info("Step 1: データ生成中...")
    r = run(f"{sys.executable} scripts/build.py")
    if r.returncode != 0:
        log.error(f"データ生成失敗: {r.stderr[:300]}")
        sys.exit(1)
    log.info("データ生成完了")

    # Step 2: Git add & commit
    log.info("Step 2: Gitコミット中...")
    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    run('git add -A')
    r = run(f'git commit -m "自動更新: {today}"')
    if 'nothing to commit' in r.stdout:
        log.info("変更なし、スキップ")
        return

    # Step 3: Push
    log.info("Step 3: GitHubへプッシュ中...")
    r = run('git push')
    if r.returncode == 0:
        log.info("✅ GitHubへのプッシュ完了")
    else:
        log.error(f"プッシュ失敗: {r.stderr[:200]}")
        sys.exit(1)

    log.info("自動更新完了")

if __name__ == '__main__':
    main()
