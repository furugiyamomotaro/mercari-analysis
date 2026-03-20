#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
auto_push.py
============
DB更新 -> HTML生成 -> GitHub自動プッシュ を完全1本化

実行方法:
  python auto_push.py          # 通常実行
  python auto_push.py --now    # 即時実行（スケジュール無視）

Windowsタスクスケジューラ:
  register_task.bat をダブルクリックで毎日5:00に自動実行

配置フォルダ: H:/mercari_github/
"""
import subprocess, sys, os, logging, argparse
from pathlib import Path
from datetime import datetime

# ============================================================
# パス設定（このファイルと同じフォルダを基準に解決）
# ============================================================
ROOT      = Path(__file__).parent                          # H:\mercari_github
DB_PATH   = Path(r"H:\mercari\data\mercari_full.db")           # DBパス
HTML_OUT  = ROOT / "index.html"                            # 出力HTML（上書き）
BUILD_PY  = ROOT / "build_mercari.py"                      # HTML生成スクリプト
LOG_FILE  = ROOT / "logs" / "auto_push.log"
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

# ============================================================
# ロガー設定
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ============================================================
# コマンド実行ヘルパー
# ============================================================
def run(cmd, cwd=None, check=False):
    """コマンドを実行してResultを返す。check=Trueなら失敗時に例外"""
    r = subprocess.run(
        cmd, shell=True, capture_output=True,
        text=True, encoding="utf-8", errors="replace",
        cwd=str(cwd or ROOT)
    )
    if r.returncode != 0 and r.stderr:
        log.warning(f"  CMD: {cmd}")
        log.warning(f"  ERR: {r.stderr.strip()[:300]}")
    if check and r.returncode != 0:
        raise RuntimeError(f"コマンド失敗: {cmd}\n{r.stderr[:300]}")
    return r

# ============================================================
# Step 1: HTML生成（DB -> index.html）
# ============================================================
def step_build():
    log.info("─── Step 1: HTML生成 ───────────────────────────")

    # DBファイル存在確認
    db = Path(os.environ.get("DB_PATH", str(DB_PATH)))
    if not db.exists():
        log.error(f"DBファイルが見つかりません: {db}")
        log.error("DB_PATH 環境変数またはスクリプト内の DB_PATH を確認してください")
        return False

    # build_mercari.py 存在確認
    if not BUILD_PY.exists():
        log.error(f"build_mercari.py が見つかりません: {BUILD_PY}")
        log.error(f"build_mercari.py を {ROOT} に配置してください")
        return False

    log.info(f"DB   : {db}")
    log.info(f"出力 : {HTML_OUT}")

    cmd = (
        f'"{sys.executable}" "{BUILD_PY}" '
        f'--db "{db}" '
        f'--out "{HTML_OUT}"'
    )
    r = run(cmd)

    if r.returncode != 0:
        log.error(f"HTML生成失敗（終了コード {r.returncode}）")
        log.error(r.stderr[:500])
        return False

    if r.stdout:
        for line in r.stdout.strip().splitlines():
            log.info(f"  {line}")

    if not HTML_OUT.exists():
        log.error(f"index.html が生成されませんでした: {HTML_OUT}")
        return False

    size_kb = HTML_OUT.stat().st_size / 1024
    log.info(f"  生成完了: {HTML_OUT.name}  ({size_kb:.1f} KB)")
    return True

# ============================================================
# Step 2: Git add / commit / push
# ============================================================
def step_git():
    log.info("─── Step 2: Git コミット & プッシュ ────────────")

    # git がインストールされているか確認
    r = run("git --version")
    if r.returncode != 0:
        log.error("git が見つかりません。Git for Windows をインストールしてください")
        return False

    # git リポジトリかどうか確認
    r = run("git rev-parse --is-inside-work-tree")
    if r.returncode != 0:
        log.error(f"{ROOT} は git リポジトリではありません")
        log.error("setup.py を実行して git 初期化してください")
        return False

    # git add
    run("git add -A", check=True)

    # 変更があるか確認
    r = run("git status --short")
    if not r.stdout.strip():
        log.info("  変更なし。コミットをスキップします")
        return True

    # git commit
    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    commit_msg = f"自動更新: {today}"
    r = run(f'git commit -m "{commit_msg}"')
    if r.returncode != 0:
        if "nothing to commit" in r.stdout.lower() or "nothing to commit" in r.stderr.lower():
            log.info("  変更なし。コミットをスキップします")
            return True
        log.error(f"コミット失敗: {r.stderr[:300]}")
        return False
    log.info(f"  コミット完了: {commit_msg}")

    # git push
    log.info("  GitHubへプッシュ中...")
    r = run("git push")
    if r.returncode != 0:
        log.error(f"プッシュ失敗: {r.stderr[:300]}")
        log.error("トークンの期限切れ・ネットワーク・認証を確認してください")
        return False

    log.info("  ✅ GitHubへのプッシュ完了")
    return True

# ============================================================
# メイン処理
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="DB->HTML->GitHub 完全自動更新")
    parser.add_argument("--now", action="store_true", help="即時実行")
    args = parser.parse_args()

    log.info("=" * 55)
    log.info(f"  メルカリ仕入判断AI 自動更新開始")
    log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    # Step 1: HTML生成
    if not step_build():
        log.error("❌ Step 1 失敗。処理を中止します")
        sys.exit(1)

    # Step 2: Git push
    if not step_git():
        log.error("❌ Step 2 失敗。HTMLは生成済みですがGitHubへの反映に失敗しました")
        sys.exit(1)

    log.info("=" * 55)
    log.info("  ✅ 全ステップ完了")
    log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

if __name__ == "__main__":
    main()
