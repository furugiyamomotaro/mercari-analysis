#!/usr/bin/env python3
"""
setup.py - 1回実行するだけでGitHub環境を全自動構築
使い方: python setup.py
"""
import os, sys, subprocess, json, shutil
from pathlib import Path

print("=" * 60)
print("  メルカリ仕入判断AI - GitHub自動セットアップ")
print("=" * 60)

# =====================
# 設定入力
# =====================
print("\n【Step 1】GitHub情報を入力してください")
print("  ※ GitHubアカウントがない場合は https://github.com で作成")
print()

github_user = input("GitHubユーザー名: ").strip()
repo_name   = input("リポジトリ名 (例: mercari-analysis): ").strip() or "mercari-analysis"
github_token = input("GitHubトークン (Settings>Developer settings>Personal access tokens>Tokens(classic)): ").strip()

if not github_user or not github_token:
    print("[ERROR] ユーザー名とトークンは必須です")
    sys.exit(1)

# =====================
# gitコマンド確認
# =====================
print("\n【Step 2】Git確認中...")
try:
    r = subprocess.run(['git','--version'], capture_output=True, text=True)
    print(f"  OK: {r.stdout.strip()}")
except FileNotFoundError:
    print("[ERROR] Gitがインストールされていません")
    print("  https://git-scm.com/download/win からインストールしてください")
    sys.exit(1)

# =====================
# GitHub CLIでリポジトリ作成
# =====================
print("\n【Step 3】GitHubリポジトリ作成中...")

import urllib.request, urllib.error

headers = {
    'Authorization': f'token {github_token}'.encode('ascii').decode('ascii'),
    'Content-Type': 'application/json',
    'User-Agent': 'mercari-setup'
}

data = json.dumps({
    "name": repo_name,
    "description": "メルカリ仕入判断AI 30分析データ",
    "private": False,
    "auto_init": False
}).encode()

try:
    req = urllib.request.Request(
        'https://api.github.com/user/repos',
        data=data, headers=headers, method='POST'
    )
    with urllib.request.urlopen(req) as res:
        repo_info = json.loads(res.read())
        repo_url = repo_info['html_url']
        print(f"  OK: {repo_url}")
except urllib.error.HTTPError as e:
    body = e.read().decode()
    if 'already exists' in body or e.code == 422:
        repo_url = f"https://github.com/{github_user}/{repo_name}"
        print(f"  既存リポジトリを使用: {repo_url}")
    else:
        print(f"[ERROR] リポジトリ作成失敗: {e.code} {body[:200]}")
        sys.exit(1)

# =====================
# ローカルGit初期化
# =====================
print("\n【Step 4】ローカルGit初期化中...")

root = Path(__file__).parent
os.chdir(root)

GIT = r"C:\Program Files\Git\bin\git.exe"

def run(cmd, check=True):
    if cmd.startswith("git "):
        cmd = '"' + GIT + '" ' + cmd[4:]
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and r.returncode != 0 and r.stderr:
        print(f"  [WARN] {r.stderr.strip()[:100]}")
    return r

run('git init')
run(f'git config user.email "mercari@analysis.local"')
run(f'git config user.name "{github_user}"')

# リモート設定（トークン認証込み）
remote_url = f"https://{github_user}:{github_token}@github.com/{github_user}/{repo_name}.git"
run('git remote remove origin', check=False)
run(f'git remote add origin {remote_url}')

# .gitignore作成
gitignore = """*.db
*.sqlite
__pycache__/
*.pyc
.env
secrets.txt
node_modules/
.DS_Store
Thumbs.db
"""
(root / '.gitignore').write_text(gitignore, encoding='utf-8')

# requirements.txt
(root / 'requirements.txt').write_text('# 標準ライブラリのみ使用\n', encoding='utf-8')

# dataディレクトリにダミーJSONを作成（Gitが空ディレクトリを追跡できるため）
data_dir = root / 'data'
data_dir.mkdir(exist_ok=True)
dummy_summary = {
    "version": "Vol1",
    "updated_at": "初回セットアップ",
    "period": "--",
    "total_items": 0,
    "filtered_items": 0,
    "avg_price": 0,
    "avg_profit": 0,
    "profit_rate": 0,
    "total_profit": 0,
    "quick_sell_count": 0,
    "quick_sell_rate": 0,
    "categories": "--",
    "db_start": "",
    "db_end": "",
    "l1_list": []
}
with open(data_dir / 'summary.json', 'w', encoding='utf-8') as f:
    json.dump(dummy_summary, f, ensure_ascii=False, indent=2)

# =====================
# GitHub Pagesの設定ファイル
# =====================
(root / '_config.yml').write_text('theme: null\n', encoding='utf-8')

# =====================
# コミット＆プッシュ
# =====================
print("\n【Step 5】GitHubにアップロード中...")

run('git add -A')
run('git commit -m "初回セットアップ: メルカリ仕入判断AI 30分析"')
run('git branch -M main')
push_r = run(f'git push -u origin main --force')

if push_r.returncode == 0:
    print("  OK: アップロード完了")
else:
    print(f"  [ERROR] プッシュ失敗: {push_r.stderr[:200]}")
    sys.exit(1)

# =====================
# GitHub Pages有効化
# =====================
print("\n【Step 6】GitHub Pages有効化中...")

pages_data = json.dumps({
    "source": {"branch": "gh-pages", "path": "/"}
}).encode()

try:
    req = urllib.request.Request(
        f'https://api.github.com/repos/{github_user}/{repo_name}/pages',
        data=pages_data, headers=headers, method='POST'
    )
    with urllib.request.urlopen(req) as res:
        pages_info = json.loads(res.read())
        pages_url = pages_info.get('html_url', f"https://{github_user}.github.io/{repo_name}")
        print(f"  OK: {pages_url}")
except Exception as e:
    pages_url = f"https://{github_user}.github.io/{repo_name}"
    print(f"  手動設定が必要な場合: Settings > Pages > Source: gh-pages")

# =====================
# 完了
# =====================
print("\n" + "=" * 60)
print("  ✅ セットアップ完了！")
print("=" * 60)
print(f"""
  リポジトリ: {repo_url}
  サイトURL:  {pages_url}
            （反映まで数分かかります）

  次のステップ:
  1. python scripts/build.py  ← データを今すぐ生成
  2. git add -A && git commit -m "データ更新" && git push
  3. あとは毎日自動更新されます ✨

  毎日自動実行: 日本時間 5:00 (GitHub Actions)
""")

# =====================
# 初回ビルドを実行するか確認
# =====================
ans = input("今すぐデータを生成しますか？ (y/N): ").strip().lower()
if ans == 'y':
    print("\n【Step 7】初回データ生成中...")
    r = subprocess.run([sys.executable, str(root / 'scripts' / 'build.py')],
                      capture_output=False)
    if r.returncode == 0:
        # 生成したデータをプッシュ
        os.chdir(root)
        run('git add -A')
        run('git commit -m "初回データ生成"')
        run('git push')
        print("\n✅ データ生成＆アップロード完了！")
        print(f"  サイトを確認: {pages_url}")
    else:
        print("\n[WARN] データ生成に問題がありました。scripts/build.py を確認してください")
