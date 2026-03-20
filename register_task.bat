@echo off
REM ============================================================
REM  register_task.bat
REM  毎日 05:00 に auto_push.py を自動実行するタスクを登録
REM  ※ 管理者として実行してください
REM  配置フォルダ: H:\mercari_github\
REM ============================================================

SET SCRIPT_DIR=%~dp0
SET SCRIPT=%SCRIPT_DIR%auto_push.py
SET PYTHON=python

echo.
echo ============================================================
echo  メルカリ仕入判断AI 自動更新タスク登録
echo ============================================================
echo.
echo  スクリプト : %SCRIPT%
echo  実行時刻   : 毎日 05:00
echo.

REM 既存タスクを削除してから再登録
schtasks /delete /tn "MercariAutoUpdate" /f >nul 2>&1

schtasks /create ^
  /tn "MercariAutoUpdate" ^
  /tr "\"%PYTHON%\" \"%SCRIPT%\"" ^
  /sc daily ^
  /st 05:00 ^
  /f ^
  /rl HIGHEST ^
  /ru "%USERNAME%"

if %errorlevel% == 0 (
    echo.
    echo  ✅ 登録完了！毎日 05:00 に自動実行されます
    echo.
    echo  手動実行:    python auto_push.py
    echo  ログ確認:    logs\auto_push.log
    echo  タスク確認:  タスクスケジューラ > MercariAutoUpdate
    echo.
) else (
    echo.
    echo  [ERROR] 登録失敗。右クリック→「管理者として実行」してください
    echo.
)

pause
