@echo off
REM タスクスケジューラ登録 - 毎日5時に自動実行
REM このファイルをダブルクリックするだけでOK

SET SCRIPT_PATH=%~dp0auto_push.py
SET PYTHON_PATH=python

echo タスクスケジューラに登録します...
echo スクリプト: %SCRIPT_PATH%

schtasks /create /tn "MercariAutoUpdate" /tr "%PYTHON_PATH% %SCRIPT_PATH%" /sc daily /st 05:00 /f /rl HIGHEST

if %errorlevel% == 0 (
    echo.
    echo ✅ 登録完了！毎日 5:00 に自動実行されます
    echo.
    echo 確認方法: タスクスケジューラ を開いて MercariAutoUpdate を確認
) else (
    echo [ERROR] 登録失敗。管理者として実行してください
)

pause
