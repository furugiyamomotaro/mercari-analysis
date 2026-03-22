@echo off
echo === Step1: データ更新中 ===
cd I:\mercari_data
python FULL_AUTO_LOCAL_Vol2.py
if errorlevel 1 goto error

echo === Step2: outputコピー中 ===
xcopy I:\mercari_data\output I:\mercari_github\output /E /I /Y

echo === Step3: GitHubにpush中 ===
cd I:\mercari_github
git add -A
git commit -m "auto update"
git push

echo === 完了！Supabaseに自動アップロードされます ===
pause
goto end

:error
echo === エラーが発生しました ===
pause

:end
