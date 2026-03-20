@echo off
echo [1/2] daily.yml を .github\workflows\ へ移動中...
mkdir "D:\mercari_github\.github\workflows" 2>nul
move /Y "D:\mercari_github\daily.yml" "D:\mercari_github\.github\workflows\daily.yml"

echo [2/2] 不要バッチを削除中...
del /F /Q "D:\mercari_github\reorganize_v1.bat"
del /F /Q "D:\mercari_github\reorganize_v2.bat"

echo 完了
pause
