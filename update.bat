@echo off 
cd /d I:\mercari_data 
python I:\mercari_data\FULL_AUTO_LOCAL_Vol7.py 
if errorlevel 1 goto error 
xcopy I:\mercari_data\output I:\mercari_github\output /E /I /Y 
cd /d I:\mercari_github 
git add -A 
git commit -m "auto update" 
git push 
goto end 
:error 
echo ÉGÉâÅ[Ç™î≠ê∂ÇµÇ‹ÇµÇΩ 
:end 
