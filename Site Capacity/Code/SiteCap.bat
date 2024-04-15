call %ACTIVATE_PATH%\activate.bat
call conda activate sitecap
call python "%UserProfile%\HP Inc\Team Site - Cap Mgmt\New Site Cap\Code\SiteCap.py"
timeout 5
pause