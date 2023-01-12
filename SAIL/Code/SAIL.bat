call %UserProfile%\Anaconda3\Scripts\activate.bat
call python "%UserProfile%\HP Inc\PrintOpsDB - DB_DailyOutput\Code\Email_CSV_SP.py"
timeout 5
call conda activate SAIL
call python "%UserProfile%\HP Inc\PrintOpsDB - DB_DailyOutput\Code\SAIL.py"
timeout 5
