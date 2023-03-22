REM  To create scheduler

set /P env_dir_path="Enter your directory name,  (eg. C\User\'Your Directory Name') : "

set original_dir="C:\Users\%env_dir_path%"
cd %original_dir%

Set "Host=%Computername%"

If "%Host%"=="SERVER2" (schtasks /create /sc hourly /mo 1 /tn "Automation_Prod" /tr "C:\Users\%env_dir_path%\Python_runnables\supp_automation\Executable\prod\run.bat" /st 00:00) 
If "%Host%"=="SERVER3" (schtasks /create /sc hourly /mo 1 /tn "Automation_UAT" /tr "C:\Users\%env_dir_path%\Python_runnables\supp_automation\Executable\uat\run.bat" /st 00:00) Else (echo Use Valid Server)
























