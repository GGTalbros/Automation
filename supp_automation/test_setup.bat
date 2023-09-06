REM  To select copy location

set /P env_dir_path="Enter your directory name,  (eg. C\User\'Your Directory Name') : "

set original_dir="C:\Users\%env_dir_path%"
cd %original_dir%

set pyrunn_dir="C:\Users\%env_dir_path%\Python_runnables"
cd %pyrunn_dir%
mkdir supp_automation

set supp_automation_dir="C:\Users\%env_dir_path%\Python_runnables\supp_automation"
cd %supp_automation_dir%
mkdir com
mkdir Executable

Set "Host=%Computername%"

set supp_server_dir="C:\Users\%env_dir_path%\Python_runnables\supp_automation\Executable"
cd %supp_server_dir%

If "%Host%"=="SQL-1"(mkdir prod)

set sub_pkg_dir="C:\Users\%env_dir_path%\Python_runnables\supp_automation\com"
cd %sub_pkg_dir%
mkdir Talbros 
cd Talbros
mkdir supplementary

::If "%Host%"=="SERVER3" (mkdir uat) else (echo Invalid Server)
::C:\Users\abc\zip_folder\Automation-main
::xcopy /s /e C:\dirA C:\dirB


echo %Host%
echo working

If "%Host%"=="SQL-1" (copy C:\Users\abc\zip_folder\Automation-main\supp_automation\Executable C:\Users\%env_dir_path%\Python_runnables\supp_automation\Executable
copy C:\Users\abc\zip_folder\Automation-main\supp_automation\Executable\prod C:\Users\%env_dir_path%\Python_runnables\supp_automation\Executable\prod
copy C:\Users\abc\zip_folder\Automation-main\supp_automation\main.py C:\Users\%env_dir_path%\Python_runnables\supp_automation
copy C:\Users\abc\zip_folder\Automation-main\supp_automation\supp_automation_config.properties C:\Users\%env_dir_path%\Python_runnables\supp_automation
copy C:\Users\abc\zip_folder\Automation-main\supp_automation\com C:\Users\%env_dir_path%\Python_runnables\supp_automation\com
copy C:\Users\abc\zip_folder\Automation-main\supp_automation\com\Talbros C:\Users\%env_dir_path%\Python_runnables\supp_automation\com\Talbros
copy C:\Users\abc\zip_folder\Automation-main\supp_automation\com\Talbros\supplementary C:\Users\%env_dir_path%\Python_runnables\supp_automation\com\Talbros\supplementary)

::If "%Host%"=="SERVER3" (copy \\192.168.0.25\UAT_Deployment\supp_automation\Executable C:\Users\%env_dir_path%\Python_runnables\supp_automation\Executable
::copy \\192.168.0.25\UAT_Deployment\supp_automation\Executable\uat C:\Users\%env_dir_path%\Python_runnables\supp_automation\Executable\uat
::copy \\192.168.0.25\UAT_Deployment\supp_automation\supp_automation.py C:\Users\%env_dir_path%\Python_runnables\supp_automation
::copy \\192.168.0.25\UAT_Deployment\supp_automation\supp_automation_config.properties C:\Users\%env_dir_path%\Python_runnables\supp_automation) else (echo Invalid Server)


pause