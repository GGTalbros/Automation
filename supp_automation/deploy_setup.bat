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

If "%Host%"=="SERVER2" (mkdir prod)

If "%Host%"=="SERVER3" (mkdir uat) else (echo Invalid Server)


set sub_pkg_dir="C:\Users\%env_dir_path%\Python_runnables\supp_automation\com"
cd %sub_pkg_dir%
mkdir Talbros 
cd Talbros
mkdir supplementary


If "%Host%"=="SERVER2" (copy \\192.168.0.60\Prod_Deployment\Automation-main\supp_automation\Executable C:\Users\%env_dir_path%\Python_runnables\supp_automation\Executable
copy \\192.168.0.60\Prod_Deployment\Automation-main\supp_automation\Executable\prod C:\Users\%env_dir_path%\Python_runnables\supp_automation\Executable\prod
copy \\192.168.0.60\Prod_Deployment\Automation-main\supp_automation\main.py C:\Users\%env_dir_path%\Python_runnables\supp_automation
copy C:\Users\abc\zip_folder\Automation-main\supp_automation\supp_automation_config.properties C:\Users\%env_dir_path%\Python_runnables\supp_automation
copy \\192.168.0.60\Prod_Deployment\Automation-main\supp_automation\com C:\Users\%env_dir_path%\Python_runnables\supp_automation\com
copy \\192.168.0.60\Prod_Deployment\Automation-main\supp_automation\com\Talbros C:\Users\%env_dir_path%\Python_runnables\supp_automation\com\Talbros
copy \\192.168.0.60\Prod_Deployment\Automation-main\supp_automation\com\Talbros\supplementary C:\Users\%env_dir_path%\Python_runnables\supp_automation\com\Talbros\supplementary)


If "%Host%"=="SERVER3" (copy \\192.168.0.25\UAT_Deployment\Automation-main\supp_automation\Executable C:\Users\%env_dir_path%\Python_runnables\supp_automation\Executable
copy \\192.168.0.25\UAT_Deployment\Automation-main\supp_automation\Executable\prod C:\Users\%env_dir_path%\Python_runnables\supp_automation\Executable\prod
copy \\192.168.0.25\UAT_Deployment\Automation-main\supp_automation\main.py C:\Users\%env_dir_path%\Python_runnables\supp_automation
copy C:\Users\abc\zip_folder\Automation-main\supp_automation\supp_automation_config.properties C:\Users\%env_dir_path%\Python_runnables\supp_automation
copy \\192.168.0.25\UAT_Deployment\Automation-main\supp_automation\com C:\Users\%env_dir_path%\Python_runnables\supp_automation\com
copy \\192.168.0.25\UAT_Deployment\Automation-main\supp_automation\com\Talbros C:\Users\%env_dir_path%\Python_runnables\supp_automation\com\Talbros
copy \\192.168.0.25\UAT_Deployment\Automation-main\supp_automation\com\Talbros\supplementary C:\Users\%env_dir_path%\Python_runnables\supp_automation\com\Talbros\supplementary) else (echo Invalid Server)


pause