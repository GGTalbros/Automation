REM  To select copy location

set /P env_dir_path="Enter your directory name,  (eg. C\User\'Your Directory Name') : "

set original_dir="C:\Users\%env_dir_path%"
cd %original_dir%

set pyrunn_dir="C:\Users\%env_dir_path%\Python_runnables"
cd %pyrunn_dir%
mkdir dly_mail

set dly_mail_dir="C:\Users\%env_dir_path%\Python_runnables\dly_mail"
cd %dly_mail_dir%
mkdir Executable

Set "Host=%Computername%"

If "%Host%"=="SERVER2" (copy \\192.168.0.60\Prod_Deployment\dly_mail\Executable C:\Users\%env_dir_path%\Python_runnables\dly_mail\Executable

copy \\192.168.0.60\Prod_Deployment\dly_mail\dly_mail.py C:\Users\%env_dir_path%\Python_runnables\dly_mail)

If "%Host%"=="SERVER3" (copy \\192.168.0.25\UAT_Deployment\dly_mail\Executable C:\Users\%env_dir_path%\Python_runnables\dly_mail\Executable

copy \\192.168.0.25\UAT_Deployment\dly_mail\dly_mail.py C:\Users\%env_dir_path%\Python_runnables\dly_mail)


call C:\Users\%env_dir_path%\Python_runnables\dly_mail\Executable\dly_env.bat

call C:\Users\%env_dir_path%\Python_runnables\dly_mail\Executable\dly_pip_ins.bat


pause