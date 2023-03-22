REM  To select copy location

set /P env_dir_path="Enter your directory name,  (eg. C\User\'Your Directory Name') : "

set original_dir="C:\Users\%env_dir_path%"
cd %original_dir%
mkdir py_venv

pip install virtualenv

set original_dir="C:\Users\%env_dir_path%"
cd %original_dir%
mkdir Python_runnables

set pyrunn_dir="C:\Users\%env_dir_path%\Python_runnables"
cd %pyrunn_dir%
mkdir hr_mail

set hr_mail_dir="C:\Users\%env_dir_path%\Python_runnables\hr_mail"
cd %hr_mail_dir%
mkdir Executable

Set "Host=%Computername%"

If "%Host%"=="SERVER2" (copy \\192.168.0.60\Prod_Deployment\hr_mail\Executable C:\Users\%env_dir_path%\Python_runnables\hr_mail\Executable

copy \\192.168.0.60\Prod_Deployment\hr_mail\hr_mail.py C:\Users\%env_dir_path%\Python_runnables\hr_mail)

If "%Host%"=="SERVER3" (copy \\192.168.0.25\UAT_Deployment\hr_mail\Executable C:\Users\%env_dir_path%\Python_runnables\hr_mail\Executable

copy \\192.168.0.25\UAT_Deployment\hr_mail\hr_mail.py C:\Users\%env_dir_path%\Python_runnables\hr_mail) 

call C:\Users\%env_dir_path%\Python_runnables\hr_mail\Executable\hr_env.bat

call C:\Users\%env_dir_path%\Python_runnables\hr_mail\Executable\hr_pip_ins.bat


pause