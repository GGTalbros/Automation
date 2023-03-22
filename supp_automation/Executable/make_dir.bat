REM  To create main Virtual Environment directory (Folder)

set /P env_dir_path="Enter the base directory name you want to make your folder in, (eg. C\User\'Your Directory Name') : "

set original_dir="C:\Users\%env_dir_path%"
cd %original_dir%

cd py_venv

set venv_root_dir="%original_dir%\py_venv\supp_automation"

cd %venv_root_dir%

virtualenv supp_automation
