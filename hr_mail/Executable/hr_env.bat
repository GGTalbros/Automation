REM    To create Virtual Environment

set /P env_dir_path="Enter your directory name,  (eg. C\User\'Your Directory Name') : "

set original_dir="C:\Users\%env_dir_path%"
cd %original_dir%

cd py_venv

set venv_root_dir="%original_dir%\py_venv"

cd %venv_root_dir%

virtualenv hr_mail