REM    To install required modules in virtual environment

set /P env_dir_path="Enter your directory name,  (eg. C\User\'Your Directory Name') : "

set original_dir="C:\Users\%env_dir_path%"

set venv_prgm_dir="%original_dir%\py_venv\supp_automation"

cd %venv_prgm_dir%

call %venv_prgm_dir%\Scripts\activate.bat

pip install pandas
pip install pyodbc
pip install secure-smtplib
pip install python-dateutil

call %venv_prgm_dir%\Scripts\deactivate.bat