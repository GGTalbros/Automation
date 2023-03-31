REM To run the program file

set original_dir="C:\Users\abc"

set venv_prgm_dir="C:\Users\abc\py_venv\hr_mail"

cd %venv_prgm_dir%

call %venv_prgm_dir%\Scripts\activate.bat

cd %original_dir%\Python_runnables\hr_mail
python "C:\Users\abc\Python_runnables\hr_mail\hr_mail.py "

cd %venv_prgm_dir%
call %venv_prgm_dir%\Scripts\deactivate.bat

cd %original_dir%



set original_dir="C:\Users\abc"

set venv_prgm_dir="C:\Users\abc\py_venv\daily_mail"

cd %venv_prgm_dir%

call %venv_prgm_dir%\Scripts\activate.bat

cd %original_dir%\Python_runnables\daily_mail
python "C:\Users\abc\Python_runnables\daily_mail\daily_mail.py "

cd %venv_prgm_dir%
call %venv_prgm_dir%\Scripts\deactivate.bat

cd %original_dir%



set original_dir="C:\Users\abc"

set venv_prgm_dir="C:\Users\abc\py_venv\supp_automation"

cd %venv_prgm_dir%

call %venv_prgm_dir%\Scripts\activate.bat

cd %original_dir%\Python_runnables\supp_automation
python "C:\Users\abc\Python_runnables\supp_automation\supp_automation.py " -e local

cd %venv_prgm_dir%
call %venv_prgm_dir%\Scripts\deactivate.bat

cd %original_dir%
