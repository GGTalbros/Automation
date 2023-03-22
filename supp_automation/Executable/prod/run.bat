REM To run the program file

set original_dir="C:\Users\Administrator"

set venv_prgm_dir="C:\Users\Administrator\py_venv\hr_mail"

cd %venv_prgm_dir%

call %venv_prgm_dir%\Scripts\activate.bat

cd %original_dir%\Python_runnables\hr_mail
python "C:\Users\Administrator\Python_runnables\hr_mail\hr_mail.py "

cd %venv_prgm_dir%
call %venv_prgm_dir%\Scripts\deactivate.bat

cd %original_dir%



set original_dir="C:\Users\Administrator"

set venv_prgm_dir="C:\Users\Administrator\py_venv\dly_mail"

cd %venv_prgm_dir%

call %venv_prgm_dir%\Scripts\activate.bat

cd %original_dir%\Python_runnables\dly_mail
python "C:\Users\Administrator\Python_runnables\dly_mail\dly_mail.py "

cd %venv_prgm_dir%
call %venv_prgm_dir%\Scripts\deactivate.bat

cd %original_dir%



set original_dir="C:\Users\Administrator"

set venv_prgm_dir="C:\Users\Administrator\py_venv\supp_automation"

cd %venv_prgm_dir%

call %venv_prgm_dir%\Scripts\activate.bat

cd %original_dir%\Python_runnables\supp_automation
python "C:\Users\Administrator\Python_runnables\supp_automation\supp_automation.py " -e prod

cd %venv_prgm_dir%
call %venv_prgm_dir%\Scripts\deactivate.bat

cd %original_dir%

pause
