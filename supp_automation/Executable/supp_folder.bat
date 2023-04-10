REM  To create folder and sub-folders for Supplementary

set /P env_dir_path="Enter your directory name,  (eg. C\User\'Your Directory Name') : "

set original_dir="C:\Users\%env_dir_path%"
cd %original_dir%
mkdir Supplementary

REM for creatingg Vecv Folder
set Supp_dir="C:\Users\%env_dir_path%\Supplementary"
cd %Supp_dir%
mkdir Vecv
mkdir VecvSpare

set Vecv_dir="C:\Users\%env_dir_path%\Supplementary\Vecv"
cd %Vecv_dir%
mkdir Grn
mkdir Price
mkdir Dtw
mkdir Supplementary

set VGrn_dir="C:\Users\%env_dir_path%\Supplementary\Vecv\Grn"
cd %VGrn_dir%
mkdir landing_zone
mkdir Processing 
mkdir Processed


set VecvSpare_dir="C:\Users\%env_dir_path%\Supplementary\VecvSpare"
cd %VecvSpare_dir%
mkdir Grn
mkdir Price
mkdir Dtw
mkdir Supplementary

set VSGrn_dir="C:\Users\%env_dir_path%\Supplementary\VecvSpare\Grn"
cd %VSGrn_dir%
mkdir landing_zone
mkdir Processing 
mkdir Processed