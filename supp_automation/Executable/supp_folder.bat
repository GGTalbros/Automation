REM  To create folder and sub-folders for Supplementary

set /P env_dir_path="Enter your directory name,  (eg. C\User\'Your Directory Name') : "

set original_dir="C:\Users\%env_dir_path%"
cd %original_dir%
mkdir Supplementary

REM for creatingg Vecv Folder
set Supp_dir="C:\Users\%env_dir_path%\Supplementary"
cd %Supp_dir%
mkdir Vecv
mkdir Mahindra

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


set Mahindra_dir="C:\Users\%env_dir_path%\Supplementary\Mahindra"
cd %Mahindra_dir%
mkdir Grn
mkdir Price
mkdir Dtw
mkdir Supplementary

set MGrn_dir="C:\Users\%env_dir_path%\Supplementary\Mahindra\Grn"
cd %MGrn_dir%
mkdir landing_zone
mkdir Processing 
mkdir Processed