@echo off
setlocal EnableDelayedExpansion

echo *********************************************
echo ACHTUNG: Sicherstellen dass nur VHI Produkt im config aktiviert ist!
echo *********************************************

python satromo_processor.py stj_VHIhist_config.py 2015-01-01
ping -n 800 localhost >nul
python satromo_processor.py stj_VHIhist_config.py 2015-02-01
ping -n 800 localhost >nul
python satromo_processor.py stj_VHIhist_config.py 2015-03-01
ping -n 800 localhost >nul
python satromo_processor.py stj_VHIhist_config.py 2015-04-01
ping -n 800 localhost >nul
python satromo_processor.py stj_VHIhist_config.py 2015-05-01
ping -n 800 localhost >nul
python satromo_processor.py stj_VHIhist_config.py 2015-06-01
ping -n 800 localhost >nul
python satromo_processor.py stj_VHIhist_config.py 2015-07-01
ping -n 800 localhost >nul
python satromo_processor.py stj_VHIhist_config.py 2015-08-01
ping -n 800 localhost >nul
python satromo_processor.py stj_VHIhist_config.py 2015-09-01
ping -n 800 localhost >nul
python satromo_processor.py stj_VHIhist_config.py 2015-10-01
ping -n 800 localhost >nul
python satromo_processor.py stj_VHIhist_config.py 2015-11-01
ping -n 800 localhost >nul
python satromo_processor.py stj_VHIhist_config.py 2015-12-01


REM :publish_loop
REM echo Start Publisher <nul
REM python satromo_publish.py stj_VHIhist_config.py 
REM echo %TIME% waiting 15min
REM ping -n 800 localhost >nul

REM for /f %%A in ('find /c /v "" ^< processing/running_tasks.csv') do set "lines=%%A"
REM if !lines! GTR 1 goto publish_loop

echo *********************************************
echo   ASSET in VHI wieder auf TRUE setzen
echo *********************************************
echo *********************************************<nul
