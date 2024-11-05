@echo off
setlocal EnableDelayedExpansion

set "config_file=%~1"
set "year=%~2"
set "month=%~3"

echo *********************************************
echo ACHTUNG: Sicherstellen dass nur VHI Produkt im config aktiviert ist!
echo *********************************************
echo Bitte Enter druecken, um fortzufahren...
pause >nul

if "!config_file!"=="" (
    echo Bitte Konfigurationsfile, Jahr und Monat als Parameter eingeben.
    echo Beispiel: batch_vhi.bat oed_prod_config.py 2024 04
    goto :eof
)

if "!year!"=="" (
    echo Bitte Jahr als Parameter eingeben.
    goto :eof
)

if "!month!"=="" (
    echo Bitte Monat als Parameter eingeben.
    goto :eof
)

set "days_in_month=31"
if !month!==04 set "days_in_month=30"
if !month!==06 set "days_in_month=30"
if !month!==09 set "days_in_month=30"
if !month!==11 set "days_in_month=30"
if !month!==02 (
    set "days_in_month=28"
    set /a "is_leap_year=(!year! %% 4)==0 && (!year! %% 100)!=0 || (!year! %% 400)==0"
    if !is_leap_year!==1 set "days_in_month=29"
)

for /L %%D in (1,1,!days_in_month!) do (
    set "day=%%D"
    if %%D LSS 10 set "day=0%%D"
    python satromo_processor.py !config_file! !year!-!month!-!day!
)

:publish_loop
ping -n 3600 localhost >nul
python satromo_publish.py !config_file!

for /f %%A in ('find /c /v "" ^< processing/running_task.csv') do set "lines=%%A"
if !lines! GTR 1 goto publish_loop

echo *********************************************
echo VHI Prozess fuer !year! !month! abgeschlossen.
echo *********************************************
echo *********************************************
pause

endlocal