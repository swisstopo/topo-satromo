@echo off
setlocal EnableDelayedExpansion

set "config_file=%~1"
set "date=%~2"

echo *********************************************
echo ACHTUNG: 1) Sicherstellen, dass nur S2_SR Produkt im config aktiviert ist!
echo ACHTUNG: 2) Sicherstellen, dass S2_SR temporal_coverage im config richtig gesetzt ist!
echo *********************************************
echo Bitte Enter druecken, um fortzufahren...
pause >nul

if "!config_file!"=="" (
    echo Bitte Konfigurationsfile und Datum als Parameter eingeben.
    echo Beispiel: batch_script.bat oed_prod_config.py 2024-08-31
    goto :eof
)

if "!date!"=="" (
    echo Bitte Datum als Parameter eingeben. zb 2024-08-31
    goto :eof
)

python satromo_processor.py !config_file! !date! >nul

echo 4.5h warten fuer Ende des ersten Durchlaufes (2 parallel exporte)<nul
ping -n 16000 localhost <nul

echo Start Export <nul
python satromo_processor.py !config_file! !date! >nul
ping -n 3600 localhost >nul

:publish_loop
echo Start Publisher <nul
python satromo_publish.py !config_file! >nul
ping -n 900 localhost >nul

for /f %%A in ('find /c /v "" ^< processing/running_tasks.csv') do set "lines=%%A"
if !lines! GTR 1 goto publish_loop

echo *********************************************
echo S2 SR Prozess fuer !date! abgeschlossen.
echo *********************************************
echo *********************************************
pause

endlocal