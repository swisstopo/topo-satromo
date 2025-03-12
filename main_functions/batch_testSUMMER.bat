@echo off
setlocal EnableDelayedExpansion



echo *********************************************
echo ACHTUNG: Sicherstellen dass nur VHI Produkt im config aktiviert ist!
echo *********************************************

REM python satromo_processor.py oed_config.py 2018-06-24
python satromo_processor.py oed_config.py 2018-07-01
python satromo_processor.py oed_config.py 2018-07-08
python satromo_processor.py oed_config.py 2018-07-15
python satromo_processor.py oed_config.py 2018-07-22
python satromo_processor.py oed_config.py 2018-07-29
python satromo_processor.py oed_config.py 2018-08-05
python satromo_processor.py oed_config.py 2018-08-12
python satromo_processor.py oed_config.py 2018-08-19
python satromo_processor.py oed_config.py 2018-08-26
python satromo_processor.py oed_config.py 2019-02-24
python satromo_processor.py oed_config.py 2019-03-24
python satromo_processor.py oed_config.py 2019-06-23
python satromo_processor.py oed_config.py 2019-06-30
python satromo_processor.py oed_config.py 2019-07-07
python satromo_processor.py oed_config.py 2019-07-14
python satromo_processor.py oed_config.py 2019-07-21
python satromo_processor.py oed_config.py 2019-07-28
python satromo_processor.py oed_config.py 2019-08-04
python satromo_processor.py oed_config.py 2019-08-11
python satromo_processor.py oed_config.py 2019-08-18
python satromo_processor.py oed_config.py 2019-08-25
python satromo_processor.py oed_config.py 2020-02-23
python satromo_processor.py oed_config.py 2020-03-22
python satromo_processor.py oed_config.py 2020-06-28
python satromo_processor.py oed_config.py 2020-07-05
python satromo_processor.py oed_config.py 2020-07-12
python satromo_processor.py oed_config.py 2020-07-19
python satromo_processor.py oed_config.py 2020-07-26
python satromo_processor.py oed_config.py 2020-08-02
python satromo_processor.py oed_config.py 2020-08-09
python satromo_processor.py oed_config.py 2020-08-16
python satromo_processor.py oed_config.py 2020-08-23
python satromo_processor.py oed_config.py 2021-02-28
python satromo_processor.py oed_config.py 2021-03-28
python satromo_processor.py oed_config.py 2021-06-27
python satromo_processor.py oed_config.py 2021-07-04
python satromo_processor.py oed_config.py 2021-07-11
python satromo_processor.py oed_config.py 2021-07-18
python satromo_processor.py oed_config.py 2021-07-25
python satromo_processor.py oed_config.py 2021-08-01
python satromo_processor.py oed_config.py 2021-08-08
python satromo_processor.py oed_config.py 2021-08-15
python satromo_processor.py oed_config.py 2021-08-22
python satromo_processor.py oed_config.py 2021-08-29
python satromo_processor.py oed_config.py 2022-02-20
python satromo_processor.py oed_config.py 2022-02-27
python satromo_processor.py oed_config.py 2022-03-27
python satromo_processor.py oed_config.py 2022-06-26
python satromo_processor.py oed_config.py 2022-07-03
python satromo_processor.py oed_config.py 2022-07-10
python satromo_processor.py oed_config.py 2022-07-17
python satromo_processor.py oed_config.py 2022-07-24
python satromo_processor.py oed_config.py 2022-07-31
python satromo_processor.py oed_config.py 2022-08-07
python satromo_processor.py oed_config.py 2022-08-14
python satromo_processor.py oed_config.py 2022-08-21
python satromo_processor.py oed_config.py 2022-08-28

ping -n 1800 localhost >nul

:publish_loop
echo Start Publisher <nul
python satromo_publish.py oed_config.py 
echo %TIME% waiting 15min
ping -n 900 localhost >nul

for /f %%A in ('find /c /v "" ^< processing/running_tasks.csv') do set "lines=%%A"
if !lines! GTR 1 goto publish_loop

echo *********************************************
echo   ASSET in VHI wieder auf TRUE setzen
echo *********************************************
echo *********************************************<nul
