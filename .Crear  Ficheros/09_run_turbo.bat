@echo off
setlocal
cd /d "%~dp0.."

set TOTAL_WORKERS=4
set /p AH_FILTER="Enter AH Filter (default 'all'): " || set AH_FILTER=all

echo Launching %TOTAL_WORKERS% workers with filter: %AH_FILTER%
echo Source: SQL history + SQL buckets

for /L %%i in (0,1,3) do (
    start "Worker %%i" cmd /k "cd /d ""%~dp0.."" && py cli_scraper.py --index %%i --total %TOTAL_WORKERS% --ah %AH_FILTER%"
)

echo All workers launched!
endlocal
