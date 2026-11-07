@echo off
REM =========================================================================
REM  SMART PARKING MONITORING SYSTEM - TIMOR PLAZA DILI
REM  Windows startup helper.
REM
REM  Usage:  run_system.bat            -> interactive menu
REM          run_system.bat setup      -> create venv + install dependencies
REM          run_system.bat data       -> generate the 10,000-record dataset
REM          run_system.bat db         -> create the database and the schema
REM          run_system.bat producer   -> start the Kafka producer
REM          run_system.bat spark      -> start Spark Structured Streaming
REM          run_system.bat dashboard  -> start the Streamlit dashboard
REM          run_system.bat demo       -> load demo data (no Kafka/Spark)
REM          run_system.bat test       -> run the test suite
REM          run_system.bat all        -> open producer, Spark and dashboard
REM =========================================================================

setlocal
cd /d "%~dp0"
set PYTHON=python
if exist "venv\Scripts\python.exe" set PYTHON=venv\Scripts\python.exe

if "%~1"=="" goto MENU
goto %~1

:MENU
cls
echo ==========================================================
echo   TIMOR PLAZA - SMART PARKING MONITORING SYSTEM
echo   Big Data Capstone Project - DIT 2026
echo ==========================================================
echo.
echo   [1] Setup        - create venv and install dependencies
echo   [2] Data         - generate the 10,000-record dataset
echo   [3] Database     - create smart_parking and its schema
echo   [4] Producer     - stream events into Kafka
echo   [5] Spark        - start Structured Streaming
echo   [6] Dashboard    - open the Streamlit dashboard
echo   [7] Demo data    - fill the database without Kafka/Spark
echo   [8] Tests        - run the test suite
echo   [9] Performance  - run the scalability test
echo   [A] ALL          - start producer + Spark + dashboard
echo   [0] Exit
echo.
set /p choice="  Select an option: "
if "%choice%"=="1" goto setup
if "%choice%"=="2" goto data
if "%choice%"=="3" goto db
if "%choice%"=="4" goto producer
if "%choice%"=="5" goto spark
if "%choice%"=="6" goto dashboard
if "%choice%"=="7" goto demo
if "%choice%"=="8" goto test
if "%choice%"=="9" goto perf
if /i "%choice%"=="A" goto all
if "%choice%"=="0" goto END
goto MENU

:setup
echo.
echo [SETUP] Creating the virtual environment...
python -m venv venv
echo [SETUP] Installing dependencies (this takes a few minutes)...
venv\Scripts\python.exe -m pip install --upgrade pip
venv\Scripts\python.exe -m pip install -r requirements.txt
echo.
echo [SETUP] Done. Copy .env.example to .env and set POSTGRES_PASSWORD.
pause
goto END

:data
echo.
echo [DATA] Generating 10,000 parking events...
%PYTHON% data_generator\generate_dataset.py --records 10000
pause
goto END

:db
echo.
echo [DATABASE] Creating the smart_parking database and its schema...
%PYTHON% database\database.py --init
pause
goto END

:producer
echo.
echo [KAFKA] Starting the producer (Ctrl+C to stop)...
%PYTHON% producer\kafka_producer.py --create-topic --rate 5
pause
goto END

:spark
echo.
echo [SPARK] Starting Structured Streaming (Ctrl+C to stop)...
%PYTHON% spark\spark_streaming.py
pause
goto END

:dashboard
echo.
echo [DASHBOARD] Starting Streamlit on http://localhost:8501 ...
%PYTHON% -m streamlit run dashboard\app.py
pause
goto END

:demo
echo.
echo [DEMO] Loading the dataset straight into PostgreSQL...
%PYTHON% scripts\load_demo_data.py --truncate
pause
goto END

:test
echo.
echo [TEST] Running the test suite...
%PYTHON% -m pytest -v
pause
goto END

:perf
echo.
echo [PERFORMANCE] Running the scalability test...
%PYTHON% scripts\performance_test.py --sizes 10000 50000 100000
pause
goto END

:all
echo.
echo [ALL] Opening one window per component...
echo       Make sure Kafka and PostgreSQL are already running.
start "Kafka Producer" cmd /k "%PYTHON% producer\kafka_producer.py --create-topic --rate 5"
timeout /t 3 /nobreak >nul
start "Spark Streaming" cmd /k "%PYTHON% spark\spark_streaming.py"
timeout /t 12 /nobreak >nul
start "Dashboard" cmd /k "%PYTHON% -m streamlit run dashboard\app.py"
echo.
echo   Producer, Spark and the dashboard are starting in separate windows.
echo   Dashboard: http://localhost:8501
pause
goto END

:END
endlocal
