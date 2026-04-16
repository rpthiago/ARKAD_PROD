@echo off
cd /d "%~dp0"

set "PYTHON_EXE=python"
if exist ".venv\Scripts\python.exe" set "PYTHON_EXE=.venv\Scripts\python.exe"

start "ARKAD Servidor" cmd /k ""%PYTHON_EXE%" servidor_arkad.py"
timeout /t 5 /nobreak >nul
start "ARKAD Dashboard" cmd /k ""%PYTHON_EXE%" -m streamlit run main.py"

echo Servidor e Dashboard iniciados.
