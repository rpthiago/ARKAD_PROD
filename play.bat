@echo off
cd /d "%~dp0"

start "ARKAD Servidor" cmd /k "python servidor_arkad.py"
start "ARKAD Dashboard" cmd /k "streamlit run main.py"

echo Servidor e Dashboard iniciados.
