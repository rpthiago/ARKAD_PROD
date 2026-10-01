@echo off
chcp 65001 > nul
cd /d "%~dp0\.."
echo ========================================================
echo   ARKAD - MONITOR CONTÍNUO DE VOOS (A CADA 3 HORAS)
echo ========================================================
python -m buscador_voos.main --monitor
pause
