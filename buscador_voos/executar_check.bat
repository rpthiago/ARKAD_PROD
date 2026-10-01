@echo off
chcp 65001 > nul
cd /d "%~dp0\.."
echo ========================================================
echo   ARKAD - BUSCADOR DE VOOS BH -^> NORDESTE (21/12 a 28/12)
echo ========================================================
python -m buscador_voos.main --check
pause
