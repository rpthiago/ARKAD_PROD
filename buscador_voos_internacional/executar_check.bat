@echo off
chcp 65001 > nul
cd /d "%~dp0\.."
echo ===========================================================================
echo   ARKAD - CHECAGEM DE PROMOÇÕES AGRESSIVAS (EUROPA, TURQUIA & JAPÃO)
echo ===========================================================================
python -m buscador_voos_internacional.main --check
pause
