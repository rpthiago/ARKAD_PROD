@echo off
chcp 65001 > nul
cd /d "%~dp0\.."
echo ===========================================================================
echo   ARKAD - CHECAGEM DE SMARTWATCHES (AMAZFIT & HUAWEI)
echo ===========================================================================
python -m buscador_smartwatch.main --check
pause
