@echo off
chcp 65001 > nul
cd /d "%~dp0\.."
echo ===========================================================================
echo   ARKAD - ENVIAR RANKING DE SMARTWATCHES PARA O TELEGRAM
echo ===========================================================================
python -m buscador_smartwatch.main --resumo
pause
