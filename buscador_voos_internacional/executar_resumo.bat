@echo off
chcp 65001 > nul
cd /d "%~dp0\.."
echo ===========================================================================
echo   ARKAD - ENVIAR RANKING INTERNACIONAL DE 10 DIAS PARA O TELEGRAM
echo ===========================================================================
python -m buscador_voos_internacional.main --resumo
pause
