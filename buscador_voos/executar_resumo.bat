@echo off
chcp 65001 > nul
cd /d "%~dp0\.."
echo ========================================================
echo   ARKAD - RANKING COMPLETO DE VOOS PARA O TELEGRAM
echo ========================================================
python -m buscador_voos.main --resumo
pause
