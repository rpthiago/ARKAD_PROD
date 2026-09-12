@echo off
REM 23:30 — (1) retenta liquidar as planilhas diarias dos ultimos 10 dias pelas bases de placar;
REM         (2) relatorio dos 5 metodos em validacao forward -> Telegram.
set PY="C:\Users\thiag\anaconda3\envs\streamlit_env\python.exe"
cd /d "C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
set PYTHONIOENCODING=utf-8
echo ===== %date% %time% ===== >> metodos_aprovados\forward_5metodos.log
%PY% liquidar_pendentes_recentes.py 10 >> metodos_aprovados\forward_5metodos.log 2>&1
%PY% relatorio_forward_5metodos.py >> metodos_aprovados\forward_5metodos.log 2>&1
