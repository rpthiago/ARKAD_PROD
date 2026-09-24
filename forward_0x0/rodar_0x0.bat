@echo off
REM Lay 0x0 XGBoost — o dia inteiro num comando: picks + ledger + liquidacao.
REM Substitui gerar_picks_dia.bat e rodar_forward_0x0.bat. Log unico: forward_0x0\rodar_0x0.log
set PY="C:\Users\thiag\anaconda3\envs\streamlit_env\python.exe"
set DIR=C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD
cd /d "%DIR%"
set PYTHONIOENCODING=utf-8
%PY% forward_0x0\rodar_0x0.py %*
