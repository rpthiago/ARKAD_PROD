@echo off
REM Picks do dia do Lay 0x0 XGBoost (regra congelada). Roda de manha. Salva picks_0x0_<data>.csv.
set PY="C:\Users\thiag\anaconda3\envs\streamlit_env\python.exe"
set DIR=C:\Users\thiag\OneDrive\Documentos\GitHub\DASHBOARD_ARKAD-1
cd /d "%DIR%"
set PYTHONIOENCODING=utf-8
%PY% forward_0x0\gerar_picks_dia.py >> forward_0x0\picks_dia.log 2>&1
REM incorpora os picks de hoje no ledger e liquida os pendentes que a base ja alcancou.
REM Diario de proposito: a base b365 publica com ~5 dias de atraso, e o job semanal deixaria
REM um pick pendente por ate uma semana.
%PY% forward_0x0\liquidar_picks_0x0.py >> forward_0x0\ledger.log 2>&1
