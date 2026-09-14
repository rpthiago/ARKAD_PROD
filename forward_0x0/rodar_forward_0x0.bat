@echo off
REM Forward 0x0 XGBoost — regenera o OOS walk-forward (base atualizada) e analisa
REM backtest + forward (>=03/08) + criterio pre-registrado. Roda 1x/semana pelo Agendador.
set PY="C:\Users\thiag\anaconda3\envs\streamlit_env\python.exe"
set DIR=C:\Users\thiag\OneDrive\Documentos\GitHub\DASHBOARD_ARKAD-1
cd /d "%DIR%"
set PYTHONIOENCODING=utf-8
echo ===== %date% %time% ===== >> forward_0x0\forward_0x0.log
%PY% gerar_oos_real_0x0_xgb.py  >> forward_0x0\forward_0x0.log 2>&1
REM liquida o ledger ANTES de analisar — o forward sai do ledger, nao da simulacao (conserto 10/09)
%PY% forward_0x0\liquidar_picks_0x0.py >> forward_0x0\forward_0x0.log 2>&1
%PY% analisar_0x0_xgb.py        >> forward_0x0\forward_0x0.log 2>&1
%PY% forward_0x0\registrar_hist.py >> forward_0x0\forward_0x0.log 2>&1
