@echo off
REM 23:30 — (1) relatorio dos 5 metodos em validacao forward -> Telegram (ledger proprio, nao toca planilhas);
REM         (2) conferencia de placares manuais x bases: SO AVISA, nao altera nada.
set PY="C:\Users\thiag\anaconda3\envs\streamlit_env\python.exe"
cd /d "C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
set PYTHONIOENCODING=utf-8
echo ===== %date% %time% ===== >> metodos_aprovados\forward_5metodos.log
%PY% relatorio_forward_5metodos.py >> metodos_aprovados\forward_5metodos.log 2>&1
%PY% conferir_placares_manuais.py >> metodos_aprovados\forward_5metodos.log 2>&1
