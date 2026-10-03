@echo off
REM Planilha do dia a partir do ledger do KO-10 (coletor), para quando o feed da API estiver vazio.
REM Grava Sinais_Metodos_Aprovados_<data>_coletor.xlsx (nao sobrescreve a planilha da API).
set PY="C:\Users\thiag\anaconda3\envs\streamlit_env\python.exe"
set DIR=C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD
cd /d "%DIR%"
set PYTHONIOENCODING=utf-8
%PY% -X utf8 sinais_dia_coletor.py %* --separado >> metodos_aprovados\sinais_coletor.log 2>&1
