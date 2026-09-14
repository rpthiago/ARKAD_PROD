@echo off
set PY="C:\Users\thiag\anaconda3\envs\streamlit_env\python.exe"
cd /d "C:\Users\thiag\OneDrive\Documentos\GitHub\DASHBOARD_ARKAD-1"
set PYTHONIOENCODING=utf-8
%PY% forward_0x0\atualizar_clv.py >> forward_0x0\clv.log 2>&1
