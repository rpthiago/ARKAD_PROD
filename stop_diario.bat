@echo off
REM Stop Red (-10% ou 2 reds) / Stop Green (+10%) do dia sobre a grade da pagina 01 (planilha diaria). A cada 5 min.
set PY="C:\Users\thiag\anaconda3\envs\streamlit_env\python.exe"
cd /d "C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
set PYTHONIOENCODING=utf-8
%PY% stop_diario.py --once >> metodos_aprovados\stop_diario.log 2>&1
