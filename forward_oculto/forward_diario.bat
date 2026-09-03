@echo off
REM Forward OCULTO 3 metodos — captura + tenta liquidar. Roda 1x/dia pelo Agendador.
set PY="C:\Users\thiag\anaconda3\envs\streamlit_env\python.exe"
set DIR=C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD\forward_oculto
cd /d "%DIR%"
%PY% forward_capturar.py  >> forward_oculto.log 2>&1
%PY% forward_liquidar.py  >> forward_oculto.log 2>&1
