@echo off
REM Forward diario dos 5 metodos do Portfolio em Validacao Forward -> Telegram. Roda 23:30 pelo Agendador.
set PY="C:\Users\thiag\anaconda3\envs\streamlit_env\python.exe"
cd /d "C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
set PYTHONIOENCODING=utf-8
echo ===== %date% %time% ===== >> metodos_aprovados\forward_5metodos.log
%PY% relatorio_forward_5metodos.py >> metodos_aprovados\forward_5metodos.log 2>&1
