@echo off
REM ============================================================================
REM atualizar_paper.bat - pipeline diario do paper trading (Agendador de Tarefas)
REM Agende p/ rodar 1x/dia (ex.: 09:00):
REM   schtasks /create /tn "ARKAD_paper" /sc daily /st 09:00 /tr "%~f0"
REM Faz, em sequencia:
REM   1) gera os sinais do dia dos 8 metodos (local, sem Streamlit Cloud)
REM   2) consolida + puxa o placar real do coletor Betfair -> paper_consolidado.csv
REM Log em consolidar_log.txt.
REM ============================================================================
cd /d "C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
echo. >> consolidar_log.txt
echo ===== %date% %time% ===== >> consolidar_log.txt
".venv\Scripts\python.exe" gerar_sinais_local.py >> consolidar_log.txt 2>&1
".venv\Scripts\python.exe" consolidar_sinais.py --dias 5 >> consolidar_log.txt 2>&1
echo (fim) >> consolidar_log.txt
