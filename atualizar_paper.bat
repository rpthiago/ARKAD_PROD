@echo off
REM ============================================================================
REM atualizar_paper.bat - roda o consolidador de sinais/placares (Agendador Tarefas)
REM Agende no "Agendador de Tarefas do Windows" p/ rodar 1x/dia (ex.: 09:00).
REM   Programa/script: C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD\atualizar_paper.bat
REM Ele: varre as planilhas locais, uniformiza, puxa o placar real do coletor
REM Betfair (VPS) e grava paper_consolidado.csv. Log em consolidar_log.txt.
REM ============================================================================
cd /d "C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
echo. >> consolidar_log.txt
echo ===== %date% %time% ===== >> consolidar_log.txt
".venv\Scripts\python.exe" consolidar_sinais.py --dias 4 >> consolidar_log.txt 2>&1
echo (fim) >> consolidar_log.txt
