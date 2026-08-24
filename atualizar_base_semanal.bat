@echo off
REM ============================================================================
REM atualizar_base_semanal.bat - baixa a base completa + sincroniza a lean + push
REM Agende SEMANAL (ex.: segunda 08:00):
REM   schtasks /create /tn "ARKAD_base_semanal" /sc weekly /d MON /st 08:00 /tr "%~f0"
REM   1) baixar_base_completa.py   -> baixa a base full do FutPythonTrader (~229 MB)
REM   2) atualizar_lean_base.py    -> gera a b365_base_lean.csv (versionada) do full
REM   3) git push da lean          -> Streamlit Cloud passa a usar dados atuais
REM Log em base_log.txt.
REM ============================================================================
cd /d "C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
echo. >> base_log.txt
echo ===== %date% %time% ===== >> base_log.txt
".venv\Scripts\python.exe" baixar_base_completa.py >> base_log.txt 2>&1
".venv\Scripts\python.exe" atualizar_lean_base.py >> base_log.txt 2>&1
git add b365_base_lean.csv >> base_log.txt 2>&1
git commit -m "update: base semanal %date%" >> base_log.txt 2>&1
git push origin main >> base_log.txt 2>&1
echo (fim) >> base_log.txt
