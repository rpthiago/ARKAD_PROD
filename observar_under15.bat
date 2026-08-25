@echo off
REM ============================================================================
REM observar_under15.bat - observacao STAKE-ZERO diaria do Lay Under 1.5 (XGBoost)
REM Agende DIARIO:
REM   schtasks /create /tn "UNDER15_observacao" /sc daily /st 08:45 /tr "%~f0"
REM Roda o observador forward-only: registra jogos futuros com sinal, liquida os
REM pendentes que ja tem placar. stake=0 (nao aposta). Log em observar_under15_log.txt.
REM ============================================================================
cd /d "C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
echo. >> observar_under15_log.txt
echo ===== %date% %time% ===== >> observar_under15_log.txt
"..\DASHBOARD_ARKAD-1\.venv\Scripts\python.exe" observar_under15_forward.py >> observar_under15_log.txt 2>&1
git add observacao_under15_forward.csv >> observar_under15_log.txt 2>&1
git commit -m "update: observacao under15 forward %date%" >> observar_under15_log.txt 2>&1
git push origin main >> observar_under15_log.txt 2>&1
echo (fim) >> observar_under15_log.txt
