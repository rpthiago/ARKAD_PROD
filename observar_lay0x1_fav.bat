@echo off
REM ============================================================================
REM observar_lay0x1_fav.bat - forward stake-zero do Lay 0x1 favoritao (Odd_H<=2.20)
REM Agende DIARIO (perto dos jogos, p/ o coletor ja ter a lay do 0-1):
REM   schtasks /create /tn "LAY0X1_FAV_forward" /sc daily /st 12:00 /tr "%~f0"
REM Puxa favoritoes do dia + lay real do 0-1 (coletor), loga pendentes e liquida.
REM ============================================================================
cd /d "C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
echo. >> lay0x1_fav_log.txt
echo ===== %date% %time% ===== >> lay0x1_fav_log.txt
"..\DASHBOARD_ARKAD-1\.venv\Scripts\python.exe" observar_lay0x1_fav.py >> lay0x1_fav_log.txt 2>&1
git add lay0x1_fav_acumulado.csv >> lay0x1_fav_log.txt 2>&1
git commit -m "update: lay0x1 favoritao forward %date%" >> lay0x1_fav_log.txt 2>&1
git push origin main >> lay0x1_fav_log.txt 2>&1
echo (fim) >> lay0x1_fav_log.txt
