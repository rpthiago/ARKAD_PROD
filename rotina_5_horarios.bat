@echo off
REM ============================================================================
REM rotina_5_horarios.bat — Radar ARKAD em 5 Horários Estratégicos (Odds Reais)
REM Dispara automaticamente nos horários de maior liquidez da Betfair:
REM   06:00 | 10:30 | 13:00 | 15:30 | 18:30
REM ============================================================================
cd /d "C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
set PYTHONIOENCODING=utf-8
"C:\Users\thiag\anaconda3\python.exe" executar_radar_5_blocos.py %* >> metodos_aprovados\radar_5_blocos.log 2>&1
