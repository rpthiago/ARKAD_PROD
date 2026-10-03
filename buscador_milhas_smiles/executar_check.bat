@echo off
chcp 65001 > nul
cd /d "%~dp0\.."
echo ========================================================
echo   RADAR DE MILHAS SMILES & PARCEIRAS — BUSCA MANUAL
echo ========================================================
C:\Users\thiag\anaconda3\python.exe -m buscador_milhas_smiles.main
pause
