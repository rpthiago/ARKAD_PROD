"""
atualizar_lean_base.py — sincroniza a b365_base_lean.csv (versionada no Git, usada pelo
Streamlit Cloud) com a base COMPLETA local. A base completa (~228 MB) nao vai pro Git; o
Cloud usa a lean. Se a lean ficar velha, o Cloud gera sinais DIFERENTES do local (bug real
de agosto/2026: lean parada em julho -> Cloud com feature de 6 semanas).

RODE ISTO sempre que atualizar a base completa, depois:
    git add b365_base_lean.csv && git commit -m "update: lean base" && git push
"""
import os
import pandas as pd

FULL = "Bases_de_Dados_API_FutPythonTrader_Bet365.csv"
LEAN = "b365_base_lean.csv"

if not os.path.exists(FULL):
    raise SystemExit("base completa nao encontrada: " + FULL)
cols = list(pd.read_csv(LEAN, nrows=1).columns) if os.path.exists(LEAN) else None
d = pd.read_csv(FULL, usecols=(lambda c: c in cols) if cols else None, low_memory=False)
d.to_csv(LEAN, index=False)
print("b365_base_lean.csv sincronizada: %d linhas | data max %s | %.0f MB"
      % (len(d), pd.to_datetime(d["Date"], errors="coerce").max().date(),
         os.path.getsize(LEAN) / 1048576))
print("agora: git add b365_base_lean.csv && git commit -m 'update: lean base' && git push")
