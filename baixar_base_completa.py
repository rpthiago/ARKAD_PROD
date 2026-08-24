"""
baixar_base_completa.py — baixa a base historica completa do FutPythonTrader (API) e salva
local em Bases_de_Dados_API_FutPythonTrader_Bet365.csv. Rode 1x/semana (Agendador).
Depois o atualizar_base_semanal.bat roda atualizar_lean_base.py + git push.

Protecao: so sobrescreve se o download for valido e nao vier truncado (menor que a base atual).
"""
import io
import os
import pandas as pd
import b365_data_utils as B

DEST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")

headers = {"User-Agent": "Mozilla/5.0"}
if getattr(B, "API_TOKEN", None):
    headers["Authorization"] = f"Token {B.API_TOKEN}"

print("baixando base completa de %s ..." % B.API_B365_HIST.split("/")[2])
r = B._get_http_session().get(B.API_B365_HIST, headers=headers, timeout=(B.CONNECT_TIMEOUT_SEC, 600))
r.raise_for_status()
df = pd.read_csv(io.StringIO(r.text))
if df.empty or "Date" not in df.columns:
    raise SystemExit("download vazio/invalido — base NAO sobrescrita")

if os.path.exists(DEST):
    antigo = sum(1 for _ in open(DEST, encoding="utf-8", errors="ignore")) - 1
    if len(df) < antigo * 0.9:
        raise SystemExit("download menor que a base atual (%d < %d) — abortando p/ nao corromper" % (len(df), antigo))

df.to_csv(DEST, index=False)
print("base salva: %d linhas | data max %s | %.0f MB"
      % (len(df), pd.to_datetime(df["Date"], errors="coerce").max().date(), os.path.getsize(DEST) / 1048576))
