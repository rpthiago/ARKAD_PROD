"""
estudo_lay0x0_vs_over05.py — Mesmo evento (sai gol / não-0-0), dois mercados.
Pega os picks HONESTOS do Lay 0x0 (+23.5%, wf_0x0_prod_full_ctx_bets.csv) e compara,
jogo a jogo, o P&L de:
  (A) Lay 0x0   — odd lay REAL Betfair (exchange, comissão 5%)
  (B) Back Over 0.5 FT — odd de back b365 (bookmaker, sem comissão)
Ambos ganham se NÃO for 0-0 (mesma win rate). Comparação JUSTA = por unidade de RISCO
(normaliza os dois para 'perda máxima = 1'), pois layar arrisca liability grande e
backear over arrisca só a stake.
"""
import re, unicodedata
import numpy as np, pandas as pd
from pathlib import Path

HERE = Path(__file__).resolve().parent
DASH = Path(r"c:/Users/thiag/OneDrive/Documentos/GitHub/DASHBOARD_ARKAD-1")
PICKS = HERE / "wf_0x0_prod_full_ctx_bets.csv"
FULL = DASH / "Bases_de_Dados_API_FutPythonTrader_Bet365.csv"
COMM = 0.05

def canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

# ── picks honestos do 0x0 (janela confiável) ─────────────────────────────────
p = pd.read_csv(PICKS)
p = p[p["mes"] >= "2025-08"].copy()
p["d"] = p["Date"].astype(str).str[:10]
p["ch"] = p["Home"].map(canon); p["ca"] = p["Away"].map(canon)

# ── odd Over 0.5 FT (back b365) casada por jogo ──────────────────────────────
b = pd.read_csv(FULL, usecols=["Date", "Home", "Away", "Odd_Over05_FT"], low_memory=False)
b["d"] = pd.to_datetime(b["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
b["ch"] = b["Home"].map(canon); b["ca"] = b["Away"].map(canon)
b["o_over"] = pd.to_numeric(b["Odd_Over05_FT"], errors="coerce")
b = b.dropna(subset=["o_over"]).drop_duplicates(["d", "ch", "ca"])

m = p.merge(b[["d", "ch", "ca", "o_over"]], on=["d", "ch", "ca"], how="left")
m = m[(m["o_over"] > 1.0)].copy()
print(f"picks 0x0 (ago/2025+): {len(p)} | com odd Over0.5 casada: {len(m)}")

# ── P&L por STAKE (forma natural de cada aposta) ─────────────────────────────
# Lay 0x0: green +0.95 ; red -(odd_lay-1)   [já em m['pnl']]
# Back Over 0.5 (bookmaker, sem comissão): green +(o_over-1) ; red -1
m["pnl_over"] = np.where(m["target"] == 1, m["o_over"] - 1.0, -1.0)

# ── Normalização para UNIDADE DE RISCO (perda máx = 1) — comparação justa ─────
m["nlay"]  = m["pnl"] / (m["odd_lay"] - 1.0)   # green 0.95/(odd-1) ; red -1
m["nover"] = m["pnl_over"]                       # já tem perda máx 1

wr = m["target"].mean()
def roi(x): return x.sum() / len(x)

print("\n=== MESMO EVENTO (não-0-0) — dois mercados, mesmos jogos ===")
print(f"  jogos: {len(m)} | win rate (não-0-0): {wr:.2%} | 0-0: {(1-wr):.2%}")
print(f"  odd LAY 0x0 (Betfair) mediana:      {m['odd_lay'].median():.1f}")
print(f"  odd Over 0.5 FT (b365) mediana:     {m['o_over'].median():.3f}")
print(f"  odd Over 0.5 'equivalente' à lay:   {(m['odd_lay']/(m['odd_lay']-1)).median():.3f}  (= odd_lay/(odd_lay-1))")

print("\n=== ROI por UNIDADE DE RISCO (perda máx = 1) — comparação justa ===")
print(f"  (A) Lay 0x0        (Betfair, 5% comm):  {roi(m['nlay']):+.2%}")
print(f"  (B) Back Over 0.5  (b365, sem comm):    {roi(m['nover']):+.2%}")

# Over 0.5 tambГ©m na EXCHANGE (5% comm no green) para comparar mesma venue
m["nover_exch"] = np.where(m["target"] == 1, (m["o_over"] - 1.0) * (1 - COMM), -1.0)
print(f"  (B') Back Over 0.5 (se na exchange, 5% comm): {roi(m['nover_exch']):+.2%}")

# quantos jogos a odd de over paga MAIS que a lay-equivalente
melhor = (m["o_over"] > m["odd_lay"] / (m["odd_lay"] - 1)).mean()
print(f"\n  Over 0.5 paga MAIS que a lay-equivalente em {melhor:.0%} dos jogos.")
print("  (>50% => backear Over 0.5 no bookmaker capta MAIS edge que layar 0x0)")

# por mГЄs (robustez)
mm = m.groupby("mes").apply(lambda g: pd.Series({
    "n": len(g), "roi_lay": roi(g["nlay"]), "roi_over": roi(g["nover"])})).reset_index()
print("\n  Por mГЄs (ROI por unidade de risco):")
print(f"  {'mes':<9}{'n':>5}{'lay0x0':>10}{'over05':>10}")
for _, r in mm.iterrows():
    print(f"  {r['mes']:<9}{int(r['n']):>5}{r['roi_lay']:>+10.1%}{r['roi_over']:>+10.1%}")

m.to_csv(HERE / "estudo_lay0x0_vs_over05_bets.csv", index=False)
print("\nSalvo: estudo_lay0x0_vs_over05_bets.csv")
