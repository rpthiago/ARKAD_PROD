"""
estudo_over05_betfair.py — Fecha a ressalva: Back Over 0.5 FT na PRÓPRIA Betfair (exchange).
Mesmos picks do Lay 0x0, mesma comissão (5%). Casa a odd Betfair Odd_Over05_FT_Back por jogo
(matcher canon+fuzzy, igual ao lay_de) e compara por unidade de risco:
  (A) Lay 0x0            (Betfair, 5% comm)
  (C) Back Over 0.5 FT   (Betfair, 5% comm)   <- o que faltava
Referência: (B) Back Over 0.5 no b365 (bookmaker) deu -0.75%.
"""
import io, re, difflib, unicodedata
import numpy as np, pandas as pd
from pathlib import Path

HERE = Path(__file__).resolve().parent
DASH = Path(r"c:/Users/thiag/OneDrive/Documentos/GitHub/DASHBOARD_ARKAD-1")
PICKS = HERE / "wf_0x0_prod_full_ctx_bets.csv"
CACHE = HERE / "_betfair_over05_cache.csv"
COMM = 0.05

def canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

# ── Betfair Over 0.5 FT (back) — download uma vez, cacheia ────────────────────
if CACHE.exists():
    bf = pd.read_csv(CACHE, low_memory=False)
    print(f"[betfair] cache: {len(bf)} jogos")
else:
    import requests
    import sys; sys.path.insert(0, str(DASH))
    from config import API_BETFAIR_BASE_URL, API_HEADERS
    print("[betfair] baixando...")
    r = requests.get(API_BETFAIR_BASE_URL, headers=dict(API_HEADERS), timeout=180)
    full = pd.read_csv(io.StringIO(r.text), low_memory=False)
    bf = full[["Date", "Home", "Away", "Odd_Over05_FT_Back", "Odd_Over05_FT_Lay"]].copy()
    bf.to_csv(CACHE, index=False)
    print(f"[betfair] cacheado: {len(bf)} jogos")

bf["d"] = pd.to_datetime(bf["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
bf["ch"] = bf["Home"].map(canon); bf["ca"] = bf["Away"].map(canon)
bf["o_bk"] = pd.to_numeric(bf["Odd_Over05_FT_Back"], errors="coerce")
exact, byday = {}, {}
for i, row in bf.iterrows():
    exact[(row["d"], row["ch"], row["ca"])] = i
    byday.setdefault(row["d"], []).append(i)

def over_de(d, h, a):
    ch, ca = canon(h), canon(a)
    idx = exact.get((d, ch, ca))
    if idx is None:
        best, bs = None, 0.0
        for i in byday.get(d, []):
            sh = difflib.SequenceMatcher(None, ch, bf.at[i, "ch"]).ratio()
            sa = difflib.SequenceMatcher(None, ca, bf.at[i, "ca"]).ratio()
            if (sh + sa) / 2 > bs and max(sh, sa) >= 0.5:
                bs, best = (sh + sa) / 2, i
        idx = best if bs >= 0.55 else None
    if idx is None: return np.nan
    v = bf.at[idx, "o_bk"]
    return v if (pd.notna(v) and v > 1.0) else np.nan

# ── picks 0x0 + casamento da odd Over 0.5 Betfair ────────────────────────────
p = pd.read_csv(PICKS)
p = p[p["mes"] >= "2025-08"].copy()
p["d"] = p["Date"].astype(str).str[:10]
p["o_over_bf"] = [over_de(d, h, a) for d, h, a in zip(p["d"], p["Home"], p["Away"])]
m = p.dropna(subset=["o_over_bf"]).copy()
print(f"picks 0x0 (ago/2025+): {len(p)} | com odd Over0.5 Betfair casada (>1): {len(m)} ({len(m)/len(p):.0%})")

wr = m["target"].mean()
# normalizado a unidade de risco (perda máx = 1)
m["nlay"]  = m["pnl"] / (m["odd_lay"] - 1.0)                                  # Lay 0x0 (5% comm)
m["nover_bf"] = np.where(m["target"] == 1, (m["o_over_bf"] - 1.0) * (1 - COMM), -1.0)  # Back Over0.5 Betfair (5% comm)
def roi(x): return x.sum() / len(x)

print("\n=== Back Over 0.5 FT na PRÓPRIA BETFAIR (exchange, 5% comm) — mesmos jogos ===")
print(f"  jogos: {len(m)} | win rate (não-0-0): {wr:.2%}")
print(f"  odd Over 0.5 Betfair (back) mediana:   {m['o_over_bf'].median():.3f}")
print(f"  odd Over 0.5 'equivalente' à lay 0x0:  {(m['odd_lay']/(m['odd_lay']-1)).median():.3f}")
print(f"  (b365 bookmaker era 1.030 — a exchange paga mais?)")
melhor = (m["o_over_bf"] > m["odd_lay"] / (m["odd_lay"] - 1)).mean()
print(f"  Over 0.5 Betfair paga MAIS que a lay-equivalente em {melhor:.0%} dos jogos.")

print("\n=== ROI por UNIDADE DE RISCO (perda máx = 1) ===")
print(f"  (A) Lay 0x0              (Betfair, 5% comm):  {roi(m['nlay']):+.2%}")
print(f"  (C) Back Over 0.5 FT     (Betfair, 5% comm):  {roi(m['nover_bf']):+.2%}")
print(f"      [ref (B) Over 0.5 b365 bookmaker: -0.75%]")

mm = m.groupby("mes").apply(lambda g: pd.Series({
    "n": len(g), "lay": roi(g["nlay"]), "over_bf": roi(g["nover_bf"])}), include_groups=False).reset_index()
print("\n  Por mes (ROI por unidade de risco):")
print(f"  {'mes':<9}{'n':>5}{'lay0x0':>10}{'over_bf':>10}")
for _, r in mm.iterrows():
    print(f"  {r['mes']:<9}{int(r['n']):>5}{r['lay']:>+10.1%}{r['over_bf']:>+10.1%}")

m.to_csv(HERE / "estudo_over05_betfair_bets.csv", index=False)
print("\nSalvo: estudo_over05_betfair_bets.csv")
