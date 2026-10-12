# -*- coding: utf-8 -*-
import sys, os, re
sys.stdout.reconfigure(encoding="utf-8")
import pandas as pd, numpy as np
S = sys.argv[1]; sys.path.insert(0, S)
import sinais_ko_core as K
B = pd.read_csv(r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD\metodos_aprovados\.cache_base_betfair.csv", low_memory=False)
B["Date"] = B["Date"].astype(str).str[:10]
B = B[B["Date"] >= "2026-01-01"]
for c in ("Odd_H_Back","Odd_A_Back","Odd_D_Back","Odd_D_Lay","Goals_H_FT","Goals_A_FT"):
    B[c] = pd.to_numeric(B.get(c), errors="coerce")
B = B.dropna(subset=["Goals_H_FT","Goals_A_FT","Odd_H_Back","Odd_A_Back","Odd_D_Back","Odd_D_Lay"])
RE_COPA = re.compile(r"\b(CUP|COPA|CHAMPIONS|EUROPA LEAGUE|CONFERENCE|LIBERTADORES|SUDAMERICANA|POKAL|COUPE|COPPA|TACA|TROPHY|SHIELD|SUPERCUP|SUPER CUP|QUALIF|PLAYOFF)\b", re.I)
B["copa"] = B["League"].astype(str).apply(lambda x: bool(RE_COPA.search(x)))
B["fav"] = B[["Odd_H_Back","Odd_A_Back"]].min(axis=1)
B["ign"] = B.apply(lambda r: K.eh_jogo_ignorado(r["Home"], r["Away"], r["League"]), axis=1)
ok = np.where(B["copa"], B["Odd_H_Back"] <= 1.40, B["fav"] <= 1.40)
D = B[ok & ~B["ign"] & B["Odd_D_Lay"].between(4.5, 10.0)].copy()
C = 0.05
D["red"] = D["Goals_H_FT"] == D["Goals_A_FT"]
D["pnl"] = np.where(D["red"], -1.0, (1-C)/(D["Odd_D_Lay"]-1))
# H4: spread do empate
D["spread"] = D["Odd_D_Lay"] / D["Odd_D_Back"]
# H5: folga de EV usando so precos
inv = 1/D["Odd_H_Back"] + 1/D["Odd_D_Back"] + 1/D["Odd_A_Back"]
D["p_justa"] = (1/D["Odd_D_Back"]) / inv              # prob justa do empate, sem overround
D["p_be"] = (1-C) / ((D["Odd_D_Lay"]-1) + (1-C))      # prob de empate no break-even
D["folga"] = D["p_be"] - D["p_justa"]                 # >0 = o lay tem margem
# H6: divisao principal
PRINC = re.compile(r"\b(1|A|PREMIER|LA LIGA|SERIE A|BUNDESLIGA|LIGUE 1|EREDIVISIE|MLS|J LEAGUE 1|K LEAGUE 1)\b", re.I)
D["principal"] = D["League"].astype(str).str.strip().str.endswith(" 1") | D["League"].astype(str).str.contains(PRINC)

CORTE = "2026-05-15"
H1 = D[D["Date"] <= CORTE]; H2 = D[D["Date"] > CORTE]
print("LAY DRAW 2026 governanca+janela: %d jogos | H1 %d | H2 %d | ROI geral %+.2f%%\n"
      % (len(D), len(H1), len(H2), 100*D["pnl"].mean()))

def tab(df, col, bins, rot):
    d = df.dropna(subset=[col]).copy(); d["f"] = pd.cut(d[col], bins)
    t = d.groupby("f", observed=True).agg(n=("red","size"), reds=("red","sum"), roi=("pnl","mean"))
    t["wr"] = 100*(1-t["reds"]/t["n"]); t["roi"] = 100*t["roi"]; t.index.name = rot
    return t[["n","wr","roi"]].round(2)

print("="*74); print("H4 — SPREAD do mercado de empate (lay/back). Previsao: apertado e melhor"); print("="*74)
bs = [1.0, 1.04, 1.08, 1.15, 1.30, 3.0]
print("\n-- H1 --"); print(tab(H1,"spread",bs,"spread").to_string())
print("\n-- H2 --"); print(tab(H2,"spread",bs,"spread").to_string())
print("\n" + "="*74); print("H5 — FOLGA de EV (prob de break-even menos prob justa). Previsao: folga maior e melhor"); print("="*74)
bf = [-1, -0.02, 0, 0.02, 0.04, 1]
print("\n-- H1 --"); print(tab(H1,"folga",bf,"folga").to_string())
print("\n-- H2 --"); print(tab(H2,"folga",bf,"folga").to_string())
print("\n" + "="*74); print("H6 — divisao principal vs secundaria"); print("="*74)
for rot, df in (("H1",H1),("H2",H2)):
    t = df.groupby("principal").agg(n=("red","size"), reds=("red","sum"), roi=("pnl","mean"), pnl=("pnl","sum"))
    t["wr"] = 100*(1-t["reds"]/t["n"]); t["roi"] = 100*t["roi"]; t.index = ["secundaria","principal"]
    print("\n--", rot); print(t[["n","wr","roi","pnl"]].round(2).to_string())
