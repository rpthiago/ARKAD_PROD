import pandas as pd, numpy as np
R = r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
rng = np.random.default_rng(20261008)
b365 = pd.read_csv(R + r"\Bases_de_Dados_API_FutPythonTrader_Bet365.csv",
                   usecols=["League","Date","Home","Away","Goals_H_FT","Goals_A_FT",
                            "Odd_H_FT","Odd_D_FT","Odd_A_FT"], low_memory=False)
bf = pd.read_csv(R + r"\Bases_de_Dados_API_FutPythonTrader_Betfair.csv",
                 usecols=["Date","Home","Away","Odd_D_Lay"], low_memory=False)
for d in (b365, bf):
    d["Date"] = pd.to_datetime(d["Date"], errors="coerce")
    for c in ("Home","Away"):
        d[c] = d[c].astype(str).str.strip()
j = b365.merge(bf, on=["Date","Home","Away"], how="inner").dropna(
    subset=["Goals_H_FT","Goals_A_FT","Odd_H_FT","Odd_D_FT","Odd_A_FT","Odd_D_Lay"])
j = j[(j[["Odd_H_FT","Odd_D_FT","Odd_A_FT","Odd_D_Lay"]] > 1).all(axis=1)]
j["ganha_lay"] = (j["Goals_H_FT"] != j["Goals_A_FT"])
sf = j[j[["Odd_H_FT","Odd_A_FT"]].min(axis=1) <= 1.40].copy()

def boot(sub, pnl, n=2000):
    dias = sub["Date"].values; uq = pd.unique(dias)
    idx = {d: np.where(dias == d)[0] for d in uq}
    out = np.empty(n)
    for i in range(n):
        esc = rng.choice(uq, size=len(uq), replace=True)
        out[i] = np.concatenate([pnl[idx[d]] for d in esc]).mean()
    return np.percentile(out, [2.5, 97.5])

sinal = sf[sf["Odd_D_FT"] >= 5.00]
print("Regra do radar: Super Fav <= 1.40 E empate b365 >= 5.00")
print("  sinais na base: %d jogos (%.1f%% de todos os Super Fav)\n" % (len(sinal), 100*len(sinal)/len(sf)))
print("%-34s %6s %7s %7s %10s %9s %9s   %s" % ("trava manual no OrbitX","N","% entra","WR","break-even","ROI c=5%","ROI c=14%","IC95 (c=5%)"))
print("-"*122)
for rot, teto in (("entra se lay <= 7.50 (proposta)", 7.50),
                  ("entra se lay <= 8.00", 8.00),
                  ("entra se lay <= 6.50", 6.50),
                  ("sem trava (entra sempre)", 1e9)):
    sub = sinal[sinal["Odd_D_Lay"] <= teto]
    if len(sub) < 30: continue
    o = sub["Odd_D_Lay"].values; g = sub["ganha_lay"].values
    be = ((o-1)/(o-0.05)).mean()
    p5 = np.where(g, (1-0.05)/(o-1), -1.0); p14 = np.where(g, (1-0.14)/(o-1), -1.0)
    ic = boot(sub, p5)
    print("%-34s %6d %6.1f%% %6.2f%% %9.2f%% %+8.2f%% %+8.2f%%   [%+.2f%%, %+.2f%%]"
          % (rot, len(sub), 100*len(sub)/len(sinal), 100*g.mean(), 100*be,
             100*p5.mean(), 100*p14.mean(), 100*ic[0], 100*ic[1]))

print("\n=== e se a regra do radar fosse OUTRA? (a escolha do 5.00 foi feita vendo o resultado) ===")
print("%-14s %6s %7s %10s %9s   %s" % ("corte b365","N","WR","break-even","ROI c=5%","IC95"))
print("-"*78)
for corte in (4.0, 4.5, 5.0, 5.5, 6.0, 6.5):
    sub = sf[(sf["Odd_D_FT"] >= corte) & (sf["Odd_D_Lay"] <= 7.50)]
    o = sub["Odd_D_Lay"].values; g = sub["ganha_lay"].values
    be = ((o-1)/(o-0.05)).mean(); p5 = np.where(g, 0.95/(o-1), -1.0)
    ic = boot(sub, p5)
    print("%-14s %6d %6.2f%% %9.2f%% %+8.2f%%   [%+.2f%%, %+.2f%%]"
          % (">= %.2f" % corte, len(sub), 100*g.mean(), 100*be, 100*p5.mean(), 100*ic[0], 100*ic[1]))
