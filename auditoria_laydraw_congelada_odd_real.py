import pandas as pd, numpy as np
R = r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
rng = np.random.default_rng(20261008)
b365 = pd.read_csv(R + r"\Bases_de_Dados_API_FutPythonTrader_Bet365.csv",
                   usecols=["League","Date","Home","Away","Goals_H_FT","Goals_A_FT",
                            "Odd_H_FT","Odd_D_FT","Odd_A_FT"], low_memory=False)
b365["Date"] = pd.to_datetime(b365["Date"], errors="coerce")
b365 = b365.dropna(subset=["Date","Goals_H_FT","Goals_A_FT"]).sort_values("Date")
b365["eh_empate"] = (b365["Goals_H_FT"] == b365["Goals_A_FT"]).astype(float)

# taxa de empate da liga AS-OF: media expandida dos jogos ANTERIORES da mesma liga
gr = b365.groupby("League")["eh_empate"]
b365["liga_draw_rate"] = gr.transform(lambda s: s.shift().expanding(min_periods=200).mean())

bf = pd.read_csv(R + r"\Bases_de_Dados_API_FutPythonTrader_Betfair.csv",
                 usecols=["Date","Home","Away","Odd_D_Lay"], low_memory=False)
bf["Date"] = pd.to_datetime(bf["Date"], errors="coerce")
for d in (b365, bf):
    for c in ("Home","Away"):
        d[c] = d[c].astype(str).str.strip()
j = b365.merge(bf, on=["Date","Home","Away"], how="inner").dropna(
    subset=["Odd_H_FT","Odd_D_FT","Odd_A_FT","Odd_D_Lay","liga_draw_rate"])
j = j[(j[["Odd_H_FT","Odd_D_FT","Odd_A_FT","Odd_D_Lay"]] > 1).all(axis=1)]
j["ganha_lay"] = j["eh_empate"] == 0

def boot(sub, pnl, n=2000):
    dias = sub["Date"].values; uq = pd.unique(dias)
    idx = {d: np.where(dias == d)[0] for d in uq}
    out = np.empty(n)
    for i in range(n):
        esc = rng.choice(uq, size=len(uq), replace=True)
        out[i] = np.concatenate([pnl[idx[d]] for d in esc]).mean()
    return np.percentile(out, [2.5, 97.5])

def linha(rot, sub):
    if len(sub) < 40:
        print("%-46s %6d  (amostra pequena demais)" % (rot, len(sub))); return
    o = sub["Odd_D_Lay"].values; g = sub["ganha_lay"].values
    be = ((o-1)/(o-0.05)).mean()
    p5 = np.where(g, 0.95/(o-1), -1.0); p65 = np.where(g, 0.935/(o-1), -1.0)
    ic = boot(sub, p5)
    print("%-46s %6d %6.2f%% %9.2f%% %+8.2f%% %+9.2f%%   [%+.2f%%, %+.2f%%]"
          % (rot, len(sub), 100*g.mean(), 100*be, 100*p5.mean(), 100*p65.mean(), 100*ic[0], 100*ic[1]))

print("jogos com liga_draw_rate as-of e odd de lay real: %d\n" % len(j))
print("%-46s %6s %7s %10s %9s %10s   %s" % ("regra","N","WR","break-even","ROI c=5%","ROI c=6.5%","IC95 (c=5%)"))
print("-"*120)
sf = j[j[["Odd_H_FT","Odd_A_FT"]].min(axis=1) <= 1.40]
linha("CONGELADA: liga_draw_rate < 0.23 (so isso)", j[j["liga_draw_rate"] < 0.23])
linha("CONGELADA + Super Fav <= 1.40", sf[sf["liga_draw_rate"] < 0.23])
linha("PROPOSTA: Super Fav <=1.40 + b365 >=5.00 + lay<=7.50",
      sf[(sf["Odd_D_FT"] >= 5.0) & (sf["Odd_D_Lay"] <= 7.5)])
linha("as duas juntas (congelada + proposta)",
      sf[(sf["liga_draw_rate"] < 0.23) & (sf["Odd_D_FT"] >= 5.0) & (sf["Odd_D_Lay"] <= 7.5)])
print("\n  (taxa de empate das ligas: mediana %.3f | %% de jogos com liga_draw_rate<0.23: %.1f%%)"
      % (j["liga_draw_rate"].median(), 100*(j["liga_draw_rate"] < 0.23).mean()))
