import pandas as pd, numpy as np
R = r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
rng = np.random.default_rng(20261008)

b365 = pd.read_csv(R + r"\Bases_de_Dados_API_FutPythonTrader_Bet365.csv",
                   usecols=["League","Date","Home","Away","Goals_H_FT","Goals_A_FT",
                            "Odd_H_FT","Odd_D_FT","Odd_A_FT"], low_memory=False)
bf = pd.read_csv(R + r"\Bases_de_Dados_API_FutPythonTrader_Betfair.csv",
                 usecols=["League","Date","Home","Away","Odd_H_Lay","Odd_A_Lay","Odd_D_Lay"],
                 low_memory=False)
for d in (b365, bf):
    d["Date"] = pd.to_datetime(d["Date"], errors="coerce")
    for c in ("Home","Away"):
        d[c] = d[c].astype(str).str.strip()
j = b365.merge(bf.drop(columns=["League"]), on=["Date","Home","Away"], how="inner").dropna(
    subset=["Goals_H_FT","Goals_A_FT","Odd_H_FT","Odd_D_FT","Odd_A_FT","Odd_D_Lay","Odd_H_Lay","Odd_A_Lay"])
j = j[(j[["Odd_H_FT","Odd_D_FT","Odd_A_FT","Odd_D_Lay","Odd_H_Lay","Odd_A_Lay"]] > 1).all(axis=1)]
j["res"] = np.where(j["Goals_H_FT"] > j["Goals_A_FT"], "H",
            np.where(j["Goals_H_FT"] < j["Goals_A_FT"], "A", "D"))

def avaliar(sub, odd, ganhou_lay, c):
    """lay com liability 1u: ganha (1-c)/(odd-1) se a selecao NAO sai, perde 1 se sai."""
    pnl = np.where(ganhou_lay, (1.0 - c) / (odd - 1.0), -1.0)
    wr = ganhou_lay.mean()
    be = ((odd - 1.0) / (odd - c)).mean()
    return pnl, wr, be

def bootstrap_dia(sub, pnl, n=2000):
    dias = sub["Date"].values
    uq = pd.unique(dias)
    idx = {d: np.where(dias == d)[0] for d in uq}
    out = np.empty(n)
    for i in range(n):
        esc = rng.choice(uq, size=len(uq), replace=True)
        out[i] = np.concatenate([pnl[idx[d]] for d in esc]).mean()
    return np.percentile(out, [2.5, 97.5])

METODOS = [
    ("Lay Draw",  lambda d: d[["Odd_H_FT","Odd_A_FT"]].min(axis=1) <= 1.40, "Odd_D_FT", "Odd_D_Lay", "D", 4.50, 10.0),
    ("Lay Home",  lambda d: d["Odd_A_FT"] <= 1.65,                          "Odd_H_FT", "Odd_H_Lay", "H", 2.00, 10.0),
    ("Lay Away",  lambda d: d["Odd_H_FT"] <= 1.40,                          "Odd_A_FT", "Odd_A_Lay", "A", 5.00, 25.0),
]

print("jogos casados: %d (%s a %s)\n" % (len(j), j["Date"].min().date(), j["Date"].max().date()))
print("%-10s %-24s %6s %7s %7s %9s   %s" % ("metodo","odd de lay usada","N","WR","break-even","ROI/liab","IC95 bootstrap-dia"))
print("-" * 112)
for nome, filt, col_b365, col_lay, sel, lo, hi in METODOS:
    base = j[filt(j)].copy()
    for rotulo, odds in (("SIMULADA 1.075x b365", base[col_b365] * 1.075),
                         ("REAL da Betfair",      base[col_lay])):
        m = (odds >= lo) & (odds <= hi)
        sub = base[m]
        o = odds[m].values
        ganhou = (sub["res"] != sel).values
        for c in (0.03, 0.065, 0.14):
            pnl, wr, be = avaliar(sub, o, ganhou, c)
            if c == 0.03:
                ic = bootstrap_dia(sub, pnl)
                ics = "[%+.2f%%, %+.2f%%]" % (100*ic[0], 100*ic[1])
            else:
                ics = ""
            print("%-10s %-24s %6d %6.2f%% %9.2f%% %+8.2f%%   %s%s"
                  % (nome if c == 0.03 and rotulo.startswith("SIM") else "",
                     ("%s  c=%.1f%%" % (rotulo, 100*c)) if c == 0.03 else ("        c=%.1f%%" % (100*c)),
                     len(sub), 100*wr, 100*be, 100*pnl.mean(), ics,
                     "" ))
    print("-" * 112)
