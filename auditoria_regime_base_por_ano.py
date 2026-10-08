import pandas as pd, numpy as np
R = r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD"
b365 = pd.read_csv(R + r"\Bases_de_Dados_API_FutPythonTrader_Bet365.csv",
                   usecols=["League","Date","Home","Away","Goals_H_FT","Goals_A_FT",
                            "Odd_H_FT","Odd_D_FT","Odd_A_FT"], low_memory=False)
bf = pd.read_csv(R + r"\Bases_de_Dados_API_FutPythonTrader_Betfair.csv",
                 usecols=["Date","Home","Away","Odd_D_Lay","Odd_D_Back"], low_memory=False)
for d in (b365, bf):
    d["Date"] = pd.to_datetime(d["Date"], errors="coerce")
    for c in ("Home","Away"):
        d[c] = d[c].astype(str).str.strip()
j = b365.merge(bf, on=["Date","Home","Away"], how="inner").dropna(
    subset=["Goals_H_FT","Goals_A_FT","Odd_H_FT","Odd_D_FT","Odd_A_FT","Odd_D_Lay"])
j = j[(j[["Odd_H_FT","Odd_D_FT","Odd_A_FT","Odd_D_Lay"]] > 1).all(axis=1)]
j["ano"] = j["Date"].dt.year
j["spread"] = j["Odd_D_Lay"] / j["Odd_D_FT"]
j["spread_exch"] = j["Odd_D_Lay"] / j["Odd_D_Back"]
j["ganha_lay"] = j["Goals_H_FT"] != j["Goals_A_FT"]

print("=== A BASE MUDOU DE REGIME? (todos os jogos casados) ===")
g = j.groupby("ano").agg(n=("spread","size"), lay_med=("Odd_D_Lay","median"),
                         spread_med=("spread","median"),
                         spread_exch=("spread_exch","median"))
print(g.round(4).to_string())
print("\n  'spread_exch' = lay/back DENTRO da Betfair. Se ele cai junto, o livro ficou mais apertado")
print("  (snapshot mais perto do KO). Se so o spread vs b365 cai, mudou a relacao com a casa.\n")

sf = j[j[["Odd_H_FT","Odd_A_FT"]].min(axis=1) <= 1.40]
print("=== MINHA AUDITORIA ANTERIOR, REFEITA SO EM 2026 ===")
print("  (Super Fav <= 1.40, trava lay <= 7.50, c=5%)\n")
print("%-16s %6s %7s %10s %9s" % ("periodo","N","WR","break-even","ROI c=5%"))
print("-"*56)
for rot, m in (("2024", sf["ano"]==2024), ("2025", sf["ano"]==2025), ("2026 (ate 31/07)", sf["ano"]==2026)):
    sub = sf[m & (sf["Odd_D_Lay"] <= 7.5) & (sf["Odd_D_FT"] >= 5.0)]
    if len(sub) < 30: 
        print("%-16s %6d  (pequena)" % (rot, len(sub))); continue
    o = sub["Odd_D_Lay"].values; gg = sub["ganha_lay"].values
    be = ((o-1)/(o-0.05)).mean(); roi = np.where(gg, 0.95/(o-1), -1.0).mean()
    print("%-16s %6d %6.2f%% %9.2f%% %+8.2f%%" % (rot, len(sub), 100*gg.mean(), 100*be, 100*roi))

print("\n=== SIGNIFICANCIA DO RESULTADO DE AGOSTO-SETEMBRO ===")
for nome, N, wr, be in (("Lay Draw", 426, 82.63, 81.24), ("Lay Home", 271, 76.38, 74.14)):
    p = wr/100.0
    se = np.sqrt(p*(1-p)/N)*100
    edge = wr - be
    z = edge/se
    from math import erfc, sqrt
    pval = 0.5*erfc(z/sqrt(2))
    print("  %-9s N=%3d | edge %+.2f pp | erro-padrao do WR %.2f pp | z = %.2f | p (unilateral) = %.3f"
          % (nome, N, edge, se, z, pval))
    print("            para o edge ser significativo (z=1.96) precisaria de N = %d jogos"
          % int(np.ceil(p*(1-p)*(1.96/(edge/100))**2)))
