# -*- coding: utf-8 -*-
"""
forward_liquidar.py — liquida os PENDENTES do forward_oculto_log.csv pelo placar FINAL
da base historica Betfair da API (autoritativo p/ Draw/Home/Over; sem bug de CS), e
imprime o placar acumulado (base e filtrado) por metodo.

  python forward_liquidar.py

A base historica atrasa ~2 semanas. Sinais sem placar ainda ficam PENDENTE.
"""
import os, sys, csv, unicodedata
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "forward_oculto_log.csv")
COMM = 0.05

def canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode().lower()
    return "".join(ch for ch in s if ch.isalnum())

def green_de(metodo, gh, ga):
    if metodo == "Lay_Home":   return ga >= gh
    if metodo == "Lay_Over45": return (gh + ga) <= 4
    if metodo == "Lay_Draw":   return gh != ga
    return None

def carregar_base():
    from futpythontrader_client import get_dataframe
    B = get_dataframe("betfair", timeout=180)
    B["Date"] = pd.to_datetime(B["Date"], errors="coerce")
    B = B.dropna(subset=["Goals_H_FT","Goals_A_FT"])
    B["k"] = (B["Date"].dt.strftime("%Y-%m-%d") + "|" + B["Home"].map(canon) + "|" + B["Away"].map(canon))
    return B.drop_duplicates("k").set_index("k")[["Goals_H_FT","Goals_A_FT"]]

def main():
    if not os.path.exists(LOG):
        print("log vazio — rode forward_capturar.py primeiro"); return
    rows = list(csv.DictReader(open(LOG, encoding="utf-8")))
    base = carregar_base()
    n_liq = 0
    for r in rows:
        if r.get("Status") != "PENDENTE": continue
        k = r["Data"] + "|" + canon(r["Home"]) + "|" + canon(r["Away"])
        if k not in base.index: continue
        gh, ga = int(base.at[k,"Goals_H_FT"]), int(base.at[k,"Goals_A_FT"])
        g = green_de(r["Metodo"], gh, ga)
        lay = float(r["Odd_Lay"])
        pnl_stake = (1 - COMM) if g else -(lay - 1)
        r["Green"] = int(bool(g)); r["Placar"] = "%dx%d" % (gh, ga)
        r["PnL_liab"] = round(pnl_stake, 4)
        r["Status"] = "GREEN" if g else "RED"
        n_liq += 1
    with open(LOG, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)

    print("liquidados agora: %d\n" % n_liq)
    df = pd.DataFrame(rows)
    df = df[df["Status"].isin(["GREEN","RED"])].copy()
    if df.empty:
        print("nada liquidado ainda (base historica ainda nao cobre as datas)"); return
    df["lay"] = df["Odd_Lay"].astype(float); df["g"] = df["Green"].astype(int)
    df["pnl"] = df.apply(lambda x: (1-COMM) if x["g"] else -(x["lay"]-1), axis=1)
    df["passa"] = df["passa_filtro"].astype(int)
    def resumo(sub, tag):
        n = len(sub)
        if n == 0: return
        roi = sub["pnl"].sum() / (sub["lay"]-1).sum() * 100
        print("  %-22s N=%-4d WR=%4.1f%% ROI/liab=%+6.1f%% lay_med=%.2f"
              % (tag, n, sub["g"].mean()*100, roi, sub["lay"].median()))
    print("=== ACUMULADO (liquidado) ===")
    for m in ["Lay_Draw","Lay_Home","Lay_Over45"]:
        s = df[df["Metodo"]==m]
        if len(s)==0: continue
        print(m + ":")
        resumo(s, "base")
        if m in ("Lay_Home","Lay_Draw"):
            resumo(s[s["passa"]==1], "FILTRADO (refinado)")
    pend = sum(1 for r in rows if r.get("Status")=="PENDENTE")
    print("\npendentes (aguardando placar da base): %d" % pend)

if __name__ == "__main__":
    main()
