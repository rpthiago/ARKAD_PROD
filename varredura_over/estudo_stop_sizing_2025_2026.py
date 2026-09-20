# -*- coding: utf-8 -*-
"""
estudo_stop_sizing_2025_2026.py — os 5 metodos do portfolio (regras da pagina 01 / sinais_do_dia) aplicados a base
historica Betfair (apicomunidade, odd de LAY pre-jogo) em 2025 e 2026, com simulacao composta de banca:
stop red (-10% do dia), stop green (+10%), e grade de percentuais por metodo. Nulo: resultados embaralhados.
RESSALVA: a odd de lay de Correct Score na base tem 3 regimes (2024-25 livro vazio, jan-abr/26 cheio) — os numeros de
0x3/2x2 aqui NAO sao executaveis como os de Draw/Home/Over; ver base-betfair-regime-odd-lay-cs.
"""
import os, sys, warnings, re
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import pandas as pd, numpy as np

BASE = os.path.join(ROOT, "metodos_aprovados", ".cache_base_betfair.csv")
BL_2X2 = ("SERBIA", "IRELAND", "TURKEY", "SCOTLAND")
C = 0.05


def sinais(b):
    rows = []
    for c in ["Odd_H_Back", "Odd_A_Back", "Odd_D_Lay", "Odd_H_Lay", "Odd_Under25_FT_Back", "Odd_Over45_FT_Lay", "Odd_CS_0x3_Lay", "Odd_CS_2x2_Lay", "Goals_H_FT", "Goals_A_FT"]:
        b[c] = pd.to_numeric(b[c], errors="coerce")
    b = b.dropna(subset=["Goals_H_FT", "Goals_A_FT", "Odd_H_Back", "Odd_A_Back"]).copy()
    b["fav"] = b[["Odd_H_Back", "Odd_A_Back"]].min(axis=1); gh, ga = b.Goals_H_FT, b.Goals_A_FT
    def add(m, mask, odd, red):
        s = b[mask].copy(); s["metodo"] = m; s["odd"] = odd[mask]; s["red"] = red[mask].astype(int); rows.append(s)
    add("Draw", (b.fav <= 1.40) & b.Odd_D_Lay.between(4.5, 10.0), b.Odd_D_Lay, gh == ga)
    add("Home", (b.Odd_A_Back <= 1.65) & b.Odd_H_Lay.between(2.0, 10.0), b.Odd_H_Lay, gh > ga)
    add("Over 4.5", (b.Odd_Under25_FT_Back <= 1.50) & b.Odd_Over45_FT_Lay.between(4.0, 20.0), b.Odd_Over45_FT_Lay, gh + ga >= 5)
    # 0x3 Top 3: under25<=2.10, lay 14-35, visitante>=1.85 -> 3 menores odds do dia
    m03 = (b.Odd_Under25_FT_Back <= 2.10) & b.Odd_CS_0x3_Lay.between(14, 35) & (b.Odd_A_Back >= 1.85)
    s = b[m03].copy(); s["rk"] = s.groupby("Date")["Odd_CS_0x3_Lay"].rank(method="first"); s = s[s.rk <= 3]
    s["metodo"] = "0x3"; s["odd"] = s.Odd_CS_0x3_Lay; s["red"] = ((s.Goals_H_FT == 0) & (s.Goals_A_FT == 3)).astype(int); rows.append(s)
    # 2x2 Top 3: lay 8-20, tendencia (u25<=2.00 ou H<=1.55 ou A<=1.60), fora blacklist -> 3 menores do dia
    m22 = b.Odd_CS_2x2_Lay.between(8, 20) & ((b.Odd_Under25_FT_Back <= 2.00) | (b.Odd_H_Back <= 1.55) | (b.Odd_A_Back <= 1.60)) & ~b.League.astype(str).str.upper().str.contains("|".join(BL_2X2))
    s = b[m22].copy(); s["rk"] = s.groupby("Date")["Odd_CS_2x2_Lay"].rank(method="first"); s = s[s.rk <= 3]
    s["metodo"] = "2x2"; s["odd"] = s.Odd_CS_2x2_Lay; s["red"] = ((s.Goals_H_FT == 2) & (s.Goals_A_FT == 2)).astype(int); rows.append(s)
    d = pd.concat(rows, ignore_index=True)
    d["t"] = pd.to_datetime(d.Date.astype(str).str[:10] + " " + d.Time.astype(str).str[:5].replace("nan", "15:00"), errors="coerce")
    d = d.dropna(subset=["t"]).sort_values("t").reset_index(drop=True); d["dia"] = d.t.dt.strftime("%Y-%m-%d")
    d["be"] = 1 - (d.odd - 1) / (d.odd - C)
    return d[["dia", "t", "League", "Home", "Away", "metodo", "odd", "red", "be"]]


def simular(df, pct, stop_red=None, stop_green=None, banca0=2000.0):
    banca = banca0; pico = banca; mdd = 0.0; n = 0; pul = 0; pnl_pul = 0.0
    for dia, g in df.groupby("dia", sort=False):
        ini = banca; bloq = None
        for r in g.itertuples():
            liab = pct[r.metodo] * banca
            ganho = (-liab if r.red else liab * (1 - C) / (r.odd - 1))
            if bloq is not None and r.t > bloq: pul += 1; pnl_pul += ganho; continue
            n += 1; banca += ganho; pico = max(pico, banca); mdd = max(mdd, (pico - banca) / pico)
            if bloq is None:
                if stop_red is not None and (banca - ini) / ini <= -stop_red: bloq = r.t + pd.Timedelta(minutes=115)
                elif stop_green is not None and (banca - ini) / ini >= stop_green: bloq = r.t + pd.Timedelta(minutes=115)
    return banca, mdd, n, pul, pnl_pul


def main():
    b = pd.read_csv(BASE, low_memory=False); b["Date"] = b.Date.astype(str).str[:10]
    b = b[(b.Date >= "2025-01-01")]
    d = sinais(b)
    print("sinais 2025-01-01 -> %s: %d | por metodo: %s" % (d.dia.max(), len(d), d.metodo.value_counts().to_dict()))
    for ano, g in d.groupby(d.dia.str[:4]):
        print("\n=== %s: %d sinais, %d dias ===" % (ano, len(g), g.dia.nunique()))
        t = g.groupby("metodo").agg(N=("red", "size"), reds=("red", "sum"), red_pct=("red", lambda x: 100 * x.mean()), be_pct=("be", lambda x: 100 * x.mean()))
        t["gap_pp"] = t.be_pct - t.red_pct; t["ROI_%"] = 100 * (((1 - g.red) * (1 - C) / (g.odd - 1) - g.red).groupby(g.metodo).mean())
        print(t.round(2).to_string())
    # grades de sizing: (Draw/Home/, CS/Over)
    grades = {"5/5": {"Draw": .05, "Home": .05, "0x3": .05, "2x2": .05, "Over 4.5": .05},
              "5/7.5": {"Draw": .05, "Home": .05, "0x3": .075, "2x2": .075, "Over 4.5": .075},
              "5/10": {"Draw": .05, "Home": .05, "0x3": .10, "2x2": .10, "Over 4.5": .10},
              "5/15": {"Draw": .05, "Home": .05, "0x3": .15, "2x2": .15, "Over 4.5": .15},
              "2.5/5": {"Draw": .025, "Home": .025, "0x3": .05, "2x2": .05, "Over 4.5": .05}}
    cen = [("sem stop", {}), ("stop red -10%", dict(stop_red=.10)), ("stop green +10%", dict(stop_green=.10)), ("red -10% + green +10%", dict(stop_red=.10, stop_green=.10))]
    rng = np.random.default_rng(1)
    for ano, g in list(d.groupby(d.dia.str[:4])) + [("2025+2026", d)]:
        print("\n=== SIMULACAO %s (banca inicial R$2.000, composta) ===" % ano)
        print("%-8s %-24s %12s %8s %6s %7s %9s" % ("sizing", "cenario", "banca final", "retorno", "maxDD", "pulados", "P&L pulado"))
        for nome, pct in grades.items():
            for cn, kw in cen:
                bf, mdd, n, pul, pp = simular(g, pct, **kw)
                print("%-8s %-24s R$%10.0f %+7.0f%% %5.1f%% %7d R$%+8.0f" % (nome, cn, bf, 100 * (bf / 2000 - 1), 100 * mdd, pul, pp))
        # nulo para o stop red e green no sizing 5/10
        pct = grades["5/10"]; b0 = simular(g, pct)[0]; br = simular(g, pct, stop_red=.10)[0]; bg = simular(g, pct, stop_green=.10)[0]
        gr, gg, dr = [], [], []
        for i in range(150):
            gg2 = g.copy(); gg2["red"] = rng.permutation(g.red.values)
            x0, m0, *_ = simular(gg2, pct); x1, m1, *_ = simular(gg2, pct, stop_red=.10); x2, *_ = simular(gg2, pct, stop_green=.10)
            gr.append(x1 - x0); gg.append(x2 - x0); dr.append(m0 - m1)
        gr, gg, dr = map(np.array, (gr, gg, dr))
        print("nulo (resultados embaralhados, 150x, sizing 5/10): stop red ganho observado R$%+.0f | mediana nulo R$%+.0f | P(>=obs)=%.2f | maxDD -%.1fpp obs vs %.1fpp nulo" % (br - b0, np.median(gr), (gr >= (br - b0)).mean(), 0, 100 * np.median(dr)))
        print("                                                   stop green ganho observado R$%+.0f | mediana nulo R$%+.0f | P(>=obs)=%.2f" % (bg - b0, np.median(gg), (gg >= (bg - b0)).mean()))
        # reds agrupam no dia?
        g2 = g.copy(); g2["antes"] = g2.groupby("dia").red.cumsum() - g2.red
        ra, rs = g2[g2.antes > 0].red.mean(), g2[g2.antes == 0].red.mean()
        print("red depois de red no dia: %.1f%% (N=%d) vs sem red antes: %.1f%% (N=%d) | dif %+.1fpp" % (100 * ra, (g2.antes > 0).sum(), 100 * rs, (g2.antes == 0).sum(), 100 * (ra - rs)))
        # maxDD tipico por sizing (embaralhado)
        for nome in ("5/5", "5/10", "5/15"):
            m = [simular(g.assign(red=rng.permutation(g.red.values)), grades[nome])[1] for _ in range(60)]
            print("maxDD tipico %s: mediana %.1f%% | p95 %.1f%%" % (nome, 100 * np.median(m), 100 * np.percentile(m, 95)))
    d.to_csv(os.path.join(ROOT, "varredura_over", "sinais_5metodos_base_2025_2026.csv"), index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
