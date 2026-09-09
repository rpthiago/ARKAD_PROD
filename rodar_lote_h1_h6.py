#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rodar_lote_h1_h6.py — roda o LOTE PRE-REGISTRADO das 6 hipoteses de HIPOTESES_CANDIDATAS_API.md,
de uma vez so, com Benjamini-Hochberg sobre M=9.

REGRAS DO LOTE (nao afrouxar depois de ver o resultado):
  * split TEMPORAL: primeira metade = treino/descoberta, segunda metade = VALIDACAO.
    Todo p-valor primario sai da metade de VALIDACAO.
  * features rolling sempre com shift(1) por time -> zero vazamento do proprio jogo.
  * 1 p-valor PRIMARIO por hipotese. O resto e diagnostico e nao entra no FDR.
  * aprova so quem sobrevive ao BH com M=9.

Entradas:
  hist_time_stats.csv     (hist_time_xg.py)
  hist_stats_ft/*.json    (cache; usado p/ campos que nao entraram no CSV — 0 requisicao)
  liga_espessura.csv      (opcional; volume medio casado na Betfair por liga -> H2)

  python rodar_lote_h1_h6.py
"""
import os, json, glob, math
import numpy as np, pandas as pd
from math import erfc

AQUI = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(AQUI, "hist_time_stats.csv")
DIR_FT = os.path.join(AQUI, "hist_stats_ft")
ESPESSURA = os.path.join(AQUI, "liga_espessura.csv")
JAN = 8                     # janela do rolling (partidas anteriores do time)
MIN_HIST = 5                # minimo de partidas anteriores p/ o jogo entrar


def pnorm2(z):
    return erfc(abs(z) / math.sqrt(2))


# ------------------------------------------------------------------ dados

def enriquecer(d):
    """Campos que nao entraram no CSV, lidos do cache em disco. Zero requisicao."""
    extras = {"opposition_half_passes": "ophalf", "accurate_passes": "passes_ok"}
    vals = {("%s_%s" % (nm, lado)): [] for nm in extras.values() for lado in "ha"}
    for eid in d.eventid:
        p = os.path.join(DIR_FT, "%s.json" % eid) if eid else None
        js = {}
        if p and os.path.exists(p):
            try:
                js = json.load(open(p, encoding="utf-8")) or {}
            except Exception:
                js = {}
        for k, nm in extras.items():
            v = js.get(k)
            for i, lado in enumerate("ha"):
                try:
                    vals["%s_%s" % (nm, lado)].append(float(str(v[i]).split("(")[0].strip()))
                except Exception:
                    vals["%s_%s" % (nm, lado)].append(np.nan)
    for c, v in vals.items():
        d[c] = v
    return d


def por_time(d):
    """Explode em 2 linhas por jogo (visao do time) p/ montar rolling sem vazamento."""
    campos = ["xg", "xg_open", "xg_set", "xgot", "shots", "sot", "bigch", "tbox",
              "saves", "corners", "poss", "ophalf"]
    linhas = []
    for _, r in d.iterrows():
        for lado, opo in (("h", "a"), ("a", "h")):
            reg = dict(data=r.data, liga=r.liga, eventid=r.eventid,
                       time=r.home if lado == "h" else r.away,
                       casa=1 if lado == "h" else 0,
                       gols_pro=r.gols_h if lado == "h" else r.gols_a,
                       gols_con=r.gols_a if lado == "h" else r.gols_h)
            for c in campos:
                reg["%s_pro" % c] = r.get("%s_%s" % (c, lado), np.nan)
                reg["%s_con" % c] = r.get("%s_%s" % (c, opo), np.nan)
            linhas.append(reg)
    t = pd.DataFrame(linhas)
    t["data"] = pd.to_datetime(t.data)
    return t.sort_values(["time", "data"]).reset_index(drop=True)


def rolling(t):
    """Media movel das JAN partidas ANTERIORES (shift(1)) — nunca inclui o jogo corrente."""
    cols = [c for c in t.columns if c.endswith("_pro") or c.endswith("_con")]
    g = t.groupby("time")
    for c in cols:
        t["r_" + c] = g[c].transform(lambda s: s.shift(1).rolling(JAN, min_periods=MIN_HIST).mean())
    t["n_hist"] = g.cumcount()
    return t


def montar():
    d = pd.read_csv(CSV, dtype={"eventid": str})
    d["eventid"] = d.eventid.fillna("").str.replace(r"\.0$", "", regex=True)
    d = d[(d.tem_stats == 1) & (d.eventid != "")].copy()
    for c in d.columns:
        if c not in ("data", "liga", "home", "away", "eventid"):
            d[c] = pd.to_numeric(d[c], errors="coerce")
    d = enriquecer(d)
    t = rolling(por_time(d))
    r = [c for c in t.columns if c.startswith("r_")]
    casa = t[t.casa == 1][["eventid"] + r + ["n_hist"]].add_suffix("_H").rename(
        columns={"eventid_H": "eventid"})
    fora = t[t.casa == 0][["eventid"] + r + ["n_hist"]].add_suffix("_A").rename(
        columns={"eventid_A": "eventid"})
    m = d.merge(casa, on="eventid").merge(fora, on="eventid")
    m["data"] = pd.to_datetime(m.data)
    m["zero_a_zero"] = ((m.gols_h == 0) & (m.gols_a == 0)).astype(int)
    m["gols_tot"] = m.gols_h + m.gols_a
    m["vence_h"] = (m.gols_h > m.gols_a).astype(int)
    m["empate"] = (m.gols_h == m.gols_a).astype(int)
    m["vence_a"] = (m.gols_h < m.gols_a).astype(int)
    m = m[(m.n_hist_H >= MIN_HIST) & (m.n_hist_A >= MIN_HIST)].sort_values("data")
    return m


# ------------------------------------------------------------------ modelagem

def logit_fit(X, y, l2=1e-3):
    X = np.column_stack([np.ones(len(X)), X])
    b = np.zeros(X.shape[1])
    for _ in range(100):
        eta = np.clip(X @ b, -30, 30)
        mu = 1 / (1 + np.exp(-eta))
        W = np.clip(mu * (1 - mu), 1e-9, None)
        z = eta + (y - mu) / W
        A = (X.T * W) @ X + l2 * np.eye(X.shape[1])
        try:
            nb = np.linalg.solve(A, (X.T * W) @ z)
        except np.linalg.LinAlgError:
            break
        if np.max(np.abs(nb - b)) < 1e-10:
            b = nb; break
        b = nb
    eta = np.clip(X @ b, -30, 30)
    mu = 1 / (1 + np.exp(-eta))
    W = np.clip(mu * (1 - mu), 1e-9, None)
    cov = np.linalg.pinv((X.T * W) @ X + l2 * np.eye(X.shape[1]))
    return b, np.sqrt(np.diag(cov)), cov


def prever(b, X):
    X = np.column_stack([np.ones(len(X)), X])
    return 1 / (1 + np.exp(-np.clip(X @ b, -30, 30)))


def logloss(y, p):
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def tabela(nomes, b, se, titulo):
    print("    %s" % titulo)
    print("      %-26s %9s %8s %8s %8s" % ("termo", "coef", "erro", "z", "p"))
    for i, nm in enumerate(["(intercepto)"] + nomes):
        z = b[i] / se[i] if se[i] else 0
        print("      %-26s %9.4f %8.4f %8.2f %8.4f" % (nm, b[i], se[i], z, pnorm2(z)))


# ------------------------------------------------------------------ hipoteses

def rodar(m):
    corte = m.data.quantile(0.5)
    tr, va = m[m.data < corte], m[m.data >= corte]
    print("split temporal: treino ate %s (N=%d) | VALIDACAO de %s (N=%d)\n"
          % (corte.date(), len(tr), corte.date(), len(va)))
    prim = {}

    # ---------------- H1: xG rolling bate gols rolling para prever 0-0?
    print("=" * 78)
    print("H1 — xG rolling no lugar de gols rolling, para prever 0-0")
    fg = ["r_gols_pro_H", "r_gols_con_H", "r_gols_pro_A", "r_gols_con_A"]
    fx = ["r_xg_pro_H", "r_xg_con_H", "r_xg_pro_A", "r_xg_con_A"]
    sub_tr = tr.dropna(subset=fg + fx); sub_va = va.dropna(subset=fg + fx)
    bg, _, _ = logit_fit(sub_tr[fg].values, sub_tr.zero_a_zero.values)
    bx, _, _ = logit_fit(sub_tr[fx].values, sub_tr.zero_a_zero.values)
    pg, px = prever(bg, sub_va[fg].values), prever(bx, sub_va[fx].values)
    y = sub_va.zero_a_zero.values
    llg, llx = logloss(y, pg), logloss(y, px)
    dif = []
    rng = np.random.default_rng(7)
    for _ in range(10000):
        i = rng.integers(0, len(y), len(y))
        dif.append(logloss(y[i], pg[i]) - logloss(y[i], px[i]))
    dif = np.sort(dif)
    p_h1 = 2 * min((dif <= 0).mean(), (dif >= 0).mean())
    print("  N validacao=%d | 0-0 na validacao: %.1f%%" % (len(y), 100 * y.mean()))
    print("  log-loss GOLS=%.5f | log-loss xG=%.5f | ganho=%.5f  IC95 [%.5f, %.5f]"
          % (llg, llx, llg - llx, dif[250], dif[9750]))
    print("  >>> p primario (bootstrap, ganho != 0) = %.4f" % p_h1)
    prim["H1 xG melhora P(0-0)"] = p_h1

    # ---------------- H2: desacordo x espessura do mercado
    print("=" * 78)
    print("H2 — o desacordo modelo-preco prediz MAIS onde o mercado e mais fino?")
    p_h2 = None
    esp = None
    if os.path.exists(ESPESSURA):
        esp = pd.read_csv(ESPESSURA)
        print("  espessura por liga: %d ligas de liga_espessura.csv" % len(esp))
    if esp is None:
        print("  [PULADA] falta liga_espessura.csv (volume medio casado por liga, do coletor).")
    else:
        v = va.dropna(subset=fx + ["odd_cs_0x0"]).copy()
        v = v[v.odd_cs_0x0 > 1.01]
        v = v.merge(esp, left_on="liga", right_on="liga_bf", how="inner")
        if len(v) < 150:
            print("  [PULADA] N=%d insuficiente apos casar espessura" % len(v))
        else:
            v["p_mod"] = prever(bx, v[fx].values)
            v["p_mkt"] = 1.0 / v.odd_cs_0x0
            v["desacordo"] = v.p_mod - v.p_mkt
            v["fino"] = -np.log(np.clip(v.volume_medio.values, 1, None))   # + = mais fino
            v["fino"] = (v.fino - v.fino.mean()) / (v.fino.std() + 1e-9)
            X = np.column_stack([np.log(np.clip(v.p_mkt, 1e-4, 1 - 1e-4) /
                                        (1 - np.clip(v.p_mkt, 1e-4, 1 - 1e-4))),
                                 v.desacordo.values, v.fino.values,
                                 (v.desacordo * v.fino).values])
            b, se, _ = logit_fit(X, v.zero_a_zero.values)
            tabela(["logit(preco)", "desacordo", "mercado fino", "desacordo x fino"], b, se,
                   "N=%d" % len(v))
            p_h2 = pnorm2(b[4] / se[4] if se[4] else 0)
            print("  >>> p primario (termo de interacao) = %.4f" % p_h2)
            prim["H2 desacordo x mercado fino"] = p_h2

    # ---------------- H3: xG aberto != xG de bola parada
    print("=" * 78)
    print("H3 — xG de bola rolando e xG de bola parada valem o mesmo?")
    f3 = ["r_xg_open_pro_H", "r_xg_set_pro_H", "r_xg_open_pro_A", "r_xg_set_pro_A"]
    s = va.dropna(subset=f3)
    b, se, cov = logit_fit(s[f3].values, s.zero_a_zero.values)
    tabela(["xG aberto casa", "xG parada casa", "xG aberto fora", "xG parada fora"], b, se,
           "N=%d (alvo: 0-0)" % len(s))
    dcoef = (b[1] - b[2]) + (b[3] - b[4])
    var = (cov[1, 1] + cov[2, 2] - 2 * cov[1, 2]) + (cov[3, 3] + cov[4, 4] - 2 * cov[3, 4])
    z3 = dcoef / math.sqrt(max(var, 1e-12))
    p_h3 = pnorm2(z3)
    print("  soma das diferencas (aberto - parada) = %.4f | z=%.2f" % (dcoef, z3))
    print("  >>> p primario (aberto != parada) = %.4f" % p_h3)
    prim["H3 xG aberto != bola parada"] = p_h3

    # ---------------- H4: superacao de xG regride
    print("=" * 78)
    print("H4 — quem vem superando o xG regride? (o mercado ancora em gols)")
    va2 = va.copy()
    va2["sup_H"] = va2.r_gols_pro_H - va2.r_xg_pro_H
    va2["sup_A"] = va2.r_gols_pro_A - va2.r_xg_pro_A
    s = va2.dropna(subset=["sup_H", "sup_A", "r_xg_pro_H", "r_xg_pro_A"])
    X = np.column_stack([s.r_xg_pro_H.values, s.r_xg_pro_A.values, s.sup_H.values, s.sup_A.values])
    b, se, cov = logit_fit(X, (s.gols_tot >= 3).astype(int).values)
    tabela(["xG casa", "xG fora", "superacao casa", "superacao fora"], b, se,
           "N=%d (alvo: 3+ gols)" % len(s))
    dsup = b[3] + b[4]
    vsup = cov[3, 3] + cov[4, 4] + 2 * cov[3, 4]
    z4 = dsup / math.sqrt(max(vsup, 1e-12))
    p_h4 = pnorm2(z4)
    direcao_ok = dsup < 0        # o mecanismo previa NEGATIVO (regride)
    print("  >>> p primario (superacao conjunta != 0) = %.4f | coef=%+.4f -> %s"
          % (p_h4, dsup,
             "direcao BATE com o mecanismo" if direcao_ok
             else "direcao CONTRARIA ao mecanismo (p pequeno NAO apoia a hipotese)"))
    prim["H4 superacao de xG regride"] = (p_h4 if direcao_ok else 1.0)

    # ---------------- H5: territorio esteril
    print("=" * 78)
    print("H5 — dominio sem entrar na area (toques na area / passes no campo adversario)")
    va3 = va.copy()
    for lado in ("H", "A"):
        va3["est_" + lado] = va3["r_tbox_pro_" + lado] / va3["r_ophalf_pro_" + lado].replace(0, np.nan)
    s = va3.dropna(subset=["est_H", "est_A", "r_xg_pro_H", "r_xg_pro_A"])
    if len(s) < 150:
        print("  [PULADA] N=%d insuficiente (campo opposition_half_passes ausente no cache)" % len(s))
        p_h5 = None
    else:
        X = np.column_stack([s.r_xg_pro_H.values, s.r_xg_pro_A.values, s.est_H.values, s.est_A.values])
        b, se, cov = logit_fit(X, s.zero_a_zero.values)
        tabela(["xG casa", "xG fora", "esterilidade casa", "esterilidade fora"], b, se,
               "N=%d (alvo: 0-0)" % len(s))
        dz = b[3] + b[4]
        vz = cov[3, 3] + cov[4, 4] + 2 * cov[3, 4]
        p_h5 = pnorm2(dz / math.sqrt(max(vz, 1e-12)))
        print("  >>> p primario = %.4f" % p_h5)
        prim["H5 territorio esteril"] = p_h5

    # ---------------- H6: goleiro segurando
    print("=" * 78)
    print("H6 — defesa sendo salva pelo goleiro (saves + xG concedido) prediz mais gols sofridos?")
    s = va.dropna(subset=["r_saves_pro_H", "r_xg_con_H", "r_gols_con_H",
                          "r_saves_pro_A", "r_xg_con_A", "r_gols_con_A"])
    X = np.column_stack([s.r_gols_con_H.values, s.r_gols_con_A.values,
                         s.r_saves_pro_H.values, s.r_saves_pro_A.values,
                         s.r_xg_con_H.values, s.r_xg_con_A.values])
    b, se, cov = logit_fit(X, (s.gols_tot >= 3).astype(int).values)
    tabela(["gols sofridos casa", "gols sofridos fora", "defesas casa", "defesas fora",
            "xG concedido casa", "xG concedido fora"], b, se, "N=%d (alvo: 3+ gols)" % len(s))
    dz = b[3] + b[4]
    vz = cov[3, 3] + cov[4, 4] + 2 * cov[3, 4]
    p_h6 = pnorm2(dz / math.sqrt(max(vz, 1e-12)))
    print("  >>> p primario (defesas conjunto != 0) = %.4f" % p_h6)
    prim["H6 goleiro segurando"] = p_h6

    # ---------------- H7/H8/H9: mesma troca de feature nos alvos da triade
    for hid, alvo, rotulo in (("H7", "vence_h", "Lay Home (mandante vence)"),
                              ("H8", "empate", "Lay Draw (empate)"),
                              ("H9", "vence_a", "Lay Away (visitante vence)")):
        print("=" * 78)
        print("%s — xG rolling x gols rolling para prever: %s" % (hid, rotulo))
        yv = va[alvo].values if alvo in va.columns else None
        st_ = tr.dropna(subset=fg + fx); sv_ = va.dropna(subset=fg + fx)
        b_g, _, _ = logit_fit(st_[fg].values, st_[alvo].values)
        b_x, _, _ = logit_fit(st_[fx].values, st_[alvo].values)
        p_g, p_x = prever(b_g, sv_[fg].values), prever(b_x, sv_[fx].values)
        yy = sv_[alvo].values
        l_g, l_x = logloss(yy, p_g), logloss(yy, p_x)
        dd = []
        rr = np.random.default_rng(13)
        for _ in range(10000):
            ii = rr.integers(0, len(yy), len(yy))
            dd.append(logloss(yy[ii], p_g[ii]) - logloss(yy[ii], p_x[ii]))
        dd = np.sort(dd)
        p_h = 2 * min((dd <= 0).mean(), (dd >= 0).mean())
        ganho = l_g - l_x
        print("  N validacao=%d | taxa do evento: %.1f%%" % (len(yy), 100 * yy.mean()))
        print("  log-loss GOLS=%.5f | xG=%.5f | ganho=%+.5f  IC95 [%+.5f, %+.5f]"
              % (l_g, l_x, ganho, dd[250], dd[9750]))
        if ganho <= 0:
            print("  >>> ganho <= 0: xG NAO melhora este alvo. p primario = 1,00")
            prim["%s %s" % (hid, rotulo)] = 1.0
        else:
            print("  >>> p primario (bootstrap) = %.4f" % p_h)
            prim["%s %s" % (hid, rotulo)] = p_h
        # diagnostico: sobra alguma coisa DEPOIS do preco?
        col_odd = {"vence_h": "odd_h", "empate": "odd_d", "vence_a": "odd_a"}[alvo]
        sp = sv_[(sv_[col_odd] > 1.01)].dropna(subset=[col_odd])
        if len(sp) > 200:
            imp = 1.0 / sp[col_odd].values
            lo = np.log(np.clip(imp, 1e-4, 1 - 1e-4) / (1 - np.clip(imp, 1e-4, 1 - 1e-4)))
            Xd = np.column_stack([lo] + [sp[c].values for c in fx])
            bd, sed, _ = logit_fit(Xd, sp[alvo].values)
            zs = [abs(bd[i] / sed[i]) if sed[i] else 0 for i in range(2, 6)]
            pmin = min(pnorm2(z) for z in zs)
            print("     [diagnostico] com o PRECO no modelo, menor p entre as 4 features de xG = %.4f"
                  % pmin)
            print("     %s" % ("-> sobra sinal alem do preco" if pmin < 0.05
                               else "-> NAO sobra sinal alem do preco: melhora o modelo, nao gera edge"))

    # ---------------- FDR
    print("=" * 78)
    print("VEREDITO DO LOTE — Benjamini-Hochberg, M=9 (pre-registrado)")
    print("  (hipotese com sinal contrario ao mecanismo declarado entra como p=1,00 —\n   um p pequeno na direcao errada refuta a tese, nao a confirma)")
    itens = [(k, v) for k, v in prim.items() if v is not None]
    M = 9
    itens.sort(key=lambda x: x[1])
    print("  %-34s %9s %11s  %s" % ("hipotese", "p", "limiar BH", "veredito"))
    aprovou = False
    for i, (k, p) in enumerate(itens, 1):
        lim = 0.05 * i / M
        ok = p <= lim
        aprovou = aprovou or ok
        print("  %-34s %9.4f %11.4f  %s" % (k, p, lim, "PASSA" if ok else "reprovada"))
    print()
    if not aprovou:
        print("  >>> NENHUMA hipotese sobrevive ao FDR. Lote encerrado — nada vai para forward.")
    else:
        print("  >>> Sobrevivente(s) vao para PRE-REGISTRO INDIVIDUAL + forward stake-zero.")
        print("      Nada vira codigo na VPS antes disso. CLV continua sendo o juiz final.")


if __name__ == "__main__":
    if not os.path.exists(CSV):
        print("[erro] falta %s — rode hist_time_xg.py antes." % CSV); raise SystemExit(1)
    m = montar()
    print("partidas utilizaveis (com rolling de >=%d jogos dos dois times): %d\n" % (MIN_HIST, len(m)))
    rodar(m)
