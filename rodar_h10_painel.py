#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rodar_h10_painel.py — H10: o PAINEL INTEIRO de estatisticas bate o PRECO?

PRE-REGISTRO (escrito antes de rodar, 2026-09-08):
  Pergunta: existe QUALQUER informacao aproveitavel no conjunto completo de estatisticas
  (26 campos por time, a favor e contra, media movel de 8 jogos), ALEM do que o preco ja diz?

  Por que teste CONJUNTO e nao coluna a coluna: 37 campos x 4 alvos = ~150 comparacoes.
  A ~5% de falso positivo isso produz ~8 "achados" por puro acaso. O teste conjunto responde
  a mesma pergunta com 1 p-valor por alvo, sem inflar multiplicidade.

  Desenho (congelado ANTES de ver resultado):
    * split TEMPORAL: 1a metade treina, 2a metade valida. p sai so da validacao.
    * rolling com shift(1) por time -> zero vazamento.
    * Modelo A = SO o preco (logit da probabilidade implicita).
      Modelo B = preco + TODO o painel rolling.
    * Estimador PRIMARIO: logistica com ridge, lambda escolhido por CV SO na metade de treino.
      Justificativa declarada: com N~2.500 de treino e ~100 features, o modelo regularizado
      linear tem o melhor compromisso vies-variancia.
    * Estimador SECUNDARIO (diagnostico, NAO entra no FDR): HistGradientBoosting, para checar
      se o que falta e nao-linearidade. Se o GBM achar o que o linear nao acha, isso e PISTA
      para um pre-registro NOVO, nao um resultado.
    * Estatistica: diferenca de log-loss fora da amostra, IC95 por bootstrap.
    * Alvos: H10a 0-0 · H10b mandante vence · H10c empate · H10d visitante vence.
    * M do Benjamini-Hochberg passa de 9 para 13.

  Aprova so se o IC95 do ganho excluir zero E sobreviver ao BH com M=13.
"""
import os, json, glob, math
import numpy as np, pandas as pd
from math import erfc
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler

AQUI = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(AQUI, "hist_time_stats.csv")
DIR_FT = os.path.join(AQUI, "hist_stats_ft")
JAN, MIN_HIST = 8, 5
M_FDR = 13

# todos os campos do payload (os 13 do CSV + os 24 que ficaram so no cache)
CACHE_FIELDS = ["ShotsOffTarget", "blocked_shots", "shots_inside_box", "shots_outside_box",
                "shots_woodwork", "big_chance_missed_title", "accurate_crosses",
                "long_balls_accurate", "dribbles_succeeded", "duel_won", "ground_duels_won",
                "aerials_won", "matchstats.headers.tackles", "interceptions", "clearances",
                "shot_blocks", "Offsides", "fouls", "yellow_cards", "red_cards",
                "player_throws", "passes", "own_half_passes", "opposition_half_passes",
                "accurate_passes"]
CSV_FIELDS = ["xg", "xg_open", "xg_set", "xg_np", "xgot", "shots", "sot", "bigch",
              "tbox", "saves", "corners", "poss"]


def num(v):
    if v is None:
        return np.nan
    try:
        return float(str(v).split("(")[0].strip().replace("%", ""))
    except Exception:
        return np.nan


def carregar():
    d = pd.read_csv(CSV, dtype={"eventid": str})
    d["eventid"] = d.eventid.fillna("").str.replace(r"\.0$", "", regex=True)
    d = d[(d.tem_stats == 1) & (d.eventid != "")].copy()
    for c in d.columns:
        if c not in ("data", "liga", "home", "away", "eventid"):
            d[c] = pd.to_numeric(d[c], errors="coerce")
    # traz os 24 campos que ficaram so no cache (0 requisicao)
    novos = {("c%d_%s" % (i, lado)): [] for i in range(len(CACHE_FIELDS)) for lado in "ha"}
    for eid in d.eventid:
        p = os.path.join(DIR_FT, "%s.json" % eid)
        js = json.load(open(p, encoding="utf-8")) if os.path.exists(p) else {}
        js = js or {}
        for i, k in enumerate(CACHE_FIELDS):
            v = js.get(k) or [None, None]
            novos["c%d_h" % i].append(num(v[0]))
            novos["c%d_a" % i].append(num(v[1]))
    for c, v in novos.items():
        d[c] = v
    return d


def montar():
    d = carregar()
    campos = CSV_FIELDS + ["c%d" % i for i in range(len(CACHE_FIELDS))]
    linhas = []
    for _, r in d.iterrows():
        for lado, opo in (("h", "a"), ("a", "h")):
            reg = dict(data=r.data, eventid=r.eventid,
                       time=r.home if lado == "h" else r.away, casa=1 if lado == "h" else 0,
                       gols_pro=r.gols_h if lado == "h" else r.gols_a,
                       gols_con=r.gols_a if lado == "h" else r.gols_h)
            for c in campos:
                reg[c + "_pro"] = r.get("%s_%s" % (c, lado), np.nan)
                reg[c + "_con"] = r.get("%s_%s" % (c, opo), np.nan)
            linhas.append(reg)
    t = pd.DataFrame(linhas)
    t["data"] = pd.to_datetime(t.data)
    t = t.sort_values(["time", "data"]).reset_index(drop=True)
    cols = [c for c in t.columns if c.endswith("_pro") or c.endswith("_con")]
    g = t.groupby("time")
    for c in cols:
        t["r_" + c] = g[c].transform(lambda s: s.shift(1).rolling(JAN, min_periods=MIN_HIST).mean())
    t["n_hist"] = g.cumcount()
    r = ["r_" + c for c in cols]
    casa = t[t.casa == 1][["eventid"] + r + ["n_hist"]].add_suffix("_H").rename(columns={"eventid_H": "eventid"})
    fora = t[t.casa == 0][["eventid"] + r + ["n_hist"]].add_suffix("_A").rename(columns={"eventid_A": "eventid"})
    m = d.merge(casa, on="eventid").merge(fora, on="eventid")
    m["data"] = pd.to_datetime(m.data)
    m["zero_a_zero"] = ((m.gols_h == 0) & (m.gols_a == 0)).astype(int)
    m["vence_h"] = (m.gols_h > m.gols_a).astype(int)
    m["empate"] = (m.gols_h == m.gols_a).astype(int)
    m["vence_a"] = (m.gols_h < m.gols_a).astype(int)
    return m[(m.n_hist_H >= MIN_HIST) & (m.n_hist_A >= MIN_HIST)].sort_values("data")


def logloss(y, p):
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def rodar_alvo(m, alvo, col_odd, rotulo, feats):
    print("=" * 78)
    print("%s — o painel inteiro bate o preco?  alvo: %s" % (rotulo, alvo))
    s = m[(m[col_odd] > 1.01)].dropna(subset=[col_odd] + feats).copy()
    imp = np.clip(1.0 / s[col_odd].values, 1e-4, 1 - 1e-4)
    s["preco"] = np.log(imp / (1 - imp))
    corte = s.data.quantile(0.5)
    tr, va = s[s.data < corte], s[s.data >= corte]
    if len(va) < 300:
        print("  [PULADO] validacao com N=%d" % len(va)); return None
    ytr, yva = tr[alvo].values, va[alvo].values

    sc = StandardScaler().fit(tr[feats].values)
    Xtr_f, Xva_f = sc.transform(tr[feats].values), sc.transform(va[feats].values)
    Xtr_p, Xva_p = tr[["preco"]].values, va[["preco"]].values

    # A: so preco
    A = LogisticRegression(C=1e6, max_iter=2000).fit(Xtr_p, ytr)
    pA = A.predict_proba(Xva_p)[:, 1]

    # B: preco + painel, ridge com lambda por CV SO no treino
    melhor, melhorC = None, None
    tss = TimeSeriesSplit(n_splits=4)
    for C in (0.003, 0.01, 0.03, 0.1, 0.3, 1.0):
        perdas = []
        XB = np.column_stack([Xtr_p, Xtr_f])
        for itr, iva in tss.split(XB):
            mdl = LogisticRegression(C=C, max_iter=3000).fit(XB[itr], ytr[itr])
            perdas.append(logloss(ytr[iva], mdl.predict_proba(XB[iva])[:, 1]))
        med = float(np.mean(perdas))
        if melhor is None or med < melhor:
            melhor, melhorC = med, C
    B = LogisticRegression(C=melhorC, max_iter=3000).fit(np.column_stack([Xtr_p, Xtr_f]), ytr)
    pB = B.predict_proba(np.column_stack([Xva_p, Xva_f]))[:, 1]

    lA, lB = logloss(yva, pA), logloss(yva, pB)
    rng = np.random.default_rng(21)
    dif = np.sort([logloss(yva[i], pA[i]) - logloss(yva[i], pB[i])
                   for i in (rng.integers(0, len(yva), len(yva)) for _ in range(10000))])
    p = 2 * min((dif <= 0).mean(), (dif >= 0).mean())
    print("  N treino=%d | N validacao=%d | taxa do evento=%.1f%% | features=%d | C escolhido=%s"
          % (len(tr), len(va), 100 * yva.mean(), len(feats), melhorC))
    print("  log-loss SO PRECO=%.5f | PRECO+PAINEL=%.5f | ganho=%+.5f  IC95 [%+.5f, %+.5f]"
          % (lA, lB, lA - lB, dif[250], dif[9750]))
    if (lA - lB) <= 0:
        print("  >>> o painel PIORA a previsao (ganho negativo). p primario = 1,00 —")
        print("      significancia na direcao errada REFUTA a tese, nao a confirma.")
        p = 1.0
    else:
        print("  >>> p primario = %.4f" % p)

    G = HistGradientBoostingClassifier(max_iter=250, learning_rate=0.05,
                                       max_leaf_nodes=15, l2_regularization=1.0,
                                       random_state=0).fit(np.column_stack([Xtr_p, Xtr_f]), ytr)
    pG = G.predict_proba(np.column_stack([Xva_p, Xva_f]))[:, 1]
    lG = logloss(yva, pG)
    print("  [diagnostico, fora do FDR] GBM: log-loss=%.5f | ganho sobre o preco=%+.5f %s"
          % (lG, lA - lG, "" if lA - lG <= 0 else "<- nao-linearidade merece pre-registro NOVO"))
    return p


def main():
    m = montar()
    feats = [c for c in m.columns if c.startswith("r_")]
    print("partidas utilizaveis: %d | features rolling no painel: %d\n" % (len(m), len(feats)))
    res = {}
    for alvo, odd, rot in (("zero_a_zero", "odd_cs_0x0", "H10a  0-0 (o alvo do Lay 0x0)"),
                           ("vence_h", "odd_h", "H10b  mandante vence (Lay Home)"),
                           ("empate", "odd_d", "H10c  empate (Lay Draw)"),
                           ("vence_a", "odd_a", "H10d  visitante vence (Lay Away)")):
        p = rodar_alvo(m, alvo, odd, rot, feats)
        if p is not None:
            res[rot] = p
    print("=" * 78)
    print("VEREDITO H10 — Benjamini-Hochberg com M=%d (9 anteriores + 4 do painel)" % M_FDR)
    itens = sorted(res.items(), key=lambda x: x[1])
    for i, (k, p) in enumerate(itens, 1):
        lim = 0.05 * i / M_FDR
        print("  %-34s p=%.4f  limiar=%.4f  %s" % (k, p, lim, "PASSA" if p <= lim else "reprovada"))
    print()
    if not any(p <= 0.05 * (i + 1) / M_FDR for i, (_, p) in enumerate(itens)):
        print("  >>> o painel INTEIRO nao bate o preco em nenhum alvo.")
        print("      Nao adianta procurar a coluna magica: a informacao toda junta ja nao basta.")


if __name__ == "__main__":
    main()
