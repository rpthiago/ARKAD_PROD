# -*- coding: utf-8 -*-
"""diagnosticar_features_under15.py — por que o modelo da p alta no feed diario e baixa no historico?

Lei no 2 do GEMINI.md: backtest e live TEM que ser o MESMO objeto. O observador achou 8 sinais
(p 0,63-0,69) em 12 dias; no historico, no MESMO intervalo de odd, a p mediana e 0,441 e nenhum
jogo passa do EV>=5%. Este script acha QUAL feature esta divergindo.
"""
import pandas as pd, numpy as np, joblib

COMM = 0.045
OC = "Odd_Under15_FT_Lay"

b = joblib.load("models/modelo_lay_under15_xgb.joblib")
F = b["features"]
feed = pd.read_parquet("scratch/feed_forward_diario.parquet")
hist = pd.read_parquet("scratch/dataset_leak_free_features.parquet")

# recorte comparavel: mesma faixa de odd
for d in (feed, hist):
    d[OC] = pd.to_numeric(d.get(OC), errors="coerce")
fe = feed[(feed[OC] >= 2.50) & (feed[OC] <= 4.50)].copy()
hi = hist[(hist[OC] >= 2.50) & (hist[OC] <= 4.50)].copy()
print("no recorte de odd 2,50-4,50:  feed=%d jogos | historico=%d jogos" % (len(fe), len(hi)))
print()

print("=== FEATURE POR FEATURE: mediana no feed x mediana no historico ===")
print("  %-20s %12s %12s %10s %9s %9s" % ("feature", "feed(med)", "hist(med)", "razao", "feed 0%", "hist 0%"))
susp = []
for c in F:
    fv = pd.to_numeric(fe[c], errors="coerce")
    hv = pd.to_numeric(hi[c], errors="coerce")
    fm, hm = fv.median(), hv.median()
    fz, hz = 100 * (fv == 0).mean(), 100 * (hv == 0).mean()
    fn, hn = 100 * fv.isna().mean(), 100 * hv.isna().mean()
    razao = (fm / hm) if (hm not in (0, np.nan) and pd.notna(hm) and hm != 0) else np.nan
    flag = ""
    if pd.notna(razao) and (razao > 1.5 or razao < 0.67):
        flag = "  <<< divergente"
    if abs(fz - hz) > 20 or abs(fn - hn) > 20:
        flag = "  <<< zeros/NaN"
    if flag:
        susp.append(c)
        print("  %-20s %12.4f %12.4f %10s %8.0f%% %8.0f%%%s"
              % (c, fm if pd.notna(fm) else float("nan"), hm if pd.notna(hm) else float("nan"),
                 ("%.2fx" % razao) if pd.notna(razao) else "-", fz, hz, flag))
print("\n  features divergentes: %d de %d" % (len(susp), len(F)))

print("\n=== TESTE DECISIVO: a p do modelo muda se eu trocar SO as features suspeitas? ===")
if susp and len(fe):
    p_feed = b["model"].predict_proba(fe[F])[:, 1]
    print("  p no feed diario (como esta):      mediana %.4f | max %.4f" % (np.median(p_feed), p_feed.max()))
    med_hist = {c: pd.to_numeric(hi[c], errors="coerce").median() for c in susp}
    fe2 = fe.copy()
    for c in susp:
        fe2[c] = med_hist[c]
    p2 = b["model"].predict_proba(fe2[F])[:, 1]
    print("  p trocando as suspeitas pela mediana historica: mediana %.4f | max %.4f"
          % (np.median(p2), p2.max()))
    print("  -> deslocamento de p: %+.4f (se for grande, sao ESSAS features que inflam)"
          % (np.median(p2) - np.median(p_feed)))

    print("\n  contribuicao individual (troca UMA feature por vez):")
    base = np.median(p_feed)
    linhas = []
    for c in susp:
        f3 = fe.copy(); f3[c] = med_hist[c]
        linhas.append((abs(np.median(b["model"].predict_proba(f3[F])[:, 1]) - base),
                       c, np.median(b["model"].predict_proba(f3[F])[:, 1]) - base))
    for _, c, delta in sorted(linhas, reverse=True)[:8]:
        print("    %-20s desloca a p mediana em %+.4f" % (c, delta))

print("\n=== e a p do modelo no HISTORICO, mesmo recorte ===")
p_hist = b["model"].predict_proba(hi[F].fillna(hi[F].median()))[:, 1]
print("  historico: p mediana %.4f | max %.4f" % (np.median(p_hist), p_hist.max()))
print("  feed     : p mediana %.4f | max %.4f" % (np.median(b['model'].predict_proba(fe[F])[:,1]),
                                                  b['model'].predict_proba(fe[F])[:,1].max()))
