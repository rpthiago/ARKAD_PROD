# -*- coding: utf-8 -*-
"""
auditar_liquidacao_base_mestre.py — confere o PLACAR e o RESULTADO de cada linha da base mestre
(metodos_aprovados/Sinais_Metodos_Aprovados_Odds_Reais_Betfair.csv) contra as bases de resultado
(Betfair apicomunidade + b365), com o mesmo casamento do relatorio_forward_5metodos.py.

Motivo (11/09): a liquidacao noturna usa a reconstrucao de placar pelo Correct Score do coletor da
VPS, que erra 27% dos placares (sempre para MENOS gols — o mercado suspende no gol tardio). Alem
disso a base tem linhas com Placar='?' e Resultado preenchido.

Saida: relatorio no terminal + metodos_aprovados/auditoria_liquidacao_base_mestre.csv (uma linha
por sinal: placar gravado, placar verificado, resultado gravado, resultado verificado, PnL dos dois).
NAO altera a base mestre.
"""
import os, sys, re, unicodedata, warnings
warnings.filterwarnings("ignore")
sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
import pandas as pd, numpy as np
import relatorio_forward_5metodos as R

BASE = os.path.join(ROOT, "metodos_aprovados", "Sinais_Metodos_Aprovados_Odds_Reais_Betfair.csv")
OUT = os.path.join(ROOT, "metodos_aprovados", "auditoria_liquidacao_base_mestre.csv")
C = 0.05

d = pd.read_csv(BASE, encoding="utf-8")
col_met = [c for c in d.columns if "todo" in c][0]
print("base mestre: %d linhas | %s -> %s" % (len(d), d.Data.min(), d.Data.max()))

def red_do_metodo(m, gh, ga):
    m = m.lower()
    if "draw" in m:     return gh == ga
    if "over 4.5" in m: return gh + ga >= 5
    if "home" in m:     return gh > ga
    if "away" in m:     return ga > gh
    mm = re.search(r"(\d)x(\d)", m)
    if mm:              return gh == int(mm.group(1)) and ga == int(mm.group(2))
    raise ValueError(m)

P = R.placares(); idx = R._por_dia(P)
rows = []
for _, r in d.iterrows():
    h, a = str(r["Home"]), str(r["Away"])
    if (not h or h == "nan") and " x " in str(r["Jogo"]):
        h, a = [x.strip() for x in str(r["Jogo"]).split(" x ", 1)]
    sc = R.achar_placar(P, idx, str(r["Data"]), h, a)
    grav = str(r.get("Placar", "")).strip()
    mg = re.match(r"^\s*(\d+)\s*[x\-]\s*(\d+)\s*$", grav)
    gh_g, ga_g = (int(mg.group(1)), int(mg.group(2))) if mg else (None, None)
    odd = float(r["Odd_Entrada"])
    res_g = str(r.get("Resultado", ""))
    if sc:
        gh_v, ga_v, fonte = sc
        red_v = red_do_metodo(r[col_met], gh_v, ga_v)
        res_v = "RED" if red_v else "GREEN"
        pnl_v = -1.0 if red_v else (1 - C) / (odd - 1)
    else:
        gh_v = ga_v = None; fonte = ""; res_v = "SEM_PLACAR"; pnl_v = np.nan
    rows.append(dict(Data=r["Data"], Metodo=r[col_met], Home=h, Away=a, Odd=odd,
                     placar_gravado=grav, placar_verificado=("%d-%d" % (gh_v, ga_v)) if sc else "",
                     fonte=fonte, resultado_gravado=res_g, resultado_verificado=res_v,
                     pnl_gravado=float(r["PnL_u"]) if pd.notna(r["PnL_u"]) else np.nan, pnl_verificado=pnl_v,
                     placar_bate=(gh_g == gh_v and ga_g == ga_v) if (sc and mg) else None,
                     resultado_bate=(res_g == res_v) if sc else None))
A = pd.DataFrame(rows); A.to_csv(OUT, index=False, encoding="utf-8-sig")

print("\n=== COBERTURA ===")
print("  com placar verificado nas bases: %d de %d" % ((A.resultado_verificado != "SEM_PLACAR").sum(), len(A)))
print("  fonte:", A.fonte.str.split(":").str[0].value_counts().to_dict())
print("  placar gravado '?' ou vazio: %d  (destes, com Resultado preenchido: %d)"
      % ((~A.placar_gravado.str.match(r"^\d+[x\-]\d+$")).sum(),
         ((~A.placar_gravado.str.match(r"^\d+[x\-]\d+$")) & A.resultado_gravado.isin(["GREEN", "RED"])).sum()))

V = A[A.resultado_verificado != "SEM_PLACAR"]
print("\n=== PLACAR (so linhas com placar gravado E verificado) ===")
pb = V[V.placar_bate.notna()]
print("  comparaveis: %d | batem: %d | DIVERGEM: %d (%.1f%%)"
      % (len(pb), pb.placar_bate.sum(), (~pb.placar_bate.astype(bool)).sum(), 100 * (~pb.placar_bate.astype(bool)).mean() if len(pb) else 0))
div = pb[~pb.placar_bate.astype(bool)]
if len(div):
    tg = lambda s: s.str.replace("x", "-").str.split("-").apply(lambda t: int(t[0]) + int(t[1]))
    menos = (tg(div.placar_gravado) < tg(div.placar_verificado)).sum()
    print("  gravado com MENOS gols que o verificado (gol tardio perdido): %d de %d" % (menos, len(div)))

print("\n=== RESULTADO (GREEN/RED) ===")
rb = V[V.resultado_gravado.isin(["GREEN", "RED"])]
err = rb[~rb.resultado_bate.astype(bool)]
print("  comparaveis: %d | batem: %d | ERRADOS: %d (%.1f%%)" % (len(rb), rb.resultado_bate.sum(), len(err), 100 * len(err) / len(rb) if len(rb) else 0))
if len(err):
    print("  falso GREEN (gravado GREEN, verdade RED): %d" % ((err.resultado_gravado == "GREEN") & (err.resultado_verificado == "RED")).sum())
    print("  falso RED   (gravado RED, verdade GREEN): %d" % ((err.resultado_gravado == "RED") & (err.resultado_verificado == "GREEN")).sum())
    print("\n  %-10s %-30s %-26s %5s %8s %8s %6s %6s" % ("data", "metodo", "jogo", "odd", "gravado", "verif.", "res_g", "res_v"))
    for _, r in err.iterrows():
        print("  %-10s %-30s %-26s %5.2f %8s %8s %6s %6s" % (r.Data, r.Metodo[:30], (r.Home + " x " + r.Away)[:26], r.Odd, r.placar_gravado, r.placar_verificado, r.resultado_gravado, r.resultado_verificado))

print("\n=== IMPACTO NO P&L (liability=1u, comissao 5%, so linhas verificadas) ===")
print("  %-34s %5s %10s %10s %9s" % ("metodo", "N", "PnL grav.", "PnL verif.", "dif"))
for m, g in V.groupby("Metodo"):
    print("  %-34s %5d %+10.3f %+10.3f %+9.3f" % (m[:34], len(g), g.pnl_gravado.sum(), g.pnl_verificado.sum(), g.pnl_verificado.sum() - g.pnl_gravado.sum()))
print("  %-34s %5d %+10.3f %+10.3f %+9.3f" % ("TOTAL", len(V), V.pnl_gravado.sum(), V.pnl_verificado.sum(), V.pnl_verificado.sum() - V.pnl_gravado.sum()))
sp = A[A.resultado_verificado == "SEM_PLACAR"]
print("\n  %d linhas SEM placar verificavel (PnL gravado nelas: %+.3f u) — nao auditaveis por esta via" % (len(sp), sp.pnl_gravado.sum()))
print("\nplanilha da auditoria: %s" % OUT)


# ---------------------------------------------------------------- versao VERIFICADA (arquivo separado)
# Nao toca a base mestre. Para as linhas verificaveis, placar/resultado/PnL vem das bases; as
# demais ficam como estao, marcadas 'nao_verificado'. Colunas *_Original preservam o que havia.
VER = os.path.join(ROOT, "metodos_aprovados", "Sinais_Metodos_Aprovados_Odds_Reais_Betfair_VERIFICADA.csv")
d2 = d.copy()
d2["Placar_Original"] = d2["Placar"]; d2["Resultado_Original"] = d2["Resultado"]; d2["PnL_u_Original"] = d2["PnL_u"]
d2["Fonte_Placar"] = "nao_verificado"
liab_rs = 100.0
for i, (_, a) in enumerate(A.iterrows()):
    if a.resultado_verificado not in ("GREEN", "RED"):
        continue
    odd = float(d2.at[i, "Odd_Entrada"]); red = a.resultado_verificado == "RED"
    pnl = -1.0 if red else (1 - C) / (odd - 1)
    d2.at[i, "Placar"] = a.placar_verificado.replace("-", "x")
    d2.at[i, "Resultado"] = a.resultado_verificado
    d2.at[i, "1/0"] = 0 if red else 1
    d2.at[i, "PnL_u"] = round(pnl, 5)
    d2.at[i, "PnL_stake_u"] = round(-(odd - 1) if red else (1 - C), 5)
    d2.at[i, "PnL_R$"] = round(pnl * liab_rs, 2)
    d2.at[i, "Fonte_Placar"] = a.fonte
d2.to_csv(VER, index=False, encoding="utf-8-sig")
tot0 = pd.to_numeric(d["PnL_u"], errors="coerce").sum(); tot1 = pd.to_numeric(d2["PnL_u"], errors="coerce").sum()
print("\n=== BASE INTEIRA (266 linhas, inclui as 99 nao verificaveis como estao) ===")
print("  PnL_u gravado:    %+.3f u" % tot0)
print("  PnL_u verificado: %+.3f u   (dif %+.3f u = R$ %+.0f)" % (tot1, tot1 - tot0, (tot1 - tot0) * liab_rs))
for m, g in d2.groupby(col_met):
    g0 = d[d[col_met] == m]
    print("  %-36s N=%3d  gravado %+7.3f  verificado %+7.3f  WR %5.1f%% -> %5.1f%%"
          % (m[:36], len(g), pd.to_numeric(g0["PnL_u"], errors="coerce").sum(), pd.to_numeric(g["PnL_u"], errors="coerce").sum(),
             100 * (g0["Resultado"] == "GREEN").mean(), 100 * (g["Resultado"] == "GREEN").mean()))
print("\nversao verificada gravada em: %s" % VER)
