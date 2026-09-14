# -*- coding: utf-8 -*-
"""
liquidar_picks_0x0.py — o LIVRO-CAIXA do forward do Lay 0x0.
=============================================================
Conserta o furo achado em 10/09/2026: o forward era contado por uma RE-SIMULACAO
(`gerar_oos_real_0x0_xgb.py`), nao pelos picks que foram realmente enviados.

Por que a re-simulacao nao serve como forward:
  - Janela de treino DIFERENTE da do gerador ao vivo. Ao vivo treina com `Date < dia`
    (ate ontem); a simulacao treina com `_month < mes` (ate o fim do mes anterior). Para um
    jogo de 15/08 o modelo ao vivo tem 2 semanas extras de treino -> `p` diferente -> conjunto
    de picks diferente. Medido: dos 5 picks enviados em 15/08, a simulacao so contem 3.
    Reading x Luton (p ao vivo 0,949) e Blackpool x Wycombe (ev ao vivo 0,039, no fio)
    sumiram da contagem, e AMBOS eram GREEN.
  - A simulacao e regenerada toda semana. Um forward pre-registrado tem que ser IMUTAVEL:
    o que foi apostado nao muda depois.

Este script trata `picks_0x0_<data>.csv` como a unica fonte de verdade do que foi enviado, e
mantem `ledger_forward_0x0.csv` append-only:
  - pick novo -> entra como PENDENTE, com a odd EXATAMENTE como foi enviada (Lei no 1: e a odd
    de LAY executavel que estava na tela; nunca re-buscar depois).
  - pick pendente com placar disponivel -> liquida uma vez e nunca mais muda.
  - linha ja liquidada -> INTOCAVEL (e o que torna o forward pre-registrado honesto).

P&L nas duas convencoes, comissao 5% (Lei no 4). A DECISAO usa liability
(ver EMENDA em PREREGISTRO_lay0x0_xgb_forward.md).
"""
import os, sys, csv, glob, re, unicodedata, warnings
from datetime import datetime
warnings.filterwarnings("ignore")
AQUI = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.dirname(AQUI)
sys.path.insert(0, BASE)
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd, numpy as np

COMISSAO = 0.05
LEDGER = os.path.join(AQUI, "ledger_forward_0x0.csv")
N_MIN = 1476   # ver EMENDA 2026-09-10 no pre-registro
# a regra congelada (pre-registro 2026-08-02) — NAO MEXER
REGRA = dict(liga_max=0.08, mkt_max=0.10, odd_min=10.0, odd_max=20.0, ev_min=0.02)

COLS = ["Data", "Home", "Away", "liga", "odd_lay_entrada", "p", "ev", "liga_0x0", "mkt_prob",
        "ko", "na_regra", "motivo_fora", "status", "gols_H", "gols_A", "placar",
        "target", "pnl_liab", "pnl_stake", "break_even", "fonte_placar", "liquidado_em",
        "pick_origem"]


def canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def chave(data, home, away):
    return "%s|%s|%s" % (data, canon(home), canon(away))


def _f(v):
    try:
        x = float(v)
        return x if np.isfinite(x) else None
    except Exception:
        return None


def na_regra(odd, liga, mkt, ev):
    """Aplica a regra congelada. Lei no 3: dado ausente -> FORA (nunca passe livre)."""
    fora = []
    if odd is None:
        fora.append("sem odd_lay")
    elif not (REGRA["odd_min"] <= odd <= REGRA["odd_max"]):
        fora.append("odd %.2f fora de %g-%g" % (odd, REGRA["odd_min"], REGRA["odd_max"]))
    if liga is None:
        fora.append("sem liga_0x0_rate")
    elif liga >= REGRA["liga_max"]:
        fora.append("liga %.3f >= %g" % (liga, REGRA["liga_max"]))
    if mkt is None:
        fora.append("sem mkt_prob")
    elif mkt >= REGRA["mkt_max"]:
        fora.append("mkt %.3f >= %g" % (mkt, REGRA["mkt_max"]))
    if ev is None:
        fora.append("sem ev")
    elif ev <= REGRA["ev_min"]:
        fora.append("ev %.4f <= %g" % (ev, REGRA["ev_min"]))
    return (len(fora) == 0), "; ".join(fora)


# ---------------------------------------------------------------- 1. ledger existente
antes = {}
if os.path.exists(LEDGER):
    for r in csv.DictReader(open(LEDGER, encoding="utf-8-sig")):
        antes[chave(r["Data"], r["Home"], r["Away"])] = r
print("ledger atual: %d linhas (%d liquidadas)" %
      (len(antes), sum(1 for r in antes.values() if r.get("status") == "LIQUIDADO")))

# ---------------------------------------------------------------- 2. picks enviados
novos = 0
for arq in sorted(glob.glob(os.path.join(AQUI, "picks_0x0_*.csv"))):
    dia = os.path.basename(arq).replace("picks_0x0_", "").replace(".csv", "")
    try:
        p = pd.read_csv(arq)
    except Exception as e:
        print("  [erro lendo %s] %s" % (os.path.basename(arq), str(e)[:60]))
        continue
    for _, x in p.iterrows():
        h, a = x.get("home"), x.get("away")
        if pd.isna(h) or pd.isna(a):
            continue
        k = chave(dia, h, a)
        if k in antes:
            continue        # ja no ledger — nunca reescreve
        odd = _f(x.get("odd_lay"))
        liga = _f(x.get("liga_0x0"))
        mkt = _f(x.get("mkt_prob"))
        ev = _f(x.get("ev"))
        ok, motivo = na_regra(odd, liga, mkt, ev)
        antes[k] = dict(Data=dia, Home=str(h), Away=str(a), liga=str(x.get("liga", "")),
                        odd_lay_entrada=odd, p=_f(x.get("p")), ev=ev, liga_0x0=liga,
                        mkt_prob=mkt, ko=str(x.get("ko", "")), na_regra=int(ok),
                        motivo_fora=motivo, status="PENDENTE", gols_H="", gols_A="",
                        placar="", target="", pnl_liab="", pnl_stake="", break_even="",
                        fonte_placar="", liquidado_em="", pick_origem=os.path.basename(arq))
        novos += 1
print("picks novos incorporados: %d" % novos)

pend = [r for r in antes.values() if r.get("status") != "LIQUIDADO"]
print("pendentes de liquidacao: %d" % len(pend))

# ---------------------------------------------------------------- 3. placares
if pend:
    print("carregando base b365 para os placares...")
    import b365_data_utils as B
    rb = B.load_b365_historical()
    rb["_d"] = pd.to_datetime(rb["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    rb["_k"] = rb["_d"] + "|" + rb["Home"].map(canon) + "|" + rb["Away"].map(canon)
    gh = pd.to_numeric(rb["Goals_H_FT"], errors="coerce")
    ga = pd.to_numeric(rb["Goals_A_FT"], errors="coerce")
    placares = {}
    for k, x, y in zip(rb["_k"], gh, ga):
        if pd.notna(x) and pd.notna(y):
            placares[k] = (int(x), int(y), "b365 Goals_*_FT")
    print("  placares disponiveis: %d | base ate %s" % (len(placares), rb["_d"].max()))

    # fonte secundaria: a planilha de preenchimento manual. A base b365 publica com ~5 dias de
    # atraso, e sem isso um pick recente fica pendente por uma semana. A b365 tem PRIORIDADE;
    # o manual so entra onde ela ainda nao chegou, e a coluna `fonte_placar` registra qual foi.
    MAN = os.path.join(BASE, "placares_manuais.xlsx")
    if os.path.exists(MAN):
        try:
            mx = pd.read_excel(MAN)
            mx["_d"] = pd.to_datetime(mx["Data"], errors="coerce").dt.strftime("%Y-%m-%d")
            n_man = 0
            for _, x in mx.iterrows():
                gm, gv = pd.to_numeric(x.get("Gols_M"), errors="coerce"), pd.to_numeric(x.get("Gols_V"), errors="coerce")
                if pd.isna(x["_d"]) or pd.isna(gm) or pd.isna(gv):
                    continue
                k = chave(x["_d"], x.get("Mandante"), x.get("Visitante"))
                if k not in placares:          # nunca sobrescreve a b365
                    placares[k] = (int(gm), int(gv), "placares_manuais.xlsx")
                    n_man += 1
            print("  + %d placares da planilha manual (so onde a b365 nao chegou)" % n_man)
        except Exception as e:
            print("  [aviso] nao consegui ler placares_manuais.xlsx: %s" % str(e)[:70])

    liq = 0
    agora = datetime.now().strftime("%Y-%m-%d %H:%M")
    for r in pend:
        sc = placares.get(chave(r["Data"], r["Home"], r["Away"]))
        if sc is None:
            r["status"] = "PENDENTE"
            continue
        h, a, fonte = sc
        odd = _f(r["odd_lay_entrada"])
        if odd is None:
            r["status"] = "SEM_ODD"
            r["motivo_fora"] = (r.get("motivo_fora") or "") + "; nao liquidavel sem odd"
            continue
        tgt = 1 if (h + a) >= 1 else 0      # lay 0x0 e GREEN se o jogo NAO terminou 0-0
        r.update(gols_H=h, gols_A=a, placar="%d-%d" % (h, a), target=tgt,
                 pnl_liab=round((1 - COMISSAO) / (odd - 1) if tgt else -1.0, 5),
                 pnl_stake=round((1 - COMISSAO) if tgt else -(odd - 1), 5),
                 break_even=round((odd - 1) / (odd - COMISSAO), 5),
                 fonte_placar=fonte, status="LIQUIDADO", liquidado_em=agora)
        liq += 1
    print("liquidados agora: %d | ainda pendentes: %d" %
          (liq, sum(1 for r in antes.values() if r.get("status") == "PENDENTE")))

# ---------------------------------------------------------------- 4. grava
linhas = sorted(antes.values(), key=lambda r: (r["Data"], canon(r["Home"])))
with open(LEDGER, "w", newline="", encoding="utf-8-sig") as fh:
    w = csv.DictWriter(fh, fieldnames=COLS, extrasaction="ignore")
    w.writeheader()
    w.writerows(linhas)
print("ledger gravado: %s (%d linhas)" % (os.path.basename(LEDGER), len(linhas)))

# ---------------------------------------------------------------- 5. resumo do forward
d = [r for r in linhas if r.get("status") == "LIQUIDADO" and str(r.get("na_regra")) == "1"]
fora = [r for r in linhas if str(r.get("na_regra")) != "1"]
print("\n" + "=" * 74)
print("FORWARD AO VIVO do Lay 0x0 — so picks enviados, so dentro da regra congelada")
print("=" * 74)
if fora:
    print("(%d pick(s) enviado(s) FORA da regra, excluidos do forward:)" % len(fora))
    for r in fora:
        print("   %s %s x %s -> %s" % (r["Data"], r["Home"], r["Away"], r["motivo_fora"]))
if not d:
    print("nenhum pick liquidado dentro da regra ainda.")
else:
    vl = np.array([float(r["pnl_liab"]) for r in d])
    vs = np.array([float(r["pnl_stake"]) for r in d])
    tg = np.array([int(r["target"]) for r in d])
    be = np.array([float(r["break_even"]) for r in d])
    print("N=%d | %dG/%dR | WR=%.2f%% vs break-even medio %.2f%% -> gap %+.2f pp"
          % (len(d), tg.sum(), len(d) - tg.sum(), 100 * tg.mean(), 100 * be.mean(),
             100 * (tg.mean() - be.mean())))
    print("LIABILITY=1u: soma %+.3f u | ROI %+.2f%%" % (vl.sum(), 100 * vl.mean()))
    print("STAKE=1u    : soma %+.3f u | ROI %+.2f%%" % (vs.sum(), 100 * vs.mean()))
    print("\npor pick:")
    print("  %-11s %-34s %6s %-7s %6s %10s" % ("data", "jogo", "odd", "placar", "res", "pnl_liab"))
    for r in d:
        print("  %-11s %-34s %6.2f %-7s %6s %+10.4f" %
              (r["Data"], ("%s x %s" % (r["Home"], r["Away"]))[:34],
               float(r["odd_lay_entrada"]), r["placar"],
               "GREEN" if int(r["target"]) else "RED", float(r["pnl_liab"])))

pen = [r for r in linhas if r.get("status") == "PENDENTE"]
if pen:
    print("\npendentes (sem placar na base ainda):")
    for r in pen:
        print("   %s %s x %s @ %s" % (r["Data"], r["Home"], r["Away"], r["odd_lay_entrada"]))
print("\nprogresso: %d de %d apostas (%.1f%%) — ver EMENDA no pre-registro"
      % (len(d), N_MIN, 100.0 * len(d) / N_MIN))
