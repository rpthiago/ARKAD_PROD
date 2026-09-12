# -*- coding: utf-8 -*-
"""
relatorio_forward_5metodos.py — Forward diario dos 5 metodos do "Portfolio de Metodos em
Validacao Forward — ARKAD", com relatorio no Telegram.
=====================================================================================
Os 5 metodos sao EXATAMENTE os do filtro padrao da pagina 01 (regras copiadas, nao reinterpretadas):
  1. Lay 0x3 Top 3         -> estrategia_lay_0x3.avaliar_jogos_lay_0x3_grade(df, top_n=3)
  2. Lay 2x2 Top 3         -> estrategia_lay_2x2.avaliar_jogos_lay_2x2_grade(df, top_n=3)
  3. Lay Draw (Fav<=1.40)  -> min(Odd_H_Back, Odd_A_Back) <= 1.40  e  4.5 <= Odd_D_Lay <= 10.0
  4. Lay Home / DC X2      -> Odd_A_Back <= 1.65  e  2.0 <= Odd_H_Lay <= 10.0
  5. Lay Over 4.5 (Under)  -> Odd_Under25_FT_Back <= 1.50  e  4.0 <= Odd_Over45_FT_Lay <= 20.0

Fonte das odds : fetch_betfair_daily (odd de LAY real da Betfair — validada 1,00x contra a API
                 direta em 17/08; Lei no 1).
Fonte do placar: base Betfair (apicomunidade) e base b365, match exato -> fuzzy no mesmo dia.
                 NAO usa a reconstrucao pelo Correct Score do coletor: validada em 11/09 contra a
                 base Betfair, erra 27% dos placares (143 de 527), SEMPRE para menos gols (o mercado
                 suspende no gol tardio) e muda a decisao G/R em 7-9% dos jogos.
Ledger         : metodos_aprovados/forward_5metodos_ledger.csv — append-only. Sinal entra PENDENTE
                 com a odd do dia; liquida UMA vez; linha liquidada nunca muda.
P&L            : LIABILITY = 1 u (= R$100), comissao 5% (Lei no 4; mesma convencao da base mestre
                 reunificada em 10/09). GREEN = 0,95/(odd-1) · RED = -1,0.
                 Obs.: a pagina 01 usa 0,965 (3,5%) — aqui e 5%, o numero honesto.

Uso:  python relatorio_forward_5metodos.py                 # incremental + Telegram
      python relatorio_forward_5metodos.py --desde 2026-08-01 --sem-telegram
"""
import os, sys, csv, re, unicodedata, argparse, warnings
from datetime import datetime, date, timedelta
warnings.filterwarnings("ignore")
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
import pandas as pd, numpy as np

COMISSAO = 0.05
LIAB_RS = 100.0
LEDGER = os.path.join(ROOT, "metodos_aprovados", "forward_5metodos_ledger.csv")
INICIO_PADRAO = "2026-08-01"

M_0X3, M_2X2, M_DRAW, M_HOME, M_O45 = ("Lay 0x3 Top 3", "Lay 2x2 Top 3", "Lay Draw (Fav<=1.40)",
                                       "Lay Home/DC X2 (FavVis<=1.65)", "Lay Over 4.5 (Under Pesado)")
ORDEM = [M_0X3, M_2X2, M_DRAW, M_HOME, M_O45]
CURTO = {M_0X3: "0x3", M_2X2: "2x2", M_DRAW: "Draw", M_HOME: "Home", M_O45: "Ov4.5"}
COLS = ["Data", "Metodo", "Liga", "Home", "Away", "Hora", "Odd_Lay", "Odd_Fav", "status",
        "gols_H", "gols_A", "placar", "resultado", "pnl_u", "pnl_rs", "break_even", "liquidado_em", "fonte_placar"]


def canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def chave(d, m, h, a):
    return "%s|%s|%s|%s" % (d, m, canon(h), canon(a))


def _num(v):
    try:
        x = float(v)
        return x if np.isfinite(x) and x > 1.0 else None
    except Exception:
        return None


# ------------------------------------------------------------------ 1. sinais do dia
def sinais_do_dia(ds):
    import b365_data_utils as B
    from estrategia_lay_0x3 import avaliar_jogos_lay_0x3_grade
    from estrategia_lay_2x2 import avaliar_jogos_lay_2x2_grade
    try:
        g = B.fetch_betfair_daily(ds)
    except Exception as e:
        print("  [feed erro %s] %s" % (ds, str(e)[:60])); return None
    df = pd.DataFrame(g) if isinstance(g, list) else g
    if df is None or df.empty:
        return []
    out = []

    def add(metodo, r, odd, fav):
        out.append(dict(Data=ds, Metodo=metodo, Liga=str(r.get("League", "")), Home=str(r.get("Home", "")),
                        Away=str(r.get("Away", "")), Hora=str(r.get("Time", ""))[:5],
                        Odd_Lay=round(odd, 2), Odd_Fav=round(fav, 2) if fav else ""))

    # 1 e 2: Top 3 menor odd, pelas funcoes oficiais
    for fn, nome in ((avaliar_jogos_lay_0x3_grade, M_0X3), (avaliar_jogos_lay_2x2_grade, M_2X2)):
        try:
            for s in fn(df, top_n=3):
                odd = _num(s.get("odd_lay"))
                if odd:
                    add(nome, {"League": s.get("league"), "Home": s.get("home"), "Away": s.get("away"),
                               "Time": s.get("hora")}, odd, 0.0)
        except Exception as e:
            print("  [%s erro] %s" % (nome, str(e)[:60]))

    oh_b = pd.to_numeric(df.get("Odd_H_Back"), errors="coerce")
    oa_b = pd.to_numeric(df.get("Odd_A_Back"), errors="coerce")
    oh_l = pd.to_numeric(df.get("Odd_H_Lay"), errors="coerce")
    od_l = pd.to_numeric(df.get("Odd_D_Lay"), errors="coerce")
    u25_b = pd.to_numeric(df.get("Odd_Under25_FT_Back"), errors="coerce")
    o45_l = pd.to_numeric(df.get("Odd_Over45_FT_Lay"), errors="coerce")
    for i, r in df.iterrows():
        h, a = oh_b.get(i), oa_b.get(i)
        # 3. Lay Draw — simetrico
        fav = min(h if pd.notna(h) else 99.0, a if pd.notna(a) else 99.0)
        if fav <= 1.40 and pd.notna(od_l.get(i)) and 4.5 <= od_l[i] <= 10.0:
            add(M_DRAW, r, float(od_l[i]), fav)
        # 4. Lay Home em fav visitante
        if pd.notna(a) and a <= 1.65 and pd.notna(oh_l.get(i)) and 2.0 <= oh_l[i] <= 10.0:
            add(M_HOME, r, float(oh_l[i]), float(a))
        # 5. Lay Over 4.5 em jogo under
        if pd.notna(u25_b.get(i)) and u25_b[i] <= 1.50 and pd.notna(o45_l.get(i)) and 4.0 <= o45_l[i] <= 20.0:
            add(M_O45, r, float(o45_l[i]), float(u25_b[i]))
    return out


# ------------------------------------------------------------------ 2. liquidacao
def red_do_metodo(m, gh, ga):
    if m == M_0X3:  return gh == 0 and ga == 3
    if m == M_2X2:  return gh == 2 and ga == 2
    if m == M_DRAW: return gh == ga
    if m == M_HOME: return gh > ga
    if m == M_O45:  return (gh + ga) >= 5
    raise ValueError(m)


CACHE_BF = os.path.join(ROOT, "metodos_aprovados", ".cache_base_betfair.csv")


def _base_betfair():
    """Base historica da Betfair (apicomunidade). MESMA fonte e MESMOS nomes do feed diario, e tem
    Goals_H_FT/Goals_A_FT -> fonte PRIMARIA do placar (match exato). Cache local de 12h."""
    import requests, io as _io
    from config import API_BETFAIR_BASE_URL, API_HEADERS
    fresca = os.path.exists(CACHE_BF) and (datetime.now().timestamp() - os.path.getmtime(CACHE_BF)) < 12 * 3600
    if not fresca:
        try:
            r = requests.get(API_BETFAIR_BASE_URL, headers=dict(API_HEADERS), timeout=180)
            r.raise_for_status()
            _io.open(CACHE_BF, "w", encoding="utf-8", newline="").write(r.text)
        except Exception as e:
            print("  [base betfair] download falhou (%s) — usando cache se existir" % str(e)[:60])
    if not os.path.exists(CACHE_BF):
        return pd.DataFrame()
    return pd.read_csv(CACHE_BF, low_memory=False)


def placares():
    """Dict {(data, canon(home), canon(away)): (gh, ga, fonte)}. Betfair primeiro; b365 (a copia mais
    recente entre ARKAD_PROD e DASHBOARD) so onde a Betfair nao tem, e sem sobrescrever."""
    out = {}
    bf = _base_betfair()
    if not bf.empty and "Goals_H_FT" in bf.columns:
        d = pd.to_datetime(bf["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
        gh = pd.to_numeric(bf["Goals_H_FT"], errors="coerce"); ga = pd.to_numeric(bf["Goals_A_FT"], errors="coerce")
        for dd, h, a, x, y in zip(d, bf["Home"], bf["Away"], gh, ga):
            if pd.notna(x) and pd.notna(y) and isinstance(dd, str):
                out[(dd, canon(h), canon(a))] = (int(x), int(y), "betfair")
        print("  placares betfair: %d (ate %s)" % (len(out), d.max()))
    # b365: pega a copia mais nova (o DASHBOARD atualiza diariamente; a do ARKAD_PROD e semanal)
    cands = [os.path.join(ROOT, "Bases_de_Dados_API_FutPythonTrader_Bet365.csv"),
             os.path.join(os.path.dirname(ROOT), "DASHBOARD_ARKAD-1", "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")]
    cands = [p for p in cands if os.path.exists(p)]
    if cands:
        p = max(cands, key=os.path.getmtime)
        try:
            rb = pd.read_csv(p, low_memory=False, usecols=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"])
            d = pd.to_datetime(rb["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
            gh = pd.to_numeric(rb["Goals_H_FT"], errors="coerce"); ga = pd.to_numeric(rb["Goals_A_FT"], errors="coerce")
            n0 = len(out)
            for dd, h, a, x, y in zip(d, rb["Home"], rb["Away"], gh, ga):
                k = (dd, canon(h), canon(a))
                if pd.notna(x) and pd.notna(y) and isinstance(dd, str) and k not in out:
                    out[k] = (int(x), int(y), "b365")
            print("  placares b365 (%s): +%d" % (os.path.basename(os.path.dirname(p)), len(out) - n0))
        except Exception as e:
            print("  [b365] %s" % str(e)[:60])
    return out


def _por_dia(P):
    idx = {}
    for (d, ch, ca), v in P.items():
        idx.setdefault(d, []).append((ch, ca, v))
    return idx


def achar_placar(P, idx, d, home, away):
    """Exato primeiro. Depois fuzzy DENTRO do mesmo dia: media dos dois lados >= 0.80 e cada lado
    >= 0.60 (o feed diario usa nome Betfair — 'DC Utd', 'Jeonbuk Motors' — e as bases usam nome
    b365 — 'DC United', 'Jeonbuk'). Devolve (gh, ga, fonte) ou None. O score fica gravado."""
    import difflib
    ch, ca = canon(home), canon(away)
    v = P.get((d, ch, ca))
    if v:
        return v[0], v[1], v[2] + ":exato"
    best, bs = None, 0.0
    for xh, xa, vv in idx.get(d, []):
        sh = difflib.SequenceMatcher(None, ch, xh).ratio()
        if sh < 0.60: continue
        sa = difflib.SequenceMatcher(None, ca, xa).ratio()
        if sa < 0.60: continue
        sc = (sh + sa) / 2
        if sc > bs:
            bs, best = sc, vv
    if best and bs >= 0.80:
        return best[0], best[1], "%s:fuzzy%.2f" % (best[2], bs)
    return None


# ------------------------------------------------------------------ 3. ledger
def carregar():
    L = {}
    if os.path.exists(LEDGER):
        for r in csv.DictReader(open(LEDGER, encoding="utf-8-sig")):
            L[chave(r["Data"], r["Metodo"], r["Home"], r["Away"])] = r
    return L


def gravar(L):
    os.makedirs(os.path.dirname(LEDGER), exist_ok=True)
    rows = sorted(L.values(), key=lambda r: (r["Data"], ORDEM.index(r["Metodo"]) if r["Metodo"] in ORDEM else 9, r["Home"]))
    with open(LEDGER, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS, extrasaction="ignore"); w.writeheader(); w.writerows(rows)


def atualizar(desde, ate):
    L = carregar()
    dias_no_ledger = {r["Data"] for r in L.values()}
    # incremental: busca so os dias sem nenhum sinal no ledger + os 2 ultimos (feed pode completar)
    d0 = datetime.strptime(desde, "%Y-%m-%d").date(); d1 = datetime.strptime(ate, "%Y-%m-%d").date()
    alvo = [d0 + timedelta(i) for i in range((d1 - d0).days + 1)]
    alvo = [d for d in alvo if d.isoformat() not in dias_no_ledger or (d1 - d).days <= 1]
    novos = 0
    for d in alvo:
        ds = d.isoformat(); s = sinais_do_dia(ds)
        if s is None:
            continue
        n = 0
        for x in s:
            k = chave(x["Data"], x["Metodo"], x["Home"], x["Away"])
            if k in L:
                continue
            x.update(status="PENDENTE", gols_H="", gols_A="", placar="", resultado="", pnl_u="", pnl_rs="",
                     break_even=round((x["Odd_Lay"] - 1) / (x["Odd_Lay"] - COMISSAO), 4), liquidado_em="")
            L[k] = x; n += 1
        novos += n
        print("  %s: %d sinais (%d novos)" % (ds, len(s), n))
    print("sinais novos incorporados: %d" % novos)

    pend = [r for r in L.values() if r.get("status") != "LIQUIDADO"]
    if pend:
        P = placares(); idx = _por_dia(P); agora = datetime.now().strftime("%Y-%m-%d %H:%M"); liq = 0
        for r in pend:
            sc = achar_placar(P, idx, r["Data"], r["Home"], r["Away"])
            if sc is None:
                continue
            gh, ga, fonte = sc; odd = float(r["Odd_Lay"])
            red = red_do_metodo(r["Metodo"], gh, ga)
            pnl = -1.0 if red else (1 - COMISSAO) / (odd - 1)
            r.update(gols_H=gh, gols_A=ga, placar="%d-%d" % (gh, ga), resultado="RED" if red else "GREEN",
                     pnl_u=round(pnl, 5), pnl_rs=round(pnl * LIAB_RS, 2), status="LIQUIDADO",
                     liquidado_em=agora, fonte_placar=fonte)
            liq += 1
        corte = (date.today() - timedelta(days=7)).isoformat()
        for r in L.values():
            if r["status"] == "PENDENTE" and r["Data"] < corte:
                r["status"] = "SEM_PLACAR"          # jogo nao existe em nenhuma base -> fora da conta
        print("liquidados agora: %d | pendentes: %d | sem placar (>7d, fora da conta): %d"
              % (liq, sum(1 for r in L.values() if r["status"] == "PENDENTE"),
                 sum(1 for r in L.values() if r["status"] == "SEM_PLACAR")))
    gravar(L)
    return L


# ------------------------------------------------------------------ 4. relatorio
def _agg(rows):
    liq = [r for r in rows if r["status"] == "LIQUIDADO"]
    g = sum(1 for r in liq if r["resultado"] == "GREEN"); n = len(liq)
    pnl = sum(float(r["pnl_u"]) for r in liq)
    return n, g, n - g, pnl, sum(1 for r in rows if r["status"] == "PENDENTE")


def _linha(nome, rows, largura=6):
    n, g, r, pnl, pend = _agg(rows)
    if n == 0 and pend == 0:
        return None
    wr = ("%3.0f%%" % (100.0 * g / n)) if n else "  --"
    p = (" (%d pend)" % pend) if pend else ""
    return "%-*s %3d  %3dG/%2dR %s  %+7.2fu%s" % (largura, nome, n, g, r, wr, pnl, p)


def montar_mensagem(L, hoje):
    """Mensagem em HTML do Telegram (<b>, <i>, <pre>) — o Markdown legado quebra com '_' e '*'."""
    rows = list(L.values())
    hoje_s = hoje.isoformat()
    linhas = ["<b>ARKAD — Forward 5 Métodos</b>",
              "<i>%s · liability 1u = R$%.0f · comissão 5%%</i>" % (hoje.strftime("%d/%m/%Y"), LIAB_RS), ""]

    def bloco(titulo, sub):
        out = ["<b>%s</b>" % titulo, "<pre>"]
        tem = False
        for m in ORDEM:
            ln = _linha(CURTO[m], [r for r in sub if r["Metodo"] == m])
            if ln: out.append(ln); tem = True
        if not tem:
            out.append("sem sinais")
        else:
            out.append("-" * 34); out.append(_linha("TOTAL", sub))
        out.append("</pre>")
        return out

    linhas += bloco("HOJE %s" % hoje.strftime("%d/%m"), [r for r in rows if r["Data"] == hoje_s])
    meses = sorted({r["Data"][:7] for r in rows}, reverse=True)
    nomes = {"08": "AGOSTO", "09": "SETEMBRO", "10": "OUTUBRO", "11": "NOVEMBRO", "12": "DEZEMBRO",
             "01": "JANEIRO", "02": "FEVEREIRO", "03": "MARÇO", "04": "ABRIL", "05": "MAIO", "06": "JUNHO", "07": "JULHO"}
    for mes in meses:
        sub = [r for r in rows if r["Data"][:7] == mes]
        linhas += [""] + bloco("%s — por método" % nomes.get(mes[5:], mes), sub)
        linhas += ["<b>%s — por dia</b>" % nomes.get(mes[5:], mes), "<pre>"]
        for d in sorted({r["Data"] for r in sub}):
            n, g, r_, pnl, pend = _agg([r for r in sub if r["Data"] == d])
            if n == 0 and pend == 0:
                continue
            tag = "%+6.2fu" % pnl if n else "   --  "
            linhas.append("%s  %2d  %2dG/%2dR  %s%s" % (d[8:10] + "/" + d[5:7], n + pend, g, r_, tag, " *" if pend else ""))
        linhas.append("</pre>")
    n, g, r_, pnl, pend = _agg(rows)
    semp = sum(1 for r in rows if r["status"] == "SEM_PLACAR")
    if semp:
        linhas += ["", "<i>%d sinais de ligas sem placar em nenhuma base ficam fora da conta.</i>" % semp]
    ini = min(x["Data"] for x in rows) if rows else "----------"
    linhas += ["", "<b>ACUMULADO desde %s/%s:</b> %d liq · %dG/%dR · WR %.1f%% · <b>%+.2fu</b> (R$ %+.0f)%s"
               % (ini[8:10], ini[5:7], n, g, r_, (100.0 * g / n) if n else 0, pnl, pnl * LIAB_RS,
                  (" · %d pendentes (*)" % pend) if pend else "")]
    return "\n".join(linhas)


def _partes(msg, lim=3900):
    """Telegram aceita 4096 chars. Quebra por linha e fecha/reabre <pre> se a quebra cair dentro."""
    if len(msg) <= lim:
        return [msg]
    partes, atual, tam = [], [], 0
    for ln in msg.split("\n"):
        if tam + len(ln) + 1 > lim and atual:
            partes.append("\n".join(atual)); atual, tam = [], 0
        atual.append(ln); tam += len(ln) + 1
    partes.append("\n".join(atual))
    for i in range(len(partes)):
        if partes[i].count("<pre>") > partes[i].count("</pre>"):
            partes[i] += "\n</pre>"
            if i + 1 < len(partes): partes[i + 1] = "<pre>\n" + partes[i + 1]
    return partes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", default=INICIO_PADRAO)
    ap.add_argument("--ate", default=date.today().isoformat())
    ap.add_argument("--sem-telegram", action="store_true")
    a = ap.parse_args()
    print("=== forward 5 metodos: %s -> %s ===" % (a.desde, a.ate))
    L = atualizar(a.desde, a.ate)
    msg = montar_mensagem(L, datetime.strptime(a.ate, "%Y-%m-%d").date())
    print("\n" + re.sub(r"</?(b|i|pre)>", "", msg))
    print("\n(%d caracteres)" % len(msg))
    if not a.sem_telegram:
        from telegram_notifier import enviar_mensagem_telegram
        partes = _partes(msg)
        for i, p in enumerate(partes):
            ok, info = enviar_mensagem_telegram(p, parse_mode="HTML")
            print("telegram parte %d/%d: %s %s" % (i + 1, len(partes), "OK" if ok else "FALHOU", "" if ok else info))


if __name__ == "__main__":
    main()
