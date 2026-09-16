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

M_0X3, M_0X3_AMPLA, M_2X2, M_DRAW, M_HOME, M_O45, M_0X2_ZEBRA, M_2X0_ZEBRA = (
    "Lay 0x3 Top 3",
    "Lay 0x3 (Regra Ampla)",
    "Lay 2x2 Top 3",
    "Lay Draw (Fav<=1.40)",
    "Lay Home/DC X2 (FavVis<=1.65)",
    "Lay Over 4.5 (Under Pesado)",
    "Lay 0x2 Zebra (Micro-Liability)",
    "Lay 2x0 Zebra (Micro-Liability)"
)
ORDEM = [M_0X3, M_0X3_AMPLA, M_2X2, M_DRAW, M_HOME, M_O45, M_0X2_ZEBRA, M_2X0_ZEBRA]
# Fora do TOTAL/ACUMULADO/gestao: a Ampla contem os MESMOS jogos do 0x3 Top 3 (somar dobra o jogo);
# as zebras sao observacao (stake zero). Aparecem como linha propria, marcadas com "*".
FORA_DO_TOTAL = {M_0X3_AMPLA, M_0X2_ZEBRA, M_2X0_ZEBRA}


def na_conta(rows):
    return [r for r in rows if r["Metodo"] not in FORA_DO_TOTAL]
CURTO = {
    M_0X3: "0x3", M_0X3_AMPLA: "0x3-Ampla", M_2X2: "2x2", M_DRAW: "Draw",
    M_HOME: "Home", M_O45: "Ov4.5", M_0X2_ZEBRA: "0x2-Zeb", M_2X0_ZEBRA: "2x0-Zeb"
}
LIMITES_ODD = {
    M_0X3: (14.0, 35.0),
    M_0X3_AMPLA: (14.0, 35.0),
    M_2X2: (8.0, 20.0),
    M_DRAW: (4.5, 10.0),
    M_HOME: (2.0, 10.0),
    M_O45: (4.0, 20.0),
    M_0X2_ZEBRA: (5.0, 25.0),
    M_2X0_ZEBRA: (5.0, 25.0),
}
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
        # sinal so entra com KO no FUTURO: o re-escaneio dos ultimos dias nunca pode adicionar jogo ja jogado
        try:
            ko = datetime.strptime("%s %s" % (ds, str(r.get("Time", ""))[:5]), "%Y-%m-%d %H:%M")
            if ko < datetime.now():
                return
        except Exception:
            if ds < date.today().isoformat():
                return
        out.append(dict(Data=ds, Metodo=metodo, Liga=str(r.get("League", "")), Home=str(r.get("Home", "")),
                        Away=str(r.get("Away", "")), Hora=str(r.get("Time", ""))[:5],
                        Odd_Lay=round(odd, 2), Odd_Fav=round(fav, 2) if fav else ""))

    # 1 e 2: Top 3 menor odd, pelas funcoes oficiais
    for fn, nome in ((avaliar_jogos_lay_0x3_grade, M_0X3), (avaliar_jogos_lay_2x2_grade, M_2X2)):
        try:
            for s in fn(df, top_n=3):
                odd = _num(s.get("odd_lay"))
                if odd:
                    lo, hi = LIMITES_ODD[nome]
                    if lo <= odd <= hi:
                        add(nome, {"League": s.get("league"), "Home": s.get("home"), "Away": s.get("away"),
                                   "Time": s.get("hora")}, odd, 0.0)
        except Exception as e:
            print("  [%s erro] %s" % (nome, str(e)[:60]))

    # 1b. Lay 0x3 Regra Ampla (sem TOP 3) — Pré-registro em forward paralelo (Recomendação Claude / Antigravity)
    try:
        for s in avaliar_jogos_lay_0x3_grade(df, top_n=None):
            odd = _num(s.get("odd_lay"))
            if odd:
                lo, hi = LIMITES_ODD[M_0X3_AMPLA]
                if lo <= odd <= hi:
                    add(M_0X3_AMPLA, {"League": s.get("league"), "Home": s.get("home"), "Away": s.get("away"),
                                      "Time": s.get("hora")}, odd, 0.0)
    except Exception as e:
        print("  [%s erro] %s" % (M_0X3_AMPLA, str(e)[:60]))

    oh_b = pd.to_numeric(df.get("Odd_H_Back"), errors="coerce")
    oa_b = pd.to_numeric(df.get("Odd_A_Back"), errors="coerce")
    oh_l = pd.to_numeric(df.get("Odd_H_Lay"), errors="coerce")
    od_l = pd.to_numeric(df.get("Odd_D_Lay"), errors="coerce")
    u25_b = pd.to_numeric(df.get("Odd_Under25_FT_Back"), errors="coerce")
    o45_l = pd.to_numeric(df.get("Odd_Over45_FT_Lay"), errors="coerce")
    l02 = pd.to_numeric(df.get("Odd_CS_0x2_Lay"), errors="coerce")
    l20 = pd.to_numeric(df.get("Odd_CS_2x0_Lay"), errors="coerce")
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
        # 6. Lay 0x2 Zebra (Mandante Fav <= 1.45 | 5.0 <= Lay 0x2 <= 25.0)
        if pd.notna(h) and h <= 1.45 and pd.notna(l02.get(i)) and 5.0 <= l02[i] <= 25.0:
            add(M_0X2_ZEBRA, r, float(l02[i]), float(h))
        # 7. Lay 2x0 Zebra (Visitante Fav <= 1.45 | 5.0 <= Lay 2x0 <= 25.0)
        if pd.notna(a) and a <= 1.45 and pd.notna(l20.get(i)) and 5.0 <= l20[i] <= 25.0:
            add(M_2X0_ZEBRA, r, float(l20[i]), float(a))
    return out


# ------------------------------------------------------------------ 2. liquidacao
def red_do_metodo(m, gh, ga):
    if m in (M_0X3, M_0X3_AMPLA): return gh == 0 and ga == 3
    if m == M_2X2:  return gh == 2 and ga == 2
    if m == M_DRAW: return gh == ga
    if m == M_HOME: return gh > ga
    if m == M_O45:  return (gh + ga) >= 5
    if m == M_0X2_ZEBRA: return gh == 0 and ga == 2
    if m == M_2X0_ZEBRA: return gh == 2 and ga == 0
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


VPS = "ubuntu@163.176.59.215"
VPS_KEY = os.path.expanduser("~/Downloads/ssh-key-2026-07-31.key")
CACHE_FT = os.path.join(ROOT, "metodos_aprovados", ".cache_placares_ft.csv")


def _placares_oficiais():
    """placares_ft.csv da VPS (liquidar_betfair_oficial.py): o runner WINNER do Correct Score
    depois que a Betfair liquidou = placar OFICIAL. Nomes identicos aos do feed diario (mesma
    fonte) -> match exato. Fonte PRIMARIA a partir de 12/09/2026. Copia via scp; se falhar, usa a
    ultima copia local."""
    import subprocess
    try:
        subprocess.run(["scp", "-q", "-i", VPS_KEY, "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=20",
                        VPS + ":/home/ubuntu/betfair-collector/placares_ft.csv", CACHE_FT],
                       capture_output=True, timeout=90)
    except Exception as e:
        print("  [placares oficiais] scp falhou (%s) — usando cache local" % str(e)[:50])
    out = {}
    if not os.path.exists(CACHE_FT):
        return out
    try:
        d = pd.read_csv(CACHE_FT, dtype=str).fillna("")
        # feminino / reserva / base ficam fora: "Aston Villa (W)" casaria (0,95) com "Aston Villa" no fuzzy
        _jr = r"\(W\)|\(Res\)|\bU1\d\b|\bU2\d\b|Women|Reserves|Youth"
        d = d[~(d["home"].str.contains(_jr, regex=True) | d["away"].str.contains(_jr, regex=True))]
    except Exception:
        return out
    OFICIAL_CAT.clear()
    for _, r in d.iterrows():
        if r.get("status") != "LIQUIDADO":
            continue
        chaves = [(r["ko"][:10], canon(r["home"]), canon(r["away"]))]
        try:
            kodt = datetime.strptime(r["ko"], "%Y-%m-%d %H:%M")
            if kodt.hour < 6:           # KO em UTC antes das 06:00 = noite do dia anterior no feed (data local)
                chaves.append(((kodt - timedelta(days=1)).strftime("%Y-%m-%d"), canon(r["home"]), canon(r["away"])))
        except Exception:
            pass
        for k in chaves:
          if r.get("gh") and r.get("ga"):
            out.setdefault(k, (int(float(r["gh"])), int(float(r["ga"])), "betfair_oficial"))
          elif r.get("cs_winner"):
            # "Any Other Home/Away Win / Draw": sem placar exato, mas o vencedor do Match Odds e o CS
            # categorico bastam para Lay Draw, Lay Home e os lays de placar exato (que viram GREEN).
            OFICIAL_CAT.setdefault(k, dict(cs=r["cs_winner"], mo=r.get("mo_winner", ""), home=r["home"], away=r["away"],
                                           ou35=r.get("ou35", "")))
    print("  placares OFICIAIS (VPS): %d exatos + %d por categoria (Any Other)" % (len(out), len(OFICIAL_CAT)))
    return out


OFICIAL_CAT = {}


def liquidar_por_categoria(metodo, cat):
    """Resultado a partir de cs_winner categorico ('Any Other Home Win' etc.) + vencedor do Match Odds.
    Devolve True (RED), False (GREEN) ou None (nao decidivel sem placar exato)."""
    cs, mo = cat["cs"], cat["mo"]
    if metodo == M_DRAW: return (mo == "The Draw") if mo else (True if cs == "Any Other Draw" else (False if cs.startswith("Any Other") else None))
    if metodo == M_HOME: return (mo == cat["home"]) if mo else (True if cs == "Any Other Home Win" else (False if cs.startswith("Any Other") else None))
    if metodo in (M_0X3, M_0X3_AMPLA, M_2X2, M_0X2_ZEBRA, M_2X0_ZEBRA):
        return False if cs.startswith("Any Other") else None        # goleada != placar exato do lay
    if metodo == M_O45:
        return None                                               # 4-0 (GREEN) e 4-1 (RED) sao ambos "Any Other"
    return None


def placares():
    """Dict {(data, canon(home), canon(away)): (gh, ga, fonte)}. Ordem: OFICIAL da VPS (liquidacao
    da Betfair) -> base Betfair -> b365 (a copia mais recente). Nunca sobrescreve fonte anterior."""
    out = _placares_oficiais()
    bf = _base_betfair()
    if not bf.empty and "Goals_H_FT" in bf.columns:
        d = pd.to_datetime(bf["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
        gh = pd.to_numeric(bf["Goals_H_FT"], errors="coerce"); ga = pd.to_numeric(bf["Goals_A_FT"], errors="coerce")
        n0 = len(out)
        for dd, h, a, x, y in zip(d, bf["Home"], bf["Away"], gh, ga):
            k = (dd, canon(h), canon(a))
            if pd.notna(x) and pd.notna(y) and isinstance(dd, str) and k not in out:
                out[k] = (int(x), int(y), "betfair")
        print("  placares base betfair: +%d (ate %s)" % (len(out) - n0, d.max()))
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
    # planilhas do dia (pagina 01/02): coluna Placar "NxM" preenchida pelo Thiago; data = nome do arquivo.
    import glob as _glob, re as _re
    n0 = len(out); n_arq = 0
    for f in sorted(_glob.glob(os.path.join(ROOT, "metodos_aprovados", "Sinais_Metodos_Aprovados_20*.xlsx"))):
        if "Odds_Reais" in f: continue
        md = _re.search(r"(\d{4}-\d{2}-\d{2})", f)
        if not md: continue
        try:
            d = pd.read_excel(f); n_arq += 1
        except Exception:
            continue          # aberta no Excel ou corrompida: pula
        for _, x in d.iterrows():
            j = str(x.get("Jogo", ""))
            m = _re.match(r"^\s*(\d+)\s*[xX\-]\s*(\d+)\s*$", str(x.get("Placar", "")).strip())
            if " x " not in j or not m: continue
            h, a = [t.strip() for t in j.split(" x ", 1)]
            k = (md.group(1), canon(h), canon(a))
            if k not in out: out[k] = (int(m.group(1)), int(m.group(2)), "planilha_dia")
    print("  placares das planilhas do dia (%d arquivos): +%d" % (n_arq, len(out) - n0))
    # base mestre (pagina 02): coluna Placar "NxM" preenchida pelo Thiago — mesmo placar nao e digitado duas vezes.
    n0 = len(out)
    try:
        mm = pd.read_csv(os.path.join(ROOT, "metodos_aprovados", "Sinais_Metodos_Aprovados_Odds_Reais_Betfair.csv"),
                         dtype=str, encoding="utf-8-sig", keep_default_na=False)
        for _, x in mm.iterrows():
            m = _re.match(r"^\s*(\d+)\s*[xX\-]\s*(\d+)\s*$", str(x.get("Placar", "")).strip())
            dd = str(x.get("Data", ""))[:10]
            if not m or not _re.match(r"^\d{4}-\d{2}-\d{2}$", dd): continue
            k = (dd, canon(x.get("Home", "")), canon(x.get("Away", "")))
            if k not in out: out[k] = (int(m.group(1)), int(m.group(2)), "base_mestre")
        print("  placares da base mestre (pagina 02): +%d" % (len(out) - n0))
    except Exception as e:
        print("  [base mestre] %s" % str(e)[:60])
    # ultima fonte: planilha manual do Thiago (Data, Mandante, Visitante, Gols_M, Gols_V). So entra onde
    # oficial/Betfair/b365 nao tem; fonte_placar='manual' fica gravada para auditoria.
    for man in (os.path.join(ROOT, "placares_manuais.xlsx"), os.path.join(os.path.dirname(ROOT), "DASHBOARD_ARKAD-1", "placares_manuais.xlsx")):
        if not os.path.exists(man): continue
        try:
            mx = pd.read_excel(man); n0 = len(out)
            mx["_d"] = pd.to_datetime(mx["Data"], errors="coerce").dt.strftime("%Y-%m-%d")
            for _, x in mx.iterrows():
                gm, gv = pd.to_numeric(x.get("Gols_M"), errors="coerce"), pd.to_numeric(x.get("Gols_V"), errors="coerce")
                if pd.isna(x["_d"]) or pd.isna(gm) or pd.isna(gv): continue
                k = (x["_d"], canon(x.get("Mandante")), canon(x.get("Visitante")))
                if k not in out: out[k] = (int(gm), int(gv), "manual")
            print("  placares manuais (%s): +%d" % (os.path.basename(os.path.dirname(man)), len(out) - n0))
        except Exception as e:
            print("  [manual] %s" % str(e)[:60])
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


def _odds_ko_coletor():
    """Lê cs_pre.csv (se existir) para obter as odds de lay reais executadas nos 15 minutos pré-KO."""
    t_csv = os.path.join(os.environ.get("TEMP", "."), "var", "cs_pre.csv")
    if not os.path.exists(t_csv):
        return {}
    try:
        d = pd.read_csv(t_csv, header=None, names=["ts", "ko", "home", "away", "mtk", "runner", "back", "back_size", "lay", "lay_size"], low_memory=False)
        for c in ("mtk", "lay"): d[c] = pd.to_numeric(d[c], errors="coerce")
        d = d.dropna(subset=["mtk", "lay"])
        w = d[(d.mtk >= -5) & (d.mtk <= 15)].sort_values("mtk")
        w["dia"] = w.ko.str[:10]
        w["k"] = w.dia + "|" + w.home.map(canon) + "|" + w.away.map(canon) + "|" + w.runner
        return w.groupby("k")["lay"].first().to_dict()
    except Exception:
        return {}


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
    metodos_por_dia = {}
    for r in L.values():
        metodos_por_dia.setdefault(r["Data"], set()).add(r["Metodo"])
    # incremental: busca so os dias sem nenhum sinal no ledger + dias que faltam M_0X3_AMPLA + os 2 ultimos (feed pode completar)
    d0 = datetime.strptime(desde, "%Y-%m-%d").date(); d1 = datetime.strptime(ate, "%Y-%m-%d").date()
    alvo = [d0 + timedelta(i) for i in range((d1 - d0).days + 1)]
    alvo = [d for d in alvo if d.isoformat() not in metodos_por_dia or M_0X3_AMPLA not in metodos_por_dia.get(d.isoformat(), set()) or M_0X2_ZEBRA not in metodos_por_dia.get(d.isoformat(), set()) or M_2X0_ZEBRA not in metodos_por_dia.get(d.isoformat(), set()) or (d1 - d).days <= 1]
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

    # dedup: o feed lista o mesmo jogo em dois dias quando o KO cai perto da meia-noite UTC.
    # Mantem a PRIMEIRA data; a repetida (ainda nao liquidada) vira DUPLICADO, fora da conta.
    grupos = {}
    for k, r in L.items():
        grupos.setdefault((r["Metodo"], canon(r["Home"]), canon(r["Away"])), []).append(k)
    for kk, ks in grupos.items():
        if len(ks) < 2: continue
        ks = sorted(ks, key=lambda k: L[k]["Data"])
        for i in range(1, len(ks)):
            a, b = L[ks[i - 1]], L[ks[i]]
            if not (0 < (datetime.strptime(b["Data"], "%Y-%m-%d") - datetime.strptime(a["Data"], "%Y-%m-%d")).days <= 1): continue
            # par no mesmo jogo em dias consecutivos: fica o LIQUIDADO; se nenhum, fica o primeiro
            perde = a if (a.get("status") != "LIQUIDADO" and b.get("status") == "LIQUIDADO") else b
            if perde.get("status") == "LIQUIDADO": continue
            perde["status"] = "DUPLICADO"; perde["resultado"] = ""; perde["pnl_u"] = ""; perde["pnl_rs"] = ""
    pend = [r for r in L.values() if r.get("status") not in ("LIQUIDADO", "FORA_DA_FAIXA_KO", "DUPLICADO", "SEM_PLACAR", "ADIADO")]
    if pend:
        P = placares(); idx = _por_dia(P); agora = datetime.now().strftime("%Y-%m-%d %H:%M"); liq = 0
        ko_odds = _odds_ko_coletor()
        for r in pend:
            sc = achar_placar(P, idx, r["Data"], r["Home"], r["Away"])
            if sc is None:
                cat = OFICIAL_CAT.get((r["Data"], canon(r["Home"]), canon(r["Away"])))
                red_cat = liquidar_por_categoria(r["Metodo"], cat) if cat else None
                if red_cat is None:
                    continue
                odd = float(r["Odd_Lay"]); lo, hi = LIMITES_ODD.get(r["Metodo"], (1.0, 1000.0))
                if odd < lo or odd > hi:
                    r.update(placar=cat["cs"], resultado="FORA_DA_FAIXA", pnl_u=0.0, pnl_rs=0.0, status="FORA_DA_FAIXA_KO",
                             liquidado_em=agora, fonte_placar="betfair_oficial:cat"); continue
                pnl = -1.0 if red_cat else (1 - COMISSAO) / (odd - 1)
                r.update(gols_H="", gols_A="", placar=cat["cs"].replace("Any Other ", "AO "), resultado="RED" if red_cat else "GREEN",
                         pnl_u=round(pnl, 5), pnl_rs=round(pnl * LIAB_RS, 2), status="LIQUIDADO", liquidado_em=agora,
                         fonte_placar="betfair_oficial:cat")
                liq += 1
                continue
            gh, ga, fonte = sc; odd = float(r["Odd_Lay"])
            lo, hi = LIMITES_ODD.get(r["Metodo"], (1.0, 1000.0))

            # Item 4: Verificação de executabilidade no KO (Item 4 da Auditoria)
            # 1. Checa odd gravada no sinal
            if odd < lo or odd > hi:
                r.update(gols_H=gh, gols_A=ga, placar="%d-%d" % (gh, ga), resultado="FORA_DA_FAIXA",
                         pnl_u=0.0, pnl_rs=0.0, status="FORA_DA_FAIXA_KO",
                         liquidado_em=agora, fonte_placar=fonte)
                continue

            # 2. Se houver captura no coletor perto do KO, checa se a odd derivou para fora da faixa no KO
            runner = "2 - 2" if "2x2" in r["Metodo"] else ("0 - 3" if "0x3" in r["Metodo"] else ("0 - 2" if "0x2" in r["Metodo"] else ("2 - 0" if "2x0" in r["Metodo"] else None)))
            if runner:
                k_ko = str(r["Data"]) + "|" + canon(r["Home"]) + "|" + canon(r["Away"]) + "|" + runner
                if k_ko in ko_odds:
                    odd_ko = ko_odds[k_ko]
                    if odd_ko < lo or odd_ko > hi:
                        r.update(gols_H=gh, gols_A=ga, placar="%d-%d" % (gh, ga), resultado="FORA_DA_FAIXA",
                                 pnl_u=0.0, pnl_rs=0.0, status="FORA_DA_FAIXA_KO",
                                 liquidado_em=agora, fonte_placar=fonte + ":ko_fora(%.1f)" % odd_ko)
                        continue

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
        print("liquidados agora: %d | pendentes: %d | fora da faixa KO: %d | sem placar (>7d, fora da conta): %d"
              % (liq, sum(1 for r in L.values() if r["status"] == "PENDENTE"),
                 sum(1 for r in L.values() if r["status"] == "FORA_DA_FAIXA_KO"),
                 sum(1 for r in L.values() if r["status"] == "SEM_PLACAR")))
    gravar(L)
    return L


# ------------------------------------------------------------------ 3b. ledger do KO-10 (executavel)
KO_LEDGER = os.path.join(ROOT, "metodos_aprovados", "forward_ko_ledger.csv")
KO_COLS = ["Data", "Metodo", "Liga", "Home", "Away", "Hora", "Odd_Lay", "Odd_Fav", "liq_lay", "min_to_ko", "ts_captura", "origem",
           "status", "gols_H", "gols_A", "placar", "resultado", "pnl_u", "pnl_rs", "break_even", "liquidado_em", "fonte_placar", "universo"]
FEED_CACHE = os.path.join(ROOT, "metodos_aprovados", ".cache_feed_jogos.csv")


def _feed_jogos(ds):
    """jogos (canon home, canon away) do feed apicomunidade no dia ds — cache local por dia."""
    cache = {}
    if os.path.exists(FEED_CACHE):
        for r in csv.DictReader(open(FEED_CACHE, encoding="utf-8")):
            cache.setdefault(r["Data"], set()).add((r["h"], r["a"]))
    if ds in cache and (ds < date.today().isoformat() or len(cache[ds]) > 0):
        return cache[ds]
    try:
        import b365_data_utils as B
        g = B.fetch_betfair_daily(ds); df = pd.DataFrame(g) if isinstance(g, list) else g
        jogos = set((canon(h), canon(a)) for h, a in zip(df["Home"], df["Away"])) if df is not None and not df.empty else set()
    except Exception:
        return cache.get(ds, set())
    if ds < date.today().isoformat() or jogos:
        with open(FEED_CACHE, "a", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            if os.path.getsize(FEED_CACHE) == 0 if os.path.exists(FEED_CACHE) else True: w.writerow(["Data", "h", "a"])
            for h, a in jogos: w.writerow([ds, h, a])
    return jogos


def _no_feed(r, jogos):
    from difflib import SequenceMatcher
    h, a = canon(r["Home"]), canon(r["Away"])
    if (h, a) in jogos: return True
    for fh, fa in jogos:
        rh, ra = SequenceMatcher(None, h, fh).ratio(), SequenceMatcher(None, a, fa).ratio()
        if min(rh, ra) >= 0.60 and max(rh, ra) >= 0.80: return True
    return False


def _ko_carregar():
    L = {}
    if os.path.exists(KO_LEDGER):
        for r in csv.DictReader(open(KO_LEDGER, encoding="utf-8-sig")):
            L[chave(r["Data"], r["Metodo"], r["Home"], r["Away"])] = r
    return L


def _ko_gravar(L):
    rows = sorted(L.values(), key=lambda r: (r["Data"], r["Hora"], r["Metodo"], r["Home"]))
    with open(KO_LEDGER, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=KO_COLS, extrasaction="ignore"); w.writeheader(); w.writerows(rows)


def atualizar_ko():
    """(1) traz as linhas 'live' novas do servico sinais-ko da VPS; (2) liquida com o MESMO placar do ledger das 06:00.
    Sinal de KO ja passado sem placar em 7 dias -> SEM_PLACAR (fora da conta)."""
    import subprocess
    L = _ko_carregar(); n0 = len(L)
    tmp = os.path.join(ROOT, "metodos_aprovados", ".cache_forward_ko_vps.csv")
    try:
        subprocess.run(["scp", "-q", "-i", VPS_KEY, "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=20",
                        VPS + ":/home/ubuntu/betfair-collector/forward_ko_ledger.csv", tmp], capture_output=True, timeout=90)
    except Exception as e:
        print("  [ko] scp falhou (%s) - usa o ledger local" % str(e)[:50])
    if os.path.exists(tmp):
        for r in csv.DictReader(open(tmp, encoding="utf-8")):
            k = chave(r["Data"], r["Metodo"], r["Home"], r["Away"])
            if k not in L:
                L[k] = {c: r.get(c, "") for c in KO_COLS}
    print("  ledger KO: %d sinais (%d novos da VPS)" % (len(L), len(L) - n0))
    sem_univ = [r for r in L.values() if not r.get("universo")]
    if sem_univ:
        por_dia = {}
        for r in sem_univ: por_dia.setdefault(r["Data"], []).append(r)
        for ds, rs in sorted(por_dia.items()):
            jogos = _feed_jogos(ds)
            for r in rs: r["universo"] = ("feed" if _no_feed(r, jogos) else "fora") if jogos else ""
        print("  ledger KO: universo preenchido em %d linhas" % sum(1 for r in sem_univ if r.get("universo")))
    pend = [r for r in L.values() if r["status"] == "PENDENTE"]
    if pend:
        P = placares(); idx = _por_dia(P); agora = datetime.now().strftime("%Y-%m-%d %H:%M"); liq = 0
        for r in pend:
            try:
                ko = datetime.strptime(r["Data"] + " " + r["Hora"], "%Y-%m-%d %H:%M")
            except Exception:
                ko = datetime.now()
            if ko > datetime.now() - timedelta(hours=2):
                continue
            sc = achar_placar(P, idx, r["Data"], r["Home"], r["Away"])
            red = None; placar = ""; fonte = ""
            if sc is not None:
                gh, ga, fonte = sc; red = red_do_metodo(r["Metodo"], gh, ga); placar = "%d-%d" % (gh, ga)
                r.update(gols_H=gh, gols_A=ga)
            else:
                cat = OFICIAL_CAT.get((r["Data"], canon(r["Home"]), canon(r["Away"])))
                if cat:
                    red = liquidar_por_categoria(r["Metodo"], cat)
                    if red is not None: placar = cat["cs"].replace("Any Other ", "AO "); fonte = "betfair_oficial:cat"
            if red is None:
                if ko < datetime.now() - timedelta(days=7): r["status"] = "SEM_PLACAR"
                continue
            odd = float(r["Odd_Lay"]); pnl = -1.0 if red else (1 - COMISSAO) / (odd - 1)
            r.update(placar=placar, resultado="RED" if red else "GREEN", pnl_u=round(pnl, 5), pnl_rs=round(pnl * LIAB_RS, 2),
                     status="LIQUIDADO", liquidado_em=agora, fonte_placar=fonte); liq += 1
        print("  ledger KO: liquidados agora %d | pendentes %d" % (liq, sum(1 for r in L.values() if r["status"] == "PENDENTE")))
    _ko_gravar(L)
    return L


def _ko_indice(LK):
    idx = {}
    for r in LK.values():
        idx.setdefault((r["Data"], r["Metodo"]), []).append((canon(r["Home"]), canon(r["Away"])))
    return idx


def executavel_no_ko(r, kidx):
    """o sinal das 06:00 tinha odd aprovada no KO-10? (exato -> fuzzy entre nomes Betfair e nomes do feed)"""
    from difflib import SequenceMatcher
    if not kidx: return None
    cands = kidx.get((r["Data"], r["Metodo"]), [])
    h, a = canon(r["Home"]), canon(r["Away"])
    for ch, ca in cands:
        if (ch, ca) == (h, a): return True
        rh, ra = SequenceMatcher(None, h, ch).ratio(), SequenceMatcher(None, a, ca).ratio()
        if min(rh, ra) >= 0.60 and max(rh, ra) >= 0.80: return True
    return False


# ------------------------------------------------------------------ 3c. saida no gol da zebra (Lay Draw) — pareado
ZEBRA_LEDGER = os.path.join(ROOT, "metodos_aprovados", "saida_zebra_ledger.csv")


def atualizar_zebra(LK):
    """traz saida_zebra_ledger.csv da VPS, junta com o local e preenche pnl_hold pelo resultado FT do sinal no ledger KO."""
    import subprocess
    tmp = os.path.join(ROOT, "metodos_aprovados", ".cache_saida_zebra_vps.csv")
    try:
        subprocess.run(["scp", "-q", "-i", VPS_KEY, "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=20",
                        VPS + ":/home/ubuntu/betfair-collector/saida_zebra_ledger.csv", tmp], capture_output=True, timeout=90)
    except Exception:
        pass
    Z = {}
    cols = None
    for f in (ZEBRA_LEDGER, tmp):
        if os.path.exists(f):
            for r in csv.DictReader(open(f, encoding="utf-8-sig")):
                cols = cols or list(r.keys())
                Z.setdefault("%s|%s|%s|%s" % (r["Data"], canon(r["Home"]), canon(r["Away"]), r["evento"]), r)
    if not Z:
        return []
    ko = {chave(r["Data"], M_DRAW, r["Home"], r["Away"]): r for r in LK.values() if r["Metodo"] == M_DRAW} if LK else {}
    for r in Z.values():
        if r.get("status") == "SAIDA" and not r.get("pnl_hold"):
            k = ko.get(chave(r["Data"], M_DRAW, r["Home"], r["Away"]))
            if k and k.get("status") == "LIQUIDADO":
                r["pnl_hold"] = k["pnl_u"]; r["resultado_ft"] = k["resultado"]
    with open(ZEBRA_LEDGER, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore"); w.writeheader(); w.writerows(sorted(Z.values(), key=lambda r: (r["Data"], r["Hora"])))
    return list(Z.values())


def bloco_zebra(Z, hoje_s):
    if not Z: return []
    ev = [r for r in Z if r.get("evento") == "GOL_ZEBRA"]
    par = [r for r in ev if r.get("pnl_hold") not in ("", None)]
    out = ["", "<b>LAY DRAW · saída no gol da zebra (pré-registro 16/09)</b>", "<pre>"]
    hoje = [r for r in ev if r["Data"] == hoje_s]
    for r in sorted(hoje, key=lambda r: r["Hora"]):
        out.append("%s %-26s gol %3s' back@%-5s saída %+.3f%s" % (r["Hora"], _esc(("%s x %s" % (r["Home"], r["Away"]))[:26]), str(r.get("minuto_gol", ""))[:3],
                   r.get("odd_out", ""), float(r["pnl_saida"] or 0), ("  hold %+.3f" % float(r["pnl_hold"])) if r.get("pnl_hold") else ""))
    if par:
        ds = sum(float(r["pnl_saida"]) for r in par); dh = sum(float(r["pnl_hold"]) for r in par)
        out.append("-" * 44)
        out.append("pareado N=%d: saída %+.2fu | segurar %+.2fu | dif %+.2fu (%+.3f/evento)" % (len(par), ds, dh, ds - dh, (ds - dh) / len(par)))
        for lado in ("casa", "fora"):
            pl = [r for r in par if r.get("fav_lado") == lado]
            if pl: out.append("  fav %-4s N=%3d dif %+.2fu" % (lado, len(pl), sum(float(r["pnl_saida"]) - float(r["pnl_hold"]) for r in pl)))
    n_ind = sum(1 for r in Z if r.get("evento") == "LADO_INDETERMINADO"); n_fav = sum(1 for r in Z if r.get("evento") == "GOL_FAV")
    out.append("eventos: zebra %d · favorito 1º %d · lado indeterminado %d" % (len(ev), n_fav, n_ind))
    out += ["</pre>", "<i>julgamento em KO ≥ 17/09, N ≥ 100 eventos, IC95 da diferença saída−segurar.</i>"]
    return out


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


def _cfg():
    import json
    p = os.path.join(ROOT, "banca_config.json")
    try:
        return json.load(open(p, encoding="utf-8"))
    except Exception:
        return dict(banca_rs=2500.0, liability_rs=50.0, n_min_escalar=100, metodos_com_dinheiro=[])


def _boot_dia(v, dias, B=10000):
    mm = {k: v[dias == k] for k in pd.unique(dias)}; ks = list(mm)
    if len(ks) < 6: return np.nan, np.nan, len(ks)
    rng = np.random.default_rng(11); r = np.empty(B)
    for i in range(B): r[i] = np.concatenate([mm[ks[j]] for j in rng.integers(0, len(ks), len(ks))]).mean()
    return np.percentile(r, 2.5), np.percentile(r, 97.5), len(ks)


def _ledger_0x0():
    """Forward ao vivo do Lay 0x0 (DASHBOARD/forward_0x0/ledger_forward_0x0.csv), no mesmo formato."""
    p = os.path.join(os.path.dirname(ROOT), "DASHBOARD_ARKAD-1", "forward_0x0", "ledger_forward_0x0.csv")
    if not os.path.exists(p): return []
    d = pd.read_csv(p, encoding="utf-8-sig")
    d = d[(d["status"] == "LIQUIDADO") & (pd.to_numeric(d["na_regra"], errors="coerce") == 1)]
    return [dict(Data=r["Data"], Metodo="Lay 0x0 XGB", status="LIQUIDADO",
                 resultado="GREEN" if int(float(r["target"])) == 1 else "RED",
                 pnl_u=float(r["pnl_liab"]), break_even=float(r["break_even"])) for _, r in d.iterrows()]


def bloco_gestao(rows):
    """Gestao com dinheiro real: por metodo, P&L em R$ na liability configurada, IC95 (bloco-dia),
    drawdown e o gatilho de escala. Regra: ESCALAR so com piso do IC95 > 0 e N >= n_min; REDUZIR se
    o teto do IC95 < 0; MANTER no resto."""
    c = _cfg(); liab = float(c["liability_rs"]); banca = float(c["banca_rs"]); nmin = int(c["n_min_escalar"])
    todos = [r for r in na_conta(rows) if r["status"] == "LIQUIDADO"] + _ledger_0x0()
    out = ["", "<b>GESTÃO — banca R$%.0f · liability R$%.0f por aposta</b>" % (banca, liab), "<pre>"]
    out.append("%-6s %4s %6s %8s %-16s %s" % ("método", "N", "WR-BE", "R$ acum", "IC95 ROI", "gatilho"))
    for m in ORDEM + ["Lay 0x0 XGB"]:
        g = [r for r in todos if r["Metodo"] == m]
        if not g: continue
        v = np.array([float(r["pnl_u"]) for r in g]); dias = np.array([r["Data"] for r in g])
        wr = np.mean([r["resultado"] == "GREEN" for r in g]); be = np.mean([float(r["break_even"]) for r in g])
        lo, hi, nk = _boot_dia(v, dias)
        if np.isfinite(hi) and hi < 0: gat = "REDUZIR"
        elif np.isfinite(lo) and lo > 0 and len(g) >= nmin: gat = "ESCALAR"
        elif not np.isfinite(lo): gat = "manter (%dd)" % nk
        else: gat = "manter"
        ic = ("[%+.1f,%+.1f]" % (100 * lo, 100 * hi)) if np.isfinite(lo) else "IC indef."
        dinheiro = "$" if m in c.get("metodos_com_dinheiro", []) else " "
        out.append("%-5s%s %4d %+5.1fpp %+8.0f %-16s %s" % (CURTO.get(m, "0x0"), dinheiro, len(g), 100 * (wr - be), v.sum() * liab, ic, gat))
    # drawdown do portfolio (todos os metodos, por dia)
    df = pd.DataFrame(todos)
    if len(df):
        df["pnl_u"] = pd.to_numeric(df["pnl_u"], errors="coerce")   # ledger vem como texto
        s = df.groupby("Data")["pnl_u"].sum().sort_index().cumsum() * liab
        pico = s.cummax(); dd = (s - pico); mdd = dd.min(); dd_atual = dd.iloc[-1]
        pior_dia = (df.groupby("Data")["pnl_u"].sum() * liab).min()
        out.append("-" * 50)
        out.append("acumulado R$%+.0f | pico R$%+.0f | drawdown atual R$%.0f | máx R$%.0f" % (s.iloc[-1], pico.iloc[-1], dd_atual, mdd))
        out.append("pior dia R$%.0f | banca aguenta %.0f dias como o pior" % (pior_dia, banca / abs(pior_dia) if pior_dia < 0 else 999))
    out += ["</pre>", "<i>$ = com dinheiro real. ESCALAR só quando o piso do IC95 passar de zero com N≥%d.</i>" % nmin]
    return out


def _esc(t):
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def bloco_marcos(rows):
    """Prazos pre-registrados (marcos.json): contagem regressiva por data e progresso de N por metodo.
    Existe para que nada seja 'lido antes da hora' — o relatorio so mostra quando chega."""
    import json
    p = os.path.join(ROOT, "marcos.json")
    try:
        M = json.load(open(p, encoding="utf-8"))["marcos"]
    except Exception:
        return []
    hoje = date.today(); out = ["", "<b>PRAZOS PRÉ-REGISTRADOS</b>", "<pre>"]
    todos = [r for r in na_conta(rows) if r["status"] == "LIQUIDADO"] + _ledger_0x0()
    for m in M:
        if "data" in m:
            d = datetime.strptime(m["data"], "%Y-%m-%d").date(); falta = (d - hoje).days
            tag = "HOJE" if falta == 0 else ("faltam %dd" % falta if falta > 0 else "VENCIDO há %dd" % -falta)
            out.append("%-12s %-10s %s" % (tag, m["data"], _esc(m["nome"][:52])))
        elif "metodo" in m:
            n = sum(1 for r in todos if r["Metodo"] == m["metodo"])
            out.append("%-12s N=%4d/%-4d %s" % ("%3.0f%%" % (100.0 * n / m["n_alvo"]), n, m["n_alvo"], _esc(m["nome"][:52])))
    out.append("</pre>")
    return out


def montar_mensagem(L, hoje, rotulo="HOJE", LK=None, ZR=None):
    """Mensagem em HTML do Telegram (<b>, <i>, <pre>). `hoje` e o DIA DE REFERENCIA do relatorio
    (na rodada das 6h e o dia anterior, com todos os placares oficiais ja liquidados)."""
    rows = list(L.values())
    krows = list(LK.values()) if LK else []
    kidx = _ko_indice(LK) if LK else {}
    hoje_s = hoje.isoformat()
    linhas = ["<b>ARKAD — Forward 5 Métodos</b>",
              "<i>jogos de %s · liability 1u = R$%.0f · comissão 5%%</i>" % (hoje.strftime("%d/%m/%Y"), LIAB_RS), ""]

    def bloco(titulo, sub):
        out = ["<b>%s</b>" % titulo, "<pre>"]
        tem = False
        for m in ORDEM:
            ln = _linha(CURTO[m] + ("*" if m in FORA_DO_TOTAL else ""), [r for r in sub if r["Metodo"] == m], largura=10)
            if ln: out.append(ln); tem = True
        if not tem:
            out.append("sem sinais")
        else:
            out.append("-" * 38); out.append(_linha("TOTAL", na_conta(sub), largura=10) or "TOTAL      sem sinais na conta")
        out.append("</pre>")
        return out

    linhas += bloco("%s %s" % (rotulo, hoje.strftime("%d/%m")), [r for r in rows if r["Data"] == hoje_s])

    # ---- JOGOS: liquidados nas ultimas 24h (o placar oficial chega ~2-3h depois do apito, entao os
    #      jogos da noite aparecem no relatorio do dia seguinte) + pendentes de hoje
    rec = sorted([r for r in rows if r["status"] == "LIQUIDADO" and r["Data"] == hoje_s],
                 key=lambda r: (r["Hora"], r["Metodo"]))
    if rec:
        linhas += ["", "<b>RESULTADOS de %s (%d liquidados)</b>" % (hoje.strftime("%d/%m"), len(rec)), "<pre>"]
        for r in rec:
            fonte = str(r.get("fonte_placar", "")).split(":")[0]
            tag = "★" if fonte == "betfair_oficial" else "b"
            ex = executavel_no_ko(r, kidx)
            exs = "" if ex is None else (" ✔" if ex else " ✘KO")
            linhas.append("%s %s %-5s %-30s @%-5.2f %-4s %s %+6.2f%s" % (
                r["Hora"], tag, CURTO[r["Metodo"]],
                _esc(("%s x %s" % (r["Home"], r["Away"]))[:30]), float(r["Odd_Lay"]), r["placar"],
                "G" if r["resultado"] == "GREEN" else "R", float(r["pnl_u"]), exs))
        linhas += ["</pre>", "<i>★ = placar oficial Betfair (VPS) · b = base · ✔ = tinha odd aprovada no KO−10 · ✘KO = só às 06:00, não executável</i>"]
    pend_hoje = [r for r in rows if r["Data"] == hoje_s and r["status"] == "PENDENTE"]
    if pend_hoje:
        linhas += ["", "<b>AINDA SEM PLACAR em %s (%d) — entram quando liquidar</b>" % (hoje.strftime("%d/%m"), len(pend_hoje)), "<pre>"]
        for r in sorted(pend_hoje, key=lambda r: (r["Hora"], r["Metodo"])):
            linhas.append("%s %-5s %-30s @%.2f" % (r["Hora"], CURTO[r["Metodo"]], _esc(("%s x %s" % (r["Home"], r["Away"]))[:30]), float(r["Odd_Lay"])))
        linhas.append("</pre>")

    meses = sorted({r["Data"][:7] for r in rows}, reverse=True)
    nomes = {"08": "AGOSTO", "09": "SETEMBRO", "10": "OUTUBRO", "11": "NOVEMBRO", "12": "DEZEMBRO",
             "01": "JANEIRO", "02": "FEVEREIRO", "03": "MARÇO", "04": "ABRIL", "05": "MAIO", "06": "JUNHO", "07": "JULHO"}
    for mes in meses:
        sub = [r for r in rows if r["Data"][:7] == mes]
        linhas += [""] + bloco("%s — por método" % nomes.get(mes[5:], mes), sub)
        linhas += ["<b>%s — por dia</b>" % nomes.get(mes[5:], mes), "<pre>"]
        for d in sorted({r["Data"] for r in sub}):
            n, g, r_, pnl, pend = _agg(na_conta([r for r in sub if r["Data"] == d]))
            if n == 0 and pend == 0:
                continue
            tag = "%+6.2fu" % pnl if n else "   --  "
            linhas.append("%s  %2d  %2dG/%2dR  %s%s" % (d[8:10] + "/" + d[5:7], n + pend, g, r_, tag, " *" if pend else ""))
        linhas.append("</pre>")
    # ---- KO-10: o ledger EXECUTAVEL (odd de lay real do coletor 4-16 min antes do apito) ----
    kfeed = [r for r in krows if r.get("universo") == "feed"]
    kfora = [r for r in krows if r.get("universo") == "fora"]
    if krows:
        kh = [r for r in kfeed if r["Data"] == hoje_s]
        linhas += [""] + bloco("KO−10 (executável, universo do feed) — %s" % hoje.strftime("%d/%m"), kh)
        for mes in sorted({r["Data"][:7] for r in kfeed}, reverse=True)[:2]:
            linhas += bloco("KO−10 — %s por método" % nomes.get(mes[5:], mes), [r for r in kfeed if r["Data"][:7] == mes])
        n, g, r_, pnl, pend = _agg(na_conta(kfeed))
        ini = min(x["Data"] for x in krows)
        linhas += ["<b>KO−10 ACUMULADO desde %s/%s (feed):</b> %d liq · %dG/%dR · WR %.1f%% · <b>%+.2fu</b> (R$ %+.0f)%s"
                   % (ini[8:10], ini[5:7], n, g, r_, (100.0 * g / n) if n else 0, pnl, pnl * LIAB_RS, (" · %d pend" % pend) if pend else "")]
        if kfora:
            ln = _linha("fora", na_conta(kfora), largura=10)
            linhas += ["<pre>%s</pre>" % (ln or ""), "<i>fora = jogos que só a Betfair tem (ligas fora do feed, liquidez baixa): FORA da conta e da gestão.</i>"]
        linhas += ["<i>KO−10 = o que dava para apostar de verdade, 4–16 min antes do apito. A GESTÃO abaixo usa este ledger.</i>"]
    linhas += bloco_zebra(ZR, hoje_s)
    linhas += bloco_gestao(kfeed if kfeed else rows)
    linhas += bloco_marcos(rows)
    n, g, r_, pnl, pend = _agg(na_conta(rows))
    semp = sum(1 for r in rows if r["status"] == "SEM_PLACAR")
    if semp:
        linhas += ["", "<i>%d sinais de ligas sem placar em nenhuma base ficam fora da conta.</i>" % semp]
    ini = min(x["Data"] for x in rows) if rows else "----------"
    linhas += ["", "<b>ACUMULADO desde %s/%s (5 métodos):</b> %d liq · %dG/%dR · WR %.1f%% · <b>%+.2fu</b> (R$ %+.0f)%s"
               % (ini[8:10], ini[5:7], n, g, r_, (100.0 * g / n) if n else 0, pnl, pnl * LIAB_RS,
                  (" · %d pendentes (*)" % pend) if pend else ""),
               "<i>* 0x3-Ampla tem os mesmos jogos do 0x3 Top 3 e as zebras são observação: fora do TOTAL e do ACUMULADO.</i>"]
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
    ap.add_argument("--ontem", action="store_true", help="relatorio do dia anterior (rodada das 6h)")
    a = ap.parse_args()
    print("=== forward 5 metodos: %s -> %s ===" % (a.desde, a.ate))
    L = atualizar(a.desde, a.ate)
    try:
        LK = atualizar_ko()
    except Exception as e:
        print("  [ko] falhou: %s" % str(e)[:80]); LK = None
    try:
        ZR = atualizar_zebra(LK)
    except Exception as e:
        print("  [zebra] falhou: %s" % str(e)[:80]); ZR = None
    ref = datetime.strptime(a.ate, "%Y-%m-%d").date()
    if a.ontem:
        ref = ref - timedelta(days=1)
    msg = montar_mensagem(L, ref, "ONTEM" if a.ontem else "HOJE", LK, ZR)
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
