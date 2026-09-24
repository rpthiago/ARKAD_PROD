# -*- coding: utf-8 -*-
"""
gerar_picks_dia.py — PICKS DO DIA do Lay 0x0 XGBoost (regra congelada).
Featuriza os jogos de hoje (build_features as-of usando o historico), treina XGBoost em
tudo < hoje, prediz hoje, aplica a regra congelada + EV na odd LAY REAL da Betfair, e
lista: jogo | odd lay | p | ev | stake sugerida (0.25 Kelly) | link do mercado.
STAKE-ZERO por padrao: mostra sugestao, NAO aposta. Voce lanca o Lay 0-0 na mao.

  python gerar_picks_dia.py [YYYY-MM-DD]
"""
import sys, os, warnings, unicodedata, re, difflib
warnings.filterwarnings("ignore")
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd, numpy as np
from datetime import datetime
from sklearn.preprocessing import StandardScaler
import b365_data_utils as B, treinar_lay_0x0_rf_v2 as T0x0
from futpythontrader_client import get_daily_dataframe
COMM = 0.05
import urllib.request, urllib.parse
def canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

def _tgcfg():
    tok = chat = None
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tg.env")
    if os.path.exists(p):
        for ln in open(p, encoding="utf-8"):
            ln = ln.strip()
            if ln.startswith("TG_TOKEN="): tok = ln.split("=", 1)[1].strip()
            elif ln.startswith("TG_CHAT="): chat = ln.split("=", 1)[1].strip()
    return tok, chat

def tg(msg):
    tok, chat = _tgcfg()
    if not (tok and chat): return
    u = "https://api.telegram.org/bot%s/sendMessage" % tok
    for i in range(0, len(msg), 4000):
        try:
            data = urllib.parse.urlencode({"chat_id": chat, "text": msg[i:i+4000],
                    "parse_mode": "HTML", "disable_web_page_preview": "true"}).encode()
            urllib.request.urlopen(u, data=data, timeout=10)
        except Exception as e:
            print("[tg erro]", str(e)[:80])

import re as _re
_datas = [a for a in sys.argv[1:] if _re.fullmatch(r"\d{4}-\d{2}-\d{2}", a)]
dia = _datas[0] if _datas else datetime.now().strftime("%Y-%m-%d")
print("Carregando base + jogos do dia (%s)..." % dia)
hist = B.load_b365_historical()
hist["Date"] = pd.to_datetime(hist["Date"], errors="coerce")
today = get_daily_dataframe("bet365", dia)
bf_daily = get_daily_dataframe("betfair", dia)
if (today is None or len(today) < 10) and (bf_daily is not None and not bf_daily.empty):
    # Mapeia colunas _Back do feed Betfair para o schema da base historica (Odd_*_FT / Odd_CS_0x0)
    bf_mapped = bf_daily.copy()
    col_map_bf = {
        "Odd_H_Back": "Odd_H_FT", "Odd_D_Back": "Odd_D_FT", "Odd_A_Back": "Odd_A_FT",
        "Odd_Over05_FT_Back": "Odd_Over05_FT", "Odd_Over15_FT_Back": "Odd_Over15_FT",
        "Odd_Over25_FT_Back": "Odd_Over25_FT", "Odd_Over35_FT_Back": "Odd_Over35_FT",
        "Odd_Under05_FT_Back": "Odd_Under05_FT", "Odd_Under15_FT_Back": "Odd_Under15_FT",
        "Odd_Under25_FT_Back": "Odd_Under25_FT", "Odd_Under35_FT_Back": "Odd_Under35_FT",
        "Odd_BTTS_Yes_Back": "Odd_BTTS_Yes", "Odd_BTTS_No_Back": "Odd_BTTS_No",
        # NAO mapear Odd_CS_0x0_Back -> Odd_CS_0x0: a feature mkt_prob_0x0 = 1/Odd_CS_0x0 foi TREINADA
        # na odd de back do b365. A odd de back do 0x0 na Betfair e 1,18-1,27x a do b365 (medido em
        # 79 jogos, 18-20/09/2026), o que baixa mkt_prob e faz o filtro congelado mkt<0.10 aprovar
        # 84,8% dos jogos em vez de 69,6% (+15,2pp) — seria afrouxar a regra pre-registrada.
        # O fallback da Betfair serve so para a GRADE de jogos do dia; sem odd de 0x0 do b365 o jogo
        # nao gera sinal (mkt_prob fica NaN e o dropna dos features o retira), como sempre foi.
    }
    for c_src, c_dst in col_map_bf.items():
        if c_src in bf_mapped.columns and c_dst not in bf_mapped.columns:
            bf_mapped[c_dst] = pd.to_numeric(bf_mapped[c_src], errors="coerce")
    if today is None or today.empty:
        today = bf_mapped
    else:
        today = pd.concat([today, bf_mapped], ignore_index=True).drop_duplicates(subset=["Home", "Away"], keep="first")
if today is None or today.empty:
    print("sem jogos no feed bet365/betfair para", dia); sys.exit()
today = today.copy(); today["Date"] = pd.to_datetime(dia)
_n0x0 = pd.to_numeric(today.get("Odd_CS_0x0"), errors="coerce").notna().sum() if "Odd_CS_0x0" in today.columns else 0
print("  grade do dia: %d jogos | com odd de 0x0 do b365 (necessaria p/ mkt_prob): %d | sem: %d"
      % (len(today), _n0x0, len(today) - _n0x0))
comb = pd.concat([hist, today], ignore_index=True, sort=False)
df, feats = T0x0.build_features(comb, "Odd_CS_0x0")
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
feats = list(feats)
tr = df[(df["Date"] < dia)].dropna(subset=feats + ["target"])
live = df[df["Date"] == dia].dropna(subset=feats).copy()
print("treino N=%d | jogos hoje featurizados=%d" % (len(tr), len(live)))
if len(tr) < 400 or live.empty:
    print("dados insuficientes"); sys.exit()
sc = StandardScaler(); Xtr = sc.fit_transform(tr[feats].fillna(0)); Xl = sc.transform(live[feats].fillna(0))
m = T0x0._build_model(); m.fit(Xtr, tr["target"].values)
live["p"] = m.predict_proba(Xl)[:, 1]

# odd LAY REAL + id do evento (link) do feed Betfair
bf = get_daily_dataframe("betfair", dia)
laymap = {}; evmap = {}; twinmap = {}; komap = {}
def _n(r, c):
    v = pd.to_numeric(r.get(c), errors="coerce"); return float(v) if pd.notna(v) and v > 1 else None
if bf is not None and not bf.empty:
    for _, r in bf.iterrows():
        k = (canon(r.get("Home","")), canon(r.get("Away","")))
        v = pd.to_numeric(r.get("Odd_CS_0x0_Lay"), errors="coerce")
        if pd.notna(v) and v > 1: laymap[k] = float(v)
        evid = r.get("ID_Evento","")
        if evid: evmap[k] = str(evid).split(".")[0]
        twinmap[k] = (_n(r,"Odd_Over05_FT_Back"), _n(r,"Odd_Under05_FT_Lay"))
        komap[k] = str(r.get("Time","") or "")

# Os nomes do feed b365 e do feed Betfair divergem ("DC United" x "DC Utd", "Jeonbuk" x "Jeonbuk Motors"):
# metade dos jogos do dia nao casava por igualdade e perdia a odd de lay (medido em 18-22/09/2026: em
# 19/09 casavam 49 por nome exato e outros 47 so por semelhanca). Mesmo criterio do relatorio das 06:00:
# cada lado >= 0.60 de semelhanca e a media dos dois >= 0.80.
_LAYKEYS = list(laymap)
def achar_chave(k):
    """chave do feed Betfair para este jogo: exata primeiro, depois a mais parecida. -> (chave, tipo) ou (None, None)"""
    if k in laymap: return k, "exato"
    best, bs = None, 0.0
    for kb in _LAYKEYS:
        sh = difflib.SequenceMatcher(None, k[0], kb[0]).ratio()
        if sh < 0.60: continue
        sa = difflib.SequenceMatcher(None, k[1], kb[1]).ratio()
        if sa < 0.60: continue
        sc = (sh + sa) / 2
        if sc > bs: bs, best = sc, kb
    return (best, "fuzzy%.2f" % bs) if (best and bs >= 0.80) else (None, None)

AGORA = datetime.now()
RETRO = "--retro" in sys.argv          # so para backfill/diagnostico: aceita jogo com KO no passado
rows = []
funil = {"jogos": 0, "sem_odd_lay": 0, "sem_liga_ou_mkt": 0, "ko_passado": 0, "ko_desconhecido": 0,
         "casou_exato": 0, "casou_fuzzy": 0,
         "reprova_liga": 0, "reprova_mkt": 0, "reprova_faixa_odd": 0, "reprova_ev": 0, "passou": 0}
for _, r in live.iterrows():
    funil["jogos"] += 1
    k0 = (canon(r["Home"]), canon(r["Away"]))
    k, tipo = achar_chave(k0)
    lay = laymap.get(k) if k else None
    if lay is None:
        funil["sem_odd_lay"] += 1
        continue
    funil["casou_exato" if tipo == "exato" else "casou_fuzzy"] += 1
    # PRE-KO obrigatorio (Lei 9: o gerador so cria PENDENTE antes da bola rolar). Sem isto, rodar o
    # botao do Streamlit a noite geraria "picks" de jogos ja encerrados e o liquidador os trataria
    # como pre-registrados.
    _hhmm = str(komap.get(k, ""))[:5]
    if _hhmm and ":" in _hhmm:
        try: _ko = datetime.strptime("%s %s" % (dia, _hhmm), "%Y-%m-%d %H:%M")
        except Exception: _ko = None
    else:
        _ko = None
    if _ko is None:
        funil["ko_desconhecido"] += 1
    elif _ko <= AGORA and not RETRO:
        funil["ko_passado"] += 1
        continue
    p = float(r["p"]); ev = p*(1-COMM) - (1-p)*(lay-1)
    liga_rate = float(r.get("liga_0x0_rate")) if pd.notna(r.get("liga_0x0_rate")) else None
    mkt = float(r.get("mkt_prob_0x0")) if pd.notna(r.get("mkt_prob_0x0")) else None
    # REGRA CONGELADA
    if liga_rate is None or mkt is None:
        funil["sem_liga_ou_mkt"] += 1
        continue
    if not (liga_rate < 0.08): funil["reprova_liga"] += 1
    elif not (mkt < 0.10): funil["reprova_mkt"] += 1
    elif not (10 <= lay <= 20): funil["reprova_faixa_odd"] += 1
    elif not (ev > 0.02): funil["reprova_ev"] += 1
    if not (liga_rate < 0.08 and mkt < 0.10 and 10 <= lay <= 20 and ev > 0.02): continue
    funil["passou"] += 1
    # 0.25 Kelly (fracao da banca em LIABILITY)
    kelly = p - (1-p)*(lay-1)/(1-COMM)
    stake_frac = max(0.0, 0.25*kelly)
    link = "https://www.betfair.bet.br/exchange/plus/football/event/%s" % evmap.get(k,"")
    o05b, u05l = twinmap.get(k, (None, None))
    rows.append(dict(jogo="%s x %s"%(r["Home"],r["Away"]), liga=r.get("League",""),
                     odd_lay=round(lay,2), p=round(p,3), ev=round(ev,3),
                     liga_0x0=round(liga_rate,3), mkt_prob=round(mkt,3),
                     stake_pct_banca=round(stake_frac*100,2), link=link,
                     home=r["Home"], away=r["Away"], ko=komap.get(k,""), casou=tipo,
                     over05_back=o05b, under05_lay=u05l))
out = pd.DataFrame(rows).sort_values("ev", ascending=False) if rows else pd.DataFrame()
# --- LOG DE CLV: grava a ENTRADA (gemeos CS_0x0 lay / Over0.5 back) p/ medir vs fechamento ---
import csv as _csv
CLVLOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "clv_0x0_log.csv")
CLVCOLS = ["Data","Home","Away","ko","e_cs0x0_lay","e_over05_back","e_under05_lay",
           "c_cs0x0_lay","c_over05_back","c_under05_lay","last_ts","status",
           "clv_lay0x0","clv_backover05","entry_ts"]
if not out.empty:
    seen = set()
    if os.path.exists(CLVLOG):
        for rr in _csv.DictReader(open(CLVLOG, encoding="utf-8-sig")):
            seen.add((rr["Data"], rr["Home"], rr["Away"]))
    novo = not os.path.exists(CLVLOG)
    with open(CLVLOG, "a", newline="", encoding="utf-8-sig") as fh:
        w = _csv.DictWriter(fh, fieldnames=CLVCOLS)
        if novo: w.writeheader()
        for _, r in out.iterrows():
            if (dia, r["home"], r["away"]) in seen: continue
            w.writerow({"Data":dia,"Home":r["home"],"Away":r["away"],"ko":r["ko"],
                        "e_cs0x0_lay":r["odd_lay"],"e_over05_back":r.get("over05_back",""),
                        "e_under05_lay":r.get("under05_lay",""),"c_cs0x0_lay":"","c_over05_back":"",
                        "c_under05_lay":"","last_ts":"","status":"OPEN","clv_lay0x0":"",
                        "clv_backover05":"","entry_ts":datetime.now().isoformat(timespec="seconds")})
picks_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "picks_0x0_%s.csv" % dia)
print("\n" + "="*70)
print("PICKS LAY 0x0 XGBoost (regra congelada) — %s — OBSERVACAO/STAKE-ZERO" % dia)
print("="*70)
if out.empty:
    print("Nenhum jogo bate a regra congelada hoje.")
    print("  funil: %d jogos do dia | sem odd de lay: %d | sem liga/mkt: %d" %
          (funil["jogos"], funil["sem_odd_lay"], funil["sem_liga_ou_mkt"]))
    print("  casamento com o feed Betfair: %d exatos + %d por semelhanca | KO ja passado: %d | KO desconhecido: %d" %
          (funil["casou_exato"], funil["casou_fuzzy"], funil["ko_passado"], funil["ko_desconhecido"]))
    print("  reprovados por  liga>=0.08: %d | mkt>=0.10: %d | odd fora de 10-20: %d | ev<=0.02: %d" %
          (funil["reprova_liga"], funil["reprova_mkt"],
           funil["reprova_faixa_odd"], funil["reprova_ev"]))
    # HEARTBEAT: sem isto, silencio no Telegram e ambiguo — nao da p/ saber se o robo morreu.
    tg("\n".join([
        "🎯 <b>PICKS LAY 0-0 — %s</b>" % dia,
        "<i>observação / stake-zero</i>",
        "➖➖➖➖➖",
        "Nenhum jogo bate a regra congelada hoje.",
        "",
        "<b>Funil:</b> %d jogos do dia · %d sem odd de lay · %d sem liga/mkt · %d com KO ja passado" %
        (funil["jogos"], funil["sem_odd_lay"], funil["sem_liga_ou_mkt"], funil["ko_passado"]),
        "Reprovados: liga %d · mkt %d · odd fora 10-20 %d · EV %d" %
        (funil["reprova_liga"], funil["reprova_mkt"],
         funil["reprova_faixa_odd"], funil["reprova_ev"]),
        "",
        "<i>Esta mensagem confirma que o robô rodou. Se um dia ela não chegar às 5:30, aí sim algo quebrou.</i>",
    ]))
else:
    out.to_csv(picks_path, index=False)
    for _, r in out.iterrows():
        print("• %-40s [%s]" % (r["jogo"][:40], r["liga"]))
        print("   Lay 0-0 @ %.2f | p=%.3f | EV=%+.3f | liga=%.3f mkt=%.3f | 0.25Kelly=%.2f%% banca"
              % (r["odd_lay"], r["p"], r["ev"], r["liga_0x0"], r["mkt_prob"], r["stake_pct_banca"]))
        print("   %s" % r["link"])
    print("\n%d pick(s). Salvo em %s" % (len(out), os.path.basename(picks_path)))
    # ---- Telegram (so quando ha pick) ----
    L = ["🎯 <b>PICKS LAY 0-0 (XGBoost) — %s</b>" % dia,
         "<i>observação / stake-zero (forward N&lt;300)</i>", "➖➖➖➖➖"]
    for _, r in out.iterrows():
        L.append("⚽ <b>%s</b>" % r["jogo"])
        L.append("   Lay 0-0 @ <b>%.2f</b> · EV %+.3f · 0.25K %.2f%% banca" % (r["odd_lay"], r["ev"], r["stake_pct_banca"]))
        L.append("   👉 %s" % r["link"])
    L.append("➖➖➖")
    L.append("<i>Lance na mão: Correct Score → '0 - 0' → Lay. Odds mudam — confira antes.</i>")
    tg("\n".join(L))
print("\nLANCE NA MAO: mercado Correct Score -> selecao '0 - 0' -> aba Lay (rosa) -> odd 10-20.")
print("STAKE-ZERO ate o forward bater N>=300. Sugestao de stake e informativa.")
