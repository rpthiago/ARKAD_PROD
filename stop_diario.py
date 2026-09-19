# -*- coding: utf-8 -*-
"""
stop_diario.py — STOP RED (−10% da banca no dia) e STOP GREEN (+10%), com alerta no Telegram.
FONTE (requisito do Thiago): a grade da pagina 01 = metodos_aprovados/Sinais_Metodos_Aprovados_YYYY-MM-DD.xlsx
(os jogos que ele executa). NAO usa o ledger do KO-10 nem o das 06:00.
Liquidacao: placar oficial da Betfair (placares_ft.csv da VPS, via scp) -> categoria Any Other -> coluna Placar da
propria planilha (se o Thiago preencheu). Sizing: 5% (Draw, Home, 0x0) / 10% (2x2, 0x3, Over 4.5) da banca do dia.
Banca do dia: banca_stop.json {"banca_fixa": null | valor}; se null, R$ 2.000 compostos com os jogos reais da pagina 02
(GREEN/RED, mesmo sizing) ate ontem. Estado (um alerta de cada por dia): stop_diario_estado.json.
Uso: python stop_diario.py [--once] [--sem-telegram] [--data YYYY-MM-DD]. Agendado a cada 5 min (ARKAD_Stop_Diario).
"""
import os, sys, re, json, csv, argparse, unicodedata, subprocess, warnings
from datetime import datetime, date, timedelta
from difflib import SequenceMatcher
warnings.filterwarnings("ignore")
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
import pandas as pd

PASTA = os.path.join(ROOT, "metodos_aprovados")
ESTADO = os.path.join(PASTA, "stop_diario_estado.json")
CFG = os.path.join(ROOT, "banca_stop.json")
CACHE_FT = os.path.join(PASTA, ".cache_placares_ft.csv")
VPS = "ubuntu@163.176.59.215"; VPS_KEY = os.path.expanduser("~/Downloads/ssh-key-2026-07-31.key")
PCT = {"Draw": 0.05, "Home": 0.05, "0x0": 0.05, "2x2": 0.10, "0x3": 0.10, "Over 4.5": 0.10}
STOP_RED_PCT, STOP_GREEN_PCT = 0.10, 0.10      # 19/09: trava de "2 reds" removida (teste do Gemini: deixava R$253 na mesa sem reduzir o DD)
COMISSAO = 0.05; DUR_MIN = 115           # jogo dura ~115 min do KO ao apito


def canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def metodo_curto(m):
    m = str(m)
    for k in ("0x3", "2x2", "Over 4.5", "0x0", "Draw", "Home"):
        if k in m: return k
    return None


def red_do_metodo(k, gh, ga):
    return {"0x3": gh == 0 and ga == 3, "2x2": gh == 2 and ga == 2, "Draw": gh == ga, "Home": gh > ga,
            "Over 4.5": gh + ga >= 5, "0x0": gh == 0 and ga == 0}[k]


def red_por_categoria(k, cs, mo, home):
    if k == "Draw": return (mo == "The Draw") if mo else (True if cs == "Any Other Draw" else (False if cs.startswith("Any Other") else None))
    if k == "Home": return (mo == home) if mo else (True if cs == "Any Other Home Win" else (False if cs.startswith("Any Other") else None))
    if k in ("0x3", "2x2", "0x0"): return False if cs.startswith("Any Other") else None
    return None


# ------------------------------------------------------------------ placares oficiais
def oficiais():
    try:
        subprocess.run(["scp", "-q", "-l", "8000", "-i", VPS_KEY, "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=20",
                        VPS + ":/home/ubuntu/betfair-collector/placares_ft.csv", CACHE_FT], capture_output=True, timeout=90)
    except Exception as e:
        print("  scp falhou (%s) — usa cache" % str(e)[:50])
    out = []
    if not os.path.exists(CACHE_FT): return out
    for r in csv.DictReader(open(CACHE_FT, encoding="utf-8")):
        if r.get("status") != "LIQUIDADO": continue
        if re.search(r"\(W\)|\(Res\)|\bU1\d\b|\bU2\d\b|Women|Reserves|Youth", r["home"] + " " + r["away"]): continue
        ko = datetime.strptime(r["ko"], "%Y-%m-%d %H:%M") - timedelta(hours=3)          # UTC -> Brasilia
        out.append(dict(dia=ko.strftime("%Y-%m-%d"), h=canon(r["home"]), a=canon(r["away"]), home=r["home"],
                        gh=int(float(r["gh"])) if r["gh"] else None, ga=int(float(r["ga"])) if r["ga"] else None,
                        cs=r.get("cs_winner", ""), mo=r.get("mo_winner", "")))
    return out


def casar(of, dia, home, away):
    h, a = canon(home), canon(away)
    cands = [o for o in of if o["dia"] == dia]
    for o in cands:
        if (o["h"], o["a"]) == (h, a): return o
    best, bs = None, 0
    for o in cands:
        rh, ra = SequenceMatcher(None, h, o["h"]).ratio(), SequenceMatcher(None, a, o["a"]).ratio()
        s = min(rh, ra)
        if s >= 0.60 and max(rh, ra) >= 0.80 and s > bs: best, bs = o, s
    return best


# ------------------------------------------------------------------ banca
def banca_do_dia(dia):
    cfg = json.load(open(CFG, encoding="utf-8")) if os.path.exists(CFG) else {}
    if cfg.get("banca_fixa"): return float(cfg["banca_fixa"]), "fixa"
    ini = float(cfg.get("banca_inicial", 2000.0))
    try:
        src = open(os.path.join(ROOT, "pages", "02_📊_Resultados_Metodos_Aprovados.py"), encoding="utf-8").read()
        cut = src.index("df_raw = carregar_dados_aprovados")
        ns = {"__file__": os.path.join(ROOT, "pages", "02.py")}
        import io, contextlib
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            exec(compile(src[:cut], "p02", "exec"), ns)
            df = ns["carregar_dados_aprovados"]("👑 Portfólio em Validação Forward (Lay 0x0 XGBoost, 0x3, 2x2, Tríade + Zebras)")
        d = df[df.Status.isin(["🟢 GREEN", "🔴 RED"]) & ~df["Método"].astype(str).str.contains("Paralelo|Zebra|Micro")].copy()
        d = d[d.Data.dt.strftime("%Y-%m-%d") < dia]
        d["k"] = d["Método"].map(metodo_curto); d = d.dropna(subset=["k"])
        d["odd"] = pd.to_numeric(d.Odd_Entrada, errors="coerce"); d = d.dropna(subset=["odd"])
        d["t"] = pd.to_datetime(d.Data.dt.strftime("%Y-%m-%d") + " " + d.Hora.astype(str).str[:5].replace("nan", "15:00"), errors="coerce")
        d = d.sort_values("t"); banca = ini
        for r in d.itertuples():
            liab = PCT[r.k] * banca
            banca += (-liab if r.Status == "🔴 RED" else liab * (1 - COMISSAO) / (r.odd - 1))
        return round(banca, 2), "composta (pagina 02, %d jogos ate %s)" % (len(d), dia)
    except Exception as e:
        print("  banca composta falhou (%s) — usa inicial" % str(e)[:60])
        return ini, "inicial"


# ------------------------------------------------------------------ dia
def avaliar_dia(dia, agora, of):
    f = os.path.join(PASTA, "Sinais_Metodos_Aprovados_%s.xlsx" % dia)
    if not os.path.exists(f):
        # a planilha do dia nao existe (ninguem gerou/baixou): escreve-a com as MESMAS regras da pagina 01
        # (relatorio_forward_5metodos.sinais_do_dia, odds do feed agora), no formato da planilha da pagina 01.
        # 19/09: o stop ficou o dia inteiro em "sem planilha" porque a grade so existia quando alguem a salvava.
        try:
            import relatorio_forward_5metodos as RF
            import io as _io, contextlib
            nomes = {RF.M_0X3: "Lay 0x3 Top 3 (Aprovado)", RF.M_2X2: "Lay 2x2 Top 3 (Aprovado)", RF.M_DRAW: "Lay Draw (Fav <= 1.40)",
                     RF.M_HOME: "Lay Home / DC X2 (Fav Visitante <= 1.65)", RF.M_O45: "Lay Over 4.5 FT (Under Pesado)"}
            # grade das 06:00 = as mesmas regras da pagina 01 no feed das 06:00 (o ledger guarda os sinais do dia
            # com Data==dia); sinais_do_dia() so aceita KO futuro, entao serve apenas se ainda for cedo.
            sig = [r for r in RF.carregar().values() if r["Data"] == dia]
            if not sig:
                with contextlib.redirect_stdout(_io.StringIO()):
                    sig = RF.sinais_do_dia(dia) or []
            rows = [dict(Data=dia, Hora=x["Hora"], Liga=x["Liga"], Jogo="%s x %s" % (x["Home"], x["Away"]), Home=x["Home"], Away=x["Away"],
                         **{"Método": nomes[x["Metodo"]]}, Mercado="", Lado="LAY", Odd_Entrada=x["Odd_Lay"], Odd_Fav=x["Odd_Fav"], Placar="", Resultado="PENDENTE", Status="⏳ PENDENTE")
                    for x in sig if x["Metodo"] in nomes]
            pd.DataFrame(rows).to_excel(f, index=False)
            print("  planilha do dia gerada agora (%s): %d sinais" % (datetime.now().strftime("%H:%M"), len(rows)))
        except Exception as e:
            print("  gerar planilha falhou: %s" % str(e)[:80])
    if not os.path.exists(f): return None
    g = pd.read_excel(f)
    col_m = [c for c in g.columns if "todo" in c.lower()][0]
    g = g.rename(columns={col_m: "Metodo"})
    g["k"] = g.Metodo.map(metodo_curto); g = g.dropna(subset=["k"])
    g["odd"] = pd.to_numeric(g.Odd_Entrada, errors="coerce"); g = g.dropna(subset=["odd"])
    g["ko"] = pd.to_datetime(dia + " " + g.Hora.astype(str).str[:5], errors="coerce"); g = g.dropna(subset=["ko"]).sort_values("ko")
    banca, origem = banca_do_dia(dia)
    linhas = []
    for r in g.itertuples():
        res = None; placar = ""
        pl = str(getattr(r, "Placar", "")).strip()
        o = casar(of, dia, r.Home, r.Away) if r.ko + timedelta(minutes=DUR_MIN) <= agora else None
        if o is not None:
            if o["gh"] is not None: res = red_do_metodo(r.k, o["gh"], o["ga"]); placar = "%d-%d" % (o["gh"], o["ga"])
            else:
                res = red_por_categoria(r.k, o["cs"], o["mo"], o["home"]); placar = o["cs"].replace("Any Other ", "AO ") if res is not None else ""
        if res is None and re.match(r"^\d+\s*[xX\-]\s*\d+$", pl):
            gh, ga = map(int, re.split(r"[xX\-]", pl)); res = red_do_metodo(r.k, gh, ga); placar = "%d-%d" % (gh, ga)
        liab = PCT[r.k] * banca
        pnl = None if res is None else (-liab if res else liab * (1 - COMISSAO) / (r.odd - 1))
        linhas.append(dict(ko=r.ko, hora=r.ko.strftime("%H:%M"), jogo="%s x %s" % (r.Home, r.Away), k=r.k, metodo=str(r.Metodo), odd=r.odd,
                           liab=liab, res=res, placar=placar, pnl=pnl, liq_em=(r.ko + timedelta(minutes=DUR_MIN)) if res is not None else None))
    liq = [l for l in linhas if l["res"] is not None]
    pnl_dia = sum(l["pnl"] for l in liq); greens = sum(1 for l in liq if not l["res"]); reds = sum(1 for l in liq if l["res"])
    return dict(dia=dia, banca=banca, origem=origem, linhas=linhas, liq=liq, pnl=pnl_dia, greens=greens, reds=reds, pct=pnl_dia / banca if banca else 0)


def _esc(t): return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def montar(tipo, D, agora):
    ult = max(l["liq_em"] for l in D["liq"])                               # momento da liquidacao que disparou
    restantes = [l for l in D["linhas"] if l["res"] is None and l["ko"] > ult]
    dt = datetime.strptime(D["dia"], "%Y-%m-%d").strftime("%d/%m/%Y")
    if tipo == "RED":
        m = ["🚨 <b>ARKAD — ALERTA: STOP RED DIÁRIO (−10% da Banca)</b>", "Data: %s | Horário: %s BRT" % (dt, agora.strftime("%H:%M")), "",
             "⚠️ O limite diário de perda foi atingido!",
             "• Saldo Realizado do Dia: <b>−R$ %.2f (%+.1f%%)</b>" % (abs(D["pnl"]), 100 * D["pct"]),
             "• Placar do Dia: %d Greens / %d Reds" % (D["greens"], D["reds"]),
             "• Banca Atual: R$ %.2f" % (D["banca"] + D["pnl"]), "", "🛑 <b>OPERAÇÃO ENCERRADA HOJE!</b>"]
        m.append("Os seguintes jogos restantes NÃO devem ser feitos:" if restantes else "Não há jogos restantes hoje.")
        m += ["❌ %s - %s (%s)" % (l["hora"], _esc(l["jogo"]), _esc(l["metodo"].split(" (")[0])) for l in restantes]
        m += ["", "Volte amanhã às 06:00 com a cabeça fresca."]
    else:
        m = ["🟢 <b>ARKAD — ALERTA: STOP GREEN (+10% / META BATIDA)</b>", "Data: %s | Horário: %s BRT" % (dt, agora.strftime("%H:%M")), "",
             "🏆 A meta de lucro diária foi atingida!",
             "• Saldo Realizado do Dia: <b>+R$ %.2f (%+.1f%%)</b>" % (D["pnl"], 100 * D["pct"]),
             "• Placar do Dia: %d Greens / %d Reds" % (D["greens"], D["reds"]),
             "• Banca Atual: R$ %.2f" % (D["banca"] + D["pnl"]), "", "💰 <b>LUCRO PROTEGIDO NO COFRE!</b>"]
        m.append("Para não devolver o lucro à noite, cancele as próximas entradas:" if restantes else "Não há mais entradas hoje.")
        m += ["🔒 %s - %s (%s)" % (l["hora"], _esc(l["jogo"]), _esc(l["metodo"].split(" (")[0])) for l in restantes]
        m += ["", "Parabéns pelo dia! Encerre as operações por hoje."]
    m.append("<i>banca do dia R$ %.0f (%s) · liability 5%% Draw/Home · 10%% 2x2/0x3/Over</i>" % (D["banca"], D["origem"]))
    return "\n".join(m)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--once", action="store_true"); ap.add_argument("--sem-telegram", action="store_true"); ap.add_argument("--data", default=None)
    a = ap.parse_args()
    agora = datetime.now(); dia = a.data or agora.strftime("%Y-%m-%d")
    est = json.load(open(ESTADO, encoding="utf-8")) if os.path.exists(ESTADO) else {}
    e = est.get(dia, {"RED": False, "GREEN": False})
    if e["RED"] and e["GREEN"]: print("%s: os dois alertas já disparados hoje" % dia); return
    of = oficiais()
    D = avaliar_dia(dia, agora, of)
    if D is None: print("sem planilha do dia %s" % dia); return
    print("%s %s | banca R$ %.0f (%s) | liquidados %d/%d | %dG/%dR | P&L R$ %+.2f (%+.1f%%)" % (dia, agora.strftime("%H:%M"), D["banca"], D["origem"], len(D["liq"]), len(D["linhas"]), D["greens"], D["reds"], D["pnl"], 100 * D["pct"]))
    for l in D["linhas"]:
        print("   %s %-9s %-34s @%-5.2f %s" % (l["hora"], l["k"], l["jogo"][:34], l["odd"], ("%s %s R$%+.0f" % (l["placar"], "RED" if l["res"] else "GREEN", l["pnl"])) if l["res"] is not None else "—"))
    disparar = None
    if not e["RED"] and D["liq"] and D["pct"] <= -STOP_RED_PCT: disparar = "RED"
    elif not e["GREEN"] and D["liq"] and D["pct"] >= STOP_GREEN_PCT: disparar = "GREEN"
    if disparar:
        msg = montar(disparar, D, agora); print("\n" + re.sub(r"</?(b|i)>", "", msg))
        if not a.sem_telegram:
            from telegram_notifier import enviar_mensagem_telegram
            ok, info = enviar_mensagem_telegram(msg, parse_mode="HTML"); print("telegram:", "OK" if ok else info)
            if ok: e[disparar] = True; est[dia] = e; json.dump(est, open(ESTADO, "w", encoding="utf-8"), indent=1)
    else:
        print("sem gatilho.")


if __name__ == "__main__":
    main()
