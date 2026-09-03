# -*- coding: utf-8 -*-
"""
forward_capturar.py — captura diaria OCULTA dos 3 metodos (odd lay real Betfair).
Roda 1x/dia (de manha, com os jogos do dia ja precificados). Idempotente: dedup por
(Data, Home, Away, Metodo). NAO liquida (isso e o forward_liquidar.py).

  python forward_capturar.py [YYYY-MM-DD]   # default = hoje

Regras congeladas: ver PRE_REGISTRO.md. NAO alterar limiares aqui sem re-pre-registrar.
"""
import os, sys, csv
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # ARKAD_PROD no path
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "forward_oculto_log.csv")
COLS = ["Data","Liga","Home","Away","ID_Evento","Metodo","Odd_Lay",
        "Odd_H_Back","Odd_A_Back","Odd_Under25_FT_Back","Odd_Over35_FT_Back",
        "passa_filtro","capturado_em","Green","PnL_liab","Placar","Status"]

def _f(row, col):
    try: return float(row.get(col))
    except (TypeError, ValueError): return None

def _in(x, lo, hi): return x is not None and lo <= x <= hi

def sinais_do_dia(df):
    """Aplica as 3 regras BASE + flag de filtro. Retorna lista de dicts (1 por sinal)."""
    out = []
    for _, r in df.iterrows():
        liga = r.get("League",""); h = r.get("Home",""); a = r.get("Away","")
        ev = r.get("ID_Evento","")
        oh, oa = _f(r,"Odd_H_Back"), _f(r,"Odd_A_Back")
        u25, o35 = _f(r,"Odd_Under25_FT_Back"), _f(r,"Odd_Over35_FT_Back")
        h_lay, d_lay = _f(r,"Odd_H_Lay"), _f(r,"Odd_D_Lay")
        a_lay = _f(r,"Odd_A_Lay")
        o45_lay = _f(r,"Odd_Over45_FT_Lay")
        base = dict(Data=str(r.get("Date",""))[:10], Liga=liga, Home=h, Away=a, ID_Evento=ev,
                    Odd_H_Back=oh, Odd_A_Back=oa, Odd_Under25_FT_Back=u25, Odd_Over35_FT_Back=o35)
        # LAY HOME
        if oa is not None and oa <= 1.65 and _in(h_lay, 2, 10):
            out.append({**base, "Metodo":"Lay_Home", "Odd_Lay":h_lay,
                        "passa_filtro": int(oa >= 1.54)})
        # LAY OVER 4.5
        if u25 is not None and u25 <= 1.50 and _in(o45_lay, 4, 20):
            out.append({**base, "Metodo":"Lay_Over45", "Odd_Lay":o45_lay,
                        "passa_filtro": 1})   # base = metodo
        # LAY AWAY (OBSERVACAO — scan amplo reprovou; vigiar se +3,8% da janela segura)
        if oh is not None and oh <= 1.45 and _in(a_lay, 2, 15):
            out.append({**base, "Metodo":"Lay_Away", "Odd_Lay":a_lay,
                        "passa_filtro": 1})   # sem sub-filtro; metodo em observacao
        # LAY DRAW
        fav = min([x for x in (oh, oa) if x is not None], default=None)
        if fav is not None and fav <= 1.40 and _in(d_lay, 4.5, 10):
            out.append({**base, "Metodo":"Lay_Draw", "Odd_Lay":d_lay,
                        "passa_filtro": int(o35 is not None and o35 >= 2.54)})
    return out

def carregar_existentes():
    seen = set()
    if os.path.exists(LOG):
        with open(LOG, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                seen.add((row["Data"], row["Home"], row["Away"], row["Metodo"]))
    return seen

def main():
    dia = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    from futpythontrader_client import get_daily_dataframe
    df = get_daily_dataframe("betfair", dia)
    if df.empty:
        print("sem jogos no feed para", dia); return
    sinais = sinais_do_dia(df)
    seen = carregar_existentes()
    novo = os.path.exists(LOG)
    n_add = 0
    with open(LOG, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        if not novo: w.writeheader()
        for s in sinais:
            key = (s["Data"], s["Home"], s["Away"], s["Metodo"])
            if key in seen: continue
            s.setdefault("passa_filtro", 0)
            s["capturado_em"] = datetime.now().isoformat(timespec="seconds")
            s["Green"] = s["PnL_liab"] = s["Placar"] = ""
            s["Status"] = "PENDENTE"
            w.writerow({c: s.get(c, "") for c in COLS})
            seen.add(key); n_add += 1
    por_met = {}
    for s in sinais: por_met[s["Metodo"]] = por_met.get(s["Metodo"], 0) + 1
    print("dia %s | jogos no feed=%d | sinais=%d %s | novos gravados=%d"
          % (dia, len(df), len(sinais), por_met, n_add))

if __name__ == "__main__":
    main()
