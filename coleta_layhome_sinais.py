import os, sys, datetime
import pandas as pd, numpy as np
sys.stdout.reconfigure(encoding="utf-8")

ODD_MIN, ODD_MAX = 1.40, 2.50
PROB_MIN = 0.65

_HISTDF = {}
def _hist_df():
    if "df" not in _HISTDF:
        from b365_data_utils import load_b365_historical
        df = load_b365_historical().copy()
        dt = pd.to_datetime(df["Date"], errors="coerce")
        corte = dt.max() - pd.Timedelta(days=450)
        _HISTDF["df"] = df[dt >= corte].reset_index(drop=True)
    return _HISTDF["df"]

def sinais_do_dia(date_str):
    from b365_data_utils import fetch_betfair_daily
    bf = fetch_betfair_daily(date_str)
    if bf is None or bf.empty: return []
    payload = bf.to_dict("records")
    hist = _hist_df()
    picks = {}
    
    try:
        mod = __import__("lay_home_trader_strategy", fromlist=["predict_and_evaluate_live"])
        res = mod.predict_and_evaluate_live(payload, hist)
    except Exception as e:
        print(f"ERRO ao executar o modelo Lay Home Trader: {e}")
        return []
        
    for g in (res or []):
        odd = pd.to_numeric(g.get("Odd_H_FT") or g.get("Odd_H_Back") or np.nan, errors="coerce")
        prob = pd.to_numeric(g.get("Prob_ML") or np.nan, errors="coerce")
        decision = str(g.get("Decision", "")).strip().upper()
        
        # Filtros de Ouro (ignorando a decisao do modulo caso queiramos ser flexiveis)
        # Mas podemos exigir que a decisao nativa seja APOSTA
        if decision != "APOSTA":
            continue
            
        home, away = str(g.get("Home","")), str(g.get("Away",""))
        key = (home, away)
        if key not in picks:
            picks[key] = dict(Home=home, Away=away, Liga=g.get("League","") or g.get("Liga",""),
                              Horario=str(g.get("Time","") or ""), odd=odd, prob=prob)
            
    out = []
    for p in picks.values():
        out.append(dict(Date=date_str, Horario=p["Horario"], Liga=p["Liga"],
                        Mandante=p["Home"], Visitante=p["Away"], Metodo="Lay Home Trader",
                        Prob_ML=round(float(p["prob"])*100,1) if pd.notna(p["prob"]) else "",
                        Odd_H_Back=round(float(p["odd"]),2) if pd.notna(p["odd"]) else "",
                        Odd_H_Lay="~"+str(round(float(p["odd"])+0.10,2)) if pd.notna(p["odd"]) else ""))
    return out

if __name__ == "__main__":
    hoje = datetime.datetime.now().strftime("%Y-%m-%d")
    sinais = sinais_do_dia(hoje)
    for s in sinais:
        print(f"{s['Date']} {s['Horario']} | {s['Liga']} | {s['Mandante']} x {s['Visitante']} | Prob: {s['Prob_ML']}% | Odd Back: {s['Odd_H_Back']}")
