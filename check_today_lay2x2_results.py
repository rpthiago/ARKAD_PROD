import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, requests

from futpythontrader_client import get_daily_dataframe
from metodo_lay2x2_strategy import validar_entrada_lay2x2

print("=== VERIFICANDO RESULTADOS DO LAY 2X2 HOJE (20/08/2026) ===", flush=True)

url = "https://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?dates=20260820&limit=1000"
espn_scores = {}
try:
    r = requests.get(url, timeout=10)
    if r.status_code == 200:
        events = r.json().get('events', [])
        for ev in events:
            try:
                comp = ev.get('competitions', [])[0]
                competitors = comp.get('competitors', [])
                h_comp = [c for c in competitors if c.get('homeAway') == 'home'][0]
                a_comp = [c for c in competitors if c.get('homeAway') == 'away'][0]
                h_name = h_comp.get('team', {}).get('displayName', '').lower().strip()
                a_name = a_comp.get('team', {}).get('displayName', '').lower().strip()
                status = comp.get('status', {}).get('type', {}).get('name', '')
                gh = int(h_comp.get('score', 0))
                ga = int(a_comp.get('score', 0))
                hk = ''.join(c for c in h_name if c.isalnum())
                ak = ''.join(c for c in a_name if c.isalnum())
                espn_scores[(hk, ak)] = (gh, ga, status)
                espn_scores[(hk[:5], ak[:5])] = (gh, ga, status)
            except Exception: pass
except Exception: pass

df_today = get_daily_dataframe("betfair", "2026-08-20")

odd_2x2_col = [c for c in df_today.columns if '2x2' in str(c).lower() and 'lay' in str(c).lower()]
odd_u25_col = [c for c in df_today.columns if ('under25_ft_back' in str(c).lower() or 'under25_ft' in str(c).lower() or 'under 2.5 ft' in str(c).lower()) and 'ht' not in str(c).lower()]
if not odd_u25_col: odd_u25_col = [c for c in df_day.columns if 'under25' in str(c).lower() and 'ht' not in str(c).lower()]
odd_h_col = [c for c in df_today.columns if str(c).lower() in ['odd_h', 'odd_h_ft', 'odd_h_ft_back', 'odd_home', 'odd_1']]
odd_a_col = [c for c in df_today.columns if str(c).lower() in ['odd_a', 'odd_a_ft', 'odd_a_ft_back', 'odd_away', 'odd_2']]

rows = []
for _, r in df_today.iterrows():
    home = str(r.get("Home", r.get("Home_Team", "")))
    away = str(r.get("Away", r.get("Away_Team", "")))
    liga = str(r.get("League", r.get("Div", "")))
    
    o_2x2 = float(pd.to_numeric(r.get(odd_2x2_col[0]), errors='coerce')) if odd_2x2_col and pd.notna(r.get(odd_2x2_col[0])) else 0.0
    o_u25 = float(pd.to_numeric(r.get(odd_u25_col[0]), errors='coerce')) if odd_u25_col and pd.notna(r.get(odd_u25_col[0])) else None
    o_h = float(pd.to_numeric(r.get(odd_h_col[0]), errors='coerce')) if odd_h_col and pd.notna(r.get(odd_h_col[0])) else None
    o_a = float(pd.to_numeric(r.get(odd_a_col[0]), errors='coerce')) if odd_a_col and pd.notna(r.get(odd_a_col[0])) else None
    
    ok, reason = validar_entrada_lay2x2(odd_lay_2x2=o_2x2, odd_under25=o_u25, odd_h=o_h, odd_a=o_a)
    if ok:
        hk = ''.join(c for c in home.lower() if c.isalnum())
        ak = ''.join(c for c in away.lower() if c.isalnum())
        score_info = espn_scores.get((hk, ak)) or espn_scores.get((hk[:5], ak[:5]))
        
        if score_info:
            gh, ga, st_n = score_info
            placar = f"{gh}x{ga}"
            res = "RED (2x2)" if (gh == 2 and ga == 2) else "GREEN"
        else:
            placar = "Sem Placar API"
            res = "Desconhecido"
            
        rows.append({
            "Liga": liga, "Home": home, "Away": away, "Odd Lay 2x2": o_2x2,
            "Odd Under 2.5": o_u25, "Placar Final": placar, "Resultado": res
        })

df_res = pd.DataFrame(rows)
print(df_res.to_string(index=False), flush=True)
