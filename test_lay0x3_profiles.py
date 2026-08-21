import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, requests
from futpythontrader_client import get_daily_dataframe

print("=== ANÁLISE DOS PERFIS DO LAY 0X3 EM AGOSTO DE 2026 ===", flush=True)

# 1. Carregar placares ESPN API para Agosto
db_scores = {}
for day in range(1, 21):
    date_str = f"202608{day:02d}"
    url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?dates={date_str}&limit=1000"
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
                    if status in ['STATUS_FULL_TIME', 'STATUS_FINAL', 'STATUS_APPROVED', 'FULL_TIME']:
                        gh = int(h_comp.get('score', 0))
                        ga = int(a_comp.get('score', 0))
                        dt_fmt = f"2026-08-{day:02d}"
                        hk = ''.join(c for c in h_name if c.isalnum())
                        ak = ''.join(c for c in a_name if c.isalnum())
                        db_scores[(dt_fmt, hk, ak)] = (gh, ga)
                        db_scores[(dt_fmt, hk[:5], ak[:5])] = (gh, ga)
                except Exception: pass
    except Exception: pass

all_days = []
for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    df = get_daily_dataframe("betfair", d_str)
    if df is not None and not df.empty:
        df["d_str"] = d_str
        all_days.append(df)

df_aug = pd.concat(all_days, ignore_index=True)

def resolve_score(r):
    gh, ga = None, None
    for gh_c in ["Goals_H_FT", "gols_mandante", "Home_Score"]:
        if gh_c in r and pd.notna(r[gh_c]):
            try: gh = int(float(r[gh_c])); break
            except: pass
    for ga_c in ["Goals_A_FT", "gols_visitante", "Away_Score"]:
        if ga_c in r and pd.notna(r[ga_c]):
            try: ga = int(float(r[ga_c])); break
            except: pass
            
    if gh is None or ga is None:
        dt = str(r["d_str"])
        home = ''.join(c for c in str(r.get("Home", r.get("Home_Team", ""))).lower() if c.isalnum())
        away = ''.join(c for c in str(r.get("Away", r.get("Away_Team", ""))).lower() if c.isalnum())
        if (dt, home, away) in db_scores:
            gh, ga = db_scores[(dt, home, away)]
        elif (dt, home[:5], away[:5]) in db_scores:
            gh, ga = db_scores[(dt, home[:5], away[:5])]
            
    return gh, ga

scores = [resolve_score(r) for idx, r in df_aug.iterrows()]
df_aug["gh"] = [s[0] for s in scores]
df_aug["ga"] = [s[1] for s in scores]

df_fin = df_aug[df_aug["gh"].notna() & df_aug["ga"].notna()].copy()

# Perfil 2: Odd_Under25 <= 2.10 AND 14.0 <= Odd_CS_0x3_Lay <= 35.0
p2_sinais = []
for idx, row in df_fin.iterrows():
    odd_h = float(row.get('Odd_H_Back') or row.get('Odd_H_FT_Back') or row.get('Odd_H_FT') or row.get('Odd_H') or 0.0)
    odd_u25 = float(row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25') or 0.0)
    odd_0x3 = float(row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3') or 0.0)
    
    if (0.0 < odd_u25 <= 2.10) and (14.0 <= odd_0x3 <= 35.0):
        gh = int(row["gh"])
        ga = int(row["ga"])
        res = "GREEN" if not (gh == 0 and ga == 3) else "RED"
        p2_sinais.append({
            "Data": row["d_str"],
            "Mandante": row.get("Home", row.get("Home_Team", "")),
            "Visitante": row.get("Away", row.get("Away_Team", "")),
            "Odd_H": odd_h,
            "Odd_U25": odd_u25,
            "Odd_Lay_0x3": odd_0x3,
            "Placar": f"{gh}x{ga}",
            "Resultado": res
        })

df_p2 = pd.DataFrame(p2_sinais)
print(f"\n[+] PERFIL LAY 0X3 (Odd U25 <= 2.10 & Odd Lay 0x3 em [14, 35]): {len(df_p2)} JOGOS APROVADOS EM AGOSTO", flush=True)
grn2 = (df_p2["Resultado"] == "GREEN").sum()
red2 = (df_p2["Resultado"] == "RED").sum()
print(f"    Greens: {grn2} | Reds: {red2} | Win Rate: {(grn2/len(df_p2)*100.0):.2f}%", flush=True)

print("\n=== PRIMEIRAS 25 ENTRADAS DO PERFIL APROVADO EM AGOSTO ===", flush=True)
print(df_p2.head(25).to_string(index=False), flush=True)
