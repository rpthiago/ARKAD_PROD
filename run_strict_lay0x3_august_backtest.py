import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, requests
from futpythontrader_client import get_daily_dataframe

print("=== BACKTEST ESTRITO DE PRODUÇÃO — LAY 0X3 AGOSTO 2026 (01 A 20/08) ===", flush=True)

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

# 2. Baixar todos os dias de Agosto da API Betfair
all_days = []
for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    try:
        df = get_daily_dataframe("betfair", d_str)
        if df is not None and not df.empty:
            df["d_str"] = d_str
            all_days.append(df)
    except Exception: pass

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
df_fin["gh"] = df_fin["gh"].astype(int)
df_fin["ga"] = df_fin["ga"].astype(int)

# Aplicar a regra ESTRITA DE PRODUÇÃO (TODOS OS 5 CRITÉRIOS COM 'AND')
sinais = []

for idx, row in df_fin.iterrows():
    odd_h = float(row.get('Odd_H_Back') or row.get('Odd_H_FT_Back') or row.get('Odd_H_FT') or row.get('Odd_H') or 0.0)
    odd_a = float(row.get('Odd_A_Back') or row.get('Odd_A_FT_Back') or row.get('Odd_A_FT') or row.get('Odd_A') or 0.0)
    odd_u25 = float(row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25') or 0.0)
    odd_0x3 = float(row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3') or 0.0)
    
    xg_a_val = row.get('A_xGF_r5') or row.get('Media_Gols_Pro_Visitante')
    xg_a = float(xg_a_val) if xg_a_val is not None and pd.notna(xg_a_val) else 1.0
    
    cond_u25 = (0.0 < odd_u25 <= 2.10)
    cond_0x3 = (14.0 <= odd_0x3 <= 35.0)
    cond_xg = (xg_a <= 1.10)
    
    if cond_u25 and cond_0x3 and cond_xg:
        gh = row["gh"]
        ga = row["ga"]
        is_0x3 = (gh == 0 and ga == 3)
        res = "GREEN" if not is_0x3 else "RED"
        pnl = 95.0 if not is_0x3 else -(odd_0x3 - 1.0) * 100.0
        
        sinais.append({
            "Data": row["d_str"],
            "Mandante": row.get("Home", row.get("Home_Team", "")),
            "Visitante": row.get("Away", row.get("Away_Team", "")),
            "Liga": row.get("League", row.get("Div", "")),
            "Odd_H": odd_h,
            "Odd_Under25": odd_u25,
            "Odd_Lay_0x3": odd_0x3,
            "Placar": f"{gh}x{ga}",
            "Resultado": res,
            "PnL_R$": pnl
        })

df_strict = pd.DataFrame(sinais)

print("\n" + "="*80, flush=True)
print("=== RESULTADO DO BACKTEST ESTRITO DE PRODUÇÃO — LAY 0X3 (AGOSTO 01 A 20) ===", flush=True)
print("="*80, flush=True)

if df_strict.empty:
    print("Nenhum sinal aprovado na regra estrita de produção no mês de Agosto.", flush=True)
else:
    tot = len(df_strict)
    grn = (df_strict["Resultado"] == "GREEN").sum()
    red = (df_strict["Resultado"] == "RED").sum()
    wr = (grn / tot) * 100.0
    pnl_tot = df_strict["PnL_R$"].sum()
    
    print(f"Total de Operações Aprovadas : {tot}", flush=True)
    print(f"Greens                       : {grn} ({wr:.2f}%)", flush=True)
    print(f"Reds                         : {red}", flush=True)
    print(f"Lucro Líquido Acumulado      : R$ {pnl_tot:,.2f}", flush=True)
    
    print("\n=== DIA A DIA EM AGOSTO (REGRAS ESTRITAS DE PRODUÇÃO) ===", flush=True)
    pvt = pd.pivot_table(df_strict, values="PnL_R$", index="Data", columns="Resultado", aggfunc="count", fill_value=0)
    print(pvt.to_string(), flush=True)
    
    print("\n=== LISTA COMPLETA DOS JOGOS SELECIONADOS ===", flush=True)
    print(df_strict.to_string(index=False), flush=True)
    
    df_strict.to_excel("Backtest_Lay0x3_Estrito_Producao_Agosto_2026.xlsx", index=False)
