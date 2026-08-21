import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, requests
from futpythontrader_client import get_daily_dataframe

print("=== INICIANDO BACKTEST DO SALDO MENOR SEM A REJEIÇÃO ODD_HANDICAP_PLUS3_INVALIDA (AGOSTO 01-20/2026) ===", flush=True)

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

print(f"[+] Base ESPN de placares finalizados: {len(db_scores)} partidas.", flush=True)

# 2. Baixar todos os dias da API Betfair
all_days = []
for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    try:
        df = get_daily_dataframe("betfair", d_str)
        if not df.empty:
            df["d_str"] = d_str
            all_days.append(df)
    except Exception: pass

df_aug = pd.concat(all_days, ignore_index=True)
print(f"[+] Total de jogos baixados da API (01 a 20/08): {len(df_aug)}", flush=True)

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

print(f"[+] Jogos com placar resolvido: {len(df_fin)} de {len(df_aug)}", flush=True)

# Avaliação do Saldo Menor SEM a trava EH+3
ops = []
for idx, r in df_fin.iterrows():
    odd_h = pd.to_numeric(r.get("Odd_H_FT_Back") or r.get("Odd_H_FT") or r.get("Odd_H"), errors="coerce")
    odd_a = pd.to_numeric(r.get("Odd_A_FT_Back") or r.get("Odd_A_FT") or r.get("Odd_A"), errors="coerce")
    odd_d = pd.to_numeric(r.get("Odd_D_FT_Back") or r.get("Odd_D_FT") or r.get("Odd_D"), errors="coerce")
    odd_u25 = pd.to_numeric(r.get("Odd_Under25_FT_Back") or r.get("Odd_Under25_FT") or r.get("Odd_Under25"), errors="coerce")
    
    if pd.isna(odd_h) or pd.isna(odd_a) or odd_h <= 1.0 or odd_a <= 1.0:
        continue
        
    is_home_zebra = odd_h > odd_a
    fav_odd = odd_a if is_home_zebra else odd_h
    zebra_odd = odd_h if is_home_zebra else odd_a
    
    # 1. Fav Odd entre 2.00 e 5.00
    if not (2.00 <= fav_odd <= 5.00):
        continue
        
    # 2. Draw Odd <= 3.42
    if pd.notna(odd_d) and odd_d > 3.42:
        continue
        
    # 3. Probabilidade da Zebra <= 45%
    if (1.0 / zebra_odd) > 0.45:
        continue
        
    # 4. Total xG / Expectativa de gols baixos (Odd Under 2.5 <= 2.00 ou xG est. <= 2.00)
    if pd.notna(odd_u25) and odd_u25 > 2.00:
        continue
        
    # Aposta no EH +3 da Zebra:
    gh = r["gh"]
    ga = r["ga"]
    
    if is_home_zebra:
        is_red = (ga - gh) >= 3
    else:
        is_red = (gh - ga) >= 3
        
    # Odd EH+3 estimada se missing
    eh_odd = round(min(1.45, max(1.08, 1.05 + (fav_odd - 2.00) * 0.10)), 2)
    
    resultado = "GREEN" if not is_red else "RED"
    pnl = round((eh_odd - 1.0) * 100.0 * 0.95, 2) if not is_red else -round((eh_odd - 1.0) * 100.0, 2)
    
    ops.append({
        "Data": r["d_str"],
        "Mandante": r.get("Home", r.get("Home_Team", "")),
        "Visitante": r.get("Away", r.get("Away_Team", "")),
        "Liga": r.get("League", r.get("Div", "")),
        "Fav_Odd": fav_odd,
        "Zebra_Odd": zebra_odd,
        "Odd_EH_Plus3": eh_odd,
        "Placar": f"{gh}x{ga}",
        "Resultado": resultado,
        "PnL_R$": pnl
    })

df_ops = pd.DataFrame(ops)
print("\n" + "="*80, flush=True)
print("=== RESULTADO DO BACKTEST SALDO MENOR SEM A REJEIÇÃO EH+3 (AGOSTO 01-20) ===", flush=True)
print("="*80, flush=True)

if df_ops.empty:
    print("Nenhuma operação encontrada com os filtros de odd e xG.", flush=True)
else:
    tot = len(df_ops)
    grn = (df_ops["Resultado"] == "GREEN").sum()
    red = (df_ops["Resultado"] == "RED").sum()
    wr = (grn / tot) * 100.0
    pnl_tot = df_ops["PnL_R$"].sum()
    
    print(f"Total de Operações Aprovadas: {tot}", flush=True)
    print(f"Greens: {grn} | Reds: {red}", flush=True)
    print(f"Win Rate: {wr:.2f}%", flush=True)
    print(f"P&L Acumulado (Stake Fixa R$ 100): R$ {pnl_tot:,.2f}", flush=True)
    
    print("\n=== DIA A DIA EM AGOSTO ===", flush=True)
    pvt = pd.pivot_table(df_ops, values="PnL_R$", index="Data", columns="Resultado", aggfunc="count", fill_value=0)
    print(pvt.to_string(), flush=True)
    
    print("\n=== DETALHE DAS OPERAÇÕES APROVADAS ===", flush=True)
    print(df_ops.to_string(), flush=True)
    
    df_ops.to_excel("Backtest_Saldo_Menor_Sem_EH_Check_Agosto.xlsx", index=False)
