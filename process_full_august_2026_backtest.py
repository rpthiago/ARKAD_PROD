import pandas as pd, numpy as np, requests, os
from futpythontrader_client import get_daily_dataframe

print("=== INICIANDO BACKTEST COMPLETO DE AGOSTO DE 2026 (01 A 20/08) VIA API ===", flush=True)

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
                except Exception:
                    pass
    except Exception:
        pass

print(f"[+] Base de placares ESPN API compilada com {len(db_scores)} jogos.", flush=True)

all_days = []
for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    try:
        df = get_daily_dataframe("betfair", d_str)
        if not df.empty:
            df["d_str"] = d_str
            all_days.append(df)
    except Exception:
        pass

df_aug = pd.concat(all_days, ignore_index=True)
print(f"[+] Total de partidas baixadas da API em Agosto (01-20/08): {len(df_aug)}", flush=True)

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

print(f"[+] Partidas de Agosto com placar final resolvido: {len(df_fin)} de {len(df_aug)}", flush=True)

all_trades = []

def calc(metodo, odd_cols, is_red, min_o, max_o, extra=None):
    c_odd = None
    for c in odd_cols:
        if c in df_fin.columns: c_odd = c; break
    if not c_odd: return
    odds = pd.to_numeric(df_fin[c_odd], errors="coerce")
    mask = (odds >= min_o) & (odds <= max_o) & odds.notna()
    if extra is not None: mask = mask & extra
    sub = df_fin[mask].copy()
    if sub.empty: return
    sub["odd"] = odds.loc[sub.index]
    sub["red"] = is_red.loc[sub.index]
    sub["pnl"] = np.where(~sub["red"], 95.0, -(sub["odd"] - 1.0) * 100.0)
    sub["res"] = np.where(~sub["red"], "GREEN", "RED")
    sub["met"] = metodo
    all_trades.append(sub[["met", "d_str", "res", "pnl"]])

ou25 = pd.to_numeric(df_fin.get("Odd_Under25_FT_Back", df_fin.get("Odd_Under25_FT", df_fin.get("Odd_Under25"))), errors="coerce")
oxg = pd.to_numeric(df_fin.get("total_xg", df_fin.get("Total_xG")), errors="coerce")
oh = pd.to_numeric(df_fin.get("Odd_H_FT_Back", df_fin.get("Odd_H_FT", df_fin.get("Odd_H"))), errors="coerce")
oa = pd.to_numeric(df_fin.get("Odd_A_FT_Back", df_fin.get("Odd_A_FT", df_fin.get("Odd_A"))), errors="coerce")

calc("Lay 0x0 RF v2", ["Odd_CS_0x0_Lay", "Odd_CS_0x0"], (df_fin["gh"]==0)&(df_fin["ga"]==0), 8.0, 16.0)
calc("Lay 0x1 RF v2", ["Odd_CS_0x1_Lay", "Odd_CS_0x1"], (df_fin["gh"]==0)&(df_fin["ga"]==1), 6.0, 12.0)
calc("Lay 1x0 RF v2", ["Odd_CS_1x0_Lay", "Odd_CS_1x0"], (df_fin["gh"]==1)&(df_fin["ga"]==0), 6.0, 12.0)
calc("Lay 2x0 RF v2", ["Odd_CS_2x0_Lay", "Odd_CS_2x0"], (df_fin["gh"]==2)&(df_fin["ga"]==0), 6.0, 12.0)
calc("Lay 0x2 RF v2", ["Odd_CS_0x2_Lay", "Odd_CS_0x2"], (df_fin["gh"]==0)&(df_fin["ga"]==2), 8.0, 16.0)
calc("Lay Draw v2", ["Odd_Lay_Draw", "Odd_D_FT"], df_fin["gh"]==df_fin["ga"], 2.80, 4.20)
calc("Lay Under 2.5 v2", ["Odd_Lay_Under25", "Odd_Under25_FT", "Odd_Under25"], (df_fin["gh"]+df_fin["ga"])<2.5, 1.70, 2.30)
c22 = (ou25<=2.0)|(oxg<=2.4)|(oh<=1.75)|(oa<=1.75)
calc("Lay 2x2 Quant", ["Odd_CS_2x2_Lay", "Odd_CS_2x2"], (df_fin["gh"]==2)&(df_fin["ga"]==2), 8.0, 14.0, c22)
calc("Lay 0x1 Agressivo", ["Odd_CS_0x1_Lay", "Odd_CS_0x1"], (df_fin["gh"]==0)&(df_fin["ga"]==1), 6.0, 12.0, oh<=1.85)

df_all = pd.concat(all_trades, ignore_index=True)

summary = []
for met in sorted(df_all["met"].unique()):
    sub = df_all[df_all["met"] == met]
    tot = len(sub)
    grn = (sub["res"] == "GREEN").sum()
    red = (sub["res"] == "RED").sum()
    wr = (grn / tot * 100.0) if tot > 0 else 0.0
    pnl = sub["pnl"].sum()
    summary.append({
        "Método": met,
        "Ops": tot,
        "Greens": grn,
        "Reds": red,
        "Win Rate": f"{wr:.2f}%",
        "P&L Agosto (R$)": f"R$ {pnl:,.2f}"
    })

print("\n" + "="*80, flush=True)
print("=== RESUMO BACKTEST COMPLETO DE AGOSTO DE 2026 (01 A 20/08) ===", flush=True)
print("="*80, flush=True)
print(pd.DataFrame(summary).to_string(index=False), flush=True)

pivot_aug = pd.pivot_table(df_all, values="pnl", index="d_str", columns="met", aggfunc="sum", fill_value=0.0)
print("\n=== PNL DIA A DIA AGOSTO 2026 ===", flush=True)
print(pivot_aug.to_string(), flush=True)
