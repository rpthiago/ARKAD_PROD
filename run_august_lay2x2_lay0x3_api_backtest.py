import os, sys, pandas as pd, numpy as np, requests, difflib
from futpythontrader_client import get_daily_dataframe

print("=== INICIANDO BACKTEST DE AGOSTO DE 2026 (01 A 20/08) PARA LAY 2X2 E LAY 0X3 ===", flush=True)

# 1. Carregar placares de ESPN API para todos os dias de Agosto (01 a 20/08/2026)
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

print(f"[+] Base de placares ESPN API compilada com {len(db_scores)} partidas finalizadas.", flush=True)

# 2. Baixar todos os jogos de Agosto (01 a 20/08) da API FutPythonTrader
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

# 3. Resolver placar final de cada partida
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

# Helper Odds
o_lay_2x2 = pd.to_numeric(df_fin.get("Odd_CS_2x2_Lay", df_fin.get("Odd_CS_2x2")), errors="coerce")
o_lay_0x3 = pd.to_numeric(df_fin.get("Odd_CS_0x3_Lay", df_fin.get("Odd_CS_0x3")), errors="coerce")

o_u25 = pd.to_numeric(df_fin.get("Odd_Under25_FT_Back", df_fin.get("Odd_Under25_FT", df_fin.get("Odd_Under25"))), errors="coerce")
o_xg = pd.to_numeric(df_fin.get("total_xg", df_fin.get("Total_xG")), errors="coerce")
xg_a = pd.to_numeric(df_fin.get("xG_A_FT", df_fin.get("xg_a")), errors="coerce")
o_h = pd.to_numeric(df_fin.get("Odd_H_FT_Back", df_fin.get("Odd_H_FT", df_fin.get("Odd_H"))), errors="coerce")
o_a = pd.to_numeric(df_fin.get("Odd_A_FT_Back", df_fin.get("Odd_A_FT", df_fin.get("Odd_A"))), errors="coerce")

# Regras Lay 2x2
cond_lay2x2 = (o_lay_2x2 >= 8.0) & (o_lay_2x2 <= 14.0) & o_lay_2x2.notna() & \
              ((o_u25 <= 2.00) | (o_xg <= 2.40) | (o_h <= 1.75) | (o_a <= 1.75))

# Regras Lay 0x3
cond_lay0x3 = (o_lay_0x3 >= 15.0) & (o_lay_0x3 <= 35.0) & o_lay_0x3.notna() & \
              ((o_u25 <= 2.00) | (o_h <= 2.20) | (xg_a <= 1.10) | (o_xg <= 2.50))

trades = []

# Lay 2x2 Trades
df_2x2 = df_fin[cond_lay2x2].copy()
if not df_2x2.empty:
    df_2x2["Metodo"] = "Lay 2x2 Quant"
    df_2x2["Odd_Exec"] = o_lay_2x2.loc[df_2x2.index]
    df_2x2["is_red"] = (df_2x2["gh"] == 2) & (df_2x2["ga"] == 2)
    df_2x2["Resultado"] = np.where(~df_2x2["is_red"], "GREEN", "RED")
    df_2x2["PnL_R$"] = np.where(~df_2x2["is_red"], 95.0, -(df_2x2["Odd_Exec"] - 1.0) * 100.0)
    trades.append(df_2x2[["Metodo", "d_str", "Home", "Away", "League", "Odd_Exec", "gh", "ga", "Resultado", "PnL_R$"]])

# Lay 0x3 Trades
df_0x3 = df_fin[cond_lay0x3].copy()
if not df_0x3.empty:
    df_0x3["Metodo"] = "Lay 0x3 Visitante"
    df_0x3["Odd_Exec"] = o_lay_0x3.loc[df_0x3.index]
    df_0x3["is_red"] = (df_0x3["gh"] == 0) & (df_0x3["ga"] == 3)
    df_0x3["Resultado"] = np.where(~df_0x3["is_red"], "GREEN", "RED")
    df_0x3["PnL_R$"] = np.where(~df_0x3["is_red"], 95.0, -(df_0x3["Odd_Exec"] - 1.0) * 100.0)
    trades.append(df_0x3[["Metodo", "d_str", "Home", "Away", "League", "Odd_Exec", "gh", "ga", "Resultado", "PnL_R$"]])

df_all = pd.concat(trades, ignore_index=True)

summary = []
for met in ["Lay 2x2 Quant", "Lay 0x3 Visitante"]:
    sub = df_all[df_all["Metodo"] == met]
    tot = len(sub)
    grn = (sub["Resultado"] == "GREEN").sum()
    red = (sub["Resultado"] == "RED").sum()
    wr = (grn / tot * 100.0) if tot > 0 else 0.0
    pnl = sub["PnL_R$"].sum()
    summary.append({
        "Método": met,
        "Total Entradas": tot,
        "Greens": grn,
        "Reds": red,
        "Win Rate": f"{wr:.2f}%",
        "P&L Acumulado Agosto (R$)": f"R$ {pnl:,.2f}"
    })

print("\n" + "="*80, flush=True)
print("=== RESUMO EXECUTIVO BACKTEST AGOSTO 2026 (API TEMPO REAL 01 A 20/08) ===", flush=True)
print("="*80, flush=True)
print(pd.DataFrame(summary).to_string(index=False), flush=True)

pivot_pnl = pd.pivot_table(df_all, values="PnL_R$", index="d_str", columns="Metodo", aggfunc="sum", fill_value=0.0)
pivot_cnt = pd.pivot_table(df_all, values="Resultado", index="d_str", columns="Metodo", aggfunc="count", fill_value=0)

print("\n=== PNL DIA A DIA AGOSTO 2026 (R$) ===", flush=True)
print(pivot_pnl.to_string(), flush=True)

print("\n=== ENTRADAS DIA A DIA AGOSTO 2026 ===", flush=True)
print(pivot_cnt.to_string(), flush=True)

# Salvar relatorio no artefato e no projeto
art_file = r"C:\Users\thiag\.gemini\antigravity\brain\95f807fc-aeff-419c-bec7-34d43b90cd11\backtest_lay2x2_lay0x3_agosto_2026.md"

def df_to_markdown(df_in, include_index=True):
    lines = []
    cols = ([df_in.index.name or "Data"] if include_index else []) + list(df_in.columns)
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
    for idx, r in df_in.iterrows():
        row_vals = ([str(idx)] if include_index else []) + [f"R$ {v:,.2f}" if isinstance(v, (int, float)) and "Rate" not in str(c) and "Entradas" not in str(c) and "Greens" not in str(c) and "Reds" not in str(c) else str(v) for c, v in zip(df_in.columns, r.values)]
        lines.append("| " + " | ".join(row_vals) + " |")
    return "\n".join(lines)

md = "# 📊 Backtest Oficial Agosto 2026 - Lay 2x2 Quant & Lay 0x3 Visitante\n\n"
md += "Compilação de todas as entradas validadas no mês de Agosto (01/08 a 20/08/2026) extraídas diretamente da **API FutPythonTrader Betfair** e resolvidas com placares oficiais.\n\n"
md += "> ⚠️ **Gestão de Referência:** Stake Fixa de **R$ 100,00** por operação (Comissão Betfair de 5%).\n\n"
md += "### 🏆 Resumo Geral de Agosto de 2026\n\n"
md += df_to_markdown(pd.DataFrame(summary), include_index=False) + "\n\n"
md += "### 💵 P&L Dia a Dia (R$)\n\n"
md += df_to_markdown(pivot_pnl, include_index=True) + "\n\n"
md += "### 🔢 Volume de Operações por Dia\n\n"
md += df_to_markdown(pivot_cnt, include_index=True) + "\n"

with open(art_file, "w", encoding="utf-8") as f:
    f.write(md)

with open("backtest_lay2x2_lay0x3_agosto_2026.md", "w", encoding="utf-8") as f:
    f.write(md)

df_all.to_excel("Backtest_Lay2x2_Lay0x3_Agosto_2026_Completo.xlsx", index=False)
print("\n[+] Relatórios e planilhas gravadas com sucesso!", flush=True)
