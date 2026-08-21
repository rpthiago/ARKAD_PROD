import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

df_raw = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_raw["Date"] = pd.to_datetime(df_raw["Date"], errors="coerce")
df_2026 = df_raw[df_raw["Date"].dt.year == 2026].copy()

print(f"=== ANÁLISE QUANTITATIVA DA REGRA 'MANDANTE MAIS FAVORITO QUE VISITANTE' (Odd_H < Odd_A) ===", flush=True)

# 1. Sem filtro de Odd_H < Odd_A (Qualquer jogo com U25 <= 2.10 e Lay 0x3 em [14, 35])
ops_todos = []
for idx, r in df_2026.iterrows():
    odd_h = float(r.get('Odd_H') or r.get('Odd_H_FT') or 0.0)
    odd_a = float(r.get('Odd_A') or r.get('Odd_A_FT') or 0.0)
    odd_u25 = float(r.get('Odd_Under25_FT') or r.get('Odd_Under25') or 0.0)
    odd_0x3 = float(r.get('Odd_CS_0x3') or 0.0)
    
    gh = r.get('Goals_H_FT')
    ga = r.get('Goals_A_FT')
    if pd.isna(gh) or pd.isna(ga): continue
    gh = int(gh); ga = int(ga)
    
    if 0.0 < odd_u25 <= 2.10 and 14.0 <= odd_0x3 <= 35.0:
        is_0x3 = (gh == 0 and ga == 3)
        res = "GREEN" if not is_0x3 else "RED"
        pnl = 95.0 if not is_0x3 else -(odd_0x3 - 1.0) * 100.0
        ops_todos.append({
            "odd_h": odd_h, "odd_a": odd_a, "odd_u25": odd_u25, "odd_0x3": odd_0x3,
            "h_fav": (odd_h < odd_a), "away_fav_167": (odd_a <= 1.80),
            "gh": gh, "ga": ga, "res": res, "pnl": pnl
        })

df_all = pd.DataFrame(ops_todos)
tot_all = len(df_all)
grn_all = (df_all["res"] == "GREEN").sum()
red_all = (df_all["res"] == "RED").sum()
pnl_all = df_all["pnl"].sum()

print(f"\n[TODOS OS JOGOS COM U25 <= 2.10 & LAY 0X3 em [14, 35]]:", flush=True)
print(f"  Total: {tot_all} | Greens: {grn_all} | Reds: {red_all} (WR: {(grn_all/tot_all*100):.2f}%) | P&L: R$ {pnl_all:,.2f}", flush=True)

# 2. Filtrando APENAS onde Odd_H < Odd_A (Mandante com odd menor que Visitante)
df_h_fav = df_all[df_all["h_fav"] == True]
tot_h = len(df_h_fav)
if tot_h > 0:
    grn_h = (df_h_fav["res"] == "GREEN").sum()
    red_h = (df_h_fav["res"] == "RED").sum()
    pnl_h = df_h_fav["pnl"].sum()
    print(f"\n[APENAS ONDE MANDANTE É MAIS FAVORITO QUE VISITANTE (Odd_H < Odd_A)]:", flush=True)
    print(f"  Total: {tot_h} | Greens: {grn_h} | Reds: {red_h} (WR: {(grn_h/tot_h*100):.2f}%) | P&L: R$ {pnl_h:,.2f}", flush=True)

# 3. Filtrando quando o VISITANTE É SUPER FAVORITO (Odd_A <= 1.80, como Motherwell x Freiburg @ 1.67)
df_away_super = df_all[df_all["away_fav_167"] == True]
tot_as = len(df_away_super)
if tot_as > 0:
    grn_as = (df_away_super["res"] == "GREEN").sum()
    red_as = (df_away_super["res"] == "RED").sum()
    pnl_as = df_away_super["pnl"].sum()
    print(f"\n[QUANDO VISITANTE É FAVORITO (Odd_A <= 1.80)]:", flush=True)
    print(f"  Total: {tot_as} | Greens: {grn_as} | Reds: {red_as} (WR: {(grn_as/tot_as*100):.2f}%) | P&L: R$ {pnl_as:,.2f}", flush=True)
    print("\nExemplo de jogos com Visitante Favorito (Odd A <= 1.80):")
    print(df_away_super[["odd_h", "odd_a", "odd_u25", "odd_0x3", "gh", "ga", "res", "pnl"]].head(15).to_string(index=False), flush=True)
