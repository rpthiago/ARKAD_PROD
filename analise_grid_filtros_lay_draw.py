import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("==================================================================", flush=True)
print("      ANÁLISE DE GRID DE FILTROS & VALIDAÇÃO OUT-OF-SAMPLE       ", flush=True)
print("==================================================================", flush=True)

df = pd.read_feather("df_eval_lay_draw.feather")
print(f"[+] Base carregada: {len(df)} jogos", flush=True)

# Divisão temporal
df["year"] = df["Date"].dt.year
df_is  = df[df["year"].between(2022, 2024)].copy()
df_oos_2025 = df[df["year"] == 2025].copy()
df_oos_2026 = df[df["year"] == 2026].copy()
df_oos_total = df[df["year"] >= 2025].copy()

print(f"In-Sample (2022-2024): {len(df_is)} jogos")
print(f"OOS 2025: {len(df_oos_2025)} jogos")
print(f"OOS 2026: {len(df_oos_2026)} jogos")
print(f"OOS Total (2025-2026): {len(df_oos_total)} jogos\n")

def test_config(df_data, odd_min, odd_max, prob_min, ev_min, fav_max=None, max_liga_draw=None, min_xgot=None, min_wr_diff=None):
    cond = (
        (df_data["Odd_D_FT"] >= odd_min) &
        (df_data["Odd_D_FT"] <= odd_max) &
        (df_data["prob_lay_win"] >= prob_min) &
        (df_data["ev_lay"] >= ev_min)
    )
    if fav_max is not None:
        fav_cond = (df_data["Odd_H_FT"] <= fav_max) | (df_data["Odd_A_FT"] <= fav_max)
        cond = cond & fav_cond
    if max_liga_draw is not None:
        cond = cond & (df_data["liga_draw_rate"] <= max_liga_draw)
    if min_xgot is not None:
        cond = cond & (df_data["total_xGOT"] >= min_xgot)
    if min_wr_diff is not None:
        cond = cond & (df_data["wr_diff"] >= min_wr_diff)
        
    sub = df_data[cond]
    n = len(sub)
    if n == 0:
        return {"n": 0, "wr": 0.0, "be_wr": 0.0, "delta_wr": 0.0, "roi": 0.0, "profit": 0.0, "profit_factor": 0.0, "max_dd": 0.0}
    
    greens = (sub["lay_win"] == 1).sum()
    reds = n - greens
    wr = greens / n
    avg_odd = sub["Odd_D_FT"].mean()
    be_wr = (avg_odd - 1.0) / (avg_odd - 0.05)
    delta_wr = (wr - be_wr) * 100.0
    
    total_profit = sub["pnl_lay"].sum()
    total_staked = n * 100.0
    roi = (total_profit / total_staked) * 100.0
    
    gross_win = greens * (100.0 * 0.95)
    gross_loss = ((sub[sub["lay_win"] == 0]["Odd_D_FT"] - 1.0) * 100.0).sum()
    profit_factor = (gross_win / gross_loss) if gross_loss > 0 else 999.0
    
    # Max drawdown
    cum_pnl = sub["pnl_lay"].cumsum()
    peak = cum_pnl.cummax()
    dd = cum_pnl - peak
    max_dd = dd.min()
    
    return {
        "n": n,
        "wr": wr * 100.0,
        "be_wr": be_wr * 100.0,
        "delta_wr": delta_wr,
        "roi": roi,
        "profit": total_profit,
        "profit_factor": profit_factor,
        "max_dd": max_dd,
        "avg_odd": avg_odd
    }

# Grid de configurações
configs = []

odd_ranges = [
    (2.80, 3.60, "Odd 2.80-3.60 (Baixa/Segura)"),
    (3.00, 4.20, "Odd 3.00-4.20 (Sweet Spot)"),
    (3.20, 4.50, "Odd 3.20-4.50 (Equilibrada)"),
    (3.00, 5.00, "Odd 3.00-5.00 (Ampla)")
]

prob_levels = [0.72, 0.75, 0.78, 0.80]
ev_levels = [0.00, 0.02, 0.05]
context_filters = [
    ("Sem Filtro Extra", None, None, None, None),
    ("Favorito <= 2.10", 2.10, None, None, None),
    ("Favorito <= 1.85", 1.85, None, None, None),
    ("Liga Anti-Empate (<=0.26)", None, 0.26, None, None),
    ("Poder Ofensivo (xGOT>=2.20)", None, None, 2.20, None),
    ("Desnível Técnico (WR Diff>=0.25)", None, None, None, 0.25),
    ("Sniper: Fav<=2.10 + Liga<=0.26", 2.10, 0.26, None, None),
    ("Sniper Plus: Fav<=2.10 + Liga<=0.26 + xGOT>=2.0", 2.10, 0.26, 2.00, None),
]

results = []

for o_min, o_max, o_lbl in odd_ranges:
    for p_min in prob_levels:
        for ev_m in ev_levels:
            for ctx_lbl, fav_m, liga_m, xgot_m, wr_diff_m in context_filters:
                r_is   = test_config(df_is, o_min, o_max, p_min, ev_m, fav_m, liga_m, xgot_m, wr_diff_m)
                r_2025 = test_config(df_oos_2025, o_min, o_max, p_min, ev_m, fav_m, liga_m, xgot_m, wr_diff_m)
                r_2026 = test_config(df_oos_2026, o_min, o_max, p_min, ev_m, fav_m, liga_m, xgot_m, wr_diff_m)
                r_oos  = test_config(df_oos_total, o_min, o_max, p_min, ev_m, fav_m, liga_m, xgot_m, wr_diff_m)
                
                # Exigir volume mínimo OOS
                if r_oos["n"] >= 100:
                    results.append({
                        "Odd_Faixa": o_lbl,
                        "Prob_Min": f"{p_min*100:.0f}%",
                        "EV_Min": f"{ev_m:+.2f}",
                        "Contexto": ctx_lbl,
                        "N_IS": r_is["n"],
                        "ROI_IS": r_is["roi"],
                        "N_2025": r_2025["n"],
                        "ROI_2025": r_2025["roi"],
                        "N_2026": r_2026["n"],
                        "ROI_2026": r_2026["roi"],
                        "N_OOS": r_oos["n"],
                        "WR_OOS": r_oos["wr"],
                        "BE_WR_OOS": r_oos["be_wr"],
                        "Delta_WR_OOS": r_oos["delta_wr"],
                        "ROI_OOS": r_oos["roi"],
                        "Profit_Factor_OOS": r_oos["profit_factor"],
                        "MaxDD_OOS": r_oos["max_dd"],
                        "Lucro_OOS_R$": r_oos["profit"]
                    })

df_res_grid = pd.DataFrame(results)
print(f"[+] Total de combinações testadas com volume estatístico: {len(df_res_grid)}")

# Ordenar pelas melhores performances Out-of-Sample (OOS Total)
df_res_grid = df_res_grid.sort_values("ROI_OOS", ascending=False).reset_index(drop=True)

print("\n==================================================================")
print("       TOP 15 MELHORES FILTROS QUANTITATIVOS (ORDENADOS POR ROI OOS)  ")
print("==================================================================")
cols_show = ["Odd_Faixa", "Prob_Min", "EV_Min", "Contexto", "N_OOS", "WR_OOS", "BE_WR_OOS", "Delta_WR_OOS", "ROI_2025", "ROI_2026", "ROI_OOS", "Profit_Factor_OOS", "MaxDD_OOS"]
print(df_res_grid[cols_show].head(15).to_string(index=False))

print("\n==================================================================")
print("       CONFIGURAÇÕES COM ROI POSITIVO EM AMBOS OS ANOS (2025 & 2026) ")
print("==================================================================")
robustas = df_res_grid[(df_res_grid["ROI_2025"] > 0) & (df_res_grid["ROI_2026"] > 0) & (df_res_grid["N_OOS"] >= 200)].copy()
print(f"Total de configurações consistentes em ambos os anos: {len(robustas)}")
if not robustas.empty:
    print(robustas[cols_show].head(15).to_string(index=False))
else:
    print("Nenhuma configuração manteve ROI > 0 em ambos os anos com N >= 200.")

df_res_grid.to_excel("estudo_filtros_lay_draw_completo.xlsx", index=False)
print("\n[+] Tabela completa exportada para estudo_filtros_lay_draw_completo.xlsx", flush=True)
