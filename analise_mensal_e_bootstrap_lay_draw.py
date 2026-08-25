import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("==================================================================", flush=True)
print("     ANÁLISE MENSAL, BOOTSTRAP (IC95%) & TESTE DE ESTRESSE       ", flush=True)
print("==================================================================", flush=True)

df = pd.read_feather("df_eval_lay_draw.feather")
df_oos = df[df["Date"].dt.year >= 2025].copy()

# Configuração Campeã 1: Sniper Conservador (Odd 2.80-3.60, Prob >= 80%, xGOT >= 2.20, EV >= 0.02)
c1 = (
    (df_oos["Odd_D_FT"] >= 2.80) &
    (df_oos["Odd_D_FT"] <= 3.60) &
    (df_oos["prob_lay_win"] >= 0.80) &
    (df_oos["total_xGOT"] >= 2.20) &
    (df_oos["ev_lay"] >= 0.02)
)
sub1 = df_oos[c1].copy()

# Configuração Campeã 2: Sniper Equilibrado (Odd 3.00-4.50, Prob >= 80%, xGOT >= 2.20, EV >= 0.02)
c2 = (
    (df_oos["Odd_D_FT"] >= 3.00) &
    (df_oos["Odd_D_FT"] <= 4.50) &
    (df_oos["prob_lay_win"] >= 0.80) &
    (df_oos["total_xGOT"] >= 2.20) &
    (df_oos["ev_lay"] >= 0.02)
)
sub2 = df_oos[c2].copy()

def report_monthly_and_bootstrap(sub, name):
    print(f"\n==================================================")
    print(f"  ESTATÍSTICAS DETALHADAS: {name}")
    print(f"==================================================")
    n = len(sub)
    greens = (sub["lay_win"] == 1).sum()
    reds = n - greens
    wr = greens / n * 100.0
    avg_odd = sub["Odd_D_FT"].mean()
    be_wr = (avg_odd - 1.0) / (avg_odd - 0.05) * 100.0
    profit = sub["pnl_lay"].sum()
    roi = (profit / (n * 100.0)) * 100.0
    
    print(f"Total de Apostas OOS (2025-2026): {n}")
    print(f"Greens: {greens} | Reds: {reds}")
    print(f"Win Rate: {wr:.2f}% vs Break-even: {be_wr:.2f}% (Margem: {wr - be_wr:+.2f}%)")
    print(f"Odd Média: {avg_odd:.2f}")
    print(f"Lucro Líquido (Stake R$ 100): R$ {profit:,.2f}")
    print(f"ROI Líquido: {roi:+.2f}%")
    
    # Detalhamento Mensal
    sub["month_year"] = sub["Date"].dt.to_period("M")
    monthly = sub.groupby("month_year").agg(
        jogos=("lay_win", "count"),
        greens=("lay_win", "sum"),
        odd_media=("Odd_D_FT", "mean"),
        lucro=("pnl_lay", "sum")
    ).reset_index()
    monthly["wr"] = (monthly["greens"] / monthly["jogos"]) * 100.0
    monthly["roi"] = (monthly["lucro"] / (monthly["jogos"] * 100.0)) * 100.0
    monthly["be_wr"] = (monthly["odd_media"] - 1.0) / (monthly["odd_media"] - 0.05) * 100.0
    
    print("\n--- PERFORMANCE MÊS A MÊS ---")
    print(monthly[["month_year", "jogos", "greens", "wr", "be_wr", "lucro", "roi"]].to_string(index=False))
    
    meses_pos = (monthly["lucro"] > 0).sum()
    meses_tot = len(monthly)
    print(f"\nMeses Lucrativos: {meses_pos}/{meses_tot} ({meses_pos/meses_tot*100:.1f}%)")
    
    # Bootstrap 10.000 iterações (IC 95%)
    np.random.seed(42)
    boot_rois = []
    pnl_array = sub["pnl_lay"].to_numpy()
    for _ in range(10000):
        sample = np.random.choice(pnl_array, size=len(pnl_array), replace=True)
        boot_rois.append((sample.sum() / (len(sample) * 100.0)) * 100.0)
        
    ic_low = np.percentile(boot_rois, 2.5)
    ic_high = np.percentile(boot_rois, 97.5)
    p_val_zero = (np.array(boot_rois) <= 0).mean()
    
    print(f"\n--- BOOTSTRAP (10.000 iterações) ---")
    print(f"Intervalo de Confiança 95% do ROI: [{ic_low:+.2f}%, {ic_high:+.2f}%]")
    print(f"P-value (Probabilidade de ROI <= 0%): {p_val_zero:.5f}")

report_monthly_and_bootstrap(sub1, "SNIPER CONSERVADOR (ODD 2.80 - 3.60 | xGOT >= 2.20 | PROB >= 80%)")
report_monthly_and_bootstrap(sub2, "SNIPER EQUILIBRADO (ODD 3.00 - 4.50 | xGOT >= 2.20 | PROB >= 80%)")
