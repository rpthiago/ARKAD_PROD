import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("==================================================================", flush=True)
print("     BACKTEST DETALHADO 2026 - LAY DRAW SNIPER (xGOT >= 2.20)     ", flush=True)
print("==================================================================", flush=True)

df = pd.read_feather("df_eval_lay_draw.feather")
df_2026 = df[df["Date"].dt.year == 2026].copy()

print(f"[+] Total de jogos no ano de 2026 avaliados com features ricas: {len(df_2026)}", flush=True)

# Aplicar a configuração exata
cond_2026 = (
    (df_2026["Odd_D_FT"] >= 3.00) &
    (df_2026["Odd_D_FT"] <= 4.50) &
    (df_2026["prob_lay_win"] >= 0.80) &
    (df_2026["total_xGOT"] >= 2.20) &
    (df_2026["ev_lay"] >= 0.02)
)

sub_2026 = df_2026[cond_2026].copy()
sub_2026 = sub_2026.sort_values("Date", kind="mergesort").reset_index(drop=True)

n = len(sub_2026)
greens = (sub_2026["lay_win"] == 1).sum()
reds = n - greens
wr = (greens / n) * 100.0 if n > 0 else 0.0
avg_odd = sub_2026["Odd_D_FT"].mean()
be_wr = ((avg_odd - 1.0) / (avg_odd - 0.05)) * 100.0 if n > 0 else 0.0
profit = sub_2026["pnl_lay"].sum()
roi = (profit / (n * 100.0)) * 100.0 if n > 0 else 0.0

gross_win = greens * 95.0
gross_loss = ((sub_2026[sub_2026["lay_win"] == 0]["Odd_D_FT"] - 1.0) * 100.0).sum()
pf = gross_win / gross_loss if gross_loss > 0 else 999.0

# Max drawdown
cum = sub_2026["pnl_lay"].cumsum()
peak = cum.cummax()
dd = cum - peak
max_dd = dd.min()

print(f"\n==================================================")
print(f"       RESUMO GERAL DO ANO DE 2026 (ATÉ AGOSTO)   ")
print(f"==================================================")
print(f"Total de Entradas: {n}")
print(f"Greens: {greens} ({wr:.2f}%) | Reds: {reds} ({100-wr:.2f}%)")
print(f"Odd Média: {avg_odd:.2f}")
print(f"Break-even Win Rate: {be_wr:.2f}% (Vantagem vs Mercado: {wr - be_wr:+.2f}%)")
print(f"Lucro Líquido (Stake R$ 100): R$ {profit:,.2f}")
print(f"ROI Líquido: {roi:+.2f}%")
print(f"Profit Factor: {pf:.2f}")
print(f"Max Drawdown: R$ {max_dd:,.2f}")

# Detalhamento Mês a Mês
sub_2026["Mes"] = sub_2026["Date"].dt.strftime("%Y-%m (%B)")
meses = sub_2026.groupby("Mes").agg(
    jogos=("lay_win", "count"),
    greens=("lay_win", "sum"),
    odd_media=("Odd_D_FT", "mean"),
    lucro=("pnl_lay", "sum")
).reset_index()

meses["reds"] = meses["jogos"] - meses["greens"]
meses["wr"] = (meses["greens"] / meses["jogos"]) * 100.0
meses["be_wr"] = ((meses["odd_media"] - 1.0) / (meses["odd_media"] - 0.05)) * 100.0
meses["roi"] = (meses["lucro"] / (meses["jogos"] * 100.0)) * 100.0

print("\n--- PERFORMANCE DETALHADA MÊS A MÊS EM 2026 ---")
cols_mes = ["Mes", "jogos", "greens", "reds", "wr", "be_wr", "lucro", "roi"]
print(meses[cols_mes].to_string(index=False))

# Exportar tabela completa para Excel
sub_export = sub_2026[[
    "Date", "League", "Home", "Away", "Goals_H_FT", "Goals_A_FT",
    "Odd_D_FT", "Odd_H_FT", "Odd_A_FT", "total_xGOT", "prob_lay_win", "ev_lay", "lay_win", "pnl_lay"
]].copy()

sub_export["Placar"] = sub_export["Goals_H_FT"].astype(str) + " x " + sub_export["Goals_A_FT"].astype(str)
sub_export["Resultado"] = np.where(sub_export["lay_win"] == 1, "GREEN", "RED")
sub_export["Data"] = sub_export["Date"].dt.strftime("%Y-%m-%d")
sub_export["Prob_IA"] = (sub_export["prob_lay_win"] * 100).round(1).astype(str) + "%"
sub_export["xGOT_Total"] = sub_export["total_xGOT"].round(2)
sub_export["EV_Lay"] = sub_export["ev_lay"].round(3)
sub_export["Lucro_R$"] = sub_export["pnl_lay"].round(2)

cols_final = ["Data", "League", "Home", "Away", "Placar", "Odd_D_FT", "Prob_IA", "xGOT_Total", "EV_Lay", "Resultado", "Lucro_R$"]
df_save = sub_export[cols_final].rename(columns={
    "League": "Liga", "Home": "Mandante", "Away": "Visitante", "Odd_D_FT": "Odd Lay Empate"
})

df_save.to_excel("Backtest_Lay_Draw_2026_Sniper.xlsx", index=False)
print("\n[+] Planilha completa exportada com sucesso: Backtest_Lay_Draw_2026_Sniper.xlsx", flush=True)
