import sys
sys.stdout.reconfigure(encoding='utf-8')
import joblib, numpy as np, pandas as pd

print("==================================================================", flush=True)
print("     AUDITORIA COMPLETA DO CAMPEÃO DA ARENA: EXTRA TREES         ", flush=True)
print("==================================================================", flush=True)

import treinar_arena_modelos_lay_draw as arena

clf_et = arena.trained_models["2. Extra Trees"]
X_test = arena.X_test
df_test = arena.df_test.copy()

COMMISSION = 0.045
STAKE = 100.0

probs_test = clf_et.predict_proba(X_test)[:, 1]
df_test["prob_lay"] = probs_test
df_test["ev_lay"] = df_test["prob_lay"] * (1.0 - COMMISSION) - (1.0 - df_test["prob_lay"]) * (df_test["Odd_D_Lay"] - 1.0)
df_test["pnl_lay"] = np.where(
    df_test["target_lay_win"] == 1,
    STAKE * (1.0 - COMMISSION),
    -STAKE * (df_test["Odd_D_Lay"] - 1.0)
)

# Filtro Campeão: Odd [3.00, 4.50] + Prob >= 75% + EV >= 0.02
cond = (
    (df_test["Odd_D_Lay"] >= 3.00) &
    (df_test["Odd_D_Lay"] <= 4.50) &
    (df_test["prob_lay"] >= 0.75) &
    (df_test["ev_lay"] >= 0.02)
)

sub = df_test[cond].copy().sort_values("Date", kind="mergesort").reset_index(drop=True)
n = len(sub)
greens = (sub["target_lay_win"] == 1).sum()
reds = n - greens
wr = greens / n * 100.0
odd_med = sub["Odd_D_Lay"].mean()
be_wr = ((odd_med - 1.0) / (odd_med - COMMISSION)) * 100.0
profit = sub["pnl_lay"].sum()
roi = profit / (n * STAKE) * 100.0

print(f"Total de Jogos no Teste Cego (Abril a Julho/2026): {n}")
print(f"Greens: {greens} ({wr:.1f}%) | Reds: {reds} ({100-wr:.1f}%)")
print(f"Odd Média Lay Betfair: {odd_med:.2f}")
print(f"Break-even Win Rate: {be_wr:.1f}% (Margem Real: {wr - be_wr:+.1f}%)")
print(f"Lucro Líquido Real (Stake R$ 100): R$ {profit:,.2f}")
print(f"ROI Líquido Real: {roi:+.1f}%")

# Mês a Mês no teste cego
sub["Mes"] = sub["Date"].dt.strftime("%Y-%m (%B)")
meses = sub.groupby("Mes").agg(
    jogos=("target_lay_win", "count"),
    greens=("target_lay_win", "sum"),
    odd_med=("Odd_D_Lay", "mean"),
    lucro=("pnl_lay", "sum")
).reset_index()
meses["reds"] = meses["jogos"] - meses["greens"]
meses["wr"] = meses["greens"] / meses["jogos"] * 100.0
meses["be_wr"] = ((meses["odd_med"] - 1.0) / (meses["odd_med"] - COMMISSION)) * 100.0
meses["roi"] = meses["lucro"] / (meses["jogos"] * STAKE) * 100.0

print("\n--- PERFORMANCE MÊS A MÊS NO TESTE CEGO (ABRIL A JULHO/2026) ---")
print(meses[["Mes", "jogos", "greens", "reds", "wr", "be_wr", "lucro", "roi"]].to_string(index=False))

# Salvar o modelo Extra Trees como o modelo oficial do Lay Draw
joblib.dump(clf_et, "modelo_lay_draw_campeao_et.pkl")
print("\n[+] Modelo Campeão Extra Trees salvo em 'modelo_lay_draw_campeao_et.pkl'!", flush=True)
