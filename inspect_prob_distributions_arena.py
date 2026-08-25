import sys
sys.stdout.reconfigure(encoding='utf-8')
import joblib, numpy as np, pandas as pd

# Carregar artefatos
rf = joblib.load("modelo_lay_draw_rf_novo.pkl")
lgb = joblib.load("modelo_lay_draw_lightgbm.pkl")
xgb = joblib.load("modelo_lay_draw_xgboost.pkl")
scaler = joblib.load("scaler_lay_draw_arena.pkl")
features = joblib.load("features_lay_draw_arena.pkl")

# Carregar base limpa
df_clean = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair.csv", low_memory=False)
# Teste em diferentes thresholds de probabilidade e EV na base OOS
# Vamos rodar uma grade de threshold no teste cego (Abril a Julho/2026)
import treinar_arena_modelos_lay_draw as arena

print("\n==================================================================")
print("  AVALIAÇÃO DE THRESHOLDS DE PROBABILIDADE NA ARENA (TESTE CEGO)  ")
print("==================================================================")

X_test = arena.X_test
df_test = arena.df_test
COMMISSION = 0.045
STAKE = 100.0

for name, clf in arena.trained_models.items():
    probs = clf.predict_proba(X_test)[:, 1]
    print(f"\n--- {name} ---")
    print(f"Média Prob: {probs.mean():.3f} | Min: {probs.min():.3f} | Max: {probs.max():.3f}")
    
    # Testar cortes realistas (ex: 74%, 75%, 76%, 77%)
    for p_cut in [0.74, 0.75, 0.76, 0.77, 0.78]:
        df_e = df_test.copy()
        df_e["prob"] = probs
        df_e["ev"] = df_e["prob"] * (1.0 - COMMISSION) - (1.0 - df_e["prob"]) * (df_e["Odd_D_Lay"] - 1.0)
        df_e["pnl"] = np.where(df_e["target_lay_win"] == 1, STAKE * (1.0 - COMMISSION), -STAKE * (df_e["Odd_D_Lay"] - 1.0))
        
        cond = (df_e["Odd_D_Lay"] >= 3.0) & (df_e["Odd_D_Lay"] <= 4.5) & (df_e["prob"] >= p_cut) & (df_e["ev"] >= 0.02)
        sub = df_e[cond]
        if len(sub) >= 15:
            gr = (sub["target_lay_win"] == 1).sum()
            n = len(sub)
            wr = gr / n * 100.0
            odd_m = sub["Odd_D_Lay"].mean()
            be = ((odd_m - 1.0) / (odd_m - COMMISSION)) * 100.0
            pnl = sub["pnl"].sum()
            roi = pnl / (n * STAKE) * 100.0
            print(f"  Prob >= {p_cut*100:.0f}%: N={n:4d} | Greens={gr:4d} | WR={wr:.1f}% vs BE={be:.1f}% (Margem: {wr-be:+.1f}%) | Lucro=R$ {pnl:8.2f} | ROI={roi:+.1f}%")
