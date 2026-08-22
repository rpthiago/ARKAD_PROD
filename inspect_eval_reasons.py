import sys
sys.stdout.reconfigure(encoding='utf-8')
import hist_rf_loader, lay_draw_rf_v2_strategy as s, b365_data_utils as b, pandas as pd

hist = hist_rf_loader.load_hist_rf()
df_bf = b.fetch_betfair_daily('2026-08-16')
res = s.predict_and_evaluate_live(df_bf.to_dict('records'), hist)

df_res = pd.DataFrame(res)
print("=== CONTAGEM DE DECISÕES NO DIA 16/08 ===", flush=True)
print(df_res["Reason"].value_counts().to_string(), flush=True)

print("\n--- AMOSTRA DE JOGOS E SEUS MOTIVOS ---", flush=True)
print(df_res[["League", "Home", "Away", "Odd_D_FT", "Prob_ML", "Decision", "Reason"]].head(25).to_string(index=False), flush=True)
