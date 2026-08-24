import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
import b365_data_utils
import hist_rf_loader
import lay_draw_rf_v2_strategy as sdraw

print("=== DISTRIBUIÇÃO DE PROBABILIDADES E FILTROS DO LAY DRAW ===", flush=True)
hist = hist_rf_loader.load_hist_rf()

for test_date in ["2026-08-16", "2026-08-23"]:
    bf = b365_data_utils.fetch_betfair_daily(test_date)
    if bf is None or bf.empty:
        continue
    
    # 1. Sem filtros extras (apenas motor cru)
    sdraw.PROB_MIN = 0.50
    sdraw.ODD_MIN = 1.01
    sdraw.ODD_MAX = 20.00
    sdraw.FAV_ODD_MAX = None
    
    res = sdraw.predict_and_evaluate_live(bf.to_dict("records"), hist)
    df_res = pd.DataFrame(res)
    
    print(f"\n--- DATA: {test_date} (Total Jogos API: {len(bf)}) ---")
    valid_probs = df_res["Prob_ML"].dropna()
    print(f"Jogos com Prob_ML calculada: {len(valid_probs)}/{len(df_res)}")
    print(f"Resumo Prob_ML: Min={valid_probs.min():.3f}, Mean={valid_probs.mean():.3f}, Max={valid_probs.max():.3f}")
    print(f"Jogos com Prob >= 75%: {(valid_probs >= 0.75).sum()}")
    print(f"Jogos com Prob >= 80%: {(valid_probs >= 0.80).sum()}")
    print(f"Jogos com Prob >= 85%: {(valid_probs >= 0.85).sum()}")
    print(f"Jogos com Prob >= 88%: {(valid_probs >= 0.88).sum()}")
    
    # Filtros padrão da página (88% prob, 4.20 odd, fav <= 2.10)
    p88 = df_res[(df_res["Prob_ML"] >= 0.88) & (df_res["Odd_D_FT"] <= 4.20) & (df_res["Odd_D_FT"] >= 3.20)]
    print(f"Jogos com Prob >= 88% e Odd [3.20, 4.20]: {len(p88)}")
    
    # Filtros reais recomendados (80% prob, 4.50 odd)
    p80 = df_res[(df_res["Prob_ML"] >= 0.80) & (df_res["Odd_D_FT"] <= 4.50) & (df_res["Odd_D_FT"] >= 3.00)]
    print(f"Jogos com Prob >= 80% e Odd [3.00, 4.50]: {len(p80)}")
    if not p80.empty:
        print("Exemplos com Prob >= 80%:")
        cols = [c for c in ["League", "Home", "Away", "Odd_D_FT", "Prob_ML", "ev_lay"] if c in p80.columns]
        print(p80[cols].head(10).to_string(index=False))
