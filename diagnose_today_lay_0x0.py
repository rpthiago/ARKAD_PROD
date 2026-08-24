import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
import b365_data_utils
import hist_rf_loader
import lay_0x0_rf_v2_strategy as s0x0

date_str = "2026-08-23"
print(f"=== DIAGNÓSTICO DETALHADO DO LAY 0x0 PARA {date_str} ===", flush=True)

bf = b365_data_utils.fetch_betfair_daily(date_str)
print(f"[+] Jogos trazidos pela API Betfair: {len(bf) if bf is not None else 0}", flush=True)

if bf is not None and not bf.empty:
    hist = hist_rf_loader.load_hist_rf()
    payload = bf.to_dict("records")
    res = s0x0.predict_and_evaluate_live(payload, hist)
    
    df_res = pd.DataFrame(res)
    print("\n--- DISTRIBUIÇÃO DOS MOTIVOS DE DECISÃO ---")
    print(df_res["Reason"].value_counts().to_string())
    
    print("\n--- EXEMPLOS DE JOGOS COM DECISÃO / REASON ---")
    cols_show = [c for c in ["League", "Home", "Away", "Odd_0x0_FT", "Odd_0x0_Lay", "Prob_ML", "ev_lay", "Decision", "Reason"] if c in df_res.columns]
    print(df_res[cols_show].head(30).to_string(index=False))
