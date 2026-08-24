import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
import b365_data_utils
import hist_rf_loader
import lay_draw_rf_v2_strategy as sdraw

print("=== INSPEÇÃO LAY DRAW: 09/08 ATÉ 23/08 ===", flush=True)
hist = hist_rf_loader.load_hist_rf()

dates = pd.date_range("2026-08-09", "2026-08-23").strftime("%Y-%m-%d")

for d in dates:
    bf = b365_data_utils.fetch_betfair_daily(d)
    if bf is None or bf.empty:
        print(f"[{d}] Sem jogos na API Betfair.")
        continue
    
    # Testar com parâmetros padrão do modelo
    sdraw.PROB_MIN = 0.85
    sdraw.ODD_MIN = 3.20
    sdraw.ODD_MAX = 4.20
    sdraw.FAV_ODD_MAX = 2.10
    
    res = sdraw.predict_and_evaluate_live(bf.to_dict("records"), hist)
    df_res = pd.DataFrame(res)
    if df_res.empty:
        print(f"[{d}] Total jogos: {len(bf)} | Resposta vazia!")
        continue
        
    aprov = df_res[df_res["Decision"] == "APOSTA"]
    reasons = df_res["Reason"].value_counts().to_dict()
    print(f"[{d}] Total jogos: {len(bf)} | Aprovados: {len(aprov)} | Motivos de skip: {reasons}")
    if not aprov.empty:
        print("    -> APROVADOS:")
        for _, r in aprov.iterrows():
            print(f"       * {r['League']} | {r['Home']} x {r['Away']} | Odd Lay: {r.get('Odd_D_FT')} | Prob ML: {r.get('Prob_ML')*100:.1f}% | EV: {r.get('ev_lay'):+.3f}")
