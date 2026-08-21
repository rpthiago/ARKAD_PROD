import pandas as pd, numpy as np
from datetime import date
import b365_data_utils, coleta_lay_cs_aovivo
import lay_2x0_rf_v2_strategy as mod

print("=== DIAGNÓSTICO DOS SINAIS DE HOJE (2026-08-20) ===", flush=True)

date_str = "2026-08-20"
bf = b365_data_utils.fetch_betfair_daily(date_str)
print(f"[+] Total de jogos obtidos na Betfair para hoje ({date_str}): {len(bf) if bf is not None else 0}", flush=True)

if bf is not None and not bf.empty:
    payload = bf.to_dict("records")
    hist = coleta_lay_cs_aovivo._hist_df()
    print(f"[+] Total de jogos na base histórica de apoio: {len(hist)}", flush=True)
    
    res = mod.predict_and_evaluate_live(payload, hist)
    print(f"[+] Total de jogos avaliados pela inteligência RF: {len(res)}", flush=True)
    
    aprovados = [g for g in res if g.get("Decision") == "APOSTA"]
    rejeitados = [g for g in res if g.get("Decision") != "APOSTA"]
    
    print(f"\n✅ Sinais Aprovados Hoje: {len(aprovados)}", flush=True)
    for a in aprovados:
        print(f"   -> {a.get('Home')} x {a.get('Away')} | Odd: {a.get('Odd_CS_2x0_Lay')} | EV: {a.get('ev_lay'):.4f}", flush=True)
        
    print(f"\n❌ Principais Motivos de Rejeição Hoje ({len(rejeitados)} jogos avaliados):", flush=True)
    motivos = pd.Series([g.get("Reason") for g in rejeitados]).value_counts()
    print(motivos.to_string(), flush=True)

    skips_hist = len(payload) - len(res)
    print(f"\n⚠️ Jogos do dia ignorados antes do modelo (falta de histórico dos times ou liga): {skips_hist} de {len(payload)}", flush=True)
