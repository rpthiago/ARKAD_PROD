import pandas as pd, numpy as np
from futpythontrader_client import get_daily_dataframe
import b365_data_utils, coleta_lay_cs_aovivo

date_str = "2026-08-20"
df_bf = get_daily_dataframe("betfair", date_str)

print("=== TESTE DE SINAIS PARA HOJE (2026-08-20) EM TODOS OS 9 MÉTODOS ===", flush=True)
print(f"[+] Total de jogos no feed da Betfair hoje: {len(df_bf)}", flush=True)

if df_bf is not None and not df_bf.empty:
    payload = df_bf.to_dict("records")
    hist = coleta_lay_cs_aovivo._hist_df()
    
    metodos = [
        ("Lay 0x0 RF v2", "lay_0x0_rf_v2_strategy", "Odd_CS_0x0_Lay"),
        ("Lay 0x1 RF v2", "lay_0x1_rf_v2_strategy", "Odd_CS_0x1_Lay"),
        ("Lay 1x0 RF v2", "lay_1x0_rf_v2_strategy", "Odd_CS_1x0_Lay"),
        ("Lay 2x0 RF v2", "lay_2x0_rf_v2_strategy", "Odd_CS_2x0_Lay"),
        ("Lay 0x2 RF v2", "lay_0x2_rf_v2_strategy", "Odd_CS_0x2_Lay"),
        ("Lay Draw v2", "lay_draw_rf_v2_strategy", "Odd_Lay_Draw"),
        ("Lay Under 2.5 v2", "lay_under25_rf_v2_strategy", "Odd_Lay_Under25"),
        ("Lay 2x2 Quant", "metodo_lay2x2_strategy", "Odd_CS_2x2_Lay"),
        ("Lay 0x1 Agressivo", "lay_0x1_agressivo_strategy", "Odd_CS_0x1_Lay")
    ]
    
    for label, mod_name, odd_col in metodos:
        try:
            mod = __import__(mod_name, fromlist=["predict_and_evaluate_live", "validar_entrada_lay2x2"])
            if hasattr(mod, "predict_and_evaluate_live"):
                res = mod.predict_and_evaluate_live(payload, hist)
                aprovados = [g for g in (res or []) if g.get("Decision") == "APOSTA"]
                print(f"[{label}] -> Avaliados: {len(res or [])} | APOSTA: {len(aprovados)}", flush=True)
                if aprovados:
                    for a in aprovados:
                        print(f"   ★ {a.get('Home')} x {a.get('Away')} (Odd: {a.get(odd_col)})", flush=True)
            elif hasattr(mod, "validar_entrada_lay2x2"):
                aprovados = []
                for g in payload:
                    ok, rsn = mod.validar_entrada_lay2x2(g)
                    if ok: aprovados.append(g)
                print(f"[{label}] -> Avaliados: {len(payload)} | APOSTA: {len(aprovados)}", flush=True)
                if aprovados:
                    for a in aprovados:
                        print(f"   ★ {a.get('Home')} x {a.get('Away')} (Odd: {a.get(odd_col)})", flush=True)
        except Exception as e:
            print(f"[{label}] -> ERRO: {e}", flush=True)
