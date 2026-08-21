import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, os
from concurrent.futures import ThreadPoolExecutor

from futpythontrader_client import get_daily_dataframe
from metodo_saldo_menor_strategy import check_entry_conditions, normalize_live_data
from metodo_lay2x2_strategy import validar_entrada_lay2x2
try:
    from lay_0x1_rf_v2_strategy import avalie_jogo_lay0x1
except ImportError:
    avalie_jogo_lay0x1 = None

print("=== INICIANDO AUDITORIA PARALELA RÁPIDA DE AGOSTO (01 A 20/08) ===", flush=True)

def process_day(day):
    d_str = f"2026-08-{day:02d}"
    res = {
        "d_str": d_str,
        "saldo_menor": 0,
        "lay_0x3": 0,
        "lay_2x2": 0,
        "lay_0x1": 0,
        "total_jogos_betfair": 0
    }
    try:
        df_day = get_daily_dataframe("betfair", d_str)
        if df_day is None or df_day.empty:
            return res
            
        res["total_jogos_betfair"] = len(df_day)
        
        # 1. Saldo Menor
        sm_c = 0
        for _, r in df_day.iterrows():
            norm = normalize_live_data(r.to_dict())
            ok, _ = check_entry_conditions(norm, check_betmines=False)
            if ok: sm_c += 1
        res["saldo_menor"] = sm_c
        
        # 2. Lay 0x3 Visitante
        l0x3_c = 0
        for _, row in df_day.iterrows():
            odd_a = float(row.get('Odd_A_Back') or row.get('Odd_A_FT_Back') or row.get('Odd_A_FT') or row.get('Odd_A') or 0.0)
            odd_u25 = float(row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25') or 0.0)
            odd_0x3 = float(row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3') or 0.0)
            xg_a_val = row.get('A_xGF_r5') or row.get('Media_Gols_Pro_Visitante') or row.get('xG_A_FT') or row.get('xg_a')
            xg_a = float(xg_a_val) if pd.notna(xg_a_val) else 1.0
            
            if 0.0 < odd_u25 <= 2.10 and 14.0 <= odd_0x3 <= 35.0 and (odd_a >= 1.85 or odd_a == 0.0) and xg_a <= 1.10:
                l0x3_c += 1
        res["lay_0x3"] = l0x3_c
        
        # 3. Lay 2x2 Quant
        l2x2_c = 0
        odd_2x2_col = [c for c in df_day.columns if '2x2' in str(c).lower() and 'lay' in str(c).lower()]
        odd_u25_col = [c for c in df_day.columns if ('under25_ft_back' in str(c).lower() or 'under25_ft' in str(c).lower() or 'under 2.5 ft' in str(c).lower()) and 'ht' not in str(c).lower()]
        if not odd_u25_col: odd_u25_col = [c for c in df_day.columns if 'under25' in str(c).lower() and 'ht' not in str(c).lower()]
        odd_h_col = [c for c in df_day.columns if str(c).lower() in ['odd_h', 'odd_h_ft', 'odd_h_ft_back', 'odd_home', 'odd_1']]
        odd_a_col = [c for c in df_day.columns if str(c).lower() in ['odd_a', 'odd_a_ft', 'odd_a_ft_back', 'odd_away', 'odd_2']]

        for _, r in df_day.iterrows():
            o_2x2 = float(pd.to_numeric(r.get(odd_2x2_col[0]), errors='coerce')) if odd_2x2_col and pd.notna(r.get(odd_2x2_col[0])) else 0.0
            o_u25 = float(pd.to_numeric(r.get(odd_u25_col[0]), errors='coerce')) if odd_u25_col and pd.notna(r.get(odd_u25_col[0])) else None
            o_h = float(pd.to_numeric(r.get(odd_h_col[0]), errors='coerce')) if odd_h_col and pd.notna(r.get(odd_h_col[0])) else None
            o_a = float(pd.to_numeric(r.get(odd_a_col[0]), errors='coerce')) if odd_a_col and pd.notna(r.get(odd_a_col[0])) else None
            
            ok, _ = validar_entrada_lay2x2(odd_lay_2x2=o_2x2, odd_under25=o_u25, odd_h=o_h, odd_a=o_a)
            if ok: l2x2_c += 1
        res["lay_2x2"] = l2x2_c

        # 4. Lay 0x1 RF v2
        l0x1_c = 0
        if avalie_jogo_lay0x1 is not None:
            for _, r in df_day.iterrows():
                try:
                    res_0x1 = avalie_jogo_lay0x1(r.to_dict())
                    if isinstance(res_0x1, tuple): ok_0x1 = res_0x1[0]
                    elif isinstance(res_0x1, dict): ok_0x1 = res_0x1.get('entrada', False) or res_0x1.get('aprovado', False)
                    else: ok_0x1 = bool(res_0x1)
                    if ok_0x1: l0x1_c += 1
                except Exception: pass
        res["lay_0x1"] = l0x1_c

    except Exception as e:
        print(f"Erro em {d_str}: {e}", flush=True)
        
    return res

with ThreadPoolExecutor(max_workers=10) as executor:
    daily_results = list(executor.map(process_day, range(1, 21)))

df_audit = pd.DataFrame(daily_results).sort_values("d_str")

print("\n" + "="*80, flush=True)
print("=== TABELA COMPLETA DE SINAIS GERADOS DIA A DIA EM AGOSTO (01 A 20/08) ===", flush=True)
print("="*80, flush=True)
print(df_audit.to_string(index=False), flush=True)

tot_sm = df_audit["saldo_menor"].sum()
tot_0x3 = df_audit["lay_0x3"].sum()
tot_2x2 = df_audit["lay_2x2"].sum()
tot_0x1 = df_audit["lay_0x1"].sum()
tot_betfair = df_audit["total_jogos_betfair"].sum()

print("\n=======================================================", flush=True)
print("=== RESUMO TOTAL DE SINAIS GERADOS EM AGOSTO (2026) ===", flush=True)
print("=======================================================", flush=True)
print(f"Total de Jogos Auditados na Betfair : {tot_betfair:,}", flush=True)
print(f"1. Método Saldo Menor              : {tot_sm} Sinais", flush=True)
print(f"2. Lay 0x3 Visitante                : {tot_0x3} Sinais", flush=True)
print(f"3. Lay 2x2 Quant                    : {tot_2x2} Sinais", flush=True)
print(f"4. Lay 0x1 RF v2                    : {tot_0x1} Sinais", flush=True)

df_audit.to_excel("Auditoria_Sinais_Agosto_2026_Completa.xlsx", index=False)
