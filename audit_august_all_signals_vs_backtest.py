import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, os

from futpythontrader_client import get_daily_dataframe

# Importar os módulos das estratégias
from metodo_saldo_menor_strategy import check_entry_conditions, normalize_live_data
from metodo_lay2x2_strategy import validar_entrada_lay2x2
try:
    from lay_0x1_rf_v2_strategy import avalie_jogo_lay0x1
except ImportError:
    avalie_jogo_lay0x1 = None

print("==========================================================================", flush=True)
print("=== AUDITORIA COMPLETA DE AGOSTO: PAINEL DE SINAIS VS BACKTEST ===", flush=True)
print("==========================================================================", flush=True)

# Coletar jogos para cada dia de Agosto (01 a 20/08)
results_by_strategy = {
    "Saldo Menor": {},
    "Lay 0x3 Visitante": {},
    "Lay 2x2 Quant": {},
    "Lay 0x1 RF v2": {},
}

for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    try:
        df_day = get_daily_dataframe("betfair", d_str)
        if df_day is None or df_day.empty:
            continue
            
        # 1. Saldo Menor
        sm_count = 0
        for _, r in df_day.iterrows():
            norm = normalize_live_data(r.to_dict())
            ok, _ = check_entry_conditions(norm, check_betmines=False)
            if ok: sm_count += 1
        results_by_strategy["Saldo Menor"][d_str] = sm_count

        # 2. Lay 0x3 Visitante
        l0x3_count = 0
        for _, row in df_day.iterrows():
            odd_a = float(row.get('Odd_A_Back') or row.get('Odd_A_FT_Back') or row.get('Odd_A_FT') or row.get('Odd_A') or 0.0)
            odd_u25 = float(row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25') or 0.0)
            odd_0x3 = float(row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3') or 0.0)
            xg_a_val = row.get('A_xGF_r5') or row.get('Media_Gols_Pro_Visitante') or row.get('xG_A_FT') or row.get('xg_a')
            xg_a = float(xg_a_val) if pd.notna(xg_a_val) else 1.0
            
            if 0.0 < odd_u25 <= 2.10 and 14.0 <= odd_0x3 <= 35.0 and (odd_a >= 1.85 or odd_a == 0.0) and xg_a <= 1.10:
                l0x3_count += 1
        results_by_strategy["Lay 0x3 Visitante"][d_str] = l0x3_count

        # 3. Lay 2x2 Quant
        l2x2_count = 0
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
            if ok: l2x2_count += 1
        results_by_strategy["Lay 2x2 Quant"][d_str] = l2x2_count

        # 4. Lay 0x1 RF v2
        l0x1_count = 0
        if avalie_jogo_lay0x1 is not None:
            for _, r in df_day.iterrows():
                try:
                    res_0x1 = avalie_jogo_lay0x1(r.to_dict())
                    if isinstance(res_0x1, tuple):
                        ok_0x1 = res_0x1[0]
                    elif isinstance(res_0x1, dict):
                        ok_0x1 = res_0x1.get('entrada', False) or res_0x1.get('aprovado', False)
                    else:
                        ok_0x1 = bool(res_0x1)
                    if ok_0x1: l0x1_count += 1
                except Exception: pass
        results_by_strategy["Lay 0x1 RF v2"][d_str] = l0x1_count

    except Exception as e:
        print(f"Erro auditando data {d_str}: {e}", flush=True)

summary_rows = []
for strat, counts in results_by_strategy.items():
    total_m = sum(counts.values())
    summary_rows.append({
        "Estratégia": strat,
        "Total Agosto (Sinais Ao Vivo)": total_m,
        "Dias Com Sinais": len([d for d, c in counts.items() if c > 0])
    })

df_summary = pd.DataFrame(summary_rows)

print("\n" + "="*80, flush=True)
print("=== RESUMO DOS SINAIS DE AGOSTO (01 A 20/08/2026) POR ESTRATÉGIA ===", flush=True)
print("="*80, flush=True)
print(df_summary.to_string(index=False), flush=True)

print("\n=== DETALHAMENTO DIA A DIA (01 A 20/08/2026) ===", flush=True)
df_daily = pd.DataFrame(results_by_strategy)
print(df_daily.to_string(), flush=True)

df_daily.to_excel("Auditoria_Sinais_vs_Backtest_Agosto_2026.xlsx")
