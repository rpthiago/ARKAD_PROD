import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
from futpythontrader_client import get_daily_dataframe
from metodo_lay2x2_strategy import validar_entrada_lay2x2, ODD_LAY_2X2_MIN, ODD_LAY_2X2_MAX

date_str = "2026-08-20"
df_day = get_daily_dataframe("betfair", date_str)

print(f"=== DIAGNÓSTICO DO LAY 2X2 QUANT HOJE ({date_str}) ===", flush=True)
print(f"[+] Total de jogos no feed Betfair de hoje: {len(df_day)}", flush=True)

if not df_day.empty:
    odd_2x2_col = [c for c in df_day.columns if '2x2' in str(c).lower() and 'lay' in str(c).lower()]
    odd_u25_col = [c for c in df_day.columns if ('under25_ft_back' in str(c).lower() or 'under25_ft' in str(c).lower() or 'under 2.5 ft' in str(c).lower()) and 'ht' not in str(c).lower()]
    if not odd_u25_col:
        odd_u25_col = [c for c in df_day.columns if 'under25' in str(c).lower() and 'ht' not in str(c).lower()]
    odd_h_col = [c for c in df_day.columns if str(c).lower() in ['odd_h', 'odd_h_ft', 'odd_h_ft_back', 'odd_home', 'odd_1']]
    odd_a_col = [c for c in df_day.columns if str(c).lower() in ['odd_a', 'odd_a_ft', 'odd_a_ft_back', 'odd_away', 'odd_2']]
    
    print(f"Colunas de Odds identificadas: 2x2={odd_2x2_col}, U25={odd_u25_col}, H={odd_h_col}, A={odd_a_col}", flush=True)
    
    reasons = {}
    aprovados = []
    
    for idx, r in df_day.iterrows():
        o_2x2 = pd.to_numeric(r.get(odd_2x2_col[0]), errors='coerce') if odd_2x2_col else 0.0
        o_u25 = pd.to_numeric(r.get(odd_u25_col[0]), errors='coerce') if odd_u25_col else None
        o_h = pd.to_numeric(r.get(odd_h_col[0]), errors='coerce') if odd_h_col else None
        o_a = pd.to_numeric(r.get(odd_a_col[0]), errors='coerce') if odd_a_col else None
        
        o_2x2 = float(o_2x2) if pd.notna(o_2x2) else 0.0
        o_u25 = float(o_u25) if pd.notna(o_u25) else None
        o_h = float(o_h) if pd.notna(o_h) else None
        o_a = float(o_a) if pd.notna(o_a) else None
        
        ok, motivo = validar_entrada_lay2x2(
            odd_lay_2x2=o_2x2,
            odd_under25=o_u25,
            odd_h=o_h,
            odd_a=o_a
        )
        
        home = str(r.get("Home", r.get("Home_Team", "")))
        away = str(r.get("Away", r.get("Away_Team", "")))
        
        if ok:
            aprovados.append(f"{home} x {away} (Odd 2x2: {o_2x2:.2f})")
        else:
            if o_2x2 <= 1.0:
                key = "Odd Lay 2x2 Ausente/Zerada"
            elif o_2x2 < ODD_LAY_2X2_MIN:
                key = f"Odd 2x2 Abaixo de 8.00"
            elif o_2x2 > ODD_LAY_2X2_MAX:
                key = f"Odd 2x2 Acima de 14.00 (Estouro de Risco)"
            else:
                key = f"Sem Tendência Under/Favorito"
                
            reasons[key] = reasons.get(key, 0) + 1
            
    print(f"\n[+] SINAIS APROVADOS HOJE NO LAY 2X2: {len(aprovados)} JOGOS", flush=True)
    for a in aprovados:
        print(f"   * {a}", flush=True)
        
    print(f"\n[-] BREAKDOWN DE MOTIVOS DE REJEIÇÃO ({len(df_day)} JOGOS):", flush=True)
    for rsn, count in sorted(reasons.items(), key=lambda x: x[1], reverse=True):
        print(f"   - {rsn}: {count} jogos", flush=True)
