from futpythontrader_client import get_daily_dataframe
df_day = get_daily_dataframe("betfair", "2026-08-16")

print("Analizando os 173 jogos de 16/08:")
reasons = {}
for idx, row in df_day.iterrows():
    odd_h = float(row.get('Odd_H_Back') or row.get('Odd_H_FT_Back') or row.get('Odd_H_FT') or row.get('Odd_H') or 0.0)
    odd_a = float(row.get('Odd_A_Back') or row.get('Odd_A_FT_Back') or row.get('Odd_A_FT') or row.get('Odd_A') or 0.0)
    odd_u25 = float(row.get('Odd_Under25_FT_Back') or row.get('Odd_Under25_FT') or row.get('Odd_Under25') or 0.0)
    odd_0x3 = float(row.get('Odd_CS_0x3_Lay') or row.get('Odd_CS_0x3') or 0.0)
    
    if odd_h <= 0.0 or odd_h > 2.20:
        rsn = "Odd_H > 2.20 ou invalida"
    elif odd_h >= odd_a:
        rsn = "Odd_H >= Odd_A"
    elif odd_u25 <= 0.0 or odd_u25 > 2.10:
        rsn = "Odd_U25 > 2.10"
    elif odd_0x3 < 14.0 or odd_0x3 > 35.0:
        rsn = "Odd_0x3 fora de [14.0, 35.0]"
    else:
        rsn = "APROVADO"
        
    reasons[rsn] = reasons.get(rsn, 0) + 1

for k, v in sorted(reasons.items(), key=lambda x: x[1], reverse=True):
    print(f"  - {k}: {v}")
