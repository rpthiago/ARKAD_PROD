import pandas as pd
from futpythontrader_client import get_daily_dataframe

print("=== CHECANDO JOGOS DE AGOSTO DE 2026 NA API FUTPYTHONTRADER ===", flush=True)

all_days = []
for day in range(1, 21):
    d_str = f"2026-08-{day:02d}"
    try:
        df = get_daily_dataframe("betfair", d_str)
        if not df.empty:
            print(f"[+] {d_str}: {len(df)} jogos baixados da API.", flush=True)
            df["d_str"] = d_str
            all_days.append(df)
        else:
            print(f"[-] {d_str}: Nenhum jogo retornado.", flush=True)
    except Exception as e:
        print(f"[!] {d_str}: Erro na API: {e}", flush=True)

if all_days:
    df_august = pd.concat(all_days, ignore_index=True)
    print(f"\n=======================================================", flush=True)
    print(f"TOTAL DE JOGOS EM AGOSTO/2026 NA API: {len(df_august)}", flush=True)
    print(f"Datas com jogos:", df_august["d_str"].value_counts().sort_index().to_dict(), flush=True)
else:
    print("Nenhum jogo retornado da API para Agosto de 2026.", flush=True)
