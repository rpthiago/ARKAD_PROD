import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("==================================================================", flush=True)
print("   RESULTADOS EXATOS DA PLANILHA DE PAPER TRADING (AGOSTO/2026)  ", flush=True)
print("                     (LAY 2X2 & LAY 0X0)                         ", flush=True)
print("==================================================================", flush=True)

df = pd.read_csv("paper_consolidado.csv")
df["Data"] = pd.to_datetime(df["Data"])
df["Odd"] = pd.to_numeric(df["Odd"], errors="coerce")

TAXA = 0.045 # 4.5% Betfair
STAKE = 100.0
RESP = 100.0

for metodo in ["Lay 2x2", "Lay 0x0"]:
    sub = df[(df["Metodo"] == metodo) & (df["Resultado"].isin(["GREEN", "RED"]))].copy()
    sub = sub.sort_values("Data", kind="mergesort").reset_index(drop=True)
    
    n = len(sub)
    greens = (sub["Resultado"] == "GREEN").sum()
    reds = n - greens
    wr = greens / n * 100.0
    odd_med = sub["Odd"].median()
    odd_mean = sub["Odd"].mean()
    be_wr = ((odd_med - 1.0) / (odd_med - TAXA)) * 100.0
    
    # 1. Stake Fixa (R$ 100)
    pnl_sf = np.where(sub["Resultado"] == "GREEN", STAKE * (1.0 - TAXA), -STAKE * (sub["Odd"] - 1.0)).sum()
    roi_sf = pnl_sf / (n * STAKE) * 100.0
    
    # 2. Responsabilidade Fixa (R$ 100 risco max)
    stake_rf = RESP / (sub["Odd"] - 1.0)
    pnl_rf = np.where(sub["Resultado"] == "GREEN", stake_rf * (1.0 - TAXA), -RESP).sum()
    roi_rf = pnl_rf / (n * RESP) * 100.0
    
    print(f"\n==================================================")
    print(f"📊 MÉTODO: {metodo} (Período: {sub['Data'].min().strftime('%d/%m')} a {sub['Data'].max().strftime('%d/%m')})")
    print(f"==================================================")
    print(f"Total de Apostas Liquidadas no Paper: {n}")
    print(f"Greens: {greens} ({wr:.1f}%) | Reds: {reds} ({100-wr:.1f}%)")
    print(f"Odd Mediana: {odd_med:.2f} (Média: {odd_mean:.2f})")
    print(f"Break-even Betfair: {be_wr:.1f}% (Margem: {wr - be_wr:+.1f}%)")
    print(f"\n💰 Na Stake Fixa (R$ 100/jogo):           Lucro = R$ {pnl_sf:+,.2f} | ROI = {roi_sf:+.1f}%")
    print(f"🛡️ Na Responsabilidade Fixa (R$ 100 risco): Lucro = R$ {pnl_rf:+,.2f} | ROI = {roi_rf:+.1f}%")
    
    # Dia a dia
    sub["Dia"] = sub["Data"].dt.strftime("%d/%m (%a)")
    dias = sub.groupby("Dia").agg(
        jogos=("Resultado", "count"),
        greens=("Resultado", lambda x: (x == "GREEN").sum()),
        odd_med=("Odd", "median")
    ).reset_index()
    dias["reds"] = dias["jogos"] - dias["greens"]
    dias["wr"] = dias["greens"] / dias["jogos"] * 100.0
    print("\n--- Desempenho Dia a Dia no Paper ---")
    print(dias[["Dia", "jogos", "greens", "reds", "wr", "odd_med"]].to_string(index=False))
