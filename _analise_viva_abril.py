import glob
import pandas as pd
import numpy as np

DIR = "Apostas_Diarias"
arquivos = glob.glob(f"{DIR}/Apostas_*.xlsx")

frames = []
for arq in arquivos:
    frames.append(pd.read_excel(arq))
df = pd.concat(frames, ignore_index=True)

# Normalização
df["Metodo"] = df["Metodo"].fillna("").astype(str).str.strip()
df["Resultado"] = df["Resultado"].fillna("").astype(str).str.strip().str.upper()
df["Odd_Base"] = pd.to_numeric(df["Odd_Base"], errors="coerce")
df["Stake"] = 500.0 # Usando stake padrão de 500 se não houver ou se quisermos comparar

def calc_pnl(row):
    res = row["Resultado"]
    odd = row["Odd_Base"]
    stake = row["Stake"]
    if "GREEN" in res:
        return stake / (odd - 1.0) * (1 - 0.065) # Com 6.5% de comissão
    if "RED" in res:
        return -stake
    return 0.0

df["PnL_Calculado"] = df.apply(calc_pnl, axis=1)

print("=== TRUE MANUAL PERFORMANCE (Abril 2026) ===")
print("Calculado usando Stake Fixo 500 e Odd_Base (com 6.5% comissão)")
resumo = df.groupby("Metodo").agg(
    Total=("Resultado", "count"),
    Greens=("Resultado", lambda x: (x == "GREEN").sum()),
    Reds=("Resultado", lambda x: (x == "RED").sum()),
    PnL_Total=("PnL_Calculado", "sum")
).reset_index()

resumo["WR%"] = (resumo["Greens"] / resumo["Total"] * 100).round(1)
print(resumo.to_string(index=False))

print("\n=== COMPARANDO COM O QUE O ENGINE VE (Ignorando missing Hora) ===")
df["Has_Hora"] = df["Hora"].notna()
resumo_hora = df.groupby(["Metodo", "Has_Hora"]).agg(
    Total=("Resultado", "count"),
    PnL_Total=("PnL_Calculado", "sum")
).reset_index()
print(resumo_hora.to_string(index=False))
