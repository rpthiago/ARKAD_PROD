import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("==================================================================", flush=True)
print("             DIAGNÓSTICO COMPLETO - LAY 1X0 NO PAPER             ", flush=True)
print("==================================================================", flush=True)

df = pd.read_csv("paper_consolidado.csv")
l10 = df[df["Metodo"] == "Lay 1x0"].copy()
l10["Odd"] = pd.to_numeric(l10["Odd"], errors="coerce")
l10 = l10.dropna(subset=["Odd", "Resultado"])

n = len(l10)
greens = (l10["Resultado"] == "GREEN").sum()
reds = (l10["Resultado"] == "RED").sum()
wr = (greens / n) * 100.0
odd_med = l10["Odd"].median()
be_wr = ((odd_med - 1.0) / (odd_med - 0.045)) * 100.0

print(f"Total de Jogos: {n}")
print(f"Greens: {greens} ({wr:.1f}%) | Reds: {reds} ({100-wr:.1f}%)")
print(f"Odd Mediana: {odd_med:.2f} (Faixa: {l10['Odd'].min():.2f} a {l10['Odd'].max():.2f})")
print(f"Break-even Win Rate: {be_wr:.1f}%")
print(f"Déficit Matemático: {wr - be_wr:.1f}%\n")

# Analisar os 23 Reds
reds_df = l10[l10["Resultado"] == "RED"].copy()
print("--- OS 23 REDS DO LAY 1X0 (PLACAR FINAL 1X0 MANDANTE) ---")
cols_r = ["Data", "Liga", "Mandante", "Visitante", "Odd", "Gols_M", "Gols_V"]
print(reds_df[cols_r].to_string(index=False))

print("\n--- DISTRIBUIÇÃO POR LIGA NOS REDS ---")
print(reds_df["Liga"].value_counts().to_string())

print("\n--- DISTRIBUIÇÃO POR FAIXA DE ODD ---")
l10["faixa_odd"] = pd.cut(l10["Odd"], bins=[0, 6.0, 8.0, 10.0, 15.0, 30.0])
print(l10.groupby("faixa_odd", observed=False).agg(
    jogos=("Resultado", "count"),
    greens=("Resultado", lambda x: (x == "GREEN").sum()),
    reds=("Resultado", lambda x: (x == "RED").sum()),
    odd_media=("Odd", "mean")
).assign(wr=lambda d: (d["greens"] / d["jogos"] * 100).round(1)).to_string())
