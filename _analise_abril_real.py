"""Análise do backtest manual de abril (Apostas_Diarias/*.xlsx)"""
import glob
from pathlib import Path
import pandas as pd
import numpy as np

DIR = Path("Apostas_Diarias")
arquivos = sorted(glob.glob(str(DIR / "Apostas_*.xlsx")))

frames = []
for arq in arquivos:
    try:
        df = pd.read_excel(arq)
        df["__arquivo"] = Path(arq).name
        frames.append(df)
    except Exception as e:
        print(f"ERRO {arq}: {e}")

if not frames:
    print("Nenhuma planilha encontrada.")
    exit()

df = pd.concat(frames, ignore_index=True)
print("=== COLUNAS ===")
print(df.columns.tolist())
print(f"\nTotal de linhas: {len(df)}")
print("\n=== AMOSTRA (3 linhas) ===")
print(df.head(3).to_string())

# Normaliza coluna Resultado
res_col = next((c for c in df.columns if "resultado" in c.lower()), None)
if res_col:
    df["Resultado"] = df[res_col].fillna("").astype(str).str.strip().str.upper()
else:
    df["Resultado"] = ""

print(f"\n=== VALORES ÚNICOS DE RESULTADO ===")
print(df["Resultado"].value_counts())

# Tenta calcular PnL
def _pnl(row):
    res = str(row.get("Resultado", "")).strip().upper()
    if res in ("", "NAN", "PENDENTE", "-"):
        return np.nan
    # tenta coluna explícita de lucro/prejuízo
    for c in ["Lucro_Prejuizo", "PnL", "PnL_Linha", "Lucro/Prejuizo", "Lucro_Prejuízo"]:
        v = row.get(c)
        if v is not None and pd.notna(v) and str(v).strip() not in ("", "nan"):
            try:
                return float(v)
            except Exception:
                pass
    # fallback stake × odd
    try:
        stake = float(row.get("Stake") or row.get("Responsabilidade_Sugerida_R$") or 0)
        odd   = float(row.get("Odd_Real_Pega") or row.get("Odd real") or row.get("Odd_Base") or 0)
    except Exception:
        return np.nan
    if stake <= 0 or odd <= 1:
        return np.nan
    if any(k in res for k in ("GREEN", "VITORIA", "VIT")):
        return round(stake / (odd - 1.0), 2)
    if any(k in res for k in ("RED", "DERROTA", "DER")):
        return round(-stake, 2)
    if any(k in res for k in ("VOID", "DEVOLVIDA", "REEMBOLSO")):
        return 0.0
    return np.nan

df["PnL"] = df.apply(_pnl, axis=1)

print("\n=== RESUMO GERAL ===")
resolvidas = df[df["PnL"].notna()]
total = len(resolvidas)
greens = (df["Resultado"].str.contains("GREEN|VITORIA|VIT", na=False)).sum()
reds   = (df["Resultado"].str.contains("RED|DERROTA|DER", na=False)).sum()
lucro  = resolvidas["PnL"].sum()
wr = greens / (greens + reds) * 100 if (greens + reds) > 0 else 0

print(f"Entradas resolvidas : {total}")
print(f"Greens              : {greens}")
print(f"Reds                : {reds}")
print(f"Win Rate            : {wr:.1f}%")
print(f"P&L Total           : R$ {lucro:+.2f}")

# Por arquivo (dia)
print("\n=== POR DIA ===")
by_day = df.groupby("__arquivo").apply(lambda g: pd.Series({
    "total": len(g),
    "greens": g["Resultado"].str.contains("GREEN|VITORIA|VIT", na=False).sum(),
    "reds":   g["Resultado"].str.contains("RED|DERROTA|DER", na=False).sum(),
    "pnl":    g["PnL"].sum(),
})).reset_index()
by_day["wr%"] = (by_day["greens"] / (by_day["greens"] + by_day["reds"]) * 100).round(1)
by_day["pnl"] = by_day["pnl"].round(2)
print(by_day.to_string(index=False))

# Por método
print("\n=== POR MÉTODO ===")
met_col = next((c for c in df.columns if "metodo" in c.lower()), None)
if met_col:
    df["Metodo"] = df[met_col]
    by_met = df.groupby("Metodo").apply(lambda g: pd.Series({
        "total": len(g),
        "greens": g["Resultado"].str.contains("GREEN|VITORIA|VIT", na=False).sum(),
        "reds":   g["Resultado"].str.contains("RED|DERROTA|DER", na=False).sum(),
        "pnl":    g["PnL"].sum(),
    })).reset_index()
    by_met["wr%"] = (by_met["greens"] / (by_met["greens"] + by_met["reds"]) * 100).round(1)
    by_met["pnl"] = by_met["pnl"].round(2)
    print(by_met.to_string(index=False))
else:
    print("Coluna Metodo não encontrada")

# Por liga
print("\n=== TOP LIGAS (por reds) ===")
liga_col = next((c for c in df.columns if "liga" in c.lower() or "league" in c.lower()), None)
if liga_col:
    df["Liga"] = df[liga_col]
    by_liga = df.groupby("Liga").apply(lambda g: pd.Series({
        "total": len(g),
        "greens": g["Resultado"].str.contains("GREEN|VITORIA|VIT", na=False).sum(),
        "reds":   g["Resultado"].str.contains("RED|DERROTA|DER", na=False).sum(),
        "pnl":    g["PnL"].sum(),
    })).reset_index()
    by_liga["wr%"] = (by_liga["greens"] / (by_liga["greens"] + by_liga["reds"]) * 100).round(1)
    by_liga["pnl"] = by_liga["pnl"].round(2)
    by_liga_sorted = by_liga.sort_values("reds", ascending=False)
    print(by_liga_sorted.to_string(index=False))
else:
    print("Coluna Liga não encontrada")

print("\n=== ODDS DAS REDS ===")
reds_df = df[df["Resultado"].str.contains("RED|DERROTA|DER", na=False)]
odd_col = next((c for c in df.columns if "odd" in c.lower()), None)
if odd_col and not reds_df.empty:
    print(reds_df[[odd_col, "Resultado", "__arquivo"] + ([met_col] if met_col else []) + ([liga_col] if liga_col else [])].to_string(index=False))

print("\nFIM.")
