import glob
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Force UTF-8 for printing
if sys.stdout.encoding != 'utf-8':
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except Exception:
        pass

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
df = df[[c for c in df.columns if not str(c).startswith("Unnamed")]].copy()

# Normaliza colunas
df["Resultado"] = df["Resultado"].fillna("").astype(str).str.strip().str.upper()

def _pnl(row):
    res = str(row.get("Resultado", "")).strip().upper()
    if res in ("", "NAN", "PENDENTE", "-"):
        return np.nan
    v = row.get("Lucro_Prejuizo")
    if v is not None and pd.notna(v) and str(v).strip() not in ("", "nan"):
        try:
            return float(v)
        except Exception:
            pass
    return np.nan

df["PnL"] = df.apply(_pnl, axis=1)

print("=== RESUMO POR MÉTODO (Manual) ===")
resolvidas = df[df["PnL"].notna()]
if not resolvidas.empty:
    by_met = resolvidas.groupby("Metodo").apply(lambda g: pd.Series({
        "Total": len(g),
        "Greens": g["Resultado"].str.contains("GREEN|VITORIA|VIT", na=False).sum(),
        "Reds":   g["Resultado"].str.contains("RED|DERROTA|DER", na=False).sum(),
        "PnL":    g["PnL"].sum(),
    })).reset_index()
    by_met["WR%"] = (by_met["Greens"] / (by_met["Greens"] + by_met["Reds"]) * 100).round(1)
    print(by_met.to_string(index=False))
else:
    print("Nenhum PnL encontrado.")

print("\n=== RESUMO GERAL ===")
total_pnl = resolvidas["PnL"].sum() if not resolvidas.empty else 0
print(f"P&L Acumulado: R$ {total_pnl:,.2f}")

print("\n=== DETALHE LAY_CS_0X1_B365 ===")
sub0x1 = resolvidas[resolvidas["Metodo"] == "Lay_CS_0x1_B365"]
if not sub0x1.empty:
    print(f"Total: {len(sub0x1)} | PnL: {sub0x1['PnL'].sum():,.2f}")
    print("Amostra Reds (se houver):")
    reds0x1 = sub0x1[sub0x1["Resultado"].str.contains("RED", na=False)]
    if not reds0x1.empty:
        print(reds0x1[["Data", "Liga", "Jogo", "Odd_Base", "PnL"]].to_string(index=False))

print("\n=== DETALHE LAY_CS_1X0_B365 ===")
sub1x0 = resolvidas[resolvidas["Metodo"] == "Lay_CS_1x0_B365"]
if not sub1x0.empty:
    print(f"Total: {len(sub1x0)} | PnL: {sub1x0['PnL'].sum():,.2f}")
    print("Amostra Reds (se houver):")
    reds1x0 = sub1x0[sub1x0["Resultado"].str.contains("RED", na=False)]
    if not reds1x0.empty:
        print(reds1x0[["Data", "Liga", "Jogo", "Odd_Base", "PnL"]].to_string(index=False))
