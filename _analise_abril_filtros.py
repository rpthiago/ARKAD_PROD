"""Análise completa com filtros corretos (odd range + blacklist)"""
import glob, pandas as pd, numpy as np, json
from pathlib import Path

DIR = Path("Apostas_Diarias")
frames = [pd.read_excel(arq) for arq in sorted(glob.glob(str(DIR / "Apostas_*.xlsx")))]
df = pd.concat(frames, ignore_index=True)
df["Resultado"] = df["Resultado"].fillna("").astype(str).str.strip().str.upper()
df["PnL"] = df["Lucro_Prejuizo"]

# Filtro correto por metodo (conforme config_prod_v1.json)
mask_0x1 = (df["Metodo"]=="Lay_CS_0x1_B365") & (df["Odd_Base"] >= 8.0) & (df["Odd_Base"] <= 11.5)
mask_1x0 = (df["Metodo"]=="Lay_CS_1x0_B365") & (df["Odd_Base"] >= 4.5) & (df["Odd_Base"] <= 11.5)
filtrados = df[mask_0x1 | mask_1x0].copy()

# Blacklist
with open("config_rodos_master.json") as f:
    rodos_cfg = json.load(f)
regras = rodos_cfg["filtros_rodo"]

def is_bloqueado(row):
    for r in regras:
        liga_r  = r.get("league", "").strip().upper()
        met_r   = r.get("method_equals", "").strip()
        odd_min = r.get("odd_min") or -99
        odd_max = r.get("odd_max") or 999
        if (str(row["Liga"]).strip().upper() == liga_r and
            str(row["Metodo"]).strip() == met_r and
            odd_min <= row["Odd_Base"] <= odd_max):
            return r["name"]
    return None

filtrados["rodo"] = filtrados.apply(is_bloqueado, axis=1)
livres     = filtrados[filtrados["rodo"].isna()].copy()
bloqueadas = filtrados[filtrados["rodo"].notna()].copy()

greens = livres["Resultado"].str.contains("GREEN|VITORIA|VIT", na=False).sum()
reds   = livres["Resultado"].str.contains("RED|DERROTA|DER", na=False).sum()
pnl    = livres["PnL"].sum()
wr     = greens / (greens + reds) * 100

print("=== RESULTADO COM FILTROS COMPLETOS (odd range + blacklist) ===")
print(f"Passam: {len(livres)} | Greens: {greens} | Reds: {reds} | WR: {wr:.1f}% | P&L: R$ {pnl:+.2f}")
print()
for met in livres["Metodo"].unique():
    sub = livres[livres["Metodo"] == met]
    g = sub["Resultado"].str.contains("GREEN|VITORIA|VIT", na=False).sum()
    r = sub["Resultado"].str.contains("RED|DERROTA|DER", na=False).sum()
    p = sub["PnL"].sum()
    wr2 = g / (g + r) * 100 if (g + r) > 0 else 0
    print(f"  {met}: {len(sub)} ent | WR {wr2:.1f}% | P&L R$ {p:+.2f}")

print()
print(f"=== BLOQUEADAS pela blacklist: {len(bloqueadas)} ===")
gb = bloqueadas["Resultado"].str.contains("GREEN|VITORIA|VIT", na=False).sum()
rb = bloqueadas["Resultado"].str.contains("RED|DERROTA|DER", na=False).sum()
pb = bloqueadas["PnL"].sum()
wrb = gb / (gb + rb) * 100 if (gb + rb) > 0 else 0
print(f"Greens: {gb} | Reds: {rb} | WR: {wrb:.1f}% | P&L: R$ {pb:+.2f}")
print(bloqueadas[["Liga","Metodo","Odd_Base","Resultado","PnL","rodo"]].to_string(index=False))

print()
print("=== REDS nas entradas que PASSAM nos filtros ===")
reds_df = livres[livres["Resultado"].str.contains("RED", na=False)]
print(reds_df[["Odd_Base","Metodo","Liga","PnL"]].to_string(index=False))
total_reds_pnl = reds_df["PnL"].sum()
print(f"Total reds: {len(reds_df)} | P&L das reds: R$ {total_reds_pnl:+.2f}")

print()
print("=== RESUMO GERAL DO MES ===")
print(f"  Total apostado (real):          {len(df)} entradas")
print(f"  Passaria no filtro completo:    {len(livres)} entradas")
print(f"  Diferença (fora do sistema):    {len(df) - len(livres)} entradas apostadas indevidamente")
print()
# Volume diario para as entradas que passariam
livres2 = livres.copy()
livres2["__arquivo"] = df.loc[livres.index, "__arquivo"]
vol_correto = livres2.groupby("__arquivo").size().reset_index(name="entradas")
print(f"  Volume diario COM filtros: media={vol_correto['entradas'].mean():.1f} | max={vol_correto['entradas'].max()}")
print()
print("Volume por dia (entradas que passariam):")
print(vol_correto.to_string(index=False))
