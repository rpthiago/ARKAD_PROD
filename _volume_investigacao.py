"""
Investiga o volume anômalo de abril vs backtest histórico.
Pergunta central: por que abril gerou ~9.6 sinais/dia quando o histórico era ~1.5/dia?
"""
import glob, pandas as pd, numpy as np, json
from pathlib import Path

# ── Carrega dados de abril ───────────────────────────────────────────────────
DIR = Path("Apostas_Diarias")
frames = []
for arq in sorted(glob.glob(str(DIR / "Apostas_*.xlsx"))):
    df2 = pd.read_excel(arq)
    df2["__arquivo"] = Path(arq).name
    frames.append(df2)
abril = pd.concat(frames, ignore_index=True)
abril["Resultado"] = abril["Resultado"].fillna("").astype(str).str.strip().str.upper()

# Aplica filtros corretos por método
mask_0x1 = (abril["Metodo"]=="Lay_CS_0x1_B365") & (abril["Odd_Base"] >= 8.0) & (abril["Odd_Base"] <= 11.5)
mask_1x0 = (abril["Metodo"]=="Lay_CS_1x0_B365") & (abril["Odd_Base"] >= 4.5) & (abril["Odd_Base"] <= 11.5)
abril_filt = abril[mask_0x1 | mask_1x0].copy()

# ── Carrega histórico ────────────────────────────────────────────────────────
hist = pd.read_csv("recalculo_sem_combos_usuario.csv")
hist["Data_Arquivo"] = pd.to_datetime(hist["Data_Arquivo"])
hist["Metodo"] = hist["Metodo"].str.strip()

# Filtros corretos no histórico
mask_h0x1 = (hist["Metodo"]=="Lay_CS_0x1_B365") & (hist["Odd_Base"] >= 8.0) & (hist["Odd_Base"] <= 11.5)
mask_h1x0 = (hist["Metodo"]=="Lay_CS_1x0_B365") & (hist["Odd_Base"] >= 4.5) & (hist["Odd_Base"] <= 11.5)
hist_filt = hist[mask_h0x1 | mask_h1x0].copy()

print("=" * 65)
print("1. VOLUME COMPARADO: HISTÓRICO vs ABRIL")
print("=" * 65)

dias_hist = hist_filt["Data_Arquivo"].dt.date.nunique()
total_hist = len(hist_filt)
media_hist = total_hist / dias_hist
print(f"\nHistórico (ago/2025–abr/2026):")
print(f"  Entradas totais (c/filtros): {total_hist}")
print(f"  Dias com pelo menos 1 sinal: {dias_hist}")
print(f"  Média por dia ativo:         {media_hist:.2f}")

dias_abril = abril_filt["__arquivo"].nunique()
total_abril = len(abril_filt)
media_abril = total_abril / dias_abril
print(f"\nAbril 2026 (manual):")
print(f"  Entradas totais (c/filtros): {total_abril}")
print(f"  Dias com pelo menos 1 sinal: {dias_abril}")
print(f"  Média por dia ativo:         {media_abril:.2f}")
print(f"  RATIO vs histórico:          {media_abril/media_hist:.1f}x mais")

print()
print("=" * 65)
print("2. LIGAS: PRESENÇA NO HISTÓRICO vs ABRIL")
print("=" * 65)

ligas_hist = set(hist_filt["Liga"].str.strip().str.upper().unique())
ligas_abril = set(abril_filt["Liga"].str.strip().str.upper().unique())

so_abril = ligas_abril - ligas_hist
so_hist  = ligas_hist  - ligas_abril
em_ambos = ligas_hist  & ligas_abril

print(f"\nLigas no histórico:            {len(ligas_hist)}")
print(f"Ligas em abril (c/filtros):    {len(ligas_abril)}")
print(f"Ligas em ambos:                {len(em_ambos)}")
print(f"Só em abril (NOVAS):           {len(so_abril)}")
print(f"Só no histórico (sumiram):     {len(so_hist)}")

print(f"\nLigas novas (só em abril):")
for l in sorted(so_abril):
    sub = abril_filt[abril_filt["Liga"].str.strip().str.upper() == l]
    g = sub["Resultado"].str.contains("GREEN|VITORIA|VIT", na=False).sum()
    r = sub["Resultado"].str.contains("RED|DERROTA|DER", na=False).sum()
    wr = g/(g+r)*100 if (g+r) > 0 else 0
    print(f"  {l:<25} {len(sub):3d} ent | WR {wr:.0f}% ({g}G/{r}R)")

print(f"\nLigas históricas ausentes em abril:")
for l in sorted(so_hist):
    sub = hist_filt[hist_filt["Liga"].str.strip().str.upper() == l]
    print(f"  {l:<25} tinha {len(sub)} ent no histórico")

print()
print("=" * 65)
print("3. DISTRIBUIÇÃO DE ODDS: HISTÓRICO vs ABRIL")
print("=" * 65)

for metodo in ["Lay_CS_0x1_B365", "Lay_CS_1x0_B365"]:
    h = hist_filt[hist_filt["Metodo"] == metodo]["Odd_Base"]
    a = abril_filt[abril_filt["Metodo"] == metodo]["Odd_Base"]
    print(f"\n{metodo}:")
    print(f"  Histórico  — min={h.min():.1f} | max={h.max():.1f} | média={h.mean():.2f} | mediana={h.median():.2f} | n={len(h)}")
    print(f"  Abril      — min={a.min():.1f} | max={a.max():.1f} | média={a.mean():.2f} | mediana={a.median():.2f} | n={len(a)}")

print()
print("=" * 65)
print("4. HIPÓTESE: LIGAS HISTÓRICAS QUE EXPLODIRAM EM ABRIL")
print("=" * 65)

meses_hist = hist_filt["Data_Arquivo"].dt.to_period("M").nunique()
print(f"\n(Histórico cobre {meses_hist} meses)\n")
print(f"{'Liga':<25} {'Esperado/mês':>13} {'Real abr':>9} {'Ratio':>7}")
print("-" * 60)
for liga in sorted(em_ambos):
    h_sub = hist_filt[hist_filt["Liga"].str.strip().str.upper() == liga]
    a_sub = abril_filt[abril_filt["Liga"].str.strip().str.upper() == liga]
    esperado = len(h_sub) / meses_hist
    ratio = len(a_sub) / esperado if esperado > 0 else 0
    flag = " ⚠️" if ratio > 2 else ""
    print(f"{liga:<25} {esperado:>13.1f} {len(a_sub):>9} {ratio:>7.1f}x{flag}")

print()
print("=" * 65)
print("5. ENTRADAS POR LIGA: WR HISTÓRICO vs ABRIL")
print("=" * 65)
print(f"\n{'Liga':<25} {'WR Hist':>9} {'n Hist':>7} {'WR Abr':>9} {'n Abr':>6} {'P&L Abr':>10}")
print("-" * 70)
todas_ligas = sorted(ligas_hist | ligas_abril)
for liga in todas_ligas:
    h_sub = hist_filt[hist_filt["Liga"].str.strip().str.upper() == liga]
    a_sub = abril_filt[abril_filt["Liga"].str.strip().str.upper() == liga]
    if len(h_sub) == 0 and len(a_sub) == 0:
        continue
    hg = (h_sub["Resultado_1_0"] == 1.0).sum() if "Resultado_1_0" in h_sub.columns else 0
    hr = (h_sub["Resultado_1_0"] == 0.0).sum() if "Resultado_1_0" in h_sub.columns else 0
    hw = hg / (hg + hr) * 100 if (hg + hr) > 0 else 0
    ag = a_sub["Resultado"].str.contains("GREEN|VITORIA|VIT", na=False).sum()
    ar = a_sub["Resultado"].str.contains("RED|DERROTA|DER", na=False).sum()
    aw = ag / (ag + ar) * 100 if (ag + ar) > 0 else 0
    ap = a_sub["Lucro_Prejuizo"].sum() if "Lucro_Prejuizo" in a_sub.columns else 0
    flag = " 🔴" if ar >= 2 else (" ⚠️" if ar == 1 else "")
    print(f"{liga:<25} {hw:>8.0f}% {len(h_sub):>7} {aw:>8.0f}% {len(a_sub):>6} {ap:>+9.0f}{flag}")

print()
print("=" * 65)
print("6. DIAGNÓSTICO DO 1x0: WR POR LIGA HISTÓRICO vs ABRIL")
print("=" * 65)

h1x0 = hist_filt[hist_filt["Metodo"] == "Lay_CS_1x0_B365"]
a1x0 = abril_filt[abril_filt["Metodo"] == "Lay_CS_1x0_B365"]
h0x1 = hist_filt[hist_filt["Metodo"] == "Lay_CS_0x1_B365"]
a0x1 = abril_filt[abril_filt["Metodo"] == "Lay_CS_0x1_B365"]

print(f"\nHistórico: 1x0={len(h1x0)} | 0x1={len(h0x1)} | ratio 1x0/0x1: {len(h1x0)/len(h0x1):.2f}")
print(f"Abril:     1x0={len(a1x0)} | 0x1={len(a0x1)} | ratio 1x0/0x1: {len(a1x0)/len(a0x1):.2f}")
print()
print("WR por liga para Lay_CS_1x0_B365:")
ligas_1x0 = set(h1x0["Liga"].str.strip().str.upper().unique()) | set(a1x0["Liga"].str.strip().str.upper().unique())
print(f"{'Liga':<25} {'WR Hist':>9} {'n Hist':>7} {'WR Abr':>9} {'n Abr':>6} {'P&L Abr':>10}")
print("-" * 70)
for liga in sorted(ligas_1x0):
    hs = h1x0[h1x0["Liga"].str.strip().str.upper() == liga]
    as_ = a1x0[a1x0["Liga"].str.strip().str.upper() == liga]
    hg = (hs["Resultado_1_0"] == 1.0).sum() if "Resultado_1_0" in hs.columns else 0
    hr = (hs["Resultado_1_0"] == 0.0).sum() if "Resultado_1_0" in hs.columns else 0
    hw = hg / (hg + hr) * 100 if (hg + hr) > 0 else 0
    ag = as_["Resultado"].str.contains("GREEN|VITORIA|VIT", na=False).sum()
    ar = as_["Resultado"].str.contains("RED|DERROTA|DER", na=False).sum()
    aw = ag / (ag + ar) * 100 if (ag + ar) > 0 else 0
    ap = as_["Lucro_Prejuizo"].sum() if "Lucro_Prejuizo" in as_.columns else 0
    flag = " 🔴" if ar >= 2 else (" ⚠️" if ar == 1 else "")
    print(f"{liga:<25} {hw:>8.0f}% {len(hs):>7} {aw:>8.0f}% {len(as_):>6} {ap:>+9.0f}{flag}")

print()
print("=" * 65)
print("7. VOLUME: ONDE O HISTÓRICO ACUMULOU SINAIS ABRIL/2026")
print("=" * 65)

# Pega apenas registros do histórico de abril/2026
hist_abril = hist_filt[hist_filt["Data_Arquivo"].dt.month == 4]
print(f"\nHistórico com datas em abril/2026: {len(hist_abril)} entradas")
if len(hist_abril) > 0:
    vol_hist_abril = hist_abril.groupby("Data_Arquivo").size().reset_index(name="entradas")
    print(f"Média/dia histórico em abril:       {vol_hist_abril['entradas'].mean():.1f}")
    print(vol_hist_abril.to_string(index=False))

print()
print("FIM.")
