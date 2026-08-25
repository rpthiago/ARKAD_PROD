import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
import unicodedata, re

print("==================================================================", flush=True)
print("  AUDITORIA COM ODD DE LAY REAL DA BETFAIR (Odd_D_Lay) - 2026    ", flush=True)
print("==================================================================", flush=True)

# 1. Carregar base de avaliação gerada
df_eval = pd.read_feather("df_eval_lay_draw.feather")
df_2026 = df_eval[df_eval["Date"].dt.year == 2026].copy()

# 2. Carregar base Betfair com Odd_D_Lay real
print("[*] Carregando base oficial Betfair com Odd_D_Lay real...", flush=True)
df_bf = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair.csv", low_memory=False)
df_bf["Date"] = pd.to_datetime(df_bf["Date"], errors="coerce")
df_bf = df_bf[df_bf["Date"].dt.year == 2026].dropna(subset=["Date", "Home", "Away", "Odd_D_Lay"]).copy()

def _canon(s):
    if pd.isna(s) or not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

df_2026["c_Home"] = df_2026["Home"].map(_canon)
df_2026["c_Away"] = df_2026["Away"].map(_canon)
df_2026["Date_str"] = df_2026["Date"].dt.strftime("%Y-%m-%d")

df_bf["c_Home"] = df_bf["Home"].map(_canon)
df_bf["c_Away"] = df_bf["Away"].map(_canon)
df_bf["Date_str"] = df_bf["Date"].dt.strftime("%Y-%m-%d")
df_bf["Odd_D_Lay_Real"] = pd.to_numeric(df_bf["Odd_D_Lay"], errors="coerce")

# Merge para casar o jogo com a Odd_D_Lay real
df_merged = pd.merge(
    df_2026,
    df_bf[["Date_str", "c_Home", "c_Away", "Odd_D_Lay_Real"]],
    on=["Date_str", "c_Home", "c_Away"],
    how="left"
)

# Onde não casar exato, aplicar spread médio conservador de Betfair (+5% sobre o back)
df_merged["Odd_D_Lay_Final"] = np.where(
    df_merged["Odd_D_Lay_Real"].notna() & (df_merged["Odd_D_Lay_Real"] > 1.0),
    df_merged["Odd_D_Lay_Real"],
    df_merged["Odd_D_FT"] * 1.05 # Spread padrão
)

print(f"[+] Total de jogos 2026 avaliados: {len(df_merged)}")
print(f"[+] Jogos com Odd_D_Lay exata da Betfair casada: {df_merged['Odd_D_Lay_Real'].notna().sum()}/{len(df_merged)}")

# Comparar Odd Back (b365) vs Odd Lay Real (Betfair)
media_back = df_merged["Odd_D_FT"].mean()
media_lay = df_merged["Odd_D_Lay_Final"].mean()
print(f"Odd Média Back (Bet365): {media_back:.2f} | Odd Média Lay Real (Betfair): {media_lay:.2f} (Spread: +{(media_lay/media_back - 1)*100:.1f}%)")

# Aplicar a estratégia com a Odd de Lay Real
COMMISSION = 0.05
STAKE = 100.0

# Recalcular EV e Break-even com a Odd de Lay Real
df_merged["ev_lay_real"] = df_merged["prob_lay_win"] * (1.0 - COMMISSION) - (1.0 - df_merged["prob_lay_win"]) * (df_merged["Odd_D_Lay_Final"] - 1.0)
df_merged["be_wr_real"] = (df_merged["Odd_D_Lay_Final"] - 1.0) / (df_merged["Odd_D_Lay_Final"] - COMMISSION)

# Recalcular PnL com a Odd de Lay Real
df_merged["pnl_lay_real"] = np.where(
    df_merged["lay_win"] == 1,
    STAKE * (1.0 - COMMISSION),
    -STAKE * (df_merged["Odd_D_Lay_Final"] - 1.0)
)

# Filtro da estratégia na Odd de Lay Real
cond = (
    (df_merged["Odd_D_Lay_Final"] >= 3.00) &
    (df_merged["Odd_D_Lay_Final"] <= 4.80) &
    (df_merged["prob_lay_win"] >= 0.80) &
    (df_merged["total_xGOT"] >= 2.20) &
    (df_merged["ev_lay_real"] >= 0.02)
)

sub_real = df_merged[cond].copy().sort_values("Date", kind="mergesort").reset_index(drop=True)

n = len(sub_real)
greens = (sub_real["lay_win"] == 1).sum()
reds = n - greens
wr = (greens / n) * 100.0 if n > 0 else 0.0
avg_odd_lay = sub_real["Odd_D_Lay_Final"].mean()
be_wr = ((avg_odd_lay - 1.0) / (avg_odd_lay - 0.05)) * 100.0 if n > 0 else 0.0
profit_real = sub_real["pnl_lay_real"].sum()
roi_real = (profit_real / (n * STAKE)) * 100.0 if n > 0 else 0.0

gross_win = greens * (STAKE * (1.0 - COMMISSION))
gross_loss = ((sub_real[sub_real["lay_win"] == 0]["Odd_D_Lay_Final"] - 1.0) * STAKE).sum()
pf_real = gross_win / gross_loss if gross_loss > 0 else 999.0

print(f"\n==================================================")
print(f"   RESULTADO REAL NA ODD DE LAY EXECUTÁVEL BETFAIR ")
print(f"==================================================")
print(f"Total de Entradas: {n}")
print(f"Greens: {greens} ({wr:.2f}%) | Reds: {reds} ({100-wr:.2f}%)")
print(f"Odd Média Lay Betfair: {avg_odd_lay:.2f}")
print(f"Break-even Win Rate Real: {be_wr:.2f}% (Margem Real: {wr - be_wr:+.2f}%)")
print(f"Lucro Líquido Real (Stake R$ 100): R$ {profit_real:,.2f}")
print(f"ROI Líquido Real: {roi_real:+.2f}%")
print(f"Profit Factor Real: {pf_real:.2f}")

# Detalhamento Mês a Mês na Odd Real
sub_real["Mes"] = sub_real["Date"].dt.strftime("%Y-%m (%B)")
meses_real = sub_real.groupby("Mes").agg(
    jogos=("lay_win", "count"),
    greens=("lay_win", "sum"),
    odd_lay_media=("Odd_D_Lay_Final", "mean"),
    lucro=("pnl_lay_real", "sum")
).reset_index()

meses_real["reds"] = meses_real["jogos"] - meses_real["greens"]
meses_real["wr"] = (meses_real["greens"] / meses_real["jogos"]) * 100.0
meses_real["be_wr"] = ((meses_real["odd_lay_media"] - 1.0) / (meses_real["odd_lay_media"] - 0.05)) * 100.0
meses_real["roi"] = (meses_real["lucro"] / (meses_real["jogos"] * STAKE)) * 100.0

print("\n--- PERFORMANCE MÊS A MÊS NA ODD REAL DE LAY BETFAIR ---")
cols_mes = ["Mes", "jogos", "greens", "reds", "wr", "be_wr", "lucro", "roi"]
print(meses_real[cols_mes].to_string(index=False))
