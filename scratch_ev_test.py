import pandas as pd
import numpy as np
import re
import unicodedata
from pathlib import Path

def canon(s):
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

# Paths
base_dir = Path("c:/Users/thiag/OneDrive/Documentos/GitHub/ARKAD_PROD")
picks_path = base_dir / "wf_0x1_bets.csv"
results_path = base_dir / "Resultados_2026_Full.csv"

print("Loading datasets...")
df_picks = pd.read_csv(picks_path)
print(f"Loaded {len(df_picks)} picks from wf_0x1_bets.csv")

# We only need key columns to speed up loading
cols_to_use = ["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT", "TotalGoals_FT", "Odd_Over15_FT", "Over15_Realizado"]
df_res = pd.read_csv(results_path, use_cols=cols_to_use) if hasattr(pd, "read_csv_cols") else pd.read_csv(results_path, low_memory=False)
df_res = df_res[cols_to_use].copy()
print(f"Loaded {len(df_res)} matches from Resultados_2026_Full.csv")

# Preprocess keys for matching
df_picks["d"] = pd.to_datetime(df_picks["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
df_picks["ch"] = df_picks["Home"].map(canon)
df_picks["ca"] = df_picks["Away"].map(canon)

df_res["d"] = pd.to_datetime(df_res["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
df_res["ch"] = df_res["Home"].map(canon)
df_res["ca"] = df_res["Away"].map(canon)

# Match exact (date + home + away)
print("Matching picks with historical results...")
exact_map = {}
for idx, row in df_res.iterrows():
    exact_map[(row["d"], row["ch"], row["ca"])] = row

matched_rows = []
not_found = 0
for idx, row in df_picks.iterrows():
    key = (row["d"], row["ch"], row["ca"])
    match = exact_map.get(key)
    if match is not None:
        matched_rows.append({
            "mes": row["mes"],
            "Date": row["Date"],
            "Home": row["Home"],
            "Away": row["Away"],
            "Goals_H_FT": match["Goals_H_FT"],
            "Goals_A_FT": match["Goals_A_FT"],
            "TotalGoals_FT": match["TotalGoals_FT"],
            "Odd_Over15_FT": match["Odd_Over15_FT"],
            "Over15_Realizado": match["Over15_Realizado"]
        })
    else:
        not_found += 1

df_matched = pd.DataFrame(matched_rows)
print(f"Successfully matched: {len(df_matched)} | Not matched: {not_found}")

if df_matched.empty:
    print("No matches could be linked.")
    exit()

# Evaluate Over 1.5 FT betting
# We filter games with a valid Odd_Over15_FT (> 1.0)
df_matched["Odd_Over15_FT"] = pd.to_numeric(df_matched["Odd_Over15_FT"], errors="coerce")
df_matched = df_matched[df_matched["Odd_Over15_FT"] > 1.0].copy()
print(f"Matched picks with valid Over 1.5 odds: {len(df_matched)}")

# Over 1.5 prediction: we win if TotalGoals_FT >= 2 or Over15_Realizado is True
df_matched["Win_Over15"] = (df_matched["TotalGoals_FT"] >= 2) | (df_matched["Over15_Realizado"] == True) | (df_matched["Over15_Realizado"] == "True")

# Return on investment (ROI) calculation
# Stake = 1 unit per bet
df_matched["PnL_Over15"] = np.where(df_matched["Win_Over15"], df_matched["Odd_Over15_FT"] - 1.0, -1.0)

win_rate = df_matched["Win_Over15"].mean()
total_pnl = df_matched["PnL_Over15"].sum()
roi = total_pnl / len(df_matched)

print("\n" + "="*50)
print("BACKTEST OVER 1.5 FT RESULTS (using Lay 0x1 Picks)")
print("="*50)
print(f"Total Bets: {len(df_matched)}")
print(f"Win Rate:   {win_rate:.2%}")
print(f"Total PnL:  {total_pnl:+.2f} units")
print(f"ROI:        {roi:+.2%}")
print("="*50)

# Group by month
df_matched["mes"] = df_matched["mes"].astype(str)
grouped = df_matched.groupby("mes").agg(
    n=("Win_Over15", "count"),
    win_rate=("Win_Over15", "mean"),
    pnl=("PnL_Over15", "sum"),
    roi=("PnL_Over15", lambda x: x.sum() / len(x))
).reset_index()

print("\nResults by Month:")
print(f"{'Month':<10}{'Bets':>6}{'Win Rate':>12}{'PnL':>10}{'ROI':>10}")
for _, r in grouped.iterrows():
    print(f"{r['mes']:<10}{int(r['n']):>6}{r['win_rate']:>12.2%}{r['pnl']:>+10.2f}{r['roi']:>+10.2%}")
