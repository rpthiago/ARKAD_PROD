import sys
sys.stdout.reconfigure(encoding='utf-8')
import hist_rf_loader, pandas as pd, numpy as np
import unicodedata, re

df_hist = hist_rf_loader.load_hist_rf()

def _canon(s):
    if pd.isna(s) or not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

df_hist["c_Home"] = df_hist["Home"].map(_canon)
df_hist["c_Away"] = df_hist["Away"].map(_canon)

# Construir visão consolidada de cada time (jogos em casa E fora)
home_records = df_hist[["Date", "c_Home", "Goals_H_FT", "Goals_A_FT", "xGOT_H_FT", "xGOT_Faced_H_FT", "Goals_Prevented_H_FT", "Big_Chances_H_FT", "Shots_On_Target_H_FT", "Possession_H_FT"]].copy()
home_records.columns = ["Date", "Team", "Gf", "Gc", "xGOT", "xGOT_faced", "GP", "BC", "SoT", "Poss"]
home_records["won"] = (home_records["Gf"] > home_records["Gc"]).astype(float)
home_records["draw"] = (home_records["Gf"] == home_records["Gc"]).astype(float)

away_records = df_hist[["Date", "c_Away", "Goals_A_FT", "Goals_H_FT", "xGOT_A_FT", "xGOT_Faced_A_FT", "Goals_Prevented_A_FT", "Big_Chances_A_FT", "Shots_On_Target_A_FT", "Possession_A_FT"]].copy()
away_records.columns = ["Date", "Team", "Gf", "Gc", "xGOT", "xGOT_faced", "GP", "BC", "SoT", "Poss"]
away_records["won"] = (away_records["Gf"] > away_records["Gc"]).astype(float)
away_records["draw"] = (away_records["Gf"] == away_records["Gc"]).astype(float)

all_team_matches = pd.concat([home_records, away_records], ignore_index=True)
all_team_matches = all_team_matches.sort_values(["Team", "Date"], kind="mergesort").reset_index(drop=True)

print(f"Total de registros de times consolidados: {len(all_team_matches)} linhas", flush=True)
print(f"Total de times únicos na base: {all_team_matches['Team'].nunique()}", flush=True)
