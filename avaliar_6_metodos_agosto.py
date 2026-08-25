import sys
sys.stdout.reconfigure(encoding='utf-8')
import joblib, numpy as np, pandas as pd

print("==================================================================", flush=True)
print("     AVALIAÇÃO DOS 6 NOVOS MÉTODOS NO MÊS DE AGOSTO DE 2026      ", flush=True)
print("==================================================================", flush=True)

import hist_rf_loader
df_hist = hist_rf_loader.load_hist_rf()
df_hist["Date"] = pd.to_datetime(df_hist["Date"])

aug_mask = (df_hist["Date"] >= "2026-08-01") & (df_hist["Date"] <= "2026-08-24")
df_aug = df_hist[aug_mask].dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()

COMMISSION = 0.045
STAKE = 100.0

df_aug["is_0x1"] = ((df_aug["Goals_H_FT"] == 0) & (df_aug["Goals_A_FT"] == 1)).astype(int)
df_aug["is_1x0"] = ((df_aug["Goals_H_FT"] == 1) & (df_aug["Goals_A_FT"] == 0)).astype(int)
df_aug["is_0x2"] = ((df_aug["Goals_H_FT"] == 0) & (df_aug["Goals_A_FT"] == 2)).astype(int)
df_aug["is_2x0"] = ((df_aug["Goals_H_FT"] == 2) & (df_aug["Goals_A_FT"] == 0)).astype(int)
df_aug["is_0x3"] = ((df_aug["Goals_H_FT"] == 0) & (df_aug["Goals_A_FT"] == 3)).astype(int)
df_aug["total_goals"] = df_aug["Goals_H_FT"] + df_aug["Goals_A_FT"]
df_aug["is_under45"] = (df_aug["total_goals"] <= 4.5).astype(int)

metodos_aug = [
    {"nome": "1. Lay 0x1", "odd_col": "Odd_CS_0x1", "tgt": "is_0x1", "mode": "LAY", "min": 6.0, "max": 16.0},
    {"nome": "2. Lay 1x0", "odd_col": "Odd_CS_1x0", "tgt": "is_1x0", "mode": "LAY", "min": 6.0, "max": 16.0},
    {"nome": "3. Lay 0x2", "odd_col": "Odd_CS_0x2", "tgt": "is_0x2", "mode": "LAY", "min": 8.0, "max": 20.0},
    {"nome": "4. Lay 2x0", "odd_col": "Odd_CS_2x0", "tgt": "is_2x0", "mode": "LAY", "min": 8.0, "max": 20.0},
    {"nome": "5. Lay 0x3", "odd_col": "Odd_CS_0x3", "tgt": "is_0x3", "mode": "LAY", "min": 15.0, "max": 35.0},
    {"nome": "6. Under 4.5 Gols", "odd_col": "Odd_Under45_FT", "tgt": "is_under45", "mode": "BACK", "min": 1.10, "max": 1.50}
]

res_aug = []
for m in metodos_aug:
    sub = df_aug.copy()
    sub["odd"] = pd.to_numeric(sub.get(m["odd_col"], np.nan), errors="coerce")
    sub = sub.dropna(subset=["odd", m["tgt"]]).copy()
    sub = sub[(sub["odd"] >= m["min"]) & (sub["odd"] <= m["max"])].copy()
    
    n = len(sub)
    if n > 0:
        if m["mode"] == "LAY":
            sub["win"] = 1 - sub[m["tgt"]]
            greens = (sub["win"] == 1).sum()
            reds = n - greens
            wr = greens / n * 100.0
            odd_m = sub["odd"].mean()
            be = ((odd_m - 1.0) / (odd_m - COMMISSION)) * 100.0
            pnl = np.where(sub["win"] == 1, STAKE * (1.0 - COMMISSION), -STAKE * (sub["odd"] - 1.0)).sum()
            roi = pnl / (n * STAKE) * 100.0
        else: # BACK UNDER 4.5
            sub["win"] = sub[m["tgt"]]
            greens = (sub["win"] == 1).sum()
            reds = n - greens
            wr = greens / n * 100.0
            odd_m = sub["odd"].mean()
            be = (1.0 / (odd_m - COMMISSION)) * 100.0
            pnl = np.where(sub["win"] == 1, STAKE * (sub["odd"] - 1.0) * (1.0 - COMMISSION), -STAKE).sum()
            roi = pnl / (n * STAKE) * 100.0
            
        res_aug.append({
            "Método": m["nome"],
            "Jogos em Agosto": n,
            "Greens": greens,
            "Reds": reds,
            "WR Real (%)": round(wr, 1),
            "BE (%)": round(be, 1),
            "Margem (%)": round(wr - be, 1),
            "Lucro (R$)": round(pnl, 2),
            "ROI (%)": round(roi, 1)
        })

df_out = pd.DataFrame(res_aug)
print(df_out.to_string(index=False))
