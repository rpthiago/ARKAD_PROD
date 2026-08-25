import sys
sys.stdout.reconfigure(encoding='utf-8')
import os, joblib, numpy as np, pandas as pd
from datetime import datetime

print("==================================================================", flush=True)
print("     ARENA DE MACHINE LEARNING & OTIMIZAÇÃO - LAY 0X0 (ARKAD)     ", flush=True)
print("==================================================================", flush=True)

# 1. Carregar base histórica completa
import hist_rf_loader
df_raw = hist_rf_loader.load_hist_rf()
print(f"[+] Base histórica carregada: {len(df_raw)} jogos", flush=True)

# 2. Carregar base oficial Betfair com Odd_CS_0x0_Lay real
print("[*] Carregando base oficial Betfair com Odd_CS_0x0_Lay real...", flush=True)
df_bf = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair.csv", low_memory=False)
df_bf["Date"] = pd.to_datetime(df_bf["Date"], errors="coerce")
df_bf = df_bf.dropna(subset=["Date", "Home", "Away", "Odd_CS_0x0_Lay"]).copy()
df_bf["Odd_CS_0x0_Lay"] = pd.to_numeric(df_bf["Odd_CS_0x0_Lay"], errors="coerce")
df_bf = df_bf[(df_bf["Odd_CS_0x0_Lay"] >= 5.0) & (df_bf["Odd_CS_0x0_Lay"] <= 35.0)].copy()

import unicodedata, re
def _canon(s):
    if pd.isna(s) or not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

df_bf["c_Home"] = df_bf["Home"].map(_canon)
df_bf["c_Away"] = df_bf["Away"].map(_canon)
df_bf["Date_str"] = df_bf["Date"].dt.strftime("%Y-%m-%d")

# 3. Construir séries temporais de mando
df = df_raw.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()
df["Date"] = pd.to_datetime(df["Date"])
df = df.sort_values("Date", kind="mergesort").reset_index(drop=True)

stat_cols = [
    "Goals_H_FT", "Goals_A_FT", "xGOT_H_FT", "xGOT_A_FT",
    "xGOT_Faced_H_FT", "xGOT_Faced_A_FT",
    "Goals_Prevented_H_FT", "Goals_Prevented_A_FT",
    "Big_Chances_H_FT", "Big_Chances_A_FT",
    "Shots_On_Target_H_FT", "Shots_On_Target_A_FT",
    "Possession_H_FT", "Possession_A_FT"
]
for c in stat_cols:
    df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0) if c in df.columns else 0.0

df["is_0x0"] = ((df["Goals_H_FT"] == 0) & (df["Goals_A_FT"] == 0)).astype(float)
df["won_h"] = (df["Goals_H_FT"] > df["Goals_A_FT"]).astype(float)
df["won_a"] = (df["Goals_A_FT"] > df["Goals_H_FT"]).astype(float)

def _decay_roll_grouped_unshifted(df_in, group_col, val_col, window=6, alpha=0.25):
    g = df_in.groupby(group_col)[val_col]
    numer = np.zeros(len(df_in)); count = np.zeros(len(df_in)); wsum = 0.0
    for j in range(window):
        sj = g.shift(j + 1)
        ej = np.exp(-alpha * j)
        m = sj.notna().to_numpy()
        numer += np.where(m, np.nan_to_num(sj.to_numpy()) * ej, 0.0)
        count += m
        wsum += ej
    res = numer / wsum
    res[count < 3] = np.nan
    return pd.Series(res, index=df_in.index)

print("[*] Construindo features de mando para o Lay 0x0...", flush=True)
dh_map = [
    ("Goals_H_FT", "H_h_Gf"), ("Goals_A_FT", "H_h_Gc"), ("xGOT_H_FT", "H_h_xGOT"),
    ("xGOT_Faced_H_FT", "H_h_xGOT_faced"), ("Goals_Prevented_H_FT", "H_h_GP"),
    ("Big_Chances_H_FT", "H_h_BC"), ("Shots_On_Target_H_FT", "H_h_SoT"),
    ("Possession_H_FT", "H_h_Poss"), ("won_h", "H_h_WR"), ("is_0x0", "H_h_0x0_rate")
]
for raw_c, feat_c in dh_map:
    df[feat_c] = _decay_roll_grouped_unshifted(df, "Home", raw_c)

da_map = [
    ("Goals_A_FT", "A_a_Gf"), ("Goals_H_FT", "A_a_Gc"), ("xGOT_A_FT", "A_a_xGOT"),
    ("xGOT_Faced_A_FT", "A_a_xGOT_faced"), ("Goals_Prevented_A_FT", "A_a_GP"),
    ("Big_Chances_A_FT", "A_a_BC"), ("Shots_On_Target_A_FT", "A_a_SoT"),
    ("Possession_A_FT", "A_a_Poss"), ("won_a", "A_a_WR"), ("is_0x0", "A_a_0x0_rate")
]
for raw_c, feat_c in da_map:
    df[feat_c] = _decay_roll_grouped_unshifted(df, "Away", raw_c)

df["liga_0x0_rate"] = df.groupby("League")["is_0x0"].transform(lambda x: x.shift(1).rolling(100, min_periods=20).mean())

df["c_Home"] = df["Home"].map(_canon)
df["c_Away"] = df["Away"].map(_canon)
df["Date_str"] = df["Date"].dt.strftime("%Y-%m-%d")

# Interações
df["total_WR"] = df["H_h_WR"] + df["A_a_WR"]
df["wr_diff"] = abs(df["H_h_WR"] - df["A_a_WR"])
df["total_xGOT"] = df["H_h_xGOT"] + df["A_a_xGOT"]
df["total_Gf"] = df["H_h_Gf"] + df["A_a_Gf"]
df["total_0x0_rate"] = (df["H_h_0x0_rate"] + df["A_a_0x0_rate"]) / 2.0

# Merge com a Odd de Lay 0x0 Real da Betfair
print("[*] Cruzando base com as Odds de Lay 0x0 Reais da Betfair...", flush=True)
df_merged = pd.merge(
    df,
    df_bf[["Date_str", "c_Home", "c_Away", "Odd_CS_0x0_Lay"]],
    on=["Date_str", "c_Home", "c_Away"],
    how="inner"
)

df_merged["mkt_prob_0x0"] = 1.0 / df_merged["Odd_CS_0x0_Lay"]

features_0x0 = [
    'H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_0x0_rate',
    'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_0x0_rate',
    'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf', 'total_0x0_rate',
    'mkt_prob_0x0', 'liga_0x0_rate'
]

# Target: 1 se Lay Ganhou (NÃO foi 0x0), 0 se foi 0x0
df_merged["target_lay_win"] = (1.0 - df_merged["is_0x0"]).astype(int)

df_clean = df_merged.dropna(subset=features_0x0 + ["target_lay_win", "Odd_CS_0x0_Lay"]).copy()
print(f"[+] Total de jogos com Odd_CS_0x0_Lay e features completas: {len(df_clean)}", flush=True)

# Divisão Temporal
train_mask = (df_clean["Date"] >= "2025-08-01") & (df_clean["Date"] <= "2026-03-31")
test_mask  = (df_clean["Date"] >= "2026-04-01") & (df_clean["Date"] <= "2026-07-31")

df_train = df_clean[train_mask].copy()
df_test  = df_clean[test_mask].copy()

print(f"\n[+] Treino Lay 0x0 (Ago/2025 a Mar/2026): {len(df_train)} jogos | Taxa Não-0x0: {df_train['target_lay_win'].mean()*100:.2f}%")
print(f"[+] Teste Cego OOS (Abr/2026 a Jul/2026): {len(df_test)} jogos | Taxa Não-0x0: {df_test['target_lay_win'].mean()*100:.2f}%\n")

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import log_loss, brier_score_loss, roc_auc_score
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
import lightgbm as lgb
import xgboost as xgb

scaler = StandardScaler()
X_train = scaler.fit_transform(df_train[features_0x0])
y_train = df_train["target_lay_win"].to_numpy()

X_test = scaler.transform(df_test[features_0x0])
y_test = df_test["target_lay_win"].to_numpy()

models_dict = {
    "1. Extra Trees": ExtraTreesClassifier(n_estimators=200, max_depth=6, min_samples_leaf=15, random_state=42, n_jobs=-1),
    "2. Random Forest": RandomForestClassifier(n_estimators=200, max_depth=6, min_samples_leaf=15, random_state=42, n_jobs=-1),
    "3. LightGBM": lgb.LGBMClassifier(n_estimators=150, max_depth=4, learning_rate=0.03, min_child_samples=25, subsample=0.8, colsample_bytree=0.8, random_state=42, verbose=-1),
    "4. XGBoost": xgb.XGBClassifier(n_estimators=150, max_depth=4, learning_rate=0.03, min_child_weight=5, subsample=0.8, colsample_bytree=0.8, random_state=42, eval_metric='logloss'),
    "5. Regressão Logística (L2)": LogisticRegression(C=0.1, penalty='l2', solver='lbfgs', max_iter=500, random_state=42)
}

COMMISSION = 0.045
STAKE = 100.0

results_models_0x0 = []

for name, clf in models_dict.items():
    print(f"[*] Treinando {name}...", flush=True)
    cal_clf = CalibratedClassifierCV(clf, cv=3, method='sigmoid')
    cal_clf.fit(X_train, y_train)
    probs_test = cal_clf.predict_proba(X_test)[:, 1]
    
    brier = brier_score_loss(y_test, probs_test)
    auc = roc_auc_score(y_test, probs_test)
    
    df_eval_m = df_test.copy()
    df_eval_m["prob_lay"] = probs_test
    df_eval_m["ev_lay"] = df_eval_m["prob_lay"] * (1.0 - COMMISSION) - (1.0 - df_eval_m["prob_lay"]) * (df_eval_m["Odd_CS_0x0_Lay"] - 1.0)
    df_eval_m["pnl_lay"] = np.where(
        df_eval_m["target_lay_win"] == 1,
        STAKE * (1.0 - COMMISSION),
        -STAKE * (df_eval_m["Odd_CS_0x0_Lay"] - 1.0)
    )
    
    # Filtro: Odd Lay 0x0 [6.00, 16.00] + Prob >= 93% + xGOT >= 2.0 + EV >= 0.01
    cond_filt = (
        (df_eval_m["Odd_CS_0x0_Lay"] >= 6.00) &
        (df_eval_m["Odd_CS_0x0_Lay"] <= 16.00) &
        (df_eval_m["prob_lay"] >= 0.93) &
        (df_eval_m["total_xGOT"] >= 2.00) &
        (df_eval_m["ev_lay"] >= 0.01)
    )
    sub = df_eval_m[cond_filt]
    n_ops = len(sub)
    if n_ops > 0:
        greens = (sub["target_lay_win"] == 1).sum()
        reds = n_ops - greens
        wr = (greens / n_ops) * 100.0
        avg_odd = sub["Odd_CS_0x0_Lay"].mean()
        be_wr = ((avg_odd - 1.0) / (avg_odd - COMMISSION)) * 100.0
        profit = sub["pnl_lay"].sum()
        roi = (profit / (n_ops * STAKE)) * 100.0
        delta_wr = wr - be_wr
    else:
        greens, reds, wr, be_wr, profit, roi, delta_wr = 0, 0, 0, 0, 0, 0, 0

    results_models_0x0.append({
        "Modelo": name,
        "Brier Score": round(brier, 4),
        "AUC-ROC": round(auc, 4),
        "Apostas Cegas": n_ops,
        "Greens": greens,
        "Reds": reds,
        "WR Cega (%)": round(wr, 1),
        "BE WR (%)": round(be_wr, 1),
        "Margem Real (%)": round(delta_wr, 1),
        "Lucro Cego (R$)": round(profit, 2),
        "ROI Cego (%)": round(roi, 1)
    })

df_res_0x0 = pd.DataFrame(results_models_0x0).sort_values("ROI Cego (%)", ascending=False).reset_index(drop=True)

print("\n==================================================================")
print("       PLACAR DA ARENA DE MODELOS - LAY 0X0 (TESTE CEGO 2026)     ")
print("==================================================================")
print(df_res_0x0.to_string(index=False))

# Salvar modelo campeão
joblib.dump(cal_clf, "modelo_lay_0x0_arena.pkl")
joblib.dump(scaler, "scaler_lay_0x0_arena.pkl")
joblib.dump(features_0x0, "features_lay_0x0_arena.pkl")
print("\n[+] Artefatos do Lay 0x0 salvos com sucesso!", flush=True)
