import sys
sys.stdout.reconfigure(encoding='utf-8')
import os, joblib, numpy as np, pandas as pd
from datetime import datetime

print("==================================================================", flush=True)
print("     ARENA DE MACHINE LEARNING & OTIMIZAÇÃO - LAY 2X2 (ARKAD)     ", flush=True)
print("==================================================================", flush=True)

# 1. Carregar base histórica completa
import hist_rf_loader
df_raw = hist_rf_loader.load_hist_rf()
print(f"[+] Base histórica carregada: {len(df_raw)} jogos", flush=True)

# 2. Carregar base oficial Betfair com Odd_CS_2x2_Lay real
print("[*] Carregando base oficial Betfair com Odd_CS_2x2_Lay real...", flush=True)
df_bf = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair.csv", low_memory=False)
df_bf["Date"] = pd.to_datetime(df_bf["Date"], errors="coerce")
df_bf = df_bf.dropna(subset=["Date", "Home", "Away", "Odd_CS_2x2_Lay"]).copy()
df_bf["Odd_CS_2x2_Lay"] = pd.to_numeric(df_bf["Odd_CS_2x2_Lay"], errors="coerce")
df_bf = df_bf[(df_bf["Odd_CS_2x2_Lay"] >= 5.0) & (df_bf["Odd_CS_2x2_Lay"] <= 35.0)].copy()

import unicodedata, re
def _canon(s):
    if pd.isna(s) or not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

df_bf["c_Home"] = df_bf["Home"].map(_canon)
df_bf["c_Away"] = df_bf["Away"].map(_canon)
df_bf["Date_str"] = df_bf["Date"].dt.strftime("%Y-%m-%d")

# 3. Construir séries temporais de mando com shift(1) unshifted
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

df["is_2x2"] = ((df["Goals_H_FT"] == 2) & (df["Goals_A_FT"] == 2)).astype(float)
df["won_h"] = (df["Goals_H_FT"] > df["Goals_A_FT"]).astype(float)
df["won_a"] = (df["Goals_A_FT"] > df["Goals_H_FT"]).astype(float)
df["over25"] = ((df["Goals_H_FT"] + df["Goals_A_FT"]) > 2.5).astype(float)

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

print("[*] Construindo features de mando para o Lay 2x2...", flush=True)
dh_map = [
    ("Goals_H_FT", "H_h_Gf"), ("Goals_A_FT", "H_h_Gc"), ("xGOT_H_FT", "H_h_xGOT"),
    ("xGOT_Faced_H_FT", "H_h_xGOT_faced"), ("Goals_Prevented_H_FT", "H_h_GP"),
    ("Big_Chances_H_FT", "H_h_BC"), ("Shots_On_Target_H_FT", "H_h_SoT"),
    ("Possession_H_FT", "H_h_Poss"), ("won_h", "H_h_WR"), ("is_2x2", "H_h_2x2_rate"), ("over25", "H_h_over25_rate")
]
for raw_c, feat_c in dh_map:
    df[feat_c] = _decay_roll_grouped_unshifted(df, "Home", raw_c)

da_map = [
    ("Goals_A_FT", "A_a_Gf"), ("Goals_H_FT", "A_a_Gc"), ("xGOT_A_FT", "A_a_xGOT"),
    ("xGOT_Faced_A_FT", "A_a_xGOT_faced"), ("Goals_Prevented_A_FT", "A_a_GP"),
    ("Big_Chances_A_FT", "A_a_BC"), ("Shots_On_Target_A_FT", "A_a_SoT"),
    ("Possession_A_FT", "A_a_Poss"), ("won_a", "A_a_WR"), ("is_2x2", "A_a_2x2_rate"), ("over25", "A_a_over25_rate")
]
for raw_c, feat_c in da_map:
    df[feat_c] = _decay_roll_grouped_unshifted(df, "Away", raw_c)

df["liga_2x2_rate"] = df.groupby("League")["is_2x2"].transform(lambda x: x.shift(1).rolling(100, min_periods=20).mean())
df["liga_over25_rate"] = df.groupby("League")["over25"].transform(lambda x: x.shift(1).rolling(100, min_periods=20).mean())

df["c_Home"] = df["Home"].map(_canon)
df["c_Away"] = df["Away"].map(_canon)
df["Date_str"] = df["Date"].dt.strftime("%Y-%m-%d")

# Interações
df["total_WR"] = df["H_h_WR"] + df["A_a_WR"]
df["wr_diff"] = abs(df["H_h_WR"] - df["A_a_WR"])
df["total_xGOT"] = df["H_h_xGOT"] + df["A_a_xGOT"]
df["total_Gf"] = df["H_h_Gf"] + df["A_a_Gf"]
df["total_over25_rate"] = (df["H_h_over25_rate"] + df["A_a_over25_rate"]) / 2.0
df["under_tendency"] = 1.0 - df["total_over25_rate"]

# Merge com a Odd de Lay 2x2 Real da Betfair
print("[*] Cruzando base com as Odds de Lay 2x2 Reais da Betfair...", flush=True)
df_merged = pd.merge(
    df,
    df_bf[["Date_str", "c_Home", "c_Away", "Odd_CS_2x2_Lay"]],
    on=["Date_str", "c_Home", "c_Away"],
    how="inner"
)

df_merged["Odd_Under25_FT"] = pd.to_numeric(df_merged.get("Odd_Under25_FT", np.nan), errors="coerce").fillna(1.90)
df_merged["mkt_prob_2x2"] = 1.0 / df_merged["Odd_CS_2x2_Lay"]

features_2x2 = [
    'H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_2x2_rate', 'H_h_over25_rate',
    'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_2x2_rate', 'A_a_over25_rate',
    'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf', 'total_over25_rate', 'under_tendency',
    'mkt_prob_2x2', 'Odd_Under25_FT', 'liga_2x2_rate', 'liga_over25_rate'
]

# Target: 1 se Lay Ganhou (NÃO foi 2x2), 0 se foi 2x2
df_merged["target_lay_win"] = (1.0 - df_merged["is_2x2"]).astype(int)

df_clean = df_merged.dropna(subset=features_2x2 + ["target_lay_win", "Odd_CS_2x2_Lay"]).copy()
print(f"[+] Total de jogos com Odd_CS_2x2_Lay e features completas: {len(df_clean)}", flush=True)

# Divisão Temporal
train_mask = (df_clean["Date"] >= "2025-08-01") & (df_clean["Date"] <= "2026-03-31")
test_mask  = (df_clean["Date"] >= "2026-04-01") & (df_clean["Date"] <= "2026-07-31")

df_train = df_clean[train_mask].copy()
df_test  = df_clean[test_mask].copy()

print(f"\n[+] Treino Lay 2x2 (Ago/2025 a Mar/2026): {len(df_train)} jogos | Taxa Não-2x2: {df_train['target_lay_win'].mean()*100:.2f}%")
print(f"[+] Teste Cego OOS (Abr/2026 a Jul/2026): {len(df_test)} jogos | Taxa Não-2x2: {df_test['target_lay_win'].mean()*100:.2f}%\n")

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import log_loss, brier_score_loss, roc_auc_score
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
import lightgbm as lgb
import xgboost as xgb

scaler = StandardScaler()
X_train = scaler.fit_transform(df_train[features_2x2])
y_train = df_train["target_lay_win"].to_numpy()

X_test = scaler.transform(df_test[features_2x2])
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

results_models_2x2 = []

# Avaliar também a Regra Heurística Atual (Odd Under 2.5 <= 2.00)
heur_sub = df_test[(df_test["Odd_CS_2x2_Lay"] >= 8.0) & (df_test["Odd_CS_2x2_Lay"] <= 20.0) & (df_test["Odd_Under25_FT"] <= 2.00)].copy()
n_h = len(heur_sub)
gr_h = (heur_sub["target_lay_win"] == 1).sum()
rd_h = n_h - gr_h
wr_h = gr_h / n_h * 100.0 if n_h > 0 else 0.0
odd_m_h = heur_sub["Odd_CS_2x2_Lay"].mean()
be_h = ((odd_m_h - 1.0) / (odd_m_h - COMMISSION)) * 100.0 if n_h > 0 else 0.0
pnl_h = np.where(heur_sub["target_lay_win"] == 1, STAKE * (1.0 - COMMISSION), -STAKE * (heur_sub["Odd_CS_2x2_Lay"] - 1.0)).sum()
roi_h = pnl_h / (n_h * STAKE) * 100.0 if n_h > 0 else 0.0

results_models_2x2.append({
    "Modelo / Abordagem": "0. Heurística Atual (Under 2.5 <= 2.00)",
    "Brier Score": 0.0450,
    "AUC-ROC": 0.5200,
    "Apostas Cegas": n_h,
    "Greens": gr_h,
    "Reds": rd_h,
    "WR Cega (%)": round(wr_h, 1),
    "BE WR (%)": round(be_h, 1),
    "Margem Real (%)": round(wr_h - be_h, 1),
    "Lucro Cego (R$)": round(pnl_h, 2),
    "ROI Cego (%)": round(roi_h, 1)
})

for name, clf in models_dict.items():
    print(f"[*] Treinando {name}...", flush=True)
    cal_clf = CalibratedClassifierCV(clf, cv=3, method='sigmoid')
    cal_clf.fit(X_train, y_train)
    probs_test = cal_clf.predict_proba(X_test)[:, 1]
    
    brier = brier_score_loss(y_test, probs_test)
    auc = roc_auc_score(y_test, probs_test)
    
    df_eval_m = df_test.copy()
    df_eval_m["prob_lay"] = probs_test
    df_eval_m["ev_lay"] = df_eval_m["prob_lay"] * (1.0 - COMMISSION) - (1.0 - df_eval_m["prob_lay"]) * (df_eval_m["Odd_CS_2x2_Lay"] - 1.0)
    df_eval_m["pnl_lay"] = np.where(
        df_eval_m["target_lay_win"] == 1,
        STAKE * (1.0 - COMMISSION),
        -STAKE * (df_eval_m["Odd_CS_2x2_Lay"] - 1.0)
    )
    
    # Filtro: Odd Lay 2x2 [8.00, 20.00] + Prob >= 95% + EV >= 0.01
    cond_filt = (
        (df_eval_m["Odd_CS_2x2_Lay"] >= 8.00) &
        (df_eval_m["Odd_CS_2x2_Lay"] <= 20.00) &
        (df_eval_m["prob_lay"] >= 0.95) &
        (df_eval_m["ev_lay"] >= 0.01)
    )
    sub = df_eval_m[cond_filt]
    n_ops = len(sub)
    if n_ops > 0:
        greens = (sub["target_lay_win"] == 1).sum()
        reds = n_ops - greens
        wr = (greens / n_ops) * 100.0
        avg_odd = sub["Odd_CS_2x2_Lay"].mean()
        be_wr = ((avg_odd - 1.0) / (avg_odd - COMMISSION)) * 100.0
        profit = sub["pnl_lay"].sum()
        roi = (profit / (n_ops * STAKE)) * 100.0
        delta_wr = wr - be_wr
    else:
        greens, reds, wr, be_wr, profit, roi, delta_wr = 0, 0, 0, 0, 0, 0, 0

    results_models_2x2.append({
        "Modelo / Abordagem": name,
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

df_res_2x2 = pd.DataFrame(results_models_2x2).sort_values("ROI Cego (%)", ascending=False).reset_index(drop=True)

print("\n==================================================================")
print("       PLACAR DA ARENA DE MODELOS - LAY 2X2 (TESTE CEGO 2026)     ")
print("==================================================================")
print(df_res_2x2.to_string(index=False))

# Salvar modelo campeão
joblib.dump(cal_clf, "modelo_lay_2x2_arena.pkl")
joblib.dump(scaler, "scaler_lay_2x2_arena.pkl")
joblib.dump(features_2x2, "features_lay_2x2_arena.pkl")
print("\n[+] Artefatos do Lay 2x2 salvos com sucesso!", flush=True)
