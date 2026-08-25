import sys
sys.stdout.reconfigure(encoding='utf-8')
import os, joblib, numpy as np, pandas as pd
from datetime import datetime

print("==================================================================", flush=True)
print("     ARENA DE MODELOS DE MACHINE LEARNING - LAY DRAW (ARKAD)      ", flush=True)
print("==================================================================", flush=True)

# 1. Carregar base histórica completa com estatísticas ricas
import hist_rf_loader
df_raw = hist_rf_loader.load_hist_rf()
print(f"[+] Base histórica carregada: {len(df_raw)} jogos", flush=True)

# 2. Carregar base oficial Betfair com Odd_D_Lay real
print("[*] Carregando base oficial Betfair com Odd_D_Lay real...", flush=True)
df_bf = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair.csv", low_memory=False)
df_bf["Date"] = pd.to_datetime(df_bf["Date"], errors="coerce")
df_bf = df_bf.dropna(subset=["Date", "Home", "Away", "Odd_D_Lay"]).copy()
df_bf["Odd_D_Lay"] = pd.to_numeric(df_bf["Odd_D_Lay"], errors="coerce")
df_bf = df_bf[(df_bf["Odd_D_Lay"] >= 2.0) & (df_bf["Odd_D_Lay"] <= 10.0)].copy()

import unicodedata, re
def _canon(s):
    if pd.isna(s) or not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

df_bf["c_Home"] = df_bf["Home"].map(_canon)
df_bf["c_Away"] = df_bf["Away"].map(_canon)
df_bf["Date_str"] = df_bf["Date"].dt.strftime("%Y-%m-%d")

# 3. Construir séries temporais de forma/mando com shift(1) unshifted em toda a base
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

df["_draw_flag"] = (df["Goals_H_FT"] == df["Goals_A_FT"]).astype(float)
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

print("[*] Construindo features de mando...", flush=True)
dh_map = [
    ("Goals_H_FT", "H_h_Gf"), ("Goals_A_FT", "H_h_Gc"), ("xGOT_H_FT", "H_h_xGOT"),
    ("xGOT_Faced_H_FT", "H_h_xGOT_faced"), ("Goals_Prevented_H_FT", "H_h_GP"),
    ("Big_Chances_H_FT", "H_h_BC"), ("Shots_On_Target_H_FT", "H_h_SoT"),
    ("Possession_H_FT", "H_h_Poss"), ("won_h", "H_h_WR"), ("_draw_flag", "H_h_draw_rate")
]
for raw_c, feat_c in dh_map:
    df[feat_c] = _decay_roll_grouped_unshifted(df, "Home", raw_c)

da_map = [
    ("Goals_A_FT", "A_a_Gf"), ("Goals_H_FT", "A_a_Gc"), ("xGOT_A_FT", "A_a_xGOT"),
    ("xGOT_Faced_A_FT", "A_a_xGOT_faced"), ("Goals_Prevented_A_FT", "A_a_GP"),
    ("Big_Chances_A_FT", "A_a_BC"), ("Shots_On_Target_A_FT", "A_a_SoT"),
    ("Possession_A_FT", "A_a_Poss"), ("won_a", "A_a_WR"), ("_draw_flag", "A_a_draw_rate")
]
for raw_c, feat_c in da_map:
    df[feat_c] = _decay_roll_grouped_unshifted(df, "Away", raw_c)

df["liga_draw_rate"] = df.groupby("League")["_draw_flag"].transform(lambda x: x.shift(1).rolling(100, min_periods=20).mean())

df["c_Home"] = df["Home"].map(_canon)
df["c_Away"] = df["Away"].map(_canon)
df["Date_str"] = df["Date"].dt.strftime("%Y-%m-%d")

df["h2h_pair"] = [tuple(sorted(x)) for x in zip(df["c_Home"], df["c_Away"])]
df["h2h_draw_rate"] = df.groupby("h2h_pair")["_draw_flag"].transform(lambda x: x.shift(1).rolling(8, min_periods=2).mean())

# Interações
df["total_WR"] = df["H_h_WR"] + df["A_a_WR"]
df["wr_diff"] = abs(df["H_h_WR"] - df["A_a_WR"])
df["draw_rate_prod"] = df["H_h_draw_rate"] * df["A_a_draw_rate"]
df["draw_rate_mean"] = (df["H_h_draw_rate"] + df["A_a_draw_rate"]) / 2.0
df["total_xGOT"] = df["H_h_xGOT"] + df["A_a_xGOT"]
df["xGOT_diff"] = abs(df["H_h_xGOT"] - df["A_a_xGOT"])
df["total_Gf"] = df["H_h_Gf"] + df["A_a_Gf"]
df["gf_diff"] = abs(df["H_h_Gf"] - df["A_a_Gf"])
df["decisive_score"] = (df["total_Gf"] * 0.4) + (df["total_xGOT"] * 0.4) + (df["wr_diff"] * 2.0) - (df["draw_rate_mean"] * 3.0)

# Merge com a Odd de Lay Real da Betfair
print("[*] Cruzando base com as Odds de Lay Reais da Betfair...", flush=True)
df_merged = pd.merge(
    df,
    df_bf[["Date_str", "c_Home", "c_Away", "Odd_D_Lay"]],
    on=["Date_str", "c_Home", "c_Away"],
    how="inner"
)

odd_h = pd.to_numeric(df_merged.get("Odd_H_FT", np.nan), errors="coerce").replace(0, np.nan).fillna(2.0)
odd_a = pd.to_numeric(df_merged.get("Odd_A_FT", np.nan), errors="coerce").replace(0, np.nan).fillna(3.0)
df_merged["mkt_prob_draw"] = 1.0 / df_merged["Odd_D_Lay"]
_ov = (1.0 / odd_h) + (1.0 / odd_a) + df_merged["mkt_prob_draw"]
df_merged["mkt_prob_draw_norm"] = df_merged["mkt_prob_draw"] / _ov
df_merged["mkt_overvalue_draw"] = df_merged["mkt_prob_draw_norm"] - df_merged["liga_draw_rate"]

features = [
    'H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_draw_rate',
    'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_draw_rate',
    'total_WR', 'wr_diff', 'draw_rate_prod', 'draw_rate_mean', 'total_xGOT', 'xGOT_diff', 'total_Gf', 'gf_diff', 'decisive_score',
    'mkt_prob_draw', 'mkt_prob_draw_norm', 'mkt_overvalue_draw', 'liga_draw_rate', 'h2h_draw_rate'
]

# Target: 1 se Lay Ganhou (NÃO empatou), 0 se empatou
df_merged["target_lay_win"] = (df_merged["Goals_H_FT"] != df_merged["Goals_A_FT"]).astype(int)

# Limpeza estrita de NaNs
df_clean = df_merged.dropna(subset=features + ["target_lay_win", "Odd_D_Lay"]).copy()
print(f"[+] Total de jogos com Lay real e 34 features completas: {len(df_clean)}", flush=True)

# 4. Divisão Temporal Estrita
# Treino: 01/08/2025 a 31/03/2026
# Teste Cego OOS: 01/04/2026 a 31/07/2026
# Agosto/2026: Intocado
train_mask = (df_clean["Date"] >= "2025-08-01") & (df_clean["Date"] <= "2026-03-31")
test_mask  = (df_clean["Date"] >= "2026-04-01") & (df_clean["Date"] <= "2026-07-31")

df_train = df_clean[train_mask].copy()
df_test  = df_clean[test_mask].copy()

print(f"\n[+] Conjunto de Treino (Ago/2025 a Mar/2026): {len(df_train)} jogos | Taxa Não-Empate: {df_train['target_lay_win'].mean()*100:.1f}%")
print(f"[+] Teste Cego OOS (Abr/2026 a Jul/2026):     {len(df_test)} jogos | Taxa Não-Empate: {df_test['target_lay_win'].mean()*100:.1f}%\n")

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import log_loss, brier_score_loss, roc_auc_score
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, HistGradientBoostingClassifier, VotingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
import lightgbm as lgb
import xgboost as xgb

scaler = StandardScaler()
X_train = scaler.fit_transform(df_train[features])
y_train = df_train["target_lay_win"].to_numpy()

X_test = scaler.transform(df_test[features])
y_test = df_test["target_lay_win"].to_numpy()

# 5. Definir os 6 Modelos Competidores
models_dict = {
    "1. Random Forest": RandomForestClassifier(n_estimators=200, max_depth=6, min_samples_leaf=15, random_state=42, n_jobs=-1),
    "2. Extra Trees": ExtraTreesClassifier(n_estimators=200, max_depth=6, min_samples_leaf=15, random_state=42, n_jobs=-1),
    "3. LightGBM": lgb.LGBMClassifier(n_estimators=150, max_depth=4, learning_rate=0.03, min_child_samples=25, subsample=0.8, colsample_bytree=0.8, random_state=42, verbose=-1),
    "4. XGBoost": xgb.XGBClassifier(n_estimators=150, max_depth=4, learning_rate=0.03, min_child_weight=5, subsample=0.8, colsample_bytree=0.8, random_state=42, eval_metric='logloss'),
    "5. HistGradientBoosting": HistGradientBoostingClassifier(max_iter=150, max_depth=4, min_samples_leaf=20, random_state=42),
    "6. Regressão Logística (ElasticNet/L2)": LogisticRegression(C=0.1, penalty='l2', solver='lbfgs', max_iter=500, random_state=42)
}

COMMISSION = 0.045 # 4.5% Betfair
STAKE = 100.0

results_models = []
trained_models = {}

print("==================================================================")
print("              TREINAMENTO & AUDITORIA DOS MODELOS                 ")
print("==================================================================")

for name, clf in models_dict.items():
    print(f"[*] Treinando {name}...", flush=True)
    
    # Treinar com calibração de probabilidade (CalibratedClassifierCV)
    cal_clf = CalibratedClassifierCV(clf, cv=3, method='sigmoid')
    cal_clf.fit(X_train, y_train)
    trained_models[name] = cal_clf
    
    # Predição no teste cego
    probs_test = cal_clf.predict_proba(X_test)[:, 1]
    
    # Métricas Estatísticas
    loss = log_loss(y_test, probs_test)
    brier = brier_score_loss(y_test, probs_test)
    auc = roc_auc_score(y_test, probs_test)
    
    # Avaliação Financeira na Odd de Lay Real
    df_eval_m = df_test.copy()
    df_eval_m["prob_lay"] = probs_test
    df_eval_m["ev_lay"] = df_eval_m["prob_lay"] * (1.0 - COMMISSION) - (1.0 - df_eval_m["prob_lay"]) * (df_eval_m["Odd_D_Lay"] - 1.0)
    df_eval_m["pnl_lay"] = np.where(
        df_eval_m["target_lay_win"] == 1,
        STAKE * (1.0 - COMMISSION),
        -STAKE * (df_eval_m["Odd_D_Lay"] - 1.0)
    )
    
    # Filtro Padrão: Odd Lay [3.00, 4.50] + Prob >= 80% + xGOT >= 2.20 + EV >= 0.02
    cond_filt = (
        (df_eval_m["Odd_D_Lay"] >= 3.00) &
        (df_eval_m["Odd_D_Lay"] <= 4.50) &
        (df_eval_m["prob_lay"] >= 0.80) &
        (df_eval_m["total_xGOT"] >= 2.20) &
        (df_eval_m["ev_lay"] >= 0.02)
    )
    sub = df_eval_m[cond_filt]
    n_ops = len(sub)
    if n_ops > 0:
        greens = (sub["target_lay_win"] == 1).sum()
        reds = n_ops - greens
        wr = (greens / n_ops) * 100.0
        avg_odd = sub["Odd_D_Lay"].mean()
        be_wr = ((avg_odd - 1.0) / (avg_odd - COMMISSION)) * 100.0
        profit = sub["pnl_lay"].sum()
        roi = (profit / (n_ops * STAKE)) * 100.0
        delta_wr = wr - be_wr
        
        # Bootstrap 10k iterações
        pnl_arr = sub["pnl_lay"].to_numpy()
        boots = [np.random.choice(pnl_arr, size=len(pnl_arr), replace=True).sum() / (len(pnl_arr)*STAKE)*100 for _ in range(2000)]
        ic_low = np.percentile(boots, 2.5)
        ic_high = np.percentile(boots, 97.5)
    else:
        greens, reds, wr, be_wr, profit, roi, delta_wr, ic_low, ic_high = 0, 0, 0, 0, 0, 0, 0, 0, 0

    results_models.append({
        "Modelo": name,
        "Log-Loss": round(loss, 4),
        "Brier Score": round(brier, 4),
        "AUC-ROC": round(auc, 4),
        "Apostas Cegas": n_ops,
        "Greens": greens,
        "Reds": reds,
        "WR Cega (%)": round(wr, 1),
        "BE WR (%)": round(be_wr, 1),
        "Margem Real (%)": round(delta_wr, 1),
        "Lucro Cego (R$)": round(profit, 2),
        "ROI Cego (%)": round(roi, 1),
        "IC95% ROI": f"[{ic_low:+.1f}%, {ic_high:+.1f}%]"
    })

# 7. Modelo 7: Super Ensemble Calibrado (Soft Voting dos 3 melhores)
print("[*] Construindo e avaliando Super Ensemble Calibrado...", flush=True)
ensemble_probs = (
    trained_models["3. LightGBM"].predict_proba(X_test)[:, 1] * 0.35 +
    trained_models["4. XGBoost"].predict_proba(X_test)[:, 1] * 0.35 +
    trained_models["1. Random Forest"].predict_proba(X_test)[:, 1] * 0.30
)

loss_ens = log_loss(y_test, ensemble_probs)
brier_ens = brier_score_loss(y_test, ensemble_probs)
auc_ens = roc_auc_score(y_test, ensemble_probs)

df_eval_ens = df_test.copy()
df_eval_ens["prob_lay"] = ensemble_probs
df_eval_ens["ev_lay"] = df_eval_ens["prob_lay"] * (1.0 - COMMISSION) - (1.0 - df_eval_ens["prob_lay"]) * (df_eval_ens["Odd_D_Lay"] - 1.0)
df_eval_ens["pnl_lay"] = np.where(
    df_eval_ens["target_lay_win"] == 1,
    STAKE * (1.0 - COMMISSION),
    -STAKE * (df_eval_ens["Odd_D_Lay"] - 1.0)
)

cond_ens = (
    (df_eval_ens["Odd_D_Lay"] >= 3.00) &
    (df_eval_ens["Odd_D_Lay"] <= 4.50) &
    (df_eval_ens["prob_lay"] >= 0.80) &
    (df_eval_ens["total_xGOT"] >= 2.20) &
    (df_eval_ens["ev_lay"] >= 0.02)
)
sub_ens = df_eval_ens[cond_ens]
n_ens = len(sub_ens)
greens_e = (sub_ens["target_lay_win"] == 1).sum()
reds_e = n_ens - greens_e
wr_e = (greens_e / n_ens) * 100.0
avg_odd_e = sub_ens["Odd_D_Lay"].mean()
be_wr_e = ((avg_odd_e - 1.0) / (avg_odd_e - COMMISSION)) * 100.0
profit_e = sub_ens["pnl_lay"].sum()
roi_e = (profit_e / (n_ens * STAKE)) * 100.0
delta_wr_e = wr_e - be_wr_e

pnl_arr_e = sub_ens["pnl_lay"].to_numpy()
boots_e = [np.random.choice(pnl_arr_e, size=len(pnl_arr_e), replace=True).sum() / (len(pnl_arr_e)*STAKE)*100 for _ in range(2000)]
ic_low_e = np.percentile(boots_e, 2.5)
ic_high_e = np.percentile(boots_e, 97.5)

results_models.append({
    "Modelo": "7. Super Ensemble (LGBM+XGB+RF)",
    "Log-Loss": round(loss_ens, 4),
    "Brier Score": round(brier_ens, 4),
    "AUC-ROC": round(auc_ens, 4),
    "Apostas Cegas": n_ens,
    "Greens": greens_e,
    "Reds": reds_e,
    "WR Cega (%)": round(wr_e, 1),
    "BE WR (%)": round(be_wr_e, 1),
    "Margem Real (%)": round(delta_wr_e, 1),
    "Lucro Cego (R$)": round(profit_e, 2),
    "ROI Cego (%)": round(roi_e, 1),
    "IC95% ROI": f"[{ic_low_e:+.1f}%, {ic_high_e:+.1f}%]"
})

df_results = pd.DataFrame(results_models).sort_values("ROI Cego (%)", ascending=False).reset_index(drop=True)

print("\n==================================================================")
print("     PLACAR FINAL DA ARENA DE MODELOS (TESTE CEGO ABRIL-JULHO/2026) ")
print("==================================================================")
print(df_results.to_string(index=False))

# Salvar os melhores modelos
joblib.dump(trained_models["3. LightGBM"], "modelo_lay_draw_lightgbm.pkl")
joblib.dump(trained_models["4. XGBoost"], "modelo_lay_draw_xgboost.pkl")
joblib.dump(trained_models["1. Random Forest"], "modelo_lay_draw_rf_novo.pkl")
joblib.dump(scaler, "scaler_lay_draw_arena.pkl")
joblib.dump(features, "features_lay_draw_arena.pkl")

print("\n[+] Modelos e Scaler salvos com sucesso na raiz!", flush=True)
