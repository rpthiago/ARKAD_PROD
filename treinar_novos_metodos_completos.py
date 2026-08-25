import sys
sys.stdout.reconfigure(encoding='utf-8')
import os, joblib, numpy as np, pandas as pd
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import log_loss, brier_score_loss, roc_auc_score
from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier
from sklearn.calibration import CalibratedClassifierCV
import lightgbm as lgb
import xgboost as xgb

print("==================================================================", flush=True)
print("  CONSTRUÇÃO & TREINAMENTO DOS 6 NOVOS MÉTODOS (ODD LAY BETFAIR) ", flush=True)
print("  1. Lay 0x1 | 2. Lay 1x0 | 3. Lay 0x2 | 4. Lay 2x0 | 5. Lay 0x3 | 6. Under 4.5", flush=True)
print("==================================================================", flush=True)

# 1. Carregar base histórica completa com estatísticas ricas
import hist_rf_loader
df_raw = hist_rf_loader.load_hist_rf()
print(f"[+] Base histórica rica carregada: {len(df_raw)} jogos", flush=True)

# 2. Carregar base oficial Betfair com Odds de Lay Reais
print("[*] Carregando base oficial Betfair com Odds de Lay...", flush=True)
df_bf = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair.csv", low_memory=False)
df_bf["Date"] = pd.to_datetime(df_bf["Date"], errors="coerce")
df_bf = df_bf.dropna(subset=["Date", "Home", "Away"]).copy()

import unicodedata, re
def _canon(s):
    if pd.isna(s) or not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

df_bf["c_Home"] = df_bf["Home"].map(_canon)
df_bf["c_Away"] = df_bf["Away"].map(_canon)
df_bf["Date_str"] = df_bf["Date"].dt.strftime("%Y-%m-%d")

# 3. Construir séries temporais de mando unshifted
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

df["total_goals"] = df["Goals_H_FT"] + df["Goals_A_FT"]
df["won_h"] = (df["Goals_H_FT"] > df["Goals_A_FT"]).astype(float)
df["won_a"] = (df["Goals_A_FT"] > df["Goals_H_FT"]).astype(float)

# Flags de placar
df["is_0x1"] = ((df["Goals_H_FT"] == 0) & (df["Goals_A_FT"] == 1)).astype(float)
df["is_1x0"] = ((df["Goals_H_FT"] == 1) & (df["Goals_A_FT"] == 0)).astype(float)
df["is_0x2"] = ((df["Goals_H_FT"] == 0) & (df["Goals_A_FT"] == 2)).astype(float)
df["is_2x0"] = ((df["Goals_H_FT"] == 2) & (df["Goals_A_FT"] == 0)).astype(float)
df["is_0x3"] = ((df["Goals_H_FT"] == 0) & (df["Goals_A_FT"] == 3)).astype(float)
df["is_over45"] = (df["total_goals"] > 4.5).astype(float)
df["is_under45"] = (df["total_goals"] <= 4.5).astype(float)

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

print("[*] Construindo features de mando para todos os métodos...", flush=True)

# Mandante em Casa
dh_map = [
    ("Goals_H_FT", "H_h_Gf"), ("Goals_A_FT", "H_h_Gc"), ("xGOT_H_FT", "H_h_xGOT"),
    ("xGOT_Faced_H_FT", "H_h_xGOT_faced"), ("Goals_Prevented_H_FT", "H_h_GP"),
    ("Big_Chances_H_FT", "H_h_BC"), ("Shots_On_Target_H_FT", "H_h_SoT"),
    ("Possession_H_FT", "H_h_Poss"), ("won_h", "H_h_WR"),
    ("is_0x1", "H_h_0x1_rate"), ("is_1x0", "H_h_1x0_rate"), ("is_0x2", "H_h_0x2_rate"),
    ("is_2x0", "H_h_2x0_rate"), ("is_0x3", "H_h_0x3_rate"), ("is_over45", "H_h_over45_rate")
]
for raw_c, feat_c in dh_map:
    df[feat_c] = _decay_roll_grouped_unshifted(df, "Home", raw_c)

# Visitante Fora
da_map = [
    ("Goals_A_FT", "A_a_Gf"), ("Goals_H_FT", "A_a_Gc"), ("xGOT_A_FT", "A_a_xGOT"),
    ("xGOT_Faced_A_FT", "A_a_xGOT_faced"), ("Goals_Prevented_A_FT", "A_a_GP"),
    ("Big_Chances_A_FT", "A_a_BC"), ("Shots_On_Target_A_FT", "A_a_SoT"),
    ("Possession_A_FT", "A_a_Poss"), ("won_a", "A_a_WR"),
    ("is_0x1", "A_a_0x1_rate"), ("is_1x0", "A_a_1x0_rate"), ("is_0x2", "A_a_0x2_rate"),
    ("is_2x0", "A_a_2x0_rate"), ("is_0x3", "A_a_0x3_rate"), ("is_over45", "A_a_over45_rate")
]
for raw_c, feat_c in da_map:
    df[feat_c] = _decay_roll_grouped_unshifted(df, "Away", raw_c)

# Interações
df["total_WR"] = df["H_h_WR"] + df["A_a_WR"]
df["wr_diff"] = abs(df["H_h_WR"] - df["A_a_WR"])
df["total_xGOT"] = df["H_h_xGOT"] + df["A_a_xGOT"]
df["total_Gf"] = df["H_h_Gf"] + df["A_a_Gf"]
df["total_Gc"] = df["H_h_Gc"] + df["A_a_Gc"]
df["c_Home"] = df["Home"].map(_canon)
df["c_Away"] = df["Away"].map(_canon)
df["Date_str"] = df["Date"].dt.strftime("%Y-%m-%d")

# Merge com a base Betfair
bf_cols = [
    "Date_str", "c_Home", "c_Away",
    "Odd_CS_0x1_Lay", "Odd_CS_1x0_Lay", "Odd_CS_0x2_Lay", "Odd_CS_2x0_Lay", "Odd_CS_0x3_Lay",
    "Odd_Under45_FT_Lay", "Odd_Over45_FT_Lay"
]
avail_bf_cols = [c for c in bf_cols if c in df_bf.columns]
df_merged = pd.merge(df, df_bf[avail_bf_cols], on=["Date_str", "c_Home", "c_Away"], how="inner")
print(f"[+] Base combinada com Odds de Lay da Betfair: {len(df_merged)} jogos", flush=True)

COMMISSION = 0.045 # 4.5% Betfair
STAKE = 100.0

# Definição dos 6 métodos
metodos_config = [
    {
        "nome": "1. Lay 0x1",
        "odd_col": "Odd_CS_0x1_Lay",
        "target_col": "is_0x1",
        "target_mode": "LAY", # Ganha se NÃO for 0x1
        "odd_min": 6.0, "odd_max": 16.0,
        "prob_min": 0.88,
        "features": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_0x1_rate',
                     'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_0x1_rate',
                     'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf']
    },
    {
        "nome": "2. Lay 1x0",
        "odd_col": "Odd_CS_1x0_Lay",
        "target_col": "is_1x0",
        "target_mode": "LAY", # Ganha se NÃO for 1x0
        "odd_min": 6.0, "odd_max": 16.0,
        "prob_min": 0.88,
        "features": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_1x0_rate',
                     'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_1x0_rate',
                     'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf']
    },
    {
        "nome": "3. Lay 0x2",
        "odd_col": "Odd_CS_0x2_Lay",
        "target_col": "is_0x2",
        "target_mode": "LAY", # Ganha se NÃO for 0x2
        "odd_min": 8.0, "odd_max": 20.0,
        "prob_min": 0.92,
        "features": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_0x2_rate',
                     'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_0x2_rate',
                     'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf']
    },
    {
        "nome": "4. Lay 2x0",
        "odd_col": "Odd_CS_2x0_Lay",
        "target_col": "is_2x0",
        "target_mode": "LAY", # Ganha se NÃO for 2x0
        "odd_min": 8.0, "odd_max": 20.0,
        "prob_min": 0.92,
        "features": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_2x0_rate',
                     'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_2x0_rate',
                     'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf']
    },
    {
        "nome": "5. Lay 0x3",
        "odd_col": "Odd_CS_0x3_Lay",
        "target_col": "is_0x3",
        "target_mode": "LAY", # Ganha se NÃO for 0x3
        "odd_min": 15.0, "odd_max": 35.0,
        "prob_min": 0.96,
        "features": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_0x3_rate',
                     'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_0x3_rate',
                     'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf']
    },
    {
        "nome": "6. Under 4.5 Gols (Back Under / Lay Over 4.5)",
        "odd_col": "Odd_Under45_FT_Lay", # ou Back Under 4.5
        "target_col": "is_under45",
        "target_mode": "BACK_UNDER", # Ganha se for <= 4.5 gols
        "odd_min": 1.10, "odd_max": 1.50,
        "prob_min": 0.85,
        "features": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_over45_rate',
                     'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_over45_rate',
                     'total_WR', 'total_xGOT', 'total_Gf', 'total_Gc']
    }
]

resumo_novos_metodos = []

for cfg in metodos_config:
    nome = cfg["nome"]
    odd_c = cfg["odd_col"]
    tgt_c = cfg["target_col"]
    feats = cfg["features"]
    
    print(f"\n==================================================")
    print(f"  TREINANDO & AVALIANDO: {nome}")
    print(f"==================================================")
    
    # Criar sub-dataframe limpo
    sub_df = df_merged.copy()
    sub_df["odd_real"] = pd.to_numeric(sub_df.get(odd_c, np.nan), errors="coerce")
    
    # Se odd_col for Under45 e faltar na Betfair, buscar da Bet365
    if "Under45" in odd_c and sub_df["odd_real"].isna().sum() > len(sub_df)*0.5:
        sub_df["odd_real"] = pd.to_numeric(sub_df.get("Odd_Under45_FT", 1.25), errors="coerce").fillna(1.25)
        
    sub_df = sub_df.dropna(subset=feats + [tgt_c, "odd_real"]).copy()
    sub_df = sub_df[(sub_df["odd_real"] >= cfg["odd_min"]) & (sub_df["odd_real"] <= cfg["odd_max"])].copy()
    
    # Target
    if cfg["target_mode"] == "LAY":
        sub_df["target_win"] = (1.0 - sub_df[tgt_c]).astype(int) # Ganha se NÃO for o placar
    else:
        sub_df["target_win"] = sub_df[tgt_c].astype(int) # Ganha se for Under 4.5
        
    # Split Temporal
    train_m = (sub_df["Date"] >= "2025-08-01") & (sub_df["Date"] <= "2026-03-31")
    test_m  = (sub_df["Date"] >= "2026-04-01") & (sub_df["Date"] <= "2026-07-31")
    aug_m   = (sub_df["Date"] >= "2026-08-01") & (sub_df["Date"] <= "2026-08-24")
    
    df_tr = sub_df[train_m].copy()
    df_te = sub_df[test_m].copy()
    df_au = sub_df[aug_m].copy()
    
    print(f"Treino: {len(df_tr)} | Teste Cego: {len(df_te)} | Agosto/2026: {len(df_au)}")
    if len(df_tr) < 100 or len(df_te) < 10:
        print("[-] Amostra insuficiente para treino seguro.")
        continue
        
    scaler_m = StandardScaler()
    X_tr = scaler_m.fit_transform(df_tr[feats])
    y_tr = df_tr["target_win"].to_numpy()
    
    X_te = scaler_m.transform(df_te[feats])
    y_te = df_te["target_win"].to_numpy()
    
    # Treinar Extra Trees
    clf_m = ExtraTreesClassifier(n_estimators=200, max_depth=6, min_samples_leaf=15, random_state=42, n_jobs=-1)
    cal_clf = CalibratedClassifierCV(clf_m, cv=3, method='sigmoid')
    cal_clf.fit(X_tr, y_tr)
    
    # Avaliar Teste Cego
    probs_te = cal_clf.predict_proba(X_te)[:, 1]
    df_te["prob"] = probs_te
    
    if cfg["target_mode"] == "LAY":
        df_te["ev"] = df_te["prob"] * (1.0 - COMMISSION) - (1.0 - df_te["prob"]) * (df_te["odd_real"] - 1.0)
        df_te["pnl"] = np.where(df_te["target_win"] == 1, STAKE * (1.0 - COMMISSION), -STAKE * (df_te["odd_real"] - 1.0))
    else: # BACK UNDER 4.5
        df_te["ev"] = df_te["prob"] * (df_te["odd_real"] - 1.0) * (1.0 - COMMISSION) - (1.0 - df_te["prob"])
        df_te["pnl"] = np.where(df_te["target_win"] == 1, STAKE * (df_te["odd_real"] - 1.0) * (1.0 - COMMISSION), -STAKE)
        
    cond_te = (df_te["prob"] >= cfg["prob_min"]) & (df_te["ev"] >= 0.01)
    sub_te = df_te[cond_te]
    n_te = len(sub_te)
    if n_te > 0:
        gr_te = (sub_te["target_win"] == 1).sum()
        rd_te = n_te - gr_te
        wr_te = gr_te / n_te * 100.0
        odd_m_te = sub_te["odd_real"].mean()
        if cfg["target_mode"] == "LAY":
            be_te = ((odd_m_te - 1.0) / (odd_m_te - COMMISSION)) * 100.0
        else:
            be_te = (1.0 / (odd_m_te - COMMISSION)) * 100.0
        pnl_te = sub_te["pnl"].sum()
        roi_te = pnl_te / (n_te * STAKE) * 100.0
    else:
        gr_te, rd_te, wr_te, be_te, pnl_te, roi_te, odd_m_te = 0, 0, 0, 0, 0, 0, 0
        
    # Avaliar Agosto/2026
    if len(df_au) > 0:
        X_au = scaler_m.transform(df_au[feats])
        probs_au = cal_clf.predict_proba(X_au)[:, 1]
        df_au["prob"] = probs_au
        if cfg["target_mode"] == "LAY":
            df_au["ev"] = df_au["prob"] * (1.0 - COMMISSION) - (1.0 - df_au["prob"]) * (df_au["odd_real"] - 1.0)
            df_au["pnl"] = np.where(df_au["target_win"] == 1, STAKE * (1.0 - COMMISSION), -STAKE * (df_au["odd_real"] - 1.0))
        else:
            df_au["ev"] = df_au["prob"] * (df_au["odd_real"] - 1.0) * (1.0 - COMMISSION) - (1.0 - df_au["prob"])
            df_au["pnl"] = np.where(df_au["target_win"] == 1, STAKE * (df_au["odd_real"] - 1.0) * (1.0 - COMMISSION), -STAKE)
            
        cond_au = (df_au["prob"] >= cfg["prob_min"]) & (df_au["ev"] >= 0.01)
        sub_au = df_au[cond_au]
        n_au = len(sub_au)
        if n_au > 0:
            gr_au = (sub_au["target_win"] == 1).sum()
            rd_au = n_au - gr_au
            wr_au = gr_au / n_au * 100.0
            odd_m_au = sub_au["odd_real"].mean()
            if cfg["target_mode"] == "LAY":
                be_au = ((odd_m_au - 1.0) / (odd_m_au - COMMISSION)) * 100.0
            else:
                be_au = (1.0 / (odd_m_au - COMMISSION)) * 100.0
            pnl_au = sub_au["pnl"].sum()
            roi_au = pnl_au / (n_au * STAKE) * 100.0
        else:
            gr_au, rd_au, wr_au, be_au, pnl_au, roi_au, odd_m_au = 0, 0, 0, 0, 0, 0, 0
    else:
        n_au, gr_au, rd_au, wr_au, be_au, pnl_au, roi_au, odd_m_au = 0, 0, 0, 0, 0, 0, 0, 0
        
    resumo_novos_metodos.append({
        "Método": nome,
        "N (Cego)": n_te,
        "WR Cega (%)": round(wr_te, 1),
        "BE Cego (%)": round(be_te, 1),
        "Margem Cega (%)": round(wr_te - be_te, 1),
        "Lucro Cego (R$)": round(pnl_te, 2),
        "ROI Cego (%)": round(roi_te, 1),
        "N (Agosto)": n_au,
        "WR Agosto (%)": round(wr_au, 1),
        "BE Agosto (%)": round(be_au, 1),
        "Margem Ago (%)": round(wr_au - be_au, 1),
        "Lucro Ago (R$)": round(pnl_au, 2),
        "ROI Agosto (%)": round(roi_au, 1)
    })

df_resumo_final = pd.DataFrame(resumo_novos_metodos)

print("\n==================================================================")
print("     QUADRO CONSOLIDADOR DOS 6 NOVOS MÉTODOS DE MACHINE LEARNING ")
print("==================================================================")
print(df_resumo_final.to_string(index=False))

df_resumo_final.to_excel("Quadro_6_Novos_Metodos_Auditados.xlsx", index=False)
print("\n[+] Tabela exportada para: Quadro_6_Novos_Metodos_Auditados.xlsx", flush=True)
