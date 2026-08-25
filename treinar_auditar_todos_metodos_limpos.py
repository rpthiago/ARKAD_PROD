import sys
sys.stdout.reconfigure(encoding='utf-8')
import os, joblib, numpy as np, pandas as pd
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import brier_score_loss, roc_auc_score

print("==================================================================", flush=True)
print("   TREINAMENTO & AUDITORIA TOTAL: TODOS OS MÉTODOS (ZERO LEAK)    ", flush=True)
print("     (ODDS DE LAY REAIS DA BETFAIR · TAXA 4,5% · TESTE CEGO PURO) ", flush=True)
print("==================================================================", flush=True)

# 1. Carregar base histórica oficial rica
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

# 3. Construir séries temporais de mando unshifted com shift(1)
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
df["is_draw"] = (df["Goals_H_FT"] == df["Goals_A_FT"]).astype(float)
df["won_h"] = (df["Goals_H_FT"] > df["Goals_A_FT"]).astype(float)
df["won_a"] = (df["Goals_A_FT"] > df["Goals_H_FT"]).astype(float)

# Flags de placar
df["is_0x0"] = ((df["Goals_H_FT"] == 0) & (df["Goals_A_FT"] == 0)).astype(float)
df["is_0x1"] = ((df["Goals_H_FT"] == 0) & (df["Goals_A_FT"] == 1)).astype(float)
df["is_1x0"] = ((df["Goals_H_FT"] == 1) & (df["Goals_A_FT"] == 0)).astype(float)
df["is_0x2"] = ((df["Goals_H_FT"] == 0) & (df["Goals_A_FT"] == 2)).astype(float)
df["is_2x0"] = ((df["Goals_H_FT"] == 2) & (df["Goals_A_FT"] == 0)).astype(float)
df["is_2x2"] = ((df["Goals_H_FT"] == 2) & (df["Goals_A_FT"] == 2)).astype(float)
df["is_0x3"] = ((df["Goals_H_FT"] == 0) & (df["Goals_A_FT"] == 3)).astype(float)
df["is_btts_yes"] = ((df["Goals_H_FT"] > 0) & (df["Goals_A_FT"] > 0)).astype(float)
df["is_btts_no"] = (1.0 - df["is_btts_yes"]).astype(float)
df["is_under25"] = (df["total_goals"] <= 2.5).astype(float)

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

print("[*] Construindo features de mando unshifted...", flush=True)

# Mandante em Casa
dh_map = [
    ("Goals_H_FT", "H_h_Gf"), ("Goals_A_FT", "H_h_Gc"), ("xGOT_H_FT", "H_h_xGOT"),
    ("xGOT_Faced_H_FT", "H_h_xGOT_faced"), ("Goals_Prevented_H_FT", "H_h_GP"),
    ("Big_Chances_H_FT", "H_h_BC"), ("Shots_On_Target_H_FT", "H_h_SoT"),
    ("Possession_H_FT", "H_h_Poss"), ("won_h", "H_h_WR"), ("is_draw", "H_h_draw_rate"),
    ("is_0x0", "H_h_0x0_rate"), ("is_0x1", "H_h_0x1_rate"), ("is_1x0", "H_h_1x0_rate"),
    ("is_0x2", "H_h_0x2_rate"), ("is_2x0", "H_h_2x0_rate"), ("is_2x2", "H_h_2x2_rate"),
    ("is_0x3", "H_h_0x3_rate"), ("is_btts_no", "H_h_btts_no_rate"), ("is_under25", "H_h_under25_rate")
]
for raw_c, feat_c in dh_map:
    df[feat_c] = _decay_roll_grouped_unshifted(df, "Home", raw_c)

# Visitante Fora
da_map = [
    ("Goals_A_FT", "A_a_Gf"), ("Goals_H_FT", "A_a_Gc"), ("xGOT_A_FT", "A_a_xGOT"),
    ("xGOT_Faced_A_FT", "A_a_xGOT_faced"), ("Goals_Prevented_A_FT", "A_a_GP"),
    ("Big_Chances_A_FT", "A_a_BC"), ("Shots_On_Target_A_FT", "A_a_SoT"),
    ("Possession_A_FT", "A_a_Poss"), ("won_a", "A_a_WR"), ("is_draw", "A_a_draw_rate"),
    ("is_0x0", "A_a_0x0_rate"), ("is_0x1", "A_a_0x1_rate"), ("is_1x0", "A_a_1x0_rate"),
    ("is_0x2", "A_a_0x2_rate"), ("is_2x0", "A_a_2x0_rate"), ("is_2x2", "A_a_2x2_rate"),
    ("is_0x3", "A_a_0x3_rate"), ("is_btts_no", "A_a_btts_no_rate"), ("is_under25", "A_a_under25_rate")
]
for raw_c, feat_c in da_map:
    df[feat_c] = _decay_roll_grouped_unshifted(df, "Away", raw_c)

df["total_WR"] = df["H_h_WR"] + df["A_a_WR"]
df["wr_diff"] = abs(df["H_h_WR"] - df["A_a_WR"])
df["total_xGOT"] = df["H_h_xGOT"] + df["A_a_xGOT"]
df["total_Gf"] = df["H_h_Gf"] + df["A_a_Gf"]
df["total_Gc"] = df["H_h_Gc"] + df["A_a_Gc"]
df["c_Home"] = df["Home"].map(_canon)
df["c_Away"] = df["Away"].map(_canon)
df["Date_str"] = df["Date"].dt.strftime("%Y-%m-%d")

# Merge com a Betfair (somente colunas com Odd de Lay ou Back real)
bf_cols = [
    "Date_str", "c_Home", "c_Away",
    "Odd_D_Lay", "Odd_H_Lay",
    "Odd_CS_0x0_Lay", "Odd_CS_0x1_Lay", "Odd_CS_1x0_Lay",
    "Odd_CS_0x2_Lay", "Odd_CS_2x0_Lay", "Odd_CS_2x2_Lay", "Odd_CS_0x3_Lay",
    "Odd_BTTS_No_Lay", "Odd_BTTS_Yes_Lay", "Odd_Under25_FT_Lay"
]
avail_bf_cols = [c for c in bf_cols if c in df_bf.columns]
df_merged = pd.merge(df, df_bf[avail_bf_cols], on=["Date_str", "c_Home", "c_Away"], how="inner")
print(f"[+] Base combinada Betfair: {len(df_merged)} jogos", flush=True)

COMMISSION = 0.045 # 4.5% Betfair
STAKE = 100.0

# Definição de todos os métodos
metodos = [
    # 1. Mercados de 2 Vias & Match Odds
    {
        "nome": "1. Lay Draw (Não-Empate)",
        "tipo": "Match Odds",
        "odd_col": "Odd_D_Lay",
        "tgt": "is_draw", "mode": "LAY",
        "min_odd": 3.00, "max_odd": 4.50,
        "prob_cut": 0.75, "min_xgot": 2.20,
        "feats": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_draw_rate',
                  'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_draw_rate',
                  'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf']
    },
    {
        "nome": "2. Back BTTS No (Ambas Não)",
        "tipo": "2-Way",
        "odd_col": "Odd_BTTS_Yes_Lay", # Lay BTTS Yes = Back BTTS No
        "tgt": "is_btts_no", "mode": "BACK",
        "min_odd": 1.70, "max_odd": 2.50,
        "prob_cut": 0.52, "min_xgot": 0.0,
        "feats": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_btts_no_rate',
                  'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_btts_no_rate',
                  'total_xGOT', 'total_Gf', 'total_Gc']
    },
    # 2. Correct Score Lays
    {
        "nome": "3. Lay 0x0",
        "tipo": "Correct Score",
        "odd_col": "Odd_CS_0x0_Lay",
        "tgt": "is_0x0", "mode": "LAY",
        "min_odd": 6.00, "max_odd": 16.00,
        "prob_cut": 0.92, "min_xgot": 2.20,
        "feats": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_0x0_rate',
                  'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_0x0_rate',
                  'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf']
    },
    {
        "nome": "4. Lay 1x0",
        "tipo": "Correct Score",
        "odd_col": "Odd_CS_1x0_Lay",
        "tgt": "is_1x0", "mode": "LAY",
        "min_odd": 6.00, "max_odd": 16.00,
        "prob_cut": 0.88, "min_xgot": 0.0,
        "feats": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_1x0_rate',
                  'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_1x0_rate',
                  'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf']
    },
    {
        "nome": "5. Lay 0x1",
        "tipo": "Correct Score",
        "odd_col": "Odd_CS_0x1_Lay",
        "tgt": "is_0x1", "mode": "LAY",
        "min_odd": 6.00, "max_odd": 16.00,
        "prob_cut": 0.88, "min_xgot": 0.0,
        "feats": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_0x1_rate',
                  'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_0x1_rate',
                  'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf']
    },
    {
        "nome": "6. Lay 2x0",
        "tipo": "Correct Score",
        "odd_col": "Odd_CS_2x0_Lay",
        "tgt": "is_2x0", "mode": "LAY",
        "min_odd": 8.00, "max_odd": 20.00,
        "prob_cut": 0.92, "min_xgot": 0.0,
        "feats": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_2x0_rate',
                  'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_2x0_rate',
                  'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf']
    },
    {
        "nome": "7. Lay 0x2",
        "tipo": "Correct Score",
        "odd_col": "Odd_CS_0x2_Lay",
        "tgt": "is_0x2", "mode": "LAY",
        "min_odd": 8.00, "max_odd": 20.00,
        "prob_cut": 0.92, "min_xgot": 0.0,
        "feats": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_0x2_rate',
                  'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_0x2_rate',
                  'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf']
    },
    {
        "nome": "8. Lay 2x2",
        "tipo": "Correct Score",
        "odd_col": "Odd_CS_2x2_Lay",
        "tgt": "is_2x2", "mode": "LAY",
        "min_odd": 8.00, "max_odd": 20.00,
        "prob_cut": 0.94, "min_xgot": 0.0,
        "feats": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_2x2_rate',
                  'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_2x2_rate',
                  'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf']
    },
    {
        "nome": "9. Lay 0x3",
        "tipo": "Correct Score",
        "odd_col": "Odd_CS_0x3_Lay",
        "tgt": "is_0x3", "mode": "LAY",
        "min_odd": 15.00, "max_odd": 35.00,
        "prob_cut": 0.96, "min_xgot": 0.0,
        "feats": ['H_h_Gf', 'H_h_Gc', 'H_h_xGOT', 'H_h_xGOT_faced', 'H_h_GP', 'H_h_BC', 'H_h_SoT', 'H_h_Poss', 'H_h_WR', 'H_h_0x3_rate',
                  'A_a_Gf', 'A_a_Gc', 'A_a_xGOT', 'A_a_xGOT_faced', 'A_a_GP', 'A_a_BC', 'A_a_SoT', 'A_a_Poss', 'A_a_WR', 'A_a_0x3_rate',
                  'total_WR', 'wr_diff', 'total_xGOT', 'total_Gf']
    }
]

resultados_auditoria_total = []

for m in metodos:
    nome = m["nome"]
    odd_col = m["odd_col"]
    tgt_col = m["tgt"]
    feats = m["feats"]
    
    # Validação inegociável da Lei 1
    assert "Lay" in odd_col, f"VIOLAÇÃO DA LEI 1: {odd_col} não é coluna de Lay!"
    
    if odd_col not in df_merged.columns:
        print(f"[-] Coluna {odd_col} não encontrada. Pulando...")
        continue
        
    sub_df = df_merged.dropna(subset=feats + [odd_col, tgt_col]).copy()
    sub_df["odd_exec"] = pd.to_numeric(sub_df[odd_col], errors="coerce")
    sub_df = sub_df[(sub_df["odd_exec"] >= m["min_odd"]) & (sub_df["odd_exec"] <= m["max_odd"])].copy()
    
    if m["mode"] == "LAY":
        sub_df["target_win"] = (1.0 - sub_df[tgt_col]).astype(int)
    else:
        sub_df["target_win"] = sub_df[tgt_col].astype(int)
        
    # Split Temporal Estrito
    df_tr = sub_df[(sub_df["Date"] >= "2025-08-01") & (sub_df["Date"] <= "2026-03-31")].copy()
    df_te = sub_df[(sub_df["Date"] >= "2026-04-01") & (sub_df["Date"] <= "2026-07-31")].copy()
    
    if len(df_tr) < 100 or len(df_te) < 10:
        continue
        
    # Treinar modelo sem leak
    sc = StandardScaler()
    X_tr = sc.fit_transform(df_tr[feats])
    y_tr = df_tr["target_win"].to_numpy()
    
    X_te = sc.transform(df_te[feats])
    y_te = df_te["target_win"].to_numpy()
    
    clf = CalibratedClassifierCV(ExtraTreesClassifier(n_estimators=200, max_depth=6, min_samples_leaf=15, random_state=42, n_jobs=-1), cv=3, method='sigmoid')
    clf.fit(X_tr, y_tr)
    
    # Avaliar Teste Cego
    probs_te = clf.predict_proba(X_te)[:, 1]
    df_te["prob"] = probs_te
    
    if m["mode"] == "LAY":
        df_te["ev"] = df_te["prob"] * (1.0 - COMMISSION) - (1.0 - df_te["prob"]) * (df_te["odd_exec"] - 1.0)
        df_te["pnl"] = np.where(df_te["target_win"] == 1, STAKE * (1.0 - COMMISSION), -STAKE * (df_te["odd_exec"] - 1.0))
    else:
        df_te["ev"] = df_te["prob"] * (df_te["odd_exec"] - 1.0) * (1.0 - COMMISSION) - (1.0 - df_te["prob"])
        df_te["pnl"] = np.where(df_te["target_win"] == 1, STAKE * (df_te["odd_exec"] - 1.0) * (1.0 - COMMISSION), -STAKE)
        
    cond_te = (df_te["prob"] >= m["prob_cut"]) & (df_te["ev"] >= 0.01)
    if m["min_xgot"] > 0:
        cond_te = cond_te & (df_te["total_xGOT"] >= m["min_xgot"])
        
    sub_te = df_te[cond_te].copy()
    n_te = len(sub_te)
    
    if n_te > 0:
        gr_te = (sub_te["target_win"] == 1).sum()
        rd_te = n_te - gr_te
        wr_te = gr_te / n_te * 100.0
        odd_m = sub_te["odd_exec"].mean()
        if m["mode"] == "LAY":
            be_te = ((odd_m - 1.0) / (odd_m - COMMISSION)) * 100.0
        else:
            be_te = (1.0 / (odd_m - COMMISSION)) * 100.0
        pnl_te = sub_te["pnl"].sum()
        roi_te = pnl_te / (n_te * STAKE) * 100.0
        margem_te = wr_te - be_te
    else:
        gr_te, rd_te, wr_te, be_te, margem_te, pnl_te, roi_te, odd_m = 0, 0, 0, 0, 0, 0, 0, 0

    if margem_te > 0.5 and roi_te > 0.0 and n_te >= 50:
        veredito = "🟢 APROVADO COM EDGE"
    elif margem_te > -1.0 and n_te >= 20:
        veredito = "🟡 WATCHLIST / BREAK-EVEN"
    else:
        veredito = "🔴 MORTO / SEM EDGE"

    resultados_auditoria_total.append({
        "Método": nome,
        "Tipo": m["tipo"],
        "Amostra Cega (N)": n_te,
        "Greens": gr_te,
        "Reds": rd_te,
        "Odd Média": round(odd_m, 2),
        "WR Cega (%)": round(wr_te, 1),
        "BE Cego (%)": round(be_te, 1),
        "Margem Real (%)": round(margem_te, 1),
        "Lucro Cego (R$)": round(pnl_te, 2),
        "ROI Cego (%)": round(roi_te, 1),
        "Veredito Científico": veredito
    })

df_res_total = pd.DataFrame(resultados_auditoria_total)

print("\n==================================================================")
print("     RESULTADO FINAL DA AUDITORIA GERAL DE TODOS OS MÉTODOS       ")
print("==================================================================")
print(df_res_total.to_string(index=False))

df_res_total.to_excel("Auditoria_Total_Metodos_Betfair_Real.xlsx", index=False)
print("\n[+] Planilha consolidada gerada: Auditoria_Total_Metodos_Betfair_Real.xlsx", flush=True)
