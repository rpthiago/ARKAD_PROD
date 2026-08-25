import sys
sys.stdout.reconfigure(encoding='utf-8')
import os, joblib, numpy as np, pandas as pd
from datetime import datetime
from pathlib import Path
from xgboost import XGBClassifier

ROOT = Path(__file__).resolve().parent
MODELS_DIR = ROOT / "models"
MODELS_DIR.mkdir(exist_ok=True)

print("="*80, flush=True)
print("  TREINAMENTO CANÔNICO: LAY UNDER 1.5 FT (XGBOOST)", flush=True)
print("="*80, flush=True)

# 1. Carregar base de dados unificada
data_path = ROOT / "scratch" / "dataset_leak_free_features.parquet"
if not data_path.exists():
    raise FileNotFoundError(f"Base de features {data_path} não encontrada.")

df = pd.read_parquet(data_path)
df['Date'] = pd.to_datetime(df['Date'])
df = df.sort_values('Date').reset_index(drop=True)

# Target: Ganha no Lay Under 1.5 se saírem >= 2 gols (Over 1.5)
df['total_goals'] = df['Goals_H_FT'] + df['Goals_A_FT']
df['is_over15'] = (df['total_goals'] > 1.5).astype(int)

features = [
    'p_H_clean', 'p_D_clean', 'p_A_clean', 'p_Over_clean', 'p_Under_clean', 'entropy_1x2',
    'VAR01', 'VAR02', 'VAR03', 'VAR04', 'VAR05', 'VAR06', 'VAR07', 'VAR08', 'VAR09', 'VAR10', 'VAR54', 'VAR55', 'VAR56',
    'H_GF_r5', 'H_GA_r5', 'H_xGF_r5', 'H_xGA_r5', 'H_SoTF_r5', 'H_SoTA_r5', 'H_CornersF_r5', 'H_CornersA_r5',
    'A_GF_r5', 'A_GA_r5', 'A_xGF_r5', 'A_xGA_r5', 'A_SoTF_r5', 'A_SoTA_r5', 'A_CornersF_r5', 'A_CornersA_r5',
    'liga_hw_rate', 'liga_draw_rate', 'liga_aw_rate', 'liga_o25_rate', 'liga_btts_rate', 'liga_0x0_rate'
]

odd_col = "Odd_Under15_FT_Lay"
odd_min, odd_max = 2.50, 4.50
valid_mask = (df[odd_col] >= odd_min) & (df[odd_col] <= odd_max) & df[odd_col].notna()

df_valid = df[valid_mask].copy()
print(f"[+] Total de jogos no universo de odd Lay Under 1.5 FT ({odd_min} - {odd_max}): {len(df_valid)}", flush=True)

# Divisão temporal: Treino histórico completo até 2025-12-31
train_mask = df_valid['Date'] < '2026-01-01'
test_mask = df_valid['Date'] >= '2026-01-01'

X_tr = df_valid.loc[train_mask, features]
y_tr = df_valid.loc[train_mask, 'is_over15'].values

X_te = df_valid.loc[test_mask, features]
y_te = df_valid.loc[test_mask, 'is_over15'].values
odds_te = df_valid.loc[test_mask, odd_col].values

print(f"[*] Treinando XGBoost Classifier com {len(X_tr)} amostras históricas...", flush=True)
model = XGBClassifier(
    n_estimators=50,
    max_depth=3,
    learning_rate=0.05,
    random_state=42,
    eval_metric='logloss',
    n_jobs=1
)
model.fit(X_tr, y_tr)

# Validação out-of-sample
COMMISSION = 0.045
p_pred = model.predict_proba(X_te)[:, 1]
ev = p_pred * (1 - COMMISSION) - (1 - p_pred) * (odds_te - 1)

for limiar in [0.05, 0.08]:
    sel = ev >= limiar
    n = sel.sum()
    if n > 0:
        y_sel = y_te[sel]
        o_sel = odds_te[sel]
        pnl = np.where(y_sel == 1, (1 - COMMISSION), -(o_sel - 1))
        roi = (pnl.sum() / n) * 100
        wr = y_sel.mean() * 100
        be_wr = ((o_sel - 1) / (o_sel - COMMISSION)).mean() * 100
        print(f"   [OOS 2026] EV >= {limiar*100:.0f}%: {n} apostas | WR: {wr:.1f}% (BE: {be_wr:.1f}%) | Lucro: {pnl.sum():+.2f}u | ROI: {roi:+.2f}%")

# Salvar modelo e metadata
bundle = {
    'model': model,
    'features': features,
    'odd_col': odd_col,
    'odd_min': odd_min,
    'odd_max': odd_max,
    'commission': COMMISSION,
    'target_mode': 'LAY_UNDER15',
    'trained_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    'train_cutoff': '2025-12-31'
}

save_file = MODELS_DIR / "modelo_lay_under15_xgb.joblib"
joblib.dump(bundle, save_file)
print(f"[OK] Modelo canônico salvo em: {save_file}", flush=True)
