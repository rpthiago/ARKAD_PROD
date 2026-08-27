# -*- coding: utf-8 -*-
"""
treinar_back_mandante_xgb.py — Treinador Canônico do Modelo Back Mandante Favorito
Segue estritamente as 5 Leis do GEMINI.md:
- Split temporal estrito: Treino < 2026 / Teste OOS 2026
- Features strictly shift(1)
- Odd de Back real com comissão de 4.5%
- Filtro tático: H_xGF_r5 >= 1.30
"""

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
print("  TREINAMENTO CANÔNICO: BACK MANDANTE FAVORITO (XGBOOST)", flush=True)
print("="*80, flush=True)

# 1. Carregar base de dados unificada
data_path = ROOT / "scratch" / "dataset_leak_free_features.parquet"
if not data_path.exists():
    raise FileNotFoundError(f"Base de features {data_path} não encontrada.")

df = pd.read_parquet(data_path)
df['Date'] = pd.to_datetime(df['Date'])
df = df.sort_values('Date').reset_index(drop=True)

# Target: Mandante vence a partida
df['is_h_win'] = (df['Goals_H_FT'] > df['Goals_A_FT']).astype(int)

features = [
    'p_H_clean', 'p_D_clean', 'p_A_clean', 'p_Over_clean', 'p_Under_clean', 'entropy_1x2',
    'VAR01', 'VAR02', 'VAR03', 'VAR04', 'VAR05', 'VAR06', 'VAR07', 'VAR08', 'VAR09', 'VAR10', 'VAR54', 'VAR55', 'VAR56',
    'H_GF_r5', 'H_GA_r5', 'H_xGF_r5', 'H_xGA_r5', 'H_SoTF_r5', 'H_SoTA_r5', 'H_CornersF_r5', 'H_CornersA_r5',
    'A_GF_r5', 'A_GA_r5', 'A_xGF_r5', 'A_xGA_r5', 'A_SoTF_r5', 'A_SoTA_r5', 'A_CornersF_r5', 'A_CornersA_r5',
    'liga_hw_rate', 'liga_draw_rate', 'liga_aw_rate', 'liga_o25_rate', 'liga_btts_rate', 'liga_0x0_rate'
]

odd_col = "Odd_H_Back"
odd_min, odd_max = 1.45, 2.20
COMMISSION = 0.045

valid_mask = (
    (df[odd_col] >= odd_min) & (df[odd_col] <= odd_max) & 
    (df['H_xGF_r5'] >= 1.30) &
    df[odd_col].notna() &
    df['Goals_H_FT'].notna() & df['Goals_A_FT'].notna()
)

df_valid = df[valid_mask].copy().reset_index(drop=True)
print(f"[+] Total de jogos no universo qualificado ({odd_min} - {odd_max} e H_xGF >= 1.30): {len(df_valid)}")

# Divisão temporal: Treino histórico completo até 2025-12-31 / Teste 2026
train_mask = df_valid['Date'] < '2026-01-01'
test_mask = df_valid['Date'] >= '2026-01-01'

X_tr = df_valid.loc[train_mask, features]
y_tr = df_valid.loc[train_mask, 'is_h_win'].values

X_te = df_valid.loc[test_mask, features]
y_te = df_valid.loc[test_mask, 'is_h_win'].values
odds_te = df_valid.loc[test_mask, odd_col].values

print(f"[*] Treinando XGBoost Classifier com {len(X_tr)} amostras históricas...", flush=True)
model = XGBClassifier(
    n_estimators=100,
    max_depth=3,
    learning_rate=0.05,
    random_state=42,
    eval_metric='logloss',
    n_jobs=1
)
model.fit(X_tr, y_tr)

# Validação OOS
p_pred = model.predict_proba(X_te)[:, 1]
ev = p_pred * (odds_te - 1.0) * (1 - COMMISSION) - (1 - p_pred)

for limiar in [0.03, 0.05]:
    sel = ev >= limiar
    n = sel.sum()
    if n > 0:
        y_sel = y_te[sel]
        o_sel = odds_te[sel]
        pnl = np.where(y_sel == 1, (o_sel - 1.0) * (1 - COMMISSION), -1.0)
        roi = (pnl.sum() / n) * 100
        wr = y_sel.mean() * 100
        be_wr = ((1.0 / (o_sel * (1 - COMMISSION)))).mean() * 100
        print(f"   [OOS 2026] EV >= {limiar*100:.0f}%: {n} apostas | WR: {wr:.1f}% (BE: {be_wr:.1f}%) | Lucro: {pnl.sum():+.2f}u | ROI: {roi:+.2f}%")

bundle = {
    'model': model,
    'features': features,
    'odd_col': odd_col,
    'odd_min': odd_min,
    'odd_max': odd_max,
    'xg_min': 1.30,
    'commission': COMMISSION,
    'target_mode': 'BACK_HOME_WIN',
    'trained_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    'train_cutoff': '2025-12-31'
}

save_file = MODELS_DIR / "modelo_back_mandante_xgb.joblib"
joblib.dump(bundle, save_file)
print(f"[OK] Modelo canônico salvo em: {save_file}", flush=True)
