import os
import pandas as pd
import numpy as np
import joblib
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.ensemble import RandomForestClassifier
import xgboost as xgb
import lightgbm as lgb

import master_feature_engineer

print('=== TREINAMENTO DA SUÍTE DE IA: XGBOOST + LIGHTGBM + RANDOM FOREST + ENSEMBLE ===\n')

# 1. Carregar base de dados histórica
df = pd.read_csv('Bases_de_Dados_API_FutPythonTrader_Bet365.csv', low_memory=False)
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df = df.dropna(subset=['Date', 'Goals_H_FT', 'Goals_A_FT']).sort_values('Date').reset_index(drop=True)

# 2. Definir Alvos de Treinamento (Targets)
# Target 1: Gols no jogo (Over 0.5 FT / Over 2.5 FT)
gols_ft = df['Goals_H_FT'] + df['Goals_A_FT']
df['Target_Over05'] = (gols_ft > 0).astype(int)
df['Target_Over25'] = (gols_ft > 2).astype(int)

# 3. Gerar 88 Features Quantitativas usando o motor mestre
print('Engenharia de 88 Features Quantitativas em processamento...')
feats = master_feature_engineer.build_master_features(df)

# Divisão Temporal Out-Of-Sample (80% Treino / 20% Teste Futuro)
split_idx = int(len(df) * 0.8)

X_train, y_train = feats.iloc[:split_idx], df['Target_Over05'].iloc[:split_idx]
X_test, y_test = feats.iloc[split_idx:], df['Target_Over05'].iloc[split_idx:]

print(f'Partidas de Treino (In-Sample): {len(X_train):,}')
print(f'Partidas de Teste (Out-Of-Sample): {len(X_test):,}\n')

# --- MODELO 1: RANDOM FOREST ---
print('1/3 Treinando Random Forest Classifier...')
rf_model = RandomForestClassifier(n_estimators=150, max_depth=8, random_state=42, n_jobs=-1)
rf_model.fit(X_train, y_train)
rf_probs = rf_model.predict_proba(X_test)[:, 1]
rf_auc = roc_auc_score(y_test, rf_probs)
rf_acc = accuracy_score(y_test, (rf_probs >= 0.5).astype(int))
print(f'   Random Forest -> ROC-AUC OOS: {rf_auc:.4f} | Acurácia: {rf_acc:.2%}')

# --- MODELO 2: XGBOOST ---
print('\n2/3 Treinando XGBoost Classifier (Gradient Boosting Extremo)...')
xgb_model = xgb.XGBClassifier(
    n_estimators=200,
    max_depth=6,
    learning_rate=0.03,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    n_jobs=-1
)
xgb_model.fit(X_train, y_train)
xgb_probs = xgb_model.predict_proba(X_test)[:, 1]
xgb_auc = roc_auc_score(y_test, xgb_probs)
xgb_acc = accuracy_score(y_test, (xgb_probs >= 0.5).astype(int))
print(f'   XGBoost -> ROC-AUC OOS: {xgb_auc:.4f} | Acurácia: {xgb_acc:.2%}')

# --- MODELO 3: LIGHTGBM ---
print('\n3/3 Treinando LightGBM Classifier (Árvores Rápidas por Histograma)...')
lgb_model = lgb.LGBMClassifier(
    n_estimators=200,
    max_depth=6,
    learning_rate=0.03,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    n_jobs=-1,
    verbose=-1
)
lgb_model.fit(X_train, y_train)
lgb_probs = lgb_model.predict_proba(X_test)[:, 1]
lgb_auc = roc_auc_score(y_test, lgb_probs)
lgb_acc = accuracy_score(y_test, (lgb_probs >= 0.5).astype(int))
print(f'   LightGBM -> ROC-AUC OOS: {lgb_auc:.4f} | Acurácia: {lgb_acc:.2%}')

# --- ENSEMBLE COMBO (MÉDIA PONDERADA DAS 3 IAs) ---
ensemble_probs = (xgb_probs * 0.40) + (lgb_probs * 0.40) + (rf_probs * 0.20)
ensemble_auc = roc_auc_score(y_test, ensemble_probs)
ensemble_acc = accuracy_score(y_test, (ensemble_probs >= 0.5).astype(int))

print('\n==================================================')
print(f'🏆 ENSEMBLE COMBO (XGBoost 40% + LightGBM 40% + RF 20%):')
print(f'   ROC-AUC Out-Of-Sample: {ensemble_auc:.4f} | Acurácia: {ensemble_acc:.2%}')
print('==================================================\n')

# Salvar modelos em pkl
joblib.dump(xgb_model, 'modelo_xgboost_quant.pkl')
joblib.dump(lgb_model, 'modelo_lightgbm_quant.pkl')
joblib.dump(rf_model, 'modelo_rf_quant.pkl')

print('Todos os modelos de IA (XGBoost, LightGBM e Random Forest) foram salvos com sucesso!')
