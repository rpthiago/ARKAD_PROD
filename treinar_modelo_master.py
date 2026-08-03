"""
Script de Treinamento e Avaliação do Modelo Mestre (Master Feature Model)
MÉTODO ARKAD_PROD

Treina um classificador Gradient Boosting / Random Forest utilizando as 88 variáveis
quantitativas de mercado sanitizadas para prever a probabilidade de ocorrência de gols.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, precision_score
import master_feature_engineer
import joblib


def run_master_training():
    print("=== TREINANDO MODELO MESTRE DE ENGENHARIA DE RECURSOS ===")
    
    # 1. Carregar dados históricos
    df = pd.read_csv('Bases_de_Dados_API_FutPythonTrader_Bet365.csv', low_memory=False)
    print(f"Total de jogos carregados: {len(df):,}")

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date', 'Goals_H_FT', 'Goals_A_FT']).sort_values('Date').reset_index(drop=True)

    # Alvo 1: Ocorrência de Gol (Over 0.5 / Lay 0x0)
    df['Target_Gol'] = ((df['Goals_H_FT'] + df['Goals_A_FT']) > 0).astype(int)

    # 2. Gerar as 88 Variáveis Quantitativas
    X_feats = master_feature_engineer.build_master_features(df)
    y_target = df['Target_Gol']

    # 3. Divisão Temporal Fora da Amostra (Walk-Forward Split 80% Treino / 20% Teste)
    split_idx = int(len(df) * 0.80)
    X_train, X_test = X_feats.iloc[:split_idx], X_feats.iloc[split_idx:]
    y_train, y_test = y_target.iloc[:split_idx], y_target.iloc[split_idx:]

    print(f"Amostra de Treino (Histórico Antigo): {len(X_train):,} jogos")
    print(f"Amostra de Teste (Fora da Amostra / Recente): {len(X_test):,} jogos")

    # 4. Treinar Random Forest / Gradient Booster
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=6,
        min_samples_leaf=50,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # 5. Avaliação Fora da Amostra (OOS)
    probs_test = model.predict_proba(X_test)[:, 1]
    auc_score = roc_auc_score(y_test, probs_test)

    print(f"\n[OK] AUC ROC Fora da Amostra: {auc_score:.4f}")

    # Top 10 Features Mais Importantes
    importances = pd.Series(model.feature_importances_, index=X_feats.columns).sort_values(ascending=False)
    print("\n--- TOP 10 FEATURES MAIS RELEVANTES REVELADAS PELO MODELO ---")
    for feat, imp in importances.head(10).items():
        print(f"{feat:15s}: {imp:.4f}")

    # Salvar modelo e metadados
    joblib.dump(model, 'modelo_mestre_quant.pkl')
    joblib.dump(X_feats.columns.tolist(), 'features_mestre_quant.pkl')
    print("\nModelo salvo com sucesso em 'modelo_mestre_quant.pkl'!")


if __name__ == '__main__':
    run_master_training()
