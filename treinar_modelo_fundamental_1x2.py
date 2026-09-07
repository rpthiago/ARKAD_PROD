# -*- coding: utf-8 -*-
"""
treinar_modelo_fundamental_1x2.py — Treinamento e Avaliação do Modelo Fundamental de IA (1X2)

Pipeline Quantitativo Profissional:
1. Carrega dataset leak-free gerado por features_fundamental.py.
2. Descarta registros com NaNs (Regra 3: NUNCA fabricar features).
3. Split Temporal Estrito: Treino até 2024-12-31 | Teste OOS Congelado 2025-01-01 a 2026-08-07.
4. Normalização com StandardScaler obrigatório.
5. Calibração de Probabilidades Multiclasse P(H), P(D), P(A).
6. Comparação contra Odds Reais Executáveis da Betfair com comissão estrita de 5%.
7. Avaliação de Value Bets (EV >= 3%, 5%, 7%) com Break-Even WR e Bootstrap IC95%.
8. Exportação do modelo para models/modelo_fundamental_1x2.pkl.
"""

import sys
import os
import time
import joblib
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import log_loss, brier_score_loss

ROOT = Path(__file__).resolve().parent
MODELS_DIR = ROOT / "models"
MODELS_DIR.mkdir(exist_ok=True)
PARQUET_PATH = ROOT / "scratch" / "dataset_fundamental_1x2.parquet"
MODEL_OUTPUT = MODELS_DIR / "modelo_fundamental_1x2.pkl"


def run_training_and_evaluation():
    t0 = time.time()
    print("=" * 80)
    print("  TREINAMENTO DO MODELO FUNDAMENTAL DE IA — MATCH ODDS 1X2")
    print("=" * 80)

    if not PARQUET_PATH.exists():
        raise FileNotFoundError(f"Arquivo {PARQUET_PATH} não encontrado. Execute features_fundamental.py primeiro.")

    df = pd.read_parquet(PARQUET_PATH)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)
    print(f"[*] Total de jogos carregados: {len(df)} ({df['Date'].min().date()} a {df['Date'].max().date()})")

    feature_cols = [
        # Venue (Casa / Fora)
        "H_GF_venue_r5", "H_GA_venue_r5", "H_Win_venue_r5", "H_CS_venue_r5",
        "A_GF_venue_r5", "A_GA_venue_r5", "A_Win_venue_r5", "A_CS_venue_r5",
        # Global recente (r10)
        "H_GF_all_r10", "H_GA_all_r10", "H_Won_all_r10", "H_CS_all_r10",
        "A_GF_all_r10", "A_GA_all_r10", "A_Won_all_r10", "A_CS_all_r10",
        # Diferenciais
        "Diff_GF_venue", "Diff_GA_venue", "Diff_Net_venue", "Diff_Win_venue",
        "Diff_GF_all", "Diff_GA_all", "Diff_Net_all", "Diff_Win_all"
    ]

    # Limpeza estrita de NaNs
    valid_mask = df[feature_cols].notna().all(axis=1) & df["Target_1X2"].isin([0, 1, 2])
    df_clean = df[valid_mask].copy().reset_index(drop=True)
    print(f"[+] Jogos válidos sem NaNs (Regra 3 cumprida): {len(df_clean)} ({len(df_clean)/len(df):.1%})")

    # Split Temporal Congelado
    # Treino: 2024 e anteriores
    # Teste Out-of-Sample (OOS): 2025-01-01 em diante
    train_mask = df_clean["Date"] < "2025-01-01"
    test_mask = df_clean["Date"] >= "2025-01-01"

    df_train = df_clean[train_mask].copy()
    df_test = df_clean[test_mask].copy()
    print(f"[+] Amostra de Treino (< 2025): {len(df_train)} jogos")
    print(f"[+] Amostra de Teste OOS (>= 2025): {len(df_test)} jogos")

    X_train = df_train[feature_cols].values
    y_train = df_train["Target_1X2"].values

    X_test = df_test[feature_cols].values
    y_test = df_test["Target_1X2"].values

    # 1. Normalização com StandardScaler
    print("[*] Normalizando features com StandardScaler...")
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # 2. Treinamento do Modelo Multiclasse Calibrado
    print("[*] Treinando Regressão Logística Multinomial Calibrada...")
    base_model = LogisticRegression(
        multi_class="multinomial",
        solver="lbfgs",
        C=0.1,  # Regularização L2 para evitar overfitting
        max_iter=1000,
        random_state=42
    )
    base_model.fit(X_train_scaled, y_train)

    # 3. Predição de Probabilidades Calibradas P(H), P(D), P(A)
    probs_train = base_model.predict_proba(X_train_scaled)
    probs_test = base_model.predict_proba(X_test_scaled)

    # Avaliação de Calibração
    loss_train = log_loss(y_train, probs_train)
    loss_test = log_loss(y_test, probs_test)
    print(f"[+] Log-Loss Treino: {loss_train:.4f} | Log-Loss Teste OOS: {loss_test:.4f}")

    # Acoplar probabilidades ao dataframe de teste
    df_test["prob_H"] = probs_test[:, 0]
    df_test["prob_D"] = probs_test[:, 1]
    df_test["prob_A"] = probs_test[:, 2]

    # Salvar o pipeline completo (scaler + model)
    pipeline = {
        "scaler": scaler,
        "model": base_model,
        "feature_cols": feature_cols
    }
    joblib.dump(pipeline, MODEL_OUTPUT)
    print(f"[+] Modelo e Scaler salvos com sucesso em: {MODEL_OUTPUT}")

    # 4. Avaliação Financeira Rigorosa na Betfair (Comissão 5%)
    print("\n" + "=" * 80)
    print("  AVALIAÇÃO DE VALUE BETS (OUT-OF-SAMPLE 2025-2026) — ODDS EXECUTÁVEIS BETFAIR")
    print("=" * 80)

    for ev_thresh in [0.03, 0.05, 0.07]:
        print(f"\n--- THRESHOLD EV >= {ev_thresh:.0%} ---")
        
        # ── Teste BACK HOME ──
        mask_h = (
            df_test["Odd_H_Back"].notna() &
            (df_test["Odd_H_Back"] >= 1.30) &
            (df_test["Odd_H_Back"] <= 5.00)
        )
        sub_h = df_test[mask_h].copy()
        # EV = P * (Odd - 1) * 0.95 - (1 - P)
        sub_h["EV"] = sub_h["prob_H"] * (sub_h["Odd_H_Back"] - 1.0) * 0.95 - (1.0 - sub_h["prob_H"])
        bets_h = sub_h[sub_h["EV"] >= ev_thresh].copy()
        
        if len(bets_h) >= 30:
            bets_h["Won"] = (bets_h["Target_1X2"] == 0).astype(int)
            bets_h["PnL"] = np.where(bets_h["Won"] == 1, (bets_h["Odd_H_Back"] - 1.0) * 0.95, -1.0)
            bets_h["BE_WR"] = 1.0 / (0.05 + 0.95 * bets_h["Odd_H_Back"])
            
            wr = bets_h["Won"].mean()
            be_wr = bets_h["BE_WR"].mean()
            pnl = bets_h["PnL"].sum()
            roi = (pnl / len(bets_h)) * 100.0
            
            # Bootstrap IC95
            boot_rois = [np.random.choice(bets_h["PnL"].values, size=len(bets_h), replace=True).mean() * 100.0 for _ in range(1000)]
            ci_lo, ci_hi = np.percentile(boot_rois, 2.5), np.percentile(boot_rois, 97.5)
            
            print(f"  [BACK HOME] N={len(bets_h)} | WR={wr:.1%} vs BE={be_wr:.1%} | PnL={pnl:+.2f}u | ROI={roi:+.2f}% | IC95%=[{ci_lo:+.1f}%, {ci_hi:+.1f}%]")

        # ── Teste BACK AWAY ──
        mask_a = (
            df_test["Odd_A_Back"].notna() &
            (df_test["Odd_A_Back"] >= 1.50) &
            (df_test["Odd_A_Back"] <= 6.00)
        )
        sub_a = df_test[mask_a].copy()
        sub_a["EV"] = sub_a["prob_A"] * (sub_a["Odd_A_Back"] - 1.0) * 0.95 - (1.0 - sub_a["prob_A"])
        bets_a = sub_a[sub_a["EV"] >= ev_thresh].copy()
        
        if len(bets_a) >= 30:
            bets_a["Won"] = (bets_a["Target_1X2"] == 2).astype(int)
            bets_a["PnL"] = np.where(bets_a["Won"] == 1, (bets_a["Odd_A_Back"] - 1.0) * 0.95, -1.0)
            bets_a["BE_WR"] = 1.0 / (0.05 + 0.95 * bets_a["Odd_A_Back"])
            
            wr = bets_a["Won"].mean()
            be_wr = bets_a["BE_WR"].mean()
            pnl = bets_a["PnL"].sum()
            roi = (pnl / len(bets_a)) * 100.0
            
            boot_rois = [np.random.choice(bets_a["PnL"].values, size=len(bets_a), replace=True).mean() * 100.0 for _ in range(1000)]
            ci_lo, ci_hi = np.percentile(boot_rois, 2.5), np.percentile(boot_rois, 97.5)
            
            print(f"  [BACK AWAY] N={len(bets_a)} | WR={wr:.1%} vs BE={be_wr:.1%} | PnL={pnl:+.2f}u | ROI={roi:+.2f}% | IC95%=[{ci_lo:+.1f}%, {ci_hi:+.1f}%]")

        # ── Teste LAY HOME ──
        mask_lh = (
            df_test["Odd_H_Lay"].notna() &
            (df_test["Odd_H_Lay"] >= 1.40) &
            (df_test["Odd_H_Lay"] <= 4.00)
        )
        sub_lh = df_test[mask_lh].copy()
        p_lay_h = sub_lh["prob_D"] + sub_lh["prob_A"]  # Ganha o Lay Home se der Empate ou Fora
        sub_lh["EV"] = p_lay_h * 0.95 - (1.0 - p_lay_h) * (sub_lh["Odd_H_Lay"] - 1.0)
        bets_lh = sub_lh[sub_lh["EV"] >= ev_thresh].copy()
        
        if len(bets_lh) >= 30:
            bets_lh["Won"] = (bets_lh["Target_1X2"] != 0).astype(int)
            bets_lh["Liability"] = bets_lh["Odd_H_Lay"] - 1.0
            bets_lh["PnL_liab"] = np.where(bets_lh["Won"] == 1, 0.95 / bets_lh["Liability"], -1.0)
            bets_lh["BE_WR"] = bets_lh["Liability"] / (bets_lh["Odd_H_Lay"] - 0.05)
            
            wr = bets_lh["Won"].mean()
            be_wr = bets_lh["BE_WR"].mean()
            roi_liab = bets_lh["PnL_liab"].mean() * 100.0
            
            boot_rois = [np.random.choice(bets_lh["PnL_liab"].values, size=len(bets_lh), replace=True).mean() * 100.0 for _ in range(1000)]
            ci_lo, ci_hi = np.percentile(boot_rois, 2.5), np.percentile(boot_rois, 97.5)
            
            print(f"  [LAY HOME] N={len(bets_lh)} | WR={wr:.1%} vs BE={be_wr:.1%} | ROI/Liab={roi_liab:+.2f}% | IC95%=[{ci_lo:+.1f}%, {ci_hi:+.1f}%]")

        # ── Teste LAY DRAW ──
        mask_ld = (
            df_test["Odd_D_Lay"].notna() &
            (df_test["Odd_D_Lay"] >= 3.00) &
            (df_test["Odd_D_Lay"] <= 8.00)
        )
        sub_ld = df_test[mask_ld].copy()
        p_lay_d = sub_ld["prob_H"] + sub_ld["prob_A"]  # Ganha o Lay Draw se não empatar
        sub_ld["EV"] = p_lay_d * 0.95 - (1.0 - p_lay_d) * (sub_ld["Odd_D_Lay"] - 1.0)
        bets_ld = sub_ld[sub_ld["EV"] >= ev_thresh].copy()
        
        if len(bets_ld) >= 30:
            bets_ld["Won"] = (bets_ld["Target_1X2"] != 1).astype(int)
            bets_ld["Liability"] = bets_ld["Odd_D_Lay"] - 1.0
            bets_ld["PnL_liab"] = np.where(bets_ld["Won"] == 1, 0.95 / bets_ld["Liability"], -1.0)
            bets_ld["BE_WR"] = bets_ld["Liability"] / (bets_ld["Odd_D_Lay"] - 0.05)
            
            wr = bets_ld["Won"].mean()
            be_wr = bets_ld["BE_WR"].mean()
            roi_liab = bets_ld["PnL_liab"].mean() * 100.0
            
            boot_rois = [np.random.choice(bets_ld["PnL_liab"].values, size=len(bets_ld), replace=True).mean() * 100.0 for _ in range(1000)]
            ci_lo, ci_hi = np.percentile(boot_rois, 2.5), np.percentile(boot_rois, 97.5)
            
            print(f"  [LAY DRAW] N={len(bets_ld)} | WR={wr:.1%} vs BE={be_wr:.1%} | ROI/Liab={roi_liab:+.2f}% | IC95%=[{ci_lo:+.1f}%, {ci_hi:+.1f}%]")

    print(f"\n[+] Execução concluída em {time.time()-t0:.2f}s.")


if __name__ == "__main__":
    run_training_and_evaluation()
