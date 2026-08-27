# -*- coding: utf-8 -*-
"""
estrategia_back_mandante.py — Módulo Operacional para Back Mandante Favorito (XGBoost)
Regras do GEMINI.md:
- Odd executável de Back na Betfair (Odd_H_Back)
- Comissão 4.5%: EV = p * (odd_back - 1) * (1 - 0.045) - (1 - p)
- Sem fabricação de features (NaN -> SKIP)
"""

import os
import joblib
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MODEL_PATH = ROOT / "models" / "modelo_back_mandante_xgb.joblib"

_MODEL_BUNDLE = None

def get_model_bundle():
    global _MODEL_BUNDLE
    if _MODEL_BUNDLE is None:
        if not MODEL_PATH.exists():
            raise FileNotFoundError(f"Modelo não encontrado em {MODEL_PATH}. Execute treinar_back_mandante_xgb.py primeiro.")
        _MODEL_BUNDLE = joblib.load(MODEL_PATH)
    return _MODEL_BUNDLE

def avaliar_jogo_back_mandante(row_dict, ev_threshold=0.03):
    """
    Avalia uma partida para entrada em Back Mandante Favorito.
    Retorna dicionário com status, probabilidade estimada, EV e sinal.
    """
    bundle = get_model_bundle()
    model = bundle['model']
    features = bundle['features']
    odd_col = bundle['odd_col']
    odd_min = bundle['odd_min']
    odd_max = bundle['odd_max']
    comm = bundle['commission']
    xg_min = bundle.get('xg_min', 1.30)
    
    # 1. Validar odd de Back real
    odd_back = float(row_dict.get(odd_col, 0.0) or 0.0)
    if odd_back < odd_min or odd_back > odd_max:
        return {'aplica': False, 'motivo': f'Odd {odd_back:.2f} fora da faixa [{odd_min}, {odd_max}]'}
        
    # 2. Validar filtro tático de produção ofensiva
    h_xg = float(row_dict.get('H_xGF_r5', 0.0) or 0.0)
    if h_xg < xg_min:
        return {'aplica': False, 'motivo': f'xGF mandante {h_xg:.2f} abaixo do mínimo {xg_min:.2f}'}
        
    # 3. Validar features (sem fabricação de dados)
    feat_values = []
    for f in features:
        val = row_dict.get(f)
        if val is None or pd.isna(val):
            return {'aplica': False, 'motivo': f'Feature {f} ausente/NaN (SKIP)'}
        feat_values.append(float(val))
        
    X = pd.DataFrame([feat_values], columns=features)
    
    # 4. Predição de probabilidade (P de vitória do mandante)
    p_win = float(model.predict_proba(X)[0, 1])
    
    # 5. Cálculo matemático do EV de BACK
    ev = p_win * (odd_back - 1.0) * (1.0 - comm) - (1.0 - p_win)
    break_even_wr = 1.0 / (odd_back * (1.0 - comm))
    
    aplica = ev >= ev_threshold
    
    return {
        'aplica': aplica,
        'metodo': 'Back Mandante Favorito (XGBoost)',
        'mercado': 'Match Odds (Home)',
        'lado': 'back',
        'odd_back': odd_back,
        'prob_estimada': p_win,
        'break_even_wr': break_even_wr,
        'ev': ev,
        'ev_pct': f"{ev*100:.1f}%",
        'motivo': 'Sinal gerado com EV+' if aplica else f'EV {ev*100:.1f}% abaixo do limiar {ev_threshold*100:.1f}%'
    }
