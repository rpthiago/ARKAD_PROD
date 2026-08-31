"""
estrategia_lay_under15.py — Módulo Operacional para Lay Under 1.5 FT (XGBoost)

Implementação honesta com base nas regras do GEMINI.md:
- Odd executável na Betfair Exchange (Odd_Under15_FT_Lay)
- Cálculo honesto de EV para LAY (comissão 4.5% / 5%):
    EV = p * (1 - 0.045) - (1 - p) * (odd_lay - 1)
- Sem fabricação de features (NaN -> SKIP)
"""

import os
import joblib
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MODEL_PATH = ROOT / "models" / "modelo_lay_under15_xgb.joblib"

_MODEL_BUNDLE = None

def get_model_bundle():
    global _MODEL_BUNDLE
    if _MODEL_BUNDLE is None:
        if not MODEL_PATH.exists():
            raise FileNotFoundError(f"Modelo não encontrado em {MODEL_PATH}. Execute treinar_lay_under15_xgb.py primeiro.")
        _MODEL_BUNDLE = joblib.load(MODEL_PATH)
    return _MODEL_BUNDLE

def avaliar_jogo_lay_under15(row_dict, ev_threshold=0.05):
    """
    Avalia uma partida para entrada em Lay Under 1.5 FT.
    Retorna dicionário com status, probabilidade estimada, EV e sinal.
    """
    bundle = get_model_bundle()
    model = bundle['model']
    features = bundle['features']
    odd_col = bundle['odd_col']
    odd_min = bundle['odd_min']
    odd_max = bundle['odd_max']
    comm = bundle['commission']
    
    # 1. Validar odd de Lay real
    odd_lay = float(row_dict.get(odd_col, 0.0) or 0.0)
    if odd_lay < odd_min or odd_lay > odd_max:
        return {'aplica': False, 'motivo': f'Odd {odd_lay:.2f} fora da faixa [{odd_min}, {odd_max}]'}
        
    # 2. Validar features (sem fabricação de dados)
    feat_values = []
    for f in features:
        val = row_dict.get(f)
        if val is None or pd.isna(val):
            return {'aplica': False, 'motivo': f'Feature {f} ausente/NaN (SKIP)'}
        feat_values.append(float(val))
        
    X = pd.DataFrame([feat_values], columns=features)
    
    # 3. Predição de probabilidade (P de o Lay ganhar = P de sair >= 2 gols)
    p_win = float(model.predict_proba(X)[0, 1])
    
    # 4. Cálculo matemático do EV de LAY
    ev = p_win * (1.0 - comm) - (1.0 - p_win) * (odd_lay - 1.0)
    break_even_wr = (odd_lay - 1.0) / (odd_lay - comm)
    
    aplica = ev >= ev_threshold
    
    return {
        'aplica': aplica,
        'metodo': 'Lay Under 1.5 FT',
        'mercado': 'Under15_FT',
        'lado': 'lay',
        'odd_lay': odd_lay,
        'prob_estimada': p_win,
        'break_even_wr': break_even_wr,
        'ev': ev,
        'ev_pct': f"{ev*100:.1f}%",
        'motivo': 'Sinal gerado com EV+' if aplica else f'EV {ev*100:.1f}% abaixo do limiar {ev_threshold*100:.1f}%'
    }
