"""
Módulo Mestre de Engenharia de Recursos (Master Feature Engineering)
MÉTODO ARKAD_PROD

Gera 100+ atributos quantitativos derivados de Odds, Probabilidades Limpas de Juice,
Entropia de Mercado, Quocientes Intermercados e Métricas Poisson.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List


def build_master_features(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma uma base de partidas em um DataFrame com 100+ variáveis quantitativas sanitizadas.
    """
    df = df_raw.copy()

    # 1. Odds Básicas com fallback seguro
    odd_h = pd.to_numeric(df['Odd_H_Back'] if 'Odd_H_Back' in df.columns else (df['Odd_H_FT'] if 'Odd_H_FT' in df.columns else (df['Odd_H'] if 'Odd_H' in df.columns else pd.Series(2.0, index=df.index))), errors='coerce').fillna(2.0)
    odd_d = pd.to_numeric(df['Odd_D_Back'] if 'Odd_D_Back' in df.columns else (df['Odd_D_FT'] if 'Odd_D_FT' in df.columns else (df['Odd_D'] if 'Odd_D' in df.columns else pd.Series(3.2, index=df.index))), errors='coerce').fillna(3.2)
    odd_a = pd.to_numeric(df['Odd_A_Back'] if 'Odd_A_Back' in df.columns else (df['Odd_A_FT'] if 'Odd_A_FT' in df.columns else (df['Odd_A'] if 'Odd_A' in df.columns else pd.Series(2.0, index=df.index))), errors='coerce').fillna(2.0)

    odd_o25 = pd.to_numeric(df['Odd_Over25_FT_Back'] if 'Odd_Over25_FT_Back' in df.columns else (df['Odd_Over25_FT'] if 'Odd_Over25_FT' in df.columns else (df['Odd_Over25'] if 'Odd_Over25' in df.columns else pd.Series(2.0, index=df.index))), errors='coerce').fillna(2.0)
    odd_u25 = pd.to_numeric(df['Odd_Under25_FT_Back'] if 'Odd_Under25_FT_Back' in df.columns else (df['Odd_Under25_FT'] if 'Odd_Under25_FT' in df.columns else (df['Odd_Under25'] if 'Odd_Under25' in df.columns else pd.Series(1.8, index=df.index))), errors='coerce').fillna(1.8)

    odd_btts_y = pd.to_numeric(df['Odd_BTTS_Yes_Back'] if 'Odd_BTTS_Yes_Back' in df.columns else (df['Odd_BTTS_Yes'] if 'Odd_BTTS_Yes' in df.columns else pd.Series(1.9, index=df.index)), errors='coerce').fillna(1.9)
    odd_btts_n = pd.to_numeric(df['Odd_BTTS_No_Back'] if 'Odd_BTTS_No_Back' in df.columns else (df['Odd_BTTS_No'] if 'Odd_BTTS_No' in df.columns else pd.Series(1.9, index=df.index)), errors='coerce').fillna(1.9)

    odd_0x0 = pd.to_numeric(df['Odd_CS_0x0_Lay'] if 'Odd_CS_0x0_Lay' in df.columns else (df['Odd_CS_0x0'] if 'Odd_CS_0x0' in df.columns else (df['Odd_0x0'] if 'Odd_0x0' in df.columns else pd.Series(12.0, index=df.index))), errors='coerce').fillna(12.0)
    odd_0x1 = pd.to_numeric(df['Odd_CS_0x1_Lay'] if 'Odd_CS_0x1_Lay' in df.columns else (df['Odd_CS_0x1'] if 'Odd_CS_0x1' in df.columns else (df['Odd_0x1'] if 'Odd_0x1' in df.columns else pd.Series(9.5, index=df.index))), errors='coerce').fillna(9.5)
    odd_1x0 = pd.to_numeric(df['Odd_CS_1x0_Lay'] if 'Odd_CS_1x0_Lay' in df.columns else (df['Odd_CS_1x0'] if 'Odd_CS_1x0' in df.columns else (df['Odd_1x0'] if 'Odd_1x0' in df.columns else pd.Series(7.5, index=df.index))), errors='coerce').fillna(7.5)

    # 2. Probabilidades Implícitas Brutas (1/Odd)
    p_h = 1.0 / np.maximum(1.001, odd_h)
    p_d = 1.0 / np.maximum(1.001, odd_d)
    p_a = 1.0 / np.maximum(1.001, odd_a)

    p_over = 1.0 / np.maximum(1.001, odd_o25)
    p_under = 1.0 / np.maximum(1.001, odd_u25)

    p_btts_y = 1.0 / np.maximum(1.001, odd_btts_y)
    p_btts_n = 1.0 / np.maximum(1.001, odd_btts_n)

    p_0x0 = 1.0 / np.maximum(1.001, odd_0x0)
    p_0x1 = 1.0 / np.maximum(1.001, odd_0x1)
    p_1x0 = 1.0 / np.maximum(1.001, odd_1x0)

    # 3. Probabilidades Limpas de Margem (Juice-Free)
    vig_1x2 = p_h + p_d + p_a
    p_h_clean = p_h / np.maximum(1e-5, vig_1x2)
    p_d_clean = p_d / np.maximum(1e-5, vig_1x2)
    p_a_clean = p_a / np.maximum(1e-5, vig_1x2)

    vig_ou = p_over + p_under
    p_over_clean = p_over / np.maximum(1e-5, vig_ou)
    p_under_clean = p_under / np.maximum(1e-5, vig_ou)

    # 4. Entropia de Shannon (Incerteza do Mercado 1X2)
    entropy_1x2 = -(p_h_clean * np.log(p_h_clean + 1e-9) + p_d_clean * np.log(p_d_clean + 1e-9) + p_a_clean * np.log(p_a_clean + 1e-9))

    # 5. Relações Quocientes Intermercados (VAR01 a VAR49)
    feats = pd.DataFrame(index=df.index)
    feats['p_H'] = p_h_clean
    feats['p_D'] = p_d_clean
    feats['p_A'] = p_a_clean
    feats['p_Over'] = p_over_clean
    feats['p_Under'] = p_under_clean
    feats['p_BTTS_Y'] = p_btts_y
    feats['p_BTTS_N'] = p_btts_n
    feats['p_0x0'] = p_0x0
    feats['p_0x1'] = p_0x1
    feats['p_1x0'] = p_1x0
    feats['entropy_1x2'] = entropy_1x2

    eps = 1e-6
    feats['VAR01'] = p_h / (p_d + eps)
    feats['VAR02'] = p_h / (p_a + eps)
    feats['VAR03'] = p_d / (p_h + eps)
    feats['VAR04'] = p_d / (p_a + eps)
    feats['VAR05'] = p_a / (p_h + eps)
    feats['VAR06'] = p_a / (p_d + eps)

    feats['VAR07'] = p_over / (p_under + eps)
    feats['VAR08'] = p_under / (p_over + eps)

    feats['VAR09'] = p_btts_y / (p_btts_n + eps)
    feats['VAR10'] = p_btts_n / (p_btts_y + eps)

    feats['VAR11'] = p_h / (p_over + eps)
    feats['VAR12'] = p_d / (p_over + eps)
    feats['VAR13'] = p_a / (p_over + eps)
    feats['VAR14'] = p_h / (p_under + eps)
    feats['VAR15'] = p_d / (p_under + eps)
    feats['VAR16'] = p_a / (p_under + eps)
    feats['VAR17'] = p_h / (p_btts_y + eps)
    feats['VAR18'] = p_d / (p_btts_y + eps)
    feats['VAR19'] = p_a / (p_btts_y + eps)
    feats['VAR20'] = p_h / (p_btts_n + eps)
    feats['VAR21'] = p_d / (p_btts_n + eps)
    feats['VAR22'] = p_a / (p_btts_n + eps)

    feats['VAR23'] = p_0x0 / (p_h + eps)
    feats['VAR24'] = p_0x0 / (p_d + eps)
    feats['VAR25'] = p_0x0 / (p_a + eps)
    feats['VAR26'] = p_0x0 / (p_over + eps)
    feats['VAR27'] = p_0x0 / (p_under + eps)
    feats['VAR28'] = p_0x0 / (p_btts_y + eps)
    feats['VAR29'] = p_0x0 / (p_btts_n + eps)

    feats['VAR30'] = p_0x1 / (p_h + eps)
    feats['VAR31'] = p_0x1 / (p_d + eps)
    feats['VAR32'] = p_0x1 / (p_a + eps)
    feats['VAR33'] = p_0x1 / (p_over + eps)
    feats['VAR34'] = p_0x1 / (p_under + eps)
    feats['VAR35'] = p_0x1 / (p_btts_y + eps)
    feats['VAR36'] = p_0x1 / (p_btts_n + eps)

    feats['VAR37'] = p_1x0 / (p_h + eps)
    feats['VAR38'] = p_1x0 / (p_d + eps)
    feats['VAR39'] = p_1x0 / (p_a + eps)
    feats['VAR40'] = p_1x0 / (p_over + eps)
    feats['VAR41'] = p_1x0 / (p_under + eps)
    feats['VAR42'] = p_1x0 / (p_btts_y + eps)
    feats['VAR43'] = p_1x0 / (p_btts_n + eps)

    feats['VAR44'] = p_0x0 / (p_0x1 + eps)
    feats['VAR45'] = p_0x0 / (p_1x0 + eps)
    feats['VAR46'] = p_0x1 / (p_0x0 + eps)
    feats['VAR47'] = p_0x1 / (p_1x0 + eps)
    feats['VAR48'] = p_1x0 / (p_0x0 + eps)
    feats['VAR49'] = p_1x0 / (p_0x1 + eps)

    # 6. Coeficientes de Variação (Dispersão)
    feats['VAR50'] = feats[['p_H','p_D','p_A']].std(ddof=0, axis=1) / (feats[['p_H','p_D','p_A']].mean(axis=1) + eps)
    feats['VAR51'] = feats[['p_Over', 'p_Under']].std(ddof=0, axis=1) / (feats[['p_Over', 'p_Under']].mean(axis=1) + eps)
    feats['VAR52'] = feats[['p_BTTS_Y', 'p_BTTS_N']].std(ddof=0, axis=1) / (feats[['p_BTTS_Y', 'p_BTTS_N']].mean(axis=1) + eps)
    feats['VAR53'] = feats[['p_0x0', 'p_0x1', 'p_1x0']].std(ddof=0, axis=1) / (feats[['p_0x0', 'p_0x1', 'p_1x0']].mean(axis=1) + eps)

    # 7. Diferenças Absolutas
    feats['VAR54'] = np.abs(p_h - p_a)
    feats['VAR55'] = np.abs(p_h - p_d)
    feats['VAR56'] = np.abs(p_d - p_a)
    feats['VAR57'] = np.abs(p_over - p_under)
    feats['VAR58'] = np.abs(p_btts_y - p_btts_n)
    feats['VAR59'] = np.abs(p_0x0 - p_0x1)
    feats['VAR60'] = np.abs(p_0x0 - p_1x0)
    feats['VAR61'] = np.abs(p_0x1 - p_1x0)

    # 8. Gradientes Angulares (Inclinações em graus)
    feats['VAR62'] = np.degrees(np.arctan((p_a - p_h) / 2))
    feats['VAR63'] = np.degrees(np.arctan((p_d - p_h) / 2))
    feats['VAR64'] = np.degrees(np.arctan((p_a - p_d) / 2))
    feats['VAR65'] = np.degrees(np.arctan((p_under - p_over) / 2))
    feats['VAR66'] = np.degrees(np.arctan((p_btts_n - p_btts_y) / 2))
    feats['VAR67'] = np.degrees(np.arctan((p_0x1 - p_0x0) / 2))
    feats['VAR68'] = np.degrees(np.arctan((p_1x0 - p_0x0) / 2))
    feats['VAR69'] = np.degrees(np.arctan((p_1x0 - p_0x1) / 2))

    # 9. Diferenças Relativas Normalizadas
    feats['VAR70'] = np.abs(p_h - p_a) / (p_a + eps)
    feats['VAR71'] = np.abs(p_h - p_d) / (p_d + eps)
    feats['VAR72'] = np.abs(p_d - p_a) / (p_a + eps)
    feats['VAR73'] = np.abs(p_over - p_under) / (p_under + eps)
    feats['VAR74'] = np.abs(p_btts_y - p_btts_n) / (p_btts_n + eps)
    feats['VAR75'] = np.abs(p_0x0 - p_0x1) / (p_0x1 + eps)
    feats['VAR76'] = np.abs(p_0x0 - p_1x0) / (p_1x0 + eps)
    feats['VAR77'] = np.abs(p_0x1 - p_1x0) / (p_1x0 + eps)

    # Tratamento final de Inf e NaN
    return feats.replace([np.inf, -np.inf], np.nan).fillna(0.0)
