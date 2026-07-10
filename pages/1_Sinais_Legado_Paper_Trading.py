import os
import sys
from pathlib import Path
import json
import io
import time
from datetime import datetime, date

import pandas as pd
import numpy as np
import joblib
import requests
import streamlit as st

# Configura a página do Streamlit
st.set_page_config(
    page_title="Paper Trading - Legado",
    page_icon="📜",
    layout="wide",
)

# Caminhos absolutos para o diretório antigo
ROOT_DIR = Path(__file__).resolve().parent.parent
LEGADO_DIR = ROOT_DIR / "estrategias_legado"

# Funções de extração e API
def get_api_token():
    token = os.environ.get("FUTPYTHON_TOKEN", "").strip()
    if not token:
        try:
            token = st.secrets.get("FUTPYTHON_TOKEN", "").strip()
        except:
            pass
    if not token:
        # Fallback local temporario (tenta pegar do config)
        try:
            sys.path.append(str(LEGADO_DIR))
            from config import API_TOKEN
            token = API_TOKEN
        except:
            pass
    return token

def fetch_list_events(target_date: str, token: str):
    headers = {"Authorization": f"Token {token}", "User-Agent": "Mozilla/5.0"}
    
    url_b365 = f"https://apicomunidade.futpythontrader.com/api/dados/jogos-do-dia/bet365/{target_date}/"
    url_bf = f"https://apicomunidade.futpythontrader.com/api/dados/jogos-do-dia/betfair/{target_date}/"
    
    frames = []
    
    try:
        r_b365 = requests.get(url_b365, headers=headers, timeout=15)
        if r_b365.status_code == 200:
            data = r_b365.json()
            if isinstance(data, dict) and "dados" in data:
                frames.append(pd.DataFrame(data["dados"]))
            elif isinstance(data, list):
                frames.append(pd.DataFrame(data))
    except Exception as e:
        st.error(f"Erro B365: {e}")
        
    try:
        r_bf = requests.get(url_bf, headers=headers, timeout=15)
        if r_bf.status_code == 200:
            data = r_bf.json()
            if isinstance(data, dict) and "dados" in data:
                frames.append(pd.DataFrame(data["dados"]))
            elif isinstance(data, list):
                frames.append(pd.DataFrame(data))
    except Exception as e:
        st.error(f"Erro Betfair: {e}")
        
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()

# Construção de features adaptadas para o Vivo (Live)
def criar_features_b365_live(dataframe):
    df = dataframe.copy()
    odds_colunas = [col for col in df.columns if "Odd_" in col]
    for col in odds_colunas:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[f"Prob_{col}"] = 1 / (df[col] + 1e-10)
    
    if "Odd_H_FT" in df.columns and "Odd_A_FT" in df.columns:
        df["Ratio_HA"] = df["Odd_H_FT"] / (df["Odd_A_FT"] + 1e-10)
    if "Odd_Over25_FT" in df.columns and "Odd_Under25_FT" in df.columns:
        df["Ratio_OverUnder"] = df["Odd_Over25_FT"] / (df["Odd_Under25_FT"] + 1e-10)
    if "Odd_BTTS_Yes" in df.columns and "Odd_BTTS_No" in df.columns:
        df["Ratio_BTTS"] = df["Odd_BTTS_Yes"] / (df["Odd_BTTS_No"] + 1e-10)
    return df

def criar_features_betfair_live(dataframe):
    df = dataframe.copy()
    odds_colunas = [col for col in df.columns if "Odd_" in col]
    for col in odds_colunas:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[f"Prob_{col}"] = 1 / (df[col] + 1e-10)
    
    for mercado in ["H", "D", "A", "Over25_FT", "Under25_FT", "Over15_FT", "Under15_FT", "Over05_FT", "Under05_FT", "BTTS_Yes", "BTTS_No"]:
        odd_back = f"Odd_{mercado}_Back"
        odd_lay = f"Odd_{mercado}_Lay"
        if odd_back in df.columns and odd_lay in df.columns:
            df[f"Ratio_{mercado}"] = df[odd_back] / (df[odd_lay] + 1e-10)
            
    if "Odd_H_Back" in df.columns and "Odd_A_Back" in df.columns:
        df["Dif_HA"] = df["Odd_H_Back"] - df["Odd_A_Back"]
        favorito_home = df["Odd_H_Back"] <= df["Odd_A_Back"]
        df["Favorito_Home"] = favorito_home.astype(int)
        
        if "Odd_CS_Goleada_H_Lay" in df.columns and "Odd_CS_Goleada_A_Lay" in df.columns:
            df["Odd_CS_Goleada_Favorito_Lay"] = np.where(
                favorito_home,
                pd.to_numeric(df["Odd_CS_Goleada_H_Lay"], errors="coerce"),
                pd.to_numeric(df["Odd_CS_Goleada_A_Lay"], errors="coerce"),
            )
            
    if "League" in df.columns:
        df = pd.concat([df, pd.get_dummies(df["League"], prefix="League")], axis=1)
        
    return df

def preparar_features_live(df_modelo, features, scaler):
    matriz = pd.DataFrame(
        {coluna: df_modelo[coluna] if coluna in df_modelo.columns else 0 for coluna in features},
        index=df_modelo.index,
    )
    matriz = matriz.apply(pd.to_numeric, errors="coerce").fillna(0).replace([np.inf, -np.inf], 0)
    if hasattr(scaler, "n_features_in_"):
        required = scaler.n_features_in_
        if matriz.shape[1] > required:
            matriz = matriz.iloc[:, :required]
        elif matriz.shape[1] < required:
            for index in range(required - matriz.shape[1]):
                matriz[f"pad_{index}"] = 0
    return matriz

def alinhar_saida_modelo_live(x_scaled, modelo):
    if hasattr(modelo, "n_features_in_"):
        required = modelo.n_features_in_
        if x_scaled.shape[1] > required:
            x_scaled = x_scaled[:, :required]
        elif x_scaled.shape[1] < required:
            padding = np.zeros((x_scaled.shape[0], required - x_scaled.shape[1]), dtype=np.float32)
            x_scaled = np.hstack([x_scaled, padding])
    return x_scaled

def resolver_arquivos_metodo_live(scope, prefix):
    if scope == "B365":
        file_prefix = f"{prefix}_b365"
    else:
        file_prefix = prefix
    return {
        "model": LEGADO_DIR / f"modelo_{file_prefix}.pkl",
        "scaler": LEGADO_DIR / f"scaler_{file_prefix}.pkl",
        "features": LEGADO_DIR / f"features_{file_prefix}.pkl"
    }

METHODS = [
    # B365
    {"scope": "B365", "prefix": "Lay_0x1", "label": "Lay 0x1 B365", "odd_col": "Odd_CS_0x1", "min_prob": 0.80},
    {"scope": "B365", "prefix": "Lay_1x0", "label": "Lay 1x0 B365", "odd_col": "Odd_CS_1x0", "min_prob": 0.80},
    {"scope": "B365", "prefix": "Back_Home", "label": "Back Home B365", "odd_col": "Odd_H_FT", "min_prob": 0.40},
    
    # Betfair
    {"scope": "Betfair", "prefix": "Lay_Goleada_H", "label": "Lay Goleada Home Betfair", "odd_col": "Odd_CS_Goleada_H_Lay", "min_prob": 0.90},
    {"scope": "Betfair", "prefix": "Lay_Goleada_A", "label": "Lay Goleada Away Betfair", "odd_col": "Odd_CS_Goleada_A_Lay", "min_prob": 0.95},
    {"scope": "Betfair", "prefix": "Lay_Away", "label": "Lay Away Betfair", "odd_col": "Odd_A_Lay", "min_prob": 0.65},
    {"scope": "Betfair", "prefix": "Lay_Goleada_Favorito", "label": "Lay Goleada Favorito Betfair", "odd_col": "Odd_CS_Goleada_Favorito_Lay", "min_prob": 0.90},
    {"scope": "Betfair", "prefix": "Over15_FT", "label": "Over 1.5 FT Betfair", "odd_col": "Odd_Over15_FT_Back", "min_prob": 0.58},
    {"scope": "Betfair", "prefix": "Over05_FT_v2", "label": "Over 0.5 FT Betfair", "odd_col": "Odd_Over05_FT_Back", "min_prob": 0.75},
    {"scope": "Betfair", "prefix": "Lay_Home", "label": "Lay Home Betfair", "odd_col": "Odd_H_Lay", "min_prob": 0.55},
    {"scope": "Betfair", "prefix": "Back_Away", "label": "Back Away Betfair", "odd_col": "Odd_A_Back", "min_prob": 0.30},
    {"scope": "Betfair", "prefix": "Under15_FT", "label": "Under 1.5 FT Betfair", "odd_col": "Odd_Under15_FT_Back", "min_prob": 0.55},
    {"scope": "Betfair", "prefix": "Back_Home", "label": "Back Home Betfair", "odd_col": "Odd_H_Back", "min_prob": 0.40},
]

def gerar_sinais(df_raw, target_date):
    df_feat_b365 = criar_features_b365_live(df_raw)
    df_feat_bf = criar_features_betfair_live(df_raw)
    
    sinais_encontrados = []
    
    for method in METHODS:
        odd_col = method["odd_col"]
        scope = method["scope"]
        
        df_base = df_feat_b365 if scope == "B365" else df_feat_bf
        if odd_col not in df_base.columns:
            continue
            
        arquivos = resolver_arquivos_metodo_live(scope, method["prefix"])
        if not (arquivos["model"].exists() and arquivos["scaler"].exists() and arquivos["features"].exists()):
            continue
            
        try:
            modelo = joblib.load(arquivos["model"])
            scaler = joblib.load(arquivos["scaler"])
            if hasattr(scaler, "feature_names_in_"):
                features = list(scaler.feature_names_in_)
            else:
                features = joblib.load(arquivos["features"])
        except Exception as e:
            continue
            
        df_modelo = df_base.dropna(subset=[odd_col]).copy()
        if df_modelo.empty:
            continue
            
        df_modelo[odd_col] = pd.to_numeric(df_modelo[odd_col], errors="coerce")
        df_modelo = df_modelo[df_modelo[odd_col].notna()].copy()
        
        if df_modelo.empty:
            continue
            
        x = preparar_features_live(df_modelo, features, scaler)
        x_scaled = scaler.transform(x.values.astype(np.float32))
        x_scaled = alinhar_saida_modelo_live(x_scaled, modelo)
        df_modelo["Prob_ML"] = modelo.predict_proba(x_scaled)[:, 1]
        
        mask = df_modelo["Prob_ML"] >= method["min_prob"]
        sinais = df_modelo[mask].copy()
        
        if not sinais.empty:
            for _, row in sinais.iterrows():
                # Formata hora e odd do sinal
                hora_str = row.get("Time", "")
                if pd.isna(hora_str): hora_str = ""
                else: hora_str = str(hora_str)[:5]
                
                odd_sinal = row.get(odd_col)
                if pd.isna(odd_sinal): odd_sinal = 0.0
                
                # Odd BF Real para fins de simulação se for B365
                odd_bf_real = None
                if scope == "B365":
                    if "0x1" in method["label"]: odd_bf_real = row.get("Odd_CS_0x1_Lay")
                    elif "1x0" in method["label"]: odd_bf_real = row.get("Odd_CS_1x0_Lay")
                else:
                    odd_bf_real = odd_sinal
                    
                odd_final = round(float(odd_bf_real), 2) if pd.notna(odd_bf_real) else round(float(odd_sinal), 2)
                tipo_operacao = "Lay" if "Lay" in method["label"] else "Back"

                sinais_encontrados.append({
                    "Data": str(target_date),
                    "Hora": hora_str,
                    "Liga": str(row.get("League", "")),
                    "Home": str(row.get("Home", "")),
                    "Away": str(row.get("Away", "")),
                    "Metodo": method["label"],
                    "Tipo": tipo_operacao,
                    "Odd": odd_final,
                    "Prob": f"{row['Prob_ML']*100:.1f}%",
                    "Resultado": "",
                    "Lucro": ""
                })
                
    if sinais_encontrados:
        df_out = pd.DataFrame(sinais_encontrados)
        return df_out.sort_values(by=["Hora", "Liga"]).reset_index(drop=True)
    return pd.DataFrame()

st.title("📜 Sinais Legado - Paper Trading")
st.markdown("""
Esta página gera os **sinais dos métodos antigos** (Lay 0x1, Goleada, etc.) que migramos da versão 1 do robô.
Esses sinais **não afetam** a sua banca no ARKAD_PROD (Lay 0x0). Servem apenas para você realizar testes no papel e anotar numa planilha para tirar a prova real do backtest!
""")

col1, col2 = st.columns([1, 3])
with col1:
    target_date = st.date_input("Data dos Jogos", value=date.today())
    gerar_btn = st.button("Gerar Sinais de Hoje", type="primary")

if gerar_btn:
    token = get_api_token()
    if not token:
        st.error("Token da FutPythonTrader não encontrado. Configure nos Secrets ou na variável de ambiente FUTPYTHON_TOKEN.")
    else:
        with st.spinner("Buscando jogos e rodando os modelos antigos..."):
            df_jogos = fetch_list_events(str(target_date), token)
            
            if df_jogos.empty:
                st.warning("Nenhum jogo encontrado para esta data ou erro na API.")
            else:
                df_sinais = gerar_sinais(df_jogos, target_date)
                
                if df_sinais.empty:
                    st.info("Os modelos rodaram, mas não encontraram nenhum jogo com padrão forte o suficiente para hoje.")
                else:
                    st.success(f"{len(df_sinais)} sinais encontrados!")
                    st.dataframe(df_sinais, use_container_width=True)
                    
                    # Gerar arquivo Excel em memória
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                        df_sinais.to_excel(writer, index=False, sheet_name='Sinais')
                    excel_data = buffer.getvalue()
                    
                    st.download_button(
                        label="📥 Baixar Sinais (Excel)",
                        data=excel_data,
                        file_name=f"sinais_legado_{target_date}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
