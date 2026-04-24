from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constantes e configuração do ambiente
FUTPYTHON_TOKEN = os.environ.get("FUTPYTHON_TOKEN")
FUTPYTHON_API_URL_TEMPLATE = "https://api.futpythontrader.com/list_events?token={token}&date={date}"
REQUEST_TIMEOUT_SEC = 8.0

# Cache simples em memória (dicionário)
# A chave será a data (str), o valor será uma tupla (timestamp, data)
_cache: dict[str, tuple[float, pd.DataFrame]] = {}
CACHE_TTL_SECONDS = 60  # 1 minuto

ROOT_DIR = Path(__file__).resolve().parent
PROD_CFG_PATH = ROOT_DIR / "config_prod_v1.json"

app = FastAPI(
    title="Servidor Arkad Proxy",
    version="2.0.0",
    description="Proxy para buscar dados da FutPythonTrader API e disponibilizá-los para o frontend Streamlit.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite todas as origens
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_futpython_data_with_cache(date_iso: str) -> tuple[pd.DataFrame, str]:
    """
    Busca dados da API FutPython para uma data específica, utilizando um cache em memória
    para evitar requisições repetidas.
    """
    now = datetime.now().timestamp()
    cached_entry = _cache.get(date_iso)

    if cached_entry:
        timestamp, data = cached_entry
        if now - timestamp < CACHE_TTL_SECONDS:
            logging.info(f"Retornando dados do cache para a data: {date_iso}")
            return data, "Dados obtidos com sucesso (cache)"

    if not FUTPYTHON_TOKEN:
        msg = "FUTPYTHON_TOKEN não configurado no ambiente do servidor."
        logging.error(msg)
        return pd.DataFrame(), msg

    url = FUTPYTHON_API_URL_TEMPLATE.format(token=FUTPYTHON_TOKEN, date=date_iso)

    try:
        logging.info(f"Buscando dados da API FutPython para a data: {date_iso}")
        response = requests.get(url, timeout=REQUEST_TIMEOUT_SEC)
        response.raise_for_status()

        data_json = response.json()
        if not data_json or not isinstance(data_json, list):
            logging.warning(f"Resposta inesperada ou vazia da API: {data_json}")
            return pd.DataFrame(), "Resposta inesperada ou vazia da API FutPython."

        df = pd.DataFrame(data_json)
        logging.info(f"{len(df)} eventos recebidos da API FutPython.")

        # Atualiza o cache
        _cache[date_iso] = (now, df.copy())

        return df, "Dados obtidos com sucesso da API FutPython."

    except requests.exceptions.Timeout:
        msg = "Timeout ao conectar com a API FutPython."
        logging.error(msg)
        return pd.DataFrame(), msg
    except requests.exceptions.RequestException as e:
        msg = f"Erro na requisição à API FutPython: {e}"
        logging.error(msg)
        return pd.DataFrame(), msg
    except Exception as e:
        msg = f"Erro inesperado ao processar dados da API: {e}"
        logging.error(msg, exc_info=True)
        return pd.DataFrame(), msg


@app.get("/sinais", summary="Obtém os sinais de jogos para uma data específica")
def get_sinais(date: str = Query(..., description="Data no formato YYYY-MM-DD")):
    """
    Endpoint que retorna os sinais (jogos) para a data fornecida.
    Os dados são buscados da API FutPython e cacheados.
    """
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD.")

    df, status = _get_futpython_data_with_cache(date)

    if df.empty:
        # Retorna 503 se a API externa estiver indisponível ou retornar erro.
        raise HTTPException(status_code=503, detail=status)

    return df.to_dict(orient="records")


@app.get("/health", summary="Verifica a saúde do servidor")
def health_check():
    """Endpoint simples para verificar se o servidor está no ar."""
    return {"status": "ok"}


# O código abaixo é para rodar localmente com `python servidor_arkad.py`.
# Em produção (Render/Heroku), o Gunicorn será usado (ver Procfile).
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
