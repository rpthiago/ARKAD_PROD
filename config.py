# config.py
# =============================================================================
# Configurações centrais — lidas de variáveis de ambiente (Railway/local)
# =============================================================================
import os


def _read_secret(name: str, default: str = "") -> str:
	value = os.getenv(name, "").strip()
	if value:
		return value

	# Streamlit Cloud usually stores secrets in st.secrets, not process env vars.
	try:
		import streamlit as st  # Local import to avoid hard dependency outside Streamlit runtime.

		secret_val = st.secrets.get(name, "")
		if secret_val is None:
			return default
		return str(secret_val).strip() or default
	except Exception:
		return default


def _read_first_available_secret(names: list[str], default: str = "") -> str:
	for name in names:
		value = _read_secret(name, "")
		if value:
			return value
	return default

# API FutPythonTrader
API_TOKEN    = _read_first_available_secret(["FUTPYTHON_TOKEN", "FUTPYTHON_API_TOKEN", "API_TOKEN"], "")
API_HEADERS  = {"User-Agent": "Mozilla/5.0"}
if API_TOKEN:
	API_HEADERS["Authorization"] = f"Token {API_TOKEN}"

# Betfair
API_BETFAIR_DIA_URL  = "https://apicomunidade.futpythontrader.com/api/dados/jogos-do-dia/betfair/{data}/"
API_BETFAIR_BASE_URL = "https://apicomunidade.futpythontrader.com/api/dados/betfair/download/"

# Bet365
API_B365_DIA_URL  = "https://apicomunidade.futpythontrader.com/api/dados/jogos-do-dia/bet365/{data}/"
API_B365_BASE_URL = "https://apicomunidade.futpythontrader.com/api/dados/bet365/download/"

# Footystats
API_FOOTSTATS_DIA_URL  = "https://apicomunidade.futpythontrader.com/api/dados/jogos-do-dia/footystats/{data}/"
API_FOOTSTATS_BASE_URL = "https://apicomunidade.futpythontrader.com/api/dados/footystats/download/"

# Compatibilidade retroativa
API_DIA_URL  = API_BETFAIR_DIA_URL
API_BASE_URL = API_BETFAIR_BASE_URL

# Telegram
TELEGRAM_TOKEN   = _read_secret("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = _read_secret("TELEGRAM_CHAT_ID", "")
