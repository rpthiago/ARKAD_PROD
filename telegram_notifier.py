import os, json, requests
from pathlib import Path

ROOT = Path("c:/Users/thiag/OneDrive/Documentos/GitHub/ARKAD_PROD")
CONFIG_FILE = ROOT / "telegram_config.json"

def carregar_config_telegram():
    """Carrega o token e chat_id do arquivo de configuração ou variáveis de ambiente."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if (not token or not chat_id) and CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                token = token or data.get("bot_token")
                chat_id = chat_id or data.get("chat_id")
        except Exception:
            pass
            
    return token, chat_id

def salvar_config_telegram(bot_token, chat_id):
    """Salva a configuração do Telegram no arquivo local."""
    data = {"bot_token": bot_token.strip(), "chat_id": str(chat_id).strip()}
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    return True

def enviar_mensagem_telegram(texto, parse_mode="Markdown"):
    """Envia uma mensagem de texto formatada para o Telegram."""
    token, chat_id = carregar_config_telegram()
    if not token or not chat_id:
        print("[!] Telegram não configurado (adicione token e chat_id).")
        return False, "Token ou Chat ID ausente."
        
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": texto,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }
    
    try:
        r = requests.post(url, json=payload, timeout=10)
        res = r.json()
        if res.get("ok"):
            return True, "Mensagem enviada com sucesso!"
        else:
            return False, f"Erro Telegram: {res.get('description')}"
    except Exception as e:
        return False, f"Falha na requisição: {str(e)}"

def enviar_documento_telegram(caminho_arquivo, legenda=""):
    """Envia um arquivo (planilha Excel, PDF) como documento para o Telegram."""
    token, chat_id = carregar_config_telegram()
    if not token or not chat_id:
        return False, "Token ou Chat ID ausente."
        
    p = Path(caminho_arquivo)
    if not p.exists():
        return False, f"Arquivo não encontrado: {caminho_arquivo}"
        
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    try:
        with open(p, "rb") as f:
            files = {"document": (p.name, f)}
            data = {"chat_id": chat_id, "caption": legenda, "parse_mode": "Markdown"}
            r = requests.post(url, data=data, files=files, timeout=20)
            res = r.json()
            if res.get("ok"):
                return True, "Arquivo enviado com sucesso!"
            else:
                return False, f"Erro Telegram: {res.get('description')}"
    except Exception as e:
        return False, f"Falha ao enviar arquivo: {str(e)}"

def testar_conexao_telegram():
    """Testa se o token do bot é válido."""
    token, _ = carregar_config_telegram()
    if not token:
        return False, "Token não configurado."
    url = f"https://api.telegram.org/bot{token}/getMe"
    try:
        r = requests.get(url, timeout=10)
        res = r.json()
        if res.get("ok"):
            bot_info = res.get("result", {})
            return True, f"Conectado ao bot: @{bot_info.get('username')} ({bot_info.get('first_name')})"
        else:
            return False, f"Token inválido: {res.get('description')}"
    except Exception as e:
        return False, f"Falha na conexão: {str(e)}"
