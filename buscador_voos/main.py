import os
import sys
import time
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

from buscador_voos.flight_searcher import buscar_voo_rota, ResultadoBusca
from buscador_voos.price_tracker import carregar_historico, avaliar_alerta, registrar_cotacao
from buscador_voos.telegram_bot import enviar_mensagem

# Configura stdout/stderr com segurança para pythonw.exe (onde são None)
if sys.stdout is None:
    sys.stdout = open(os.devnull, "w", encoding="utf-8")
elif getattr(sys.stdout, "encoding", None) and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

if sys.stderr is None:
    sys.stderr = open(os.devnull, "w", encoding="utf-8")
elif getattr(sys.stderr, "encoding", None) and sys.stderr.encoding.lower() != "utf-8":
    try:
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("buscador_voos")

CONFIG_PATH = Path(__file__).resolve().parent / "config.json"


def carregar_config() -> Dict[str, Any]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def formatar_alerta_telegram(res: ResultadoBusca, cfg_dest: Dict[str, Any], motivo: str, detalhe: str) -> str:
    voo_ref = res.menor_ate_1_parada or res.menor_geral
    cidade = cfg_dest.get("cidade", res.destino)
    uf = cfg_dest.get("uf", "")
    emoji = cfg_dest.get("emoji", "🏖️")
    preco_alvo = cfg_dest.get("preco_alvo", 0)

    paradas_str = "Voo Direto" if voo_ref.stops == 0 else f"{voo_ref.stops} escala(s)"
    
    nivel_map = {
        "baixo": "🟢 BAIXO (abaixo do padrão)",
        "normal": "🟡 NORMAL",
        "alto": "🔴 ALTO"
    }
    nivel_str = nivel_map.get(res.price_level, res.price_level.upper())

    info_direto = ""
    if res.menor_direto and res.menor_direto.price != voo_ref.price:
        info_direto = f"\n🛫 <b>Opção Direta:</b> R$ {res.menor_direto.price:,} ({res.menor_direto.airline})"

    msg = (
        f"🚨 <b>ALERTA DE PREÇO BOM! {emoji}</b>\n\n"
        f"✈️ <b>BH (CNF) ➔ {cidade}/{uf} ({res.destino})</b>\n"
        f"🗓️ <b>{res.data_ida} a {res.data_volta}</b> (Ida e Volta)\n\n"
        f"💰 <b>Preço Encontrado: R$ {voo_ref.price:,}</b>\n"
        f"🎯 <b>Preço Alvo:</b> R$ {preco_alvo:,}\n"
        f"🏷️ <b>Cia:</b> {voo_ref.airline}\n"
        f"🛑 <b>Conexão:</b> {paradas_str}\n"
        f"📊 <b>Nível de Preço:</b> {nivel_str}\n"
        f"{info_direto}\n"
        f"💡 <b>Gatilho:</b> {detalhe}\n\n"
        f"🔗 <a href=\"{res.url_google_flights}\"><b>👉 Clique aqui para Ver no Google Flights</b></a>"
    )
    return msg


def formatar_resumo_telegram(resultados: List[Dict[str, Any]], data_ida: str, data_volta: str) -> str:
    linhas = []
    # Ordena pelo menor preço
    ordenados = sorted(resultados, key=lambda x: x.get("preco", 999999))

    for idx, item in enumerate(ordenados, 1):
        if not item.get("success"):
            linhas.append(f"{idx}. ❌ <b>{item['cidade']} ({item['codigo']}):</b> Não disponível")
            continue

        cidade = item["cidade"]
        uf = item.get("uf", "")
        code = item["codigo"]
        emoji = item.get("emoji", "✈️")
        preco = item["preco"]
        cia = item["cia"]
        stops = item["stops"]
        stops_txt = "Direto" if stops == 0 else f"{stops}p"
        
        direto_txt = ""
        if item.get("preco_direto") and item.get("preco_direto") != preco:
            direto_txt = f" | Direto: R$ {item['preco_direto']:,}"

        status_alvo = "🎯" if preco <= item.get("preco_alvo", 0) else ""

        linhas.append(
            f"{idx}. {emoji} <b>{cidade}/{uf} ({code}):</b> R$ {preco:,} ({cia}, {stops_txt}{direto_txt}) {status_alvo}"
        )

    tabela = "\n".join(linhas)

    msg = (
        f"📊 <b>RANKING DE VOOS: BH (CNF) ➔ NORDESTE</b>\n"
        f"🗓️ <b>Período: {data_ida} a {data_volta}</b> (Ida e Volta)\n\n"
        f"{tabela}\n\n"
        f"💡 <i>🎯 = Atingiu preço alvo. Monitoramento automático ativo no ARKAD!</i>"
    )
    return msg


def executar_varredura(config: Dict[str, Any], enviar_resumo: bool = False, apenas_destino: str = None) -> List[Dict[str, Any]]:
    origem = config.get("origem", "CNF")
    data_ida = config.get("data_ida", "2026-12-21")
    data_volta = config.get("data_volta", "2026-12-28")
    adultos = config.get("adultos", 1)
    classe = config.get("classe", "economy")
    destinos = config.get("destinos", [])
    regras = config.get("regras_alerta", {})

    historico = carregar_historico()
    resultados_resumo = []

    print("\n" + "=" * 70)
    print(f"🔍 INICIANDO BUSCA DE VOOS: {origem} ➔ NORDESTE ({data_ida} a {data_volta})")
    print("=" * 70)

    for cfg_d in destinos:
        dest_code = cfg_d["codigo"]
        cidade = cfg_d["cidade"]
        uf = cfg_d.get("uf", "")
        preco_alvo = cfg_d.get("preco_alvo", 0)

        if apenas_destino and dest_code.upper() != apenas_destino.upper():
            continue

        print(f"\n🔎 Consultando {cidade}/{uf} ({dest_code})...", end=" ", flush=True)

        res = buscar_voo_rota(
            origem=origem,
            destino=dest_code,
            data_ida=data_ida,
            data_volta=data_volta,
            adultos=adultos,
            classe=classe,
            max_tentativas=3,
        )

        if not res.success or not res.menor_geral:
            print(f"❌ Falha: {res.erro}")
            resultados_resumo.append({
                "codigo": dest_code,
                "cidade": cidade,
                "uf": uf,
                "emoji": cfg_d.get("emoji", "✈️"),
                "success": False,
                "preco_alvo": preco_alvo
            })
            time.sleep(1.5)
            continue

        voo_ref = res.menor_ate_1_parada or res.menor_geral
        preco = voo_ref.price
        paradas_txt = "Direto" if voo_ref.stops == 0 else f"{voo_ref.stops} parada(s)"
        print(f"✅ R$ {preco:,} ({voo_ref.airline}, {paradas_txt}) | Nível: {res.price_level}")

        # Avalia se deve alertar
        deve_alertar, motivo, detalhe = avaliar_alerta(res, cfg_d, regras, historico)

        if deve_alertar:
            print(f"   🚨 DISPARANDO ALERTA TELEGRAM! Motivo: {motivo} ({detalhe})")
            texto_alerta = formatar_alerta_telegram(res, cfg_d, motivo, detalhe)
            sucesso_envio = enviar_mensagem(texto_alerta)
            registrar_cotacao(res, historico, foi_alertado=sucesso_envio)
        else:
            registrar_cotacao(res, historico, foi_alertado=False)

        res_dict = {
            "codigo": dest_code,
            "cidade": cidade,
            "uf": uf,
            "emoji": cfg_d.get("emoji", "✈️"),
            "success": True,
            "preco": preco,
            "cia": voo_ref.airline,
            "stops": voo_ref.stops,
            "preco_alvo": preco_alvo,
            "preco_direto": res.menor_direto.price if res.menor_direto else None,
            "url": res.url_google_flights
        }
        resultados_resumo.append(res_dict)

        # Pausa amigável entre requisições para evitar rate-limit
        time.sleep(1.8)

    print("\n" + "=" * 70)
    print("🏁 Varredura concluída!")
    print("=" * 70)

    if enviar_resumo and resultados_resumo:
        print("📨 Enviando Ranking Resumo para o Telegram...")
        texto_resumo = formatar_resumo_telegram(resultados_resumo, data_ida, data_volta)
        enviar_mensagem(texto_resumo)
        print("✅ Resumo enviado!")

    return resultados_resumo


def exibir_status_historico():
    historico = carregar_historico()
    if not historico:
        print("Nenhum histórico registrado ainda. Execute uma busca com --check ou --resumo.")
        return

    print("\n📊 HISTÓRICO DE PREÇOS REGISTRADOS:")
    print("-" * 75)
    print(f"{'DEST':<6} {'ÚLTIMO':<10} {'MENOR HIST':<12} {'MAIOR HIST':<12} {'DIRETO':<10} {'CIA':<12} {'ATUALIZADO'}")
    print("-" * 75)

    for code, dados in sorted(historico.items()):
        ult = f"R$ {dados.get('ultimo_preco', 0):,}"
        menor = f"R$ {dados.get('menor_preco_historico', 0):,}"
        maior = f"R$ {dados.get('maior_preco_historico', 0):,}"
        direto = f"R$ {dados.get('ultimo_preco_direto', 0):,}" if dados.get("ultimo_preco_direto") else "-"
        cia = dados.get("ultima_cia", "-")[:10]
        data = dados.get("ultima_atualizacao", "-")
        print(f"{code:<6} {ult:<10} {menor:<12} {maior:<12} {direto:<10} {cia:<12} {data}")
    print("-" * 75)


def modo_monitoramento(config: Dict[str, Any], intervalo_minutos: int = None):
    minutos = intervalo_minutos or config.get("intervalo_monitoramento_minutos", 180)
    print(f"\n🔄 MODO MONITORAMENTO ATIVADO (Checando a cada {minutos} minutos / {minutos/60:.1f}h)")
    print("Pressione Ctrl+C para encerrar.\n")

    while True:
        try:
            executar_varredura(config, enviar_resumo=False)
            print(f"\n⏳ Dormindo por {minutos} minutos até a próxima verificação...")
            time.sleep(minutos * 60)
        except KeyboardInterrupt:
            print("\n🛑 Monitoramento encerrado pelo usuário.")
            break
        except Exception as e:
            logger.error("Erro inesperado durante ciclo de monitoramento: %s", e)
            print(f"Erro: {e}. Aguardando 5 minutos para retomar...")
            time.sleep(300)


def main():
    parser = argparse.ArgumentParser(description="Buscador de Voos BH -> Nordeste com Alertas Telegram")
    parser.add_argument("--check", action="store_true", help="Executa uma varredura agora e alerta somente se houver preço bom")
    parser.add_argument("--resumo", action="store_true", help="Executa uma varredura e envia o resumo completo com ranking no Telegram")
    parser.add_argument("--monitor", action="store_true", help="Executa em modo contínuo (loop de monitoramento)")
    parser.add_argument("--intervalo", type=int, default=None, help="Intervalo em minutos para o modo monitor (padrão: 180 min)")
    parser.add_argument("--status", action="store_true", help="Exibe o histórico de preços salvo localmente")
    parser.add_argument("--destino", type=str, default=None, help="Consulta apenas um aeroporto específico (ex: SSA, REC, MCZ)")

    args = parser.parse_args()
    config = carregar_config()

    if args.status:
        exibir_status_historico()
    elif args.monitor:
        modo_monitoramento(config, args.intervalo)
    elif args.resumo:
        executar_varredura(config, enviar_resumo=True, apenas_destino=args.destino)
    else:
        # Padrão: check
        executar_varredura(config, enviar_resumo=False, apenas_destino=args.destino)


if __name__ == "__main__":
    main()
