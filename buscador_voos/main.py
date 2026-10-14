import os
import sys

# Garante carregamento das DLLs OpenSSL do Anaconda em segundo plano (Task Scheduler sem conda activate)
def _configurar_dlls_anaconda():
    prefixos = [getattr(sys, "prefix", None), getattr(sys, "base_prefix", None)]
    for p in prefixos:
        if not p:
            continue
        lib_bin = os.path.join(p, "Library", "bin")
        if os.path.isdir(lib_bin):
            if hasattr(os, "add_dll_directory"):
                try:
                    os.add_dll_directory(lib_bin)
                except Exception:
                    pass
            if lib_bin not in os.environ.get("PATH", ""):
                os.environ["PATH"] = lib_bin + os.pathsep + os.environ.get("PATH", "")

_configurar_dlls_anaconda()

import time
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

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


def formatar_alerta_telegram(res: ResultadoBusca, cfg_item: Dict[str, Any], motivo: str, detalhe: str) -> str:
    voo_ref = res.menor_ate_1_parada or res.menor_geral
    rotulo = cfg_item.get("rotulo", res.data_ida)
    emoji = cfg_item.get("emoji", "🏖️")
    preco_alvo = cfg_item.get("preco_alvo", 0)
    preco_alvo_direto = cfg_item.get("preco_alvo_direto")

    paradas_str = "Voo Direto" if voo_ref.stops == 0 else f"{voo_ref.stops} escala(s)"
    
    nivel_map = {
        "baixo": "🟢 BAIXO (abaixo do padrão)",
        "normal": "🟡 NORMAL",
        "alto": "🔴 ALTO"
    }
    nivel_str = nivel_map.get(res.price_level, res.price_level.upper())

    info_direto = ""
    if res.menor_direto and res.menor_direto.price != voo_ref.price:
        dep_arr = f" ({res.menor_direto.dep_time} ➔ {res.menor_direto.arr_time})" if res.menor_direto.dep_time else ""
        info_direto = f"\n🛫 <b>Opção Voo Direto:</b> R$ {res.menor_direto.price:,} ({res.menor_direto.airline}{dep_arr})"
    elif voo_ref.stops == 0:
        info_direto = "\n✨ <b>Este é um VOO DIRETO!</b>"

    horarios_str = ""
    if voo_ref.dep_time and voo_ref.arr_time:
        horarios_str = f"⏰ <b>Horário:</b> {voo_ref.dep_time} ➔ {voo_ref.arr_time}\n"

    alvo_txt = f"R$ {preco_alvo:,}"
    if preco_alvo_direto:
        alvo_txt += f" (Direto: R$ {preco_alvo_direto:,})"

    periodo_str = f"{rotulo} (Somente Volta)" if not res.data_volta else f"{res.data_ida} a {res.data_volta} (Ida e Volta)"

    origem_cidade = cfg_item.get("origem_cidade", "Salvador/BA") if res.origem == "SSA" else res.origem
    destino_cidade = cfg_item.get("destino_cidade", "BH/Confins") if res.destino == "CNF" else res.destino

    msg = (
        f"🚨 <b>ALERTA DE PREÇO BOM — VOLTA! {emoji}</b>\n\n"
        f"✈️ <b>{origem_cidade} ({res.origem}) ➔ {destino_cidade} ({res.destino})</b>\n"
        f"🗓️ <b>{periodo_str}</b>\n\n"
        f"💰 <b>Menor Preço: R$ {voo_ref.price:,}</b>\n"
        f"🎯 <b>Preço Alvo:</b> {alvo_txt}\n"
        f"🏷️ <b>Cia:</b> {voo_ref.airline}\n"
        f"🛑 <b>Conexão:</b> {paradas_str}\n"
        f"{horarios_str}"
        f"📊 <b>Nível de Preço:</b> {nivel_str}\n"
        f"{info_direto}\n"
        f"💡 <b>Gatilho:</b> {detalhe}\n\n"
        f"🔗 <a href=\"{res.url_google_flights}\"><b>👉 Clique aqui para Ver no Google Flights</b></a>"
    )
    return msg


def formatar_resumo_telegram(resultados: List[Dict[str, Any]], config: Dict[str, Any]) -> str:
    linhas = []
    for item in resultados:
        if not item.get("success"):
            linhas.append(f"❌ <b>{item.get('rotulo', 'Trecho')}:</b> Não disponível ({item.get('erro', '')})")
            continue

        rotulo = item.get("rotulo", item.get("data"))
        preco = item["preco"]
        cia = item["cia"]
        stops = item["stops"]
        stops_txt = "Direto" if stops == 0 else f"{stops} escala(s)"
        
        direto_txt = ""
        if item.get("preco_direto") and item.get("preco_direto") != preco:
            cia_dir = item.get("cia_direto", "Gol")
            direto_txt = f"\n   ↳ 🛫 Voo Direto: <b>R$ {item['preco_direto']:,}</b> ({cia_dir})"
        elif stops == 0:
            direto_txt = " (Voo Direto ✨)"

        status_alvo = " 🎯" if preco <= item.get("preco_alvo", 0) or (item.get("preco_direto") and item.get("preco_direto") <= item.get("preco_alvo_direto", 0)) else ""

        linhas.append(
            f"🗓️ <b>{rotulo}:</b>\n"
            f"   💰 Menor: <b>R$ {preco:,}</b> ({cia}, {stops_txt}){status_alvo}{direto_txt}\n"
            f"   🔗 <a href=\"{item['url']}\">Ver no Google Flights</a>"
        )

    tabela = "\n\n".join(linhas)

    origem_txt = config.get("origem_nome", "Salvador (SSA)")
    destino_txt = config.get("destino_nome", "BH/Confins (CNF)")

    msg = (
        f"📊 <b>MONITOR DE VOLTA: {origem_txt} ➔ {destino_txt}</b>\n"
        f"🏖️ <i>Trechos de Volta pós-Réveillon (28/12 e 29/12)</i>\n\n"
        f"{tabela}\n\n"
        f"💡 <i>🎯 = Atingiu preço alvo. Monitoramento ativo 4x ao dia no ARKAD!</i>"
    )
    return msg


def executar_varredura(config: Dict[str, Any], enviar_resumo: bool = False, apenas_destino: str = None) -> List[Dict[str, Any]]:
    if not config.get("ativo", True):
        logger.info("Monitor de voos SSA->CNF desativado por configuração (ativo=false).")
        print("\n⏸️ Monitor SSA ➔ CNF desativado por configuração (ativo: false). Nenhuma busca realizada.")
        return []
    origem = config.get("origem", "SSA")
    destino = config.get("destino", "CNF")
    adultos = config.get("adultos", 1)
    classe = config.get("classe", "economy")
    regras = config.get("regras_alerta", {})
    trechos = config.get("trechos", [])

    historico = carregar_historico()
    resultados_resumo = []

    print("\n" + "=" * 70)
    print(f"🔍 INICIANDO BUSCA DE VOOS DE VOLTA: {origem} ➔ {destino}")
    print("=" * 70)

    # Se a configuração usar o modelo novo com múltiplos trechos/datas de volta
    if trechos:
        for t in trechos:
            t_data = t["data"]
            rotulo = t.get("rotulo", t_data)
            t_id = t.get("id", f"{origem}_{destino}_{t_data}")
            preco_alvo = t.get("preco_alvo", 500)
            preco_alvo_direto = t.get("preco_alvo_direto", 600)

            print(f"\n🔎 Consultando {rotulo} ({origem} ➔ {destino})...", end=" ", flush=True)

            res = buscar_voo_rota(
                origem=origem,
                destino=destino,
                data_ida=t_data,
                data_volta=None,
                adultos=adultos,
                classe=classe,
                max_tentativas=3,
            )

            if not res.success or not res.menor_geral:
                print(f"❌ Falha: {res.erro}")
                resultados_resumo.append({
                    "id": t_id,
                    "rotulo": rotulo,
                    "data": t_data,
                    "success": False,
                    "erro": res.erro,
                    "preco_alvo": preco_alvo
                })
                time.sleep(1.5)
                continue

            voo_ref = res.menor_ate_1_parada or res.menor_geral
            preco = voo_ref.price
            paradas_txt = "Direto" if voo_ref.stops == 0 else f"{voo_ref.stops} parada(s)"
            direto_txt = f" | Direto: R$ {res.menor_direto.price} ({res.menor_direto.airline})" if res.menor_direto else ""
            print(f"✅ R$ {preco:,} ({voo_ref.airline}, {paradas_txt}){direto_txt} | Nível: {res.price_level}")

            deve_alertar, motivo, detalhe = avaliar_alerta(res, t, regras, historico, chave_rota=t_id)

            if deve_alertar:
                print(f"   🚨 DISPARANDO ALERTA TELEGRAM! Motivo: {motivo} ({detalhe})")
                texto_alerta = formatar_alerta_telegram(res, t, motivo, detalhe)
                sucesso_envio = enviar_mensagem(texto_alerta)
                registrar_cotacao(res, historico, chave_rota=t_id, foi_alertado=sucesso_envio)
            else:
                registrar_cotacao(res, historico, chave_rota=t_id, foi_alertado=False)

            res_dict = {
                "id": t_id,
                "rotulo": rotulo,
                "data": t_data,
                "success": True,
                "preco": preco,
                "cia": voo_ref.airline,
                "stops": voo_ref.stops,
                "preco_alvo": preco_alvo,
                "preco_alvo_direto": preco_alvo_direto,
                "preco_direto": res.menor_direto.price if res.menor_direto else None,
                "cia_direto": res.menor_direto.airline if res.menor_direto else None,
                "url": res.url_google_flights
            }
            resultados_resumo.append(res_dict)
            time.sleep(1.8)

    else:
        # Fallback de compatibilidade caso ainda use lista 'destinos'
        data_ida = config.get("data_ida", "2026-12-21")
        data_volta = config.get("data_volta", "2026-12-28")
        destinos = config.get("destinos", [])
        for cfg_d in destinos:
            dest_code = cfg_d["codigo"]
            if apenas_destino and dest_code.upper() != apenas_destino.upper():
                continue
            res = buscar_voo_rota(origem, dest_code, data_ida, data_volta, adultos, classe)
            if res.success and res.menor_geral:
                voo_ref = res.menor_ate_1_parada or res.menor_geral
                deve_alertar, motivo, detalhe = avaliar_alerta(res, cfg_d, regras, historico)
                if deve_alertar:
                    texto_alerta = formatar_alerta_telegram(res, cfg_d, motivo, detalhe)
                    sucesso_envio = enviar_mensagem(texto_alerta)
                    registrar_cotacao(res, historico, foi_alertado=sucesso_envio)
                else:
                    registrar_cotacao(res, historico, foi_alertado=False)
                resultados_resumo.append({
                    "rotulo": f"{cfg_d['cidade']} ({dest_code})",
                    "success": True,
                    "preco": voo_ref.price,
                    "cia": voo_ref.airline,
                    "stops": voo_ref.stops,
                    "url": res.url_google_flights
                })
            time.sleep(1.8)

    print("\n" + "=" * 70)
    print("🏁 Varredura concluída!")
    print("=" * 70)

    if enviar_resumo and resultados_resumo:
        print("📨 Enviando Resumo para o Telegram...")
        texto_resumo = formatar_resumo_telegram(resultados_resumo, config)
        enviar_mensagem(texto_resumo)
        print("✅ Resumo enviado!")

    return resultados_resumo


def exibir_status_historico():
    historico = carregar_historico()
    if not historico:
        print("Nenhum histórico registrado ainda. Execute uma busca com --check ou --resumo.")
        return

    print("\n📊 HISTÓRICO DE PREÇOS REGISTRADOS:")
    print("-" * 85)
    print(f"{'ROTA':<22} {'ÚLTIMO':<10} {'MENOR HIST':<12} {'MAIOR HIST':<12} {'DIRETO':<10} {'CIA':<12} {'ATUALIZADO'}")
    print("-" * 85)

    for code, dados in sorted(historico.items()):
        ult = f"R$ {dados.get('ultimo_preco', 0):,}"
        menor = f"R$ {dados.get('menor_preco_historico', 0):,}"
        maior = f"R$ {dados.get('maior_preco_historico', 0):,}"
        direto = f"R$ {dados.get('ultimo_preco_direto', 0):,}" if dados.get("ultimo_preco_direto") else "-"
        cia = dados.get("ultima_cia", "-")[:10]
        data = dados.get("ultima_atualizacao", "-")
        print(f"{code:<22} {ult:<10} {menor:<12} {maior:<12} {direto:<10} {cia:<12} {data}")
    print("-" * 85)


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
    parser = argparse.ArgumentParser(description="Monitor de Voos de Volta Salvador (SSA) -> Confins (CNF)")
    parser.add_argument("--check", action="store_true", help="Executa uma varredura agora e alerta somente se houver preço bom")
    parser.add_argument("--resumo", action="store_true", help="Executa uma varredura e envia o resumo completo no Telegram")
    parser.add_argument("--monitor", action="store_true", help="Executa em modo contínuo (loop de monitoramento)")
    parser.add_argument("--intervalo", type=int, default=None, help="Intervalo em minutos para o modo monitor (padrão: 180 min)")
    parser.add_argument("--status", action="store_true", help="Exibe o histórico de preços salvo localmente")
    parser.add_argument("--destino", type=str, default=None, help="Consulta apenas um filtro específico")

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
