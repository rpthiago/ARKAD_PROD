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
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Safe stdout/stderr for pythonw.exe
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
logger = logging.getLogger("buscador_internacional")

CONFIG_PATH = Path(__file__).resolve().parent / "config_internacional.json"

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from buscador_voos.telegram_bot import enviar_mensagem
from buscador_voos_internacional.flight_scanner import (
    consultar_voo_internacional,
    CotacaoRota,
    gerar_url_skyscanner,
)
from buscador_voos_internacional.price_history import (
    carregar_historico,
    avaliar_alerta_internacional,
    registrar_cotacao_internacional,
)


def carregar_config() -> Dict[str, Any]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def formatar_alerta_telegram(
    cotacao: CotacaoRota,
    cfg_dest: Dict[str, Any],
    origens_map: Dict[str, str],
    motivo: str,
    detalhes: str,
    duracao_dias: int,
) -> str:
    voo = cotacao.menor_voo
    cidade = cfg_dest.get("cidade", cotacao.destino)
    pais = cfg_dest.get("pais", "")
    emoji = cfg_dest.get("emoji", "✈️")
    teto = cfg_dest.get("preco_teto_alerta", 0)
    nome_evento = cfg_dest.get("nome", "")
    origem_nome = origens_map.get(cotacao.origem, cotacao.origem)

    paradas_str = "Voo Direto (Sem Escalas)" if voo.stops == 0 else f"{voo.stops} escala(s)"
    nivel_map = {
        "baixo": "🟢 BAIXO (tarifa anormalmente barata)",
        "normal": "🟡 NORMAL",
        "alto": "🔴 ALTO",
    }
    nivel_str = nivel_map.get(cotacao.price_level, cotacao.price_level.upper())

    info_direto = ""
    if cotacao.menor_direto and cotacao.menor_direto.price != voo.price:
        info_direto = f"🛫 <b>Opção Direta:</b> R$ {cotacao.menor_direto.price:,} ({cotacao.menor_direto.airline})\n"

    evento_hdr = f" — {nome_evento}" if nome_evento else ""
    url_sky = getattr(cotacao, "url_skyscanner", "")
    if not url_sky and cotacao.data_volta:
        url_sky = gerar_url_skyscanner(cotacao.origem, cotacao.destino, cotacao.data_ida, cotacao.data_volta)

    msg = (
        f"🚨 🔥 <b>MEGA PROMOÇÃO INTERNACIONAL! {emoji}</b>{evento_hdr}\n\n"
        f"✈️ <b>{origem_nome} ➔ {cidade}/{pais} ({cotacao.destino})</b>\n"
        f"🗓️ <b>{cotacao.data_ida} a {cotacao.data_volta}</b> ({duracao_dias} dias)\n\n"
        f"💰 <b>Preço: R$ {voo.price:,}</b> <i>(Ida e Volta com taxas!)</i>\n"
        f"🎯 <b>Teto Agressivo:</b> R$ {teto:,}\n"
        f"🏷️ <b>Cia:</b> {voo.airline}\n"
        f"🛑 <b>Conexão:</b> {paradas_str}\n"
        f"📊 <b>Nível de Preço:</b> {nivel_str}\n"
        f"{info_direto}"
        f"⚡ <b>Gatilho:</b> {detalhes}\n\n"
        f"🔗 <a href=\"{cotacao.url_google_flights}\"><b>👉 Ver no Google Flights</b></a>\n"
        f"✈️ <a href=\"{url_sky}\"><b>👉 Comparar no Skyscanner</b></a>"
    )
    return msg


def formatar_resumo_internacional(ranking: List[Dict[str, Any]]) -> str:
    linhas = []
    # Agrupa por identificador único (id_rota se existir, senão destino) e pega a melhor opção encontrada
    por_destino = {}
    for item in ranking:
        if not item.get("success"):
            continue
        chave_grp = item.get("id_rota") or item["destino"]
        if chave_grp not in por_destino or item["preco"] < por_destino[chave_grp]["preco"]:
            por_destino[chave_grp] = item

    ordenados = sorted(por_destino.values(), key=lambda x: x["preco"])

    for idx, it in enumerate(ordenados, 1):
        cidade = it["cidade"]
        pais = it["pais"]
        emoji = it.get("emoji", "✈️")
        preco = it["preco"]
        orig = it["origem"]
        cia = it["cia"]
        stops = "Direto" if it["stops"] == 0 else f"{it['stops']}p"
        datas = f"{it['data_ida']} a {it['data_volta']}"
        status = "🎯 PROMOÇÃO!" if (it.get("teto") and preco <= it["teto"]) else ""
        nome_extra = f" [{it['nome_evento']}]" if it.get("nome_evento") else ""

        url_gf = it.get("url", "")
        url_sky = it.get("url_skyscanner", "")
        links_parts = []
        if url_gf:
            links_parts.append(f"<a href=\"{url_gf}\">Google Flights</a>")
        if url_sky:
            links_parts.append(f"<a href=\"{url_sky}\">Skyscanner</a>")
        links_str = "   🔗 " + " | ".join(links_parts) if links_parts else ""

        linhas.append(
            f"{idx}. {emoji} <b>{cidade}/{pais} ({it['destino']}){nome_extra}: R$ {preco:,}</b>\n"
            f"   ↳ <i>Saindo de {orig} | {cia} ({stops}) | {datas}</i> {status}"
            + (f"\n{links_str}" if links_str else "")
        )

    tabela = "\n\n".join(linhas) if linhas else "Nenhuma cotação disponível no momento."

    msg = (
        f"🌍 <b>RANKING INTERNACIONAL DE MENORES PREÇOS</b>\n"
        f"🛫 <i>Saídas monitoradas: São Paulo (GRU), Rio (GIG) e BH (CNF)</i>\n\n"
        f"{tabela}\n\n"
        f"💡 <i>Alertas individuais são disparados imediatamente assim que qualquer tarifa furar o teto estipulado!</i>"
    )
    return msg


def gerar_datas_amostra(meses: List[Dict[str, Any]], dias_saida: List[int], duracao_dias: int) -> List[tuple]:
    pares = []
    for m in meses:
        ano = m["ano"]
        mes = m["mes"]
        for dia in dias_saida:
            try:
                dt_ida = datetime(ano, mes, dia)
                dt_volta = dt_ida + timedelta(days=duracao_dias)
                pares.append((dt_ida.strftime("%Y-%m-%d"), dt_volta.strftime("%Y-%m-%d"), m.get("nome", "")))
            except ValueError:
                pass
    return pares


def executar_varredura_internacional(config: Dict[str, Any], enviar_resumo: bool = False, apenas_destino: str = None) -> List[Dict[str, Any]]:
    origens = config.get("origens", [])
    destinos = config.get("destinos", [])
    duracao = config.get("duracao_viagem_dias", 10)
    meses = config.get("meses_monitorados", [])
    dias_saida = config.get("dias_saida_amostra", [10, 20])
    regras = config.get("regras_alerta", {})

    origens_map = {o["codigo"]: o["nome"] for o in origens}
    historico = carregar_historico()

    datas_amostra = gerar_datas_amostra(meses, dias_saida, duracao)
    resultados = []

    print("\n" + "=" * 75)
    print(f"🌍 INICIANDO MONITORAMENTO INTERNACIONAL (10 DIAS) — EUROPA, TURQUIA & JAPÃO")
    print(f"🛫 Origens: {', '.join([o['codigo'] for o in origens])}")
    print(f"🗓️ Datas amostradas: {len(datas_amostra)} janelas ao longo de {len(meses)} meses")
    print("=" * 75)

    for dest in destinos:
        dest_code = dest["codigo"]
        cidade = dest["cidade"]
        pais = dest["pais"]
        emoji = dest.get("emoji", "✈️")
        teto = dest.get("preco_teto_alerta", 999999)

        if apenas_destino and dest_code.upper() != apenas_destino.upper():
            continue

        print(f"\n🔎 {emoji} Verificando {cidade}/{pais} ({dest_code}) [Teto Alvo: R$ {teto:,}]...")

        # Para otimizar tempo e foco:
        # GRU é prioritária (onde saem 90% das promoções de Madri, Londres, Turquia e Japão)
        # GIG e CNF são verificadas nas janelas principais
        for orig in origens:
            orig_code = orig["codigo"]

            for dt_ida, dt_volta, mes_nome in datas_amostra:
                cot = consultar_voo_internacional(
                    origem=orig_code,
                    destino=dest_code,
                    data_ida=dt_ida,
                    data_volta=dt_volta,
                    adultos=1,
                    classe="economy",
                    max_tentativas=2,
                )

                if not cot.success or not cot.menor_voo:
                    time.sleep(1.0)
                    continue

                voo = cot.menor_voo
                preco = voo.price
                stops_txt = "Direto" if voo.stops == 0 else f"{voo.stops}p"

                # Avalia alerta
                deve, motivo, detalhe = avaliar_alerta_internacional(cot, dest, regras, historico)

                if deve:
                    print(f"   🚨 DISPARANDO MEGA PROMOÇÃO TELEGRAM! {orig_code}➔{dest_code}: R$ {preco:,} ({detalhe})")
                    msg_alerta = formatar_alerta_telegram(cot, dest, origens_map, motivo, detalhe, duracao)
                    ok = enviar_mensagem(msg_alerta)
                    registrar_cotacao_internacional(cot, historico, foi_alertado=ok)
                else:
                    registrar_cotacao_internacional(cot, historico, foi_alertado=False)

                resultados.append({
                    "origem": orig_code,
                    "destino": dest_code,
                    "cidade": cidade,
                    "pais": pais,
                    "emoji": emoji,
                    "teto": teto,
                    "success": True,
                    "preco": preco,
                    "cia": voo.airline,
                    "stops": voo.stops,
                    "data_ida": dt_ida,
                    "data_volta": dt_volta,
                    "url": cot.url_google_flights,
                    "url_skyscanner": cot.url_skyscanner,
                })

                # Pausa amigável
                time.sleep(1.2)

        # Pequena pausa entre destinos
        time.sleep(1.5)

    # Varredura de Rotas Específicas / Eventos (ex: Carnaval Santiago)
    rotas_esp = config.get("rotas_especificas", [])
    if rotas_esp:
        print("\n" + "-" * 75)
        print("🎯 VERIFICANDO ROTAS ESPECÍFICAS / EVENTOS ESPECIAIS:")
        print("-" * 75)

        for rota in rotas_esp:
            dest_code = rota["destino"]
            cidade = rota.get("cidade", dest_code)
            pais = rota.get("pais", "")
            emoji = rota.get("emoji", "✈️")
            teto = rota.get("preco_teto_alerta", 999999)
            dt_ida = rota["data_ida"]
            dt_volta = rota["data_volta"]
            duracao_rota = rota.get("duracao_dias", 7)
            nome_evento = rota.get("nome", "")
            origens_rota = rota.get("origens", origens)

            if apenas_destino and dest_code.upper() != apenas_destino.upper():
                continue

            print(f"\n🔎 {emoji} Verificando rota específica: {nome_evento} ({cidade}/{pais} - {dest_code}) [Teto: R$ {teto:,}]...")

            for orig in origens_rota:
                orig_code = orig["codigo"]
                cot = consultar_voo_internacional(
                    origem=orig_code,
                    destino=dest_code,
                    data_ida=dt_ida,
                    data_volta=dt_volta,
                    adultos=1,
                    classe="economy",
                    max_tentativas=2,
                )

                if not cot.success or not cot.menor_voo:
                    time.sleep(1.0)
                    continue

                voo = cot.menor_voo
                preco = voo.price

                deve, motivo, detalhe = avaliar_alerta_internacional(cot, rota, regras, historico)

                if deve:
                    print(f"   🚨 DISPARANDO PROMOÇÃO TELEGRAM! {orig_code}➔{dest_code}: R$ {preco:,} ({detalhe})")
                    msg_alerta = formatar_alerta_telegram(cot, rota, origens_map, motivo, detalhe, duracao_rota)
                    ok = enviar_mensagem(msg_alerta)
                    registrar_cotacao_internacional(cot, historico, foi_alertado=ok)
                else:
                    registrar_cotacao_internacional(cot, historico, foi_alertado=False)

                resultados.append({
                    "id_rota": rota.get("id", f"{dest_code}_{dt_ida}"),
                    "nome_evento": nome_evento,
                    "origem": orig_code,
                    "destino": dest_code,
                    "cidade": cidade,
                    "pais": pais,
                    "emoji": emoji,
                    "teto": teto,
                    "success": True,
                    "preco": preco,
                    "cia": voo.airline,
                    "stops": voo.stops,
                    "data_ida": dt_ida,
                    "data_volta": dt_volta,
                    "url": cot.url_google_flights,
                    "url_skyscanner": cot.url_skyscanner,
                })

                time.sleep(1.2)

    print("\n" + "=" * 75)
    print("🏁 Varredura internacional concluída!")
    print("=" * 75)

    if enviar_resumo and resultados:
        print("📨 Enviando Ranking Internacional para o Telegram...")
        msg_resumo = formatar_resumo_internacional(resultados)
        enviar_mensagem(msg_resumo)
        print("✅ Ranking enviado!")

    return resultados


def exibir_status_historico():
    historico = carregar_historico()
    if not historico:
        print("Nenhum histórico internacional registrado ainda.")
        return

    print("\n📊 HISTÓRICO DE ROTAS INTERNACIONAIS:")
    print("-" * 80)
    print(f"{'ROTA':<12} {'DATAS':<24} {'ÚLTIMO':<12} {'MENOR':<12} {'CIA':<15} {'ATUALIZADO'}")
    print("-" * 80)

    for rota, d in sorted(historico.items()):
        datas = f"{d.get('data_ida', '')} -> {d.get('data_volta', '')}"
        ult = f"R$ {d.get('ultimo_preco', 0):,}"
        menor = f"R$ {d.get('menor_historico', 0):,}"
        cia = d.get("ultima_cia", "-")[:13]
        dt = d.get("ultima_atualizacao", "-")
        print(f"{rota:<12} {datas:<24} {ult:<12} {menor:<12} {cia:<15} {dt}")
    print("-" * 80)


def enviar_resumo_do_historico(config: Dict[str, Any]) -> bool:
    destinos_map = {d["codigo"]: d for d in config.get("destinos", [])}
    rotas_esp = config.get("rotas_especificas", [])
    rotas_esp_map = {}
    for r in rotas_esp:
        rotas_esp_map[f"{r['destino']}_{r['data_ida']}"] = r
        if r["destino"] not in destinos_map:
            destinos_map[r["destino"]] = r

    historico = carregar_historico()
    resultados = []
    for chave, item in historico.items():
        partes = chave.split("_")
        if len(partes) < 3:
            continue
        orig, dest, data_ida = partes[0], partes[1], partes[2]

        esp_key = f"{dest}_{data_ida}"
        if esp_key in rotas_esp_map:
            dest_cfg = rotas_esp_map[esp_key]
            id_rota = dest_cfg.get("id", esp_key)
            nome_evento = dest_cfg.get("nome", "")
        else:
            dest_cfg = destinos_map.get(dest, {})
            id_rota = dest
            nome_evento = ""

        preco = item.get("ultimo_preco", 0)
        if preco and preco > 0:
            url_gf = item.get("url", "")
            url_sky = item.get("url_skyscanner", "")
            data_volta = item.get("data_volta", "")
            if not url_sky and data_volta:
                url_sky = gerar_url_skyscanner(orig, dest, data_ida, data_volta)

            resultados.append({
                "id_rota": id_rota,
                "nome_evento": nome_evento,
                "origem": orig,
                "destino": dest,
                "cidade": dest_cfg.get("cidade", dest),
                "pais": dest_cfg.get("pais", ""),
                "emoji": dest_cfg.get("emoji", "✈️"),
                "teto": dest_cfg.get("preco_teto_alerta", 0),
                "success": True,
                "preco": preco,
                "cia": item.get("ultima_cia", ""),
                "stops": item.get("ultimas_paradas", 1),
                "data_ida": item.get("data_ida", data_ida),
                "data_volta": data_volta,
                "url": url_gf,
                "url_skyscanner": url_sky,
            })
    if resultados:
        print("📨 Enviando Ranking Internacional consolidado para o Telegram...")
        msg_resumo = formatar_resumo_internacional(resultados)
        sucesso = enviar_mensagem(msg_resumo)
        if sucesso:
            print("✅ Ranking enviado com sucesso!")
        else:
            print("❌ Falha ao enviar ranking.")
        return sucesso
    else:
        print("Nenhum histórico disponível para envio.")
        return False


def main():
    parser = argparse.ArgumentParser(description="Monitor de Voos Internacionais Agressivos (Europa, Turquia e Japão)")
    parser.add_argument("--check", action="store_true", help="Executa varredura silenciosa e alerta somente se houver preço agressivo")
    parser.add_argument("--resumo", action="store_true", help="Executa varredura completa e envia o resumo com ranking no Telegram")
    parser.add_argument("--resumo-historico", action="store_true", help="Envia o ranking dos menores preços diretamente a partir do histórico recente consolidado")
    parser.add_argument("--status", action="store_true", help="Exibe o histórico de preços salvo localmente")
    parser.add_argument("--destino", type=str, default=None, help="Consulta apenas um destino específico (ex: MAD, LIS, LHR, IST, NRT)")

    args = parser.parse_args()
    config = carregar_config()

    if args.status:
        exibir_status_historico()
    elif args.resumo_historico:
        enviar_resumo_do_historico(config)
    elif args.resumo:
        executar_varredura_internacional(config, enviar_resumo=True, apenas_destino=args.destino)
    else:
        executar_varredura_internacional(config, enviar_resumo=False, apenas_destino=args.destino)


if __name__ == "__main__":
    main()
