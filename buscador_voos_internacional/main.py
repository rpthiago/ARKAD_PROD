import os
import sys
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
from buscador_voos_internacional.flight_scanner import consultar_voo_internacional, CotacaoRota
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

    msg = (
        f"🚨 🔥 <b>MEGA PROMOÇÃO INTERNACIONAL! {emoji}</b>\n\n"
        f"✈️ <b>{origem_nome} ➔ {cidade}/{pais} ({cotacao.destino})</b>\n"
        f"🗓️ <b>{cotacao.data_ida} a {cotacao.data_volta}</b> ({duracao_dias} dias)\n\n"
        f"💰 <b>Preço: R$ {voo.price:,}</b> <i>(Ida e Volta com taxas!)</i>\n"
        f"🎯 <b>Teto Agressivo:</b> R$ {teto:,}\n"
        f"🏷️ <b>Cia:</b> {voo.airline}\n"
        f"🛑 <b>Conexão:</b> {paradas_str}\n"
        f"📊 <b>Nível de Preço:</b> {nivel_str}\n"
        f"{info_direto}"
        f"⚡ <b>Gatilho:</b> {detalhes}\n\n"
        f"🔗 <a href=\"{cotacao.url_google_flights}\"><b>👉 Ver e Emitir no Google Flights</b></a>"
    )
    return msg


def formatar_resumo_internacional(ranking: List[Dict[str, Any]]) -> str:
    linhas = []
    # Agrupa por destino e pega a melhor opção encontrada
    por_destino = {}
    for item in ranking:
        if not item.get("success"):
            continue
        dest = item["destino"]
        if dest not in por_destino or item["preco"] < por_destino[dest]["preco"]:
            por_destino[dest] = item

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
        status = "🎯 PROMOÇÃO!" if preco <= it["teto"] else ""

        linhas.append(
            f"{idx}. {emoji} <b>{cidade}/{pais} ({it['destino']}): R$ {preco:,}</b>\n"
            f"   ↳ <i>Saindo de {orig} | {cia} ({stops}) | {datas}</i> {status}"
        )

    tabela = "\n\n".join(linhas) if linhas else "Nenhuma cotação disponível no momento."

    msg = (
        f"🌍 <b>RANKING INTERNACIONAL DE MENORES PREÇOS (10 DIAS)</b>\n"
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
                })

                # Pausa amigável
                time.sleep(1.2)

        # Pequena pausa entre destinos
        time.sleep(1.5)

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


def main():
    parser = argparse.ArgumentParser(description="Monitor de Voos Internacionais Agressivos (Europa, Turquia e Japão)")
    parser.add_argument("--check", action="store_true", help="Executa varredura silenciosa e alerta somente se houver preço agressivo")
    parser.add_argument("--resumo", action="store_true", help="Executa varredura e envia o resumo completo com ranking no Telegram")
    parser.add_argument("--status", action="store_true", help="Exibe o histórico de preços salvo localmente")
    parser.add_argument("--destino", type=str, default=None, help="Consulta apenas um destino específico (ex: MAD, LIS, LHR, IST, NRT)")

    args = parser.parse_args()
    config = carregar_config()

    if args.status:
        exibir_status_historico()
    elif args.resumo:
        executar_varredura_internacional(config, enviar_resumo=True, apenas_destino=args.destino)
    else:
        executar_varredura_internacional(config, enviar_resumo=False, apenas_destino=args.destino)


if __name__ == "__main__":
    main()
