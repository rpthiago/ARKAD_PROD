"""
Ponto de entrada principal do Buscador de Milhas Smiles & Parceiras.
Executa o escaneamento de rotas, avalia tetos agressivos, calcula o CPM equivalente
e envia alertas acionáveis com deep links para o grupo do Telegram.
"""
import os
import sys
import time
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

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
logger = logging.getLogger("smiles_main")

CONFIG_PATH = Path(__file__).resolve().parent / "config_smiles.json"
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from buscador_voos.telegram_bot import enviar_mensagem
from buscador_milhas_smiles.smiles_scanner import cotar_voo_smiles, CotacaoSmiles
from buscador_milhas_smiles.historico_smiles import (
    avaliar_alerta_smiles,
    registrar_alerta_smiles,
)


def carregar_config() -> Dict[str, Any]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def formatar_alerta_smiles(
    cotacao: CotacaoSmiles,
    cfg_dest: Dict[str, Any],
    origens_map: Dict[str, str],
    detalhes: str,
    duracao_dias: int = 10,
) -> str:
    cidade = cfg_dest.get("cidade", cotacao.destino)
    pais = cfg_dest.get("pais", "")
    uf = cfg_dest.get("uf", "")
    local_str = f"{cidade}/{uf}" if uf else f"{cidade}/{pais}"
    emoji = cfg_dest.get("emoji", "✈️")
    origem_nome = origens_map.get(cotacao.origem, cotacao.origem)

    paradas_str = "Voo Direto" if cotacao.paradas == 0 else f"{cotacao.paradas} conexão(ões)"
    safe_url = cotacao.url_emissao_smiles.replace("&", "&amp;")

    if cotacao.tipo_viagem == "ida_e_volta":
        periodo_str = f"{cotacao.data_ida} a {cotacao.data_volta} ({duracao_dias} dias)"
        trecho_milhas = cotacao.milhas_por_trecho or int(cotacao.milhas / 2)
        if cotacao.tipo_precificacao == "dinamica_gol":
            milhas_label = "Milhas Estimadas (GOL Dinâmico):"
            sub_label = f"   ↳ <i>(Estimativa via modelo R$ 18/milheiro | ~{trecho_milhas:,} por trecho)</i>"
        elif cotacao.tipo_precificacao == "dinamica_internacional":
            milhas_label = "Milhas Estimadas (Internacional Dinâmico):"
            sub_label = f"   ↳ <i>(Estimativa via modelo R$ 13,50/milheiro comercial | ~{trecho_milhas:,} por trecho)</i>"
        else:
            milhas_label = "Milhas Estimadas:"
            sub_label = f"   ↳ <i>(~{trecho_milhas:,} por trecho)</i>"

        milhas_bloco = (
            f"🎟️ <b>{milhas_label}</b> {cotacao.milhas:,} milhas <b>(TOTAL IDA E VOLTA)</b>\n"
            f"{sub_label}"
        )
        teto_bloco = f"🎯 <b>Teto Agressivo:</b> {cotacao.teto_milhas:,} milhas (Ida e Volta)"
        link_bloco = f"🔗 <a href=\"{safe_url}\"><b>👉 Abrir Pesquisa de Ida e Volta na Smiles</b></a>"
        if cotacao.url_emissao_somente_ida:
            safe_ida_url = cotacao.url_emissao_somente_ida.replace("&", "&amp;")
            link_bloco += f"\n   ↳ <i>Prefere emitir só a ida? <a href=\"{safe_ida_url}\">Ver Somente Ida (~{trecho_milhas:,} milhas)</a></i>"
    else:
        periodo_str = f"{cotacao.data_ida} (Somente Ida)"
        milhas_bloco = f"🎟️ <b>Milhas Estimadas:</b> {cotacao.milhas:,} milhas <b>(SOMENTE IDA)</b>"
        teto_bloco = f"🎯 <b>Teto Agressivo:</b> {cotacao.teto_milhas:,} milhas (Por Trecho)"
        link_bloco = f"🔗 <a href=\"{safe_url}\"><b>👉 Abrir Pesquisa de Somente Ida na Smiles</b></a>"

    eco_pct = round((cotacao.economia_reais / cotacao.preco_dinheiro_estimado) * 100, 1) if cotacao.preco_dinheiro_estimado > 0 else 0
    cia_label = "Cia:" if cotacao.cia_aerea == "GOL" else "Cia Parceira:"
    cotacao_time_str = f" <i>({cotacao.data_cotacao_dinheiro})</i>" if cotacao.data_cotacao_dinheiro else ""

    msg = (
        f"🚨 💎 <b>OPORTUNIDADE DE MILHAS SMILES! {emoji}</b>\n\n"
        f"✈️ <b>{origem_nome} ➔ {local_str} ({cotacao.destino})</b>\n"
        f"🗓️ <b>{periodo_str}</b> | 💺 <b>{cotacao.cabine}</b>\n\n"
        f"{milhas_bloco}\n"
        f"{teto_bloco}\n"
        f"🏷️ <b>{cia_label}</b> {cotacao.cia_aerea}\n"
        f"🛑 <b>Voo:</b> {paradas_str}\n"
        f"💵 <b>Taxas de Embarque:</b> R$ {cotacao.taxas_embarque_reais:,.2f}\n\n"
        f"📊 <b>Custo Estimado em Milhas: R$ {cotacao.custo_total_equivalente_reais:,.2f}</b>\n"
        f"   <i>(Base: Milhas @ R$ {cotacao.cpm_referencia:.2f}/milheiro + Taxas)</i>\n"
        f"💰 <b>Tarifa em Dinheiro (Google Flights):</b> R$ {cotacao.preco_dinheiro_estimado:,.2f}{cotacao_time_str}\n"
        f"🟢 <b>Economia Teórica: R$ {cotacao.economia_reais:,.2f} ({eco_pct}%)</b>\n\n"
        f"⚡ <b>Gatilho:</b> {detalhes}\n"
        f"💡 <i>Verifique a disponibilidade de assentos award no link Smiles abaixo:</i>\n\n"
        f"{link_bloco}"
    )
    return msg



def formatar_resumo_smiles(cotacoes: List[CotacaoSmiles], cfg: Dict[str, Any]) -> str:
    origens_map = cfg.get("origens", {})
    destinos_map = cfg.get("destinos", {})

    linhas = [
        "📊 📋 <b>RADAR SMILES — RESUMO DE SWEET SPOTS & MILHAS</b>\n"
        f"<i>Custo de milheiro base: R$ {cfg.get('cpm_referencia_reais', 15.50):.2f}</i>\n"
    ]

    for c in cotacoes:
        dest_cfg = destinos_map.get(c.destino, {})
        emoji = dest_cfg.get("emoji", "✈️")
        cidade = dest_cfg.get("cidade", c.destino)
        orig_nome = origens_map.get(c.origem, c.origem).split(" ")[0]
        
        status_emoji = "🟢" if c.atingiu_teto_agressivo else "⚪"
        tipo_str = "I/V" if c.tipo_viagem == "ida_e_volta" else "Ida"

        safe_link = c.url_emissao_smiles.replace("&", "&amp;")
        linha = (
            f"{status_emoji} {emoji} <b>{orig_nome} ➔ {cidade} ({c.destino})</b> [{tipo_str}]\n"
            f"   🎟️ <b>{c.milhas:,} milhas</b> | Custo: <b>R$ {c.custo_total_equivalente_reais:,.0f}</b> | Cia: {c.cia_aerea}\n"
            f"   🔗 <a href=\"{safe_link}\">Ver na Smiles</a>"
        )
        linhas.append(linha)

    linhas.append("\n💡 <i>Dica: Compare sempre o custo total em R$ (milhas + taxas) com a tarifa pagante em dinheiro.</i>")
    return "\n\n".join(linhas)


def executar_busca(
    dry_run: bool = False,
    enviar_resumo: bool = False,
    origem_filtro: Optional[str] = None,
    destino_filtro: Optional[str] = None,
    permitir_ao_vivo: bool = False,
) -> None:
    cfg = carregar_config()
    origens = cfg.get("origens", {})
    destinos = cfg.get("destinos", {})
    cpm_ref = cfg.get("cpm_referencia_reais", 15.50)
    cooldown_horas = cfg.get("cooldown_alerta_horas", 12)

    logger.info("Iniciando Radar de Milhas Smiles...")
    logger.info(f"Origens: {list(origens.keys())} | Destinos: {list(destinos.keys())}")

    datas_monitoradas = cfg.get("datas_monitoradas", {})

    total_avaliados = 0
    total_alertas = 0
    melhores_cotacoes: List[CotacaoSmiles] = []
    candidatos_alerta = []

    for dest_code, dest_cfg in destinos.items():
        if destino_filtro and dest_code != destino_filtro:
            continue

        tipo_destino = dest_cfg.get("tipo", "internacional")
        datas_config = datas_monitoradas.get(tipo_destino, [])
        if not datas_config:
            datas_config = [{"data_ida": "2027-05-10", "data_volta": "2027-05-20", "duracao_dias": 10, "descricao": "Maio/2027"}]

        # Origens permitidas para este destino (ex: CNF para Nordeste)
        origens_permitidas = dest_cfg.get("origens", list(origens.keys()))

        for orig_code in origens_permitidas:
            if origem_filtro and orig_code != origem_filtro:
                continue

            for d_info in datas_config:
                data_ida = d_info["data_ida"]
                data_volta = d_info.get("data_volta")
                duracao = d_info.get("duracao_dias", 10)
                desc_periodo = d_info.get("descricao", "")

                total_avaliados += 1
                cotacao = cotar_voo_smiles(
                    origem=orig_code,
                    destino=dest_code,
                    data_ida=data_ida,
                    data_volta=data_volta,
                    cfg_dest=dest_cfg,
                    cpm_ref=cpm_ref,
                    permitir_ao_vivo=permitir_ao_vivo,
                )

                melhores_cotacoes.append(cotacao)

                # Avalia alerta de teto agressivo
                deve_alertar, motivo, detalhes = avaliar_alerta_smiles(
                    origem=orig_code,
                    destino=dest_code,
                    data_ida=data_ida,
                    data_volta=data_volta,
                    milhas=cotacao.milhas,
                    teto_milhas=cotacao.teto_milhas,
                    cooldown_horas=cooldown_horas,
                )

                if deve_alertar:
                    gatilho_detalhe = f"{desc_periodo} | {detalhes}" if desc_periodo else detalhes
                    candidatos_alerta.append((cotacao, dest_cfg, gatilho_detalhe, duracao))

    # Ordena os alertas candidatos pelos melhores ganhos/economia financeira
    candidatos_alerta.sort(key=lambda item: item[0].economia_reais, reverse=True)

    max_alertas = cfg.get("max_alertas_por_execucao", 6)
    alertas_selecionados = candidatos_alerta[:max_alertas]

    for cotacao, dest_cfg, gatilho_detalhe, duracao in alertas_selecionados:
        total_alertas += 1
        msg = formatar_alerta_smiles(
            cotacao=cotacao,
            cfg_dest=dest_cfg,
            origens_map=origens,
            detalhes=gatilho_detalhe,
            duracao_dias=duracao,
        )

        if dry_run:
            logger.info(f"[DRY-RUN] Alerta Milhas: {cotacao.origem} ➔ {cotacao.destino} | {cotacao.milhas:,} milhas | {cotacao.cia_aerea}")
        else:
            logger.info(f"[ALERTA ENVIADO] {cotacao.origem} ➔ {cotacao.destino} | {cotacao.milhas:,} milhas | Economia: R$ {cotacao.economia_reais:,.2f}")
            enviar_mensagem(msg)
            registrar_alerta_smiles(
                origem=cotacao.origem,
                destino=cotacao.destino,
                data_ida=cotacao.data_ida,
                data_volta=cotacao.data_volta,
                milhas=cotacao.milhas,
                cia_aerea=cotacao.cia_aerea,
                custo_reais=cotacao.custo_total_equivalente_reais,
            )
            time.sleep(2.0)  # Respeita o rate-limit da API do Telegram

    # Envia resumo se solicitado
    if enviar_resumo and melhores_cotacoes:
        # Pega as 8 melhores opções de menor custo
        top_cotacoes = sorted(melhores_cotacoes, key=lambda c: c.custo_total_equivalente_reais)[:8]
        msg_resumo = formatar_resumo_smiles(top_cotacoes, cfg)
        if dry_run:
            logger.info(f"[DRY-RUN RESUMO]\n{msg_resumo}")
        else:
            enviar_mensagem(msg_resumo)

    logger.info(f"Finalizado. Avaliados: {total_avaliados}, Alertas gerados: {total_alertas}")


def main():
    parser = argparse.ArgumentParser(description="Radar de Milhas Smiles & Parceiras")
    parser.add_argument("--dry-run", action="store_true", help="Executa sem enviar mensagens ao Telegram")
    parser.add_argument("--resumo", action="store_true", help="Gera e envia resumo geral de rotas")
    parser.add_argument("--origem", type=str, default=None, help="Filtrar por aeroporto de origem (ex: GRU, CNF)")
    parser.add_argument("--destino", type=str, default=None, help="Filtrar por aeroporto de destino (ex: MAD, LIS)")
    parser.add_argument("--test", action="store_true", help="Envia mensagens de teste de emissão Smiles no Telegram")
    parser.add_argument("--ao-vivo", action="store_true", help="Permite consultas ao vivo no Google Flights caso não haja cache recente")

    args = parser.parse_args()

    if args.test:
        cfg = carregar_config()
        # Teste 1: Internacional (Madrid)
        cotacao_mad = cotar_voo_smiles(
            origem="GRU",
            destino="MAD",
            data_ida="2027-05-10",
            data_volta="2027-05-20",
            cfg_dest=cfg["destinos"]["MAD"],
            cpm_ref=15.50,
        )
        msg_mad = formatar_alerta_smiles(
            cotacao=cotacao_mad,
            cfg_dest=cfg["destinos"]["MAD"],
            origens_map=cfg["origens"],
            detalhes="Viagem 10 dias | Teste de Homologação Internacional",
            duracao_dias=10,
        )
        # Teste 2: Nordeste na data escolhida (Porto Seguro/BA saindo de CNF)
        dest_ne = "BPS" if "BPS" in cfg["destinos"] else "SSA"
        cotacao_ne = cotar_voo_smiles(
            origem="CNF",
            destino=dest_ne,
            data_ida="2026-12-21",
            data_volta="2026-12-28",
            cfg_dest=cfg["destinos"][dest_ne],
            cpm_ref=15.50,
        )
        msg_ne = formatar_alerta_smiles(
            cotacao=cotacao_ne,
            cfg_dest=cfg["destinos"][dest_ne],
            origens_map=cfg["origens"],
            detalhes="Data Escolhida: Semana do Réveillon 2026 | Teste Nordeste",
            duracao_dias=7,
        )

        print("\n=== TESTE INTERNACIONAL ===")
        print(msg_mad)
        print("\n=== TESTE NORDESTE ===")
        print(msg_ne)

        print("\nEnviando para o Telegram...")
        enviar_mensagem(msg_mad)
        time.sleep(1)
        enviar_mensagem(msg_ne)
        print("Mensagens de teste enviadas com sucesso ao grupo de voos!")
        return

    executar_busca(
        dry_run=args.dry_run,
        enviar_resumo=args.resumo,
        origem_filtro=args.origem,
        destino_filtro=args.destino,
        permitir_ao_vivo=args.ao_vivo,
    )


if __name__ == "__main__":
    main()
