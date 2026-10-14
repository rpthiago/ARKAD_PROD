"""
Ponto de entrada do Buscador de Smartwatches ARKAD.
Comandos:
  --check: Varredura com disparo de alertas de oportunidade (desconto >= R$ 150 ou cupom).
  --resumo: Varredura com envio do ranking atual completo para o Telegram.
  --status: Exibe preços salvos localmente.
  --teste-alerta: Dispara um alerta de teste para verificar a entrega no Telegram.
"""
import os
import sys
import json
import time
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Any

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
logger = logging.getLogger("smartwatch_main")

CONFIG_PATH = Path(__file__).resolve().parent / "config_smartwatch.json"
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from buscador_smartwatch.pelando_scanner import buscar_ofertas_pelando, OfertaSmartwatch
from buscador_smartwatch.buscape_scanner import buscar_ofertas_buscape
from buscador_smartwatch.tracker_history import (
    carregar_historico,
    salvar_historico,
    avaliar_alerta_smartwatch,
    registrar_oferta,
)
from buscador_smartwatch.telegram_sender import (
    enviar_mensagem,
    formatar_alerta_smartwatch,
    formatar_resumo_smartwatches,
)


def carregar_config() -> Dict[str, Any]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def executar_varredura(enviar_resumo: bool = False, apenas_modelo: str = None) -> List[OfertaSmartwatch]:
    cfg = carregar_config()
    modelos = cfg.get("modelos", [])
    palavras_negativas = cfg.get("palavras_negativas", [])
    desconto_min = cfg.get("desconto_minimo_alerta_reais", 200.0)
    cooldown = cfg.get("cooldown_horas", 12)

    historico = carregar_historico()

    print("\n" + "=" * 75)
    print("⌚ INICIANDO RADAR DE SMARTWATCHES (AMAZFIT & HUAWEI)")
    print(f"🎯 Critério de Alerta: Queda >= R$ {desconto_min:,.2f} ou Cupom Ativo | Apenas NOVOS")
    print("=" * 75)

    todas_ofertas: List[OfertaSmartwatch] = []
    melhores_por_modelo: Dict[str, OfertaSmartwatch] = {}

    import primp
    client = primp.Client(impersonate="chrome_131")

    for mod in modelos:
        m_id = mod["id"]
        if apenas_modelo and m_id != apenas_modelo:
            continue

        print(f"\n🔎 Consultando {mod['marca']} {mod['modelo']} ({mod['tamanho_caixa']})...")

        # 1. Busca no Pelando (Promoções da comunidade com cupons)
        of_pel = buscar_ofertas_pelando(mod, palavras_negativas, client=client)
        print(f"   ↳ Pelando: {len(of_pel)} ofertas válidas encontradas.")

        # 2. Busca no Buscapé (Comparador multilojas: Amazon, ML, KaBuM, Magalu)
        of_busc = buscar_ofertas_buscape(mod, palavras_negativas, client=client)
        print(f"   ↳ Buscapé: {len(of_busc)} ofertas válidas encontradas.")

        of_total = of_pel + of_busc
        todas_ofertas.extend(of_total)

        for of in of_total:
            # Registra melhor do modelo para resumo
            if m_id not in melhores_por_modelo or of.preco < melhores_por_modelo[m_id].preco:
                melhores_por_modelo[m_id] = of

            # Avalia alerta
            deve_alertar, motivo, detalhes = avaliar_alerta_smartwatch(
                oferta=of,
                cfg_modelo=mod,
                desconto_minimo_reais=desconto_min,
                cooldown_horas=cooldown,
                historico=historico
            )

            if deve_alertar:
                print(f"   🚨 DISPARANDO ALERTA TELEGRAM! R$ {of.preco:,.2f} ({motivo})")
                msg_alerta = formatar_alerta_smartwatch(of, motivo, detalhes)
                sucesso = enviar_mensagem(msg_alerta)
                registrar_oferta(of, historico, foi_alertado=sucesso)
                time.sleep(2.0)
            else:
                registrar_oferta(of, historico, foi_alertado=False)

        time.sleep(1.0)

    salvar_historico(historico)

    print("\n" + "=" * 75)
    print(f"🏁 Varredura concluída! Total de {len(todas_ofertas)} ofertas avaliadas.")
    print("=" * 75)

    if enviar_resumo and melhores_por_modelo:
        print("📨 Enviando Ranking Resumo para o Telegram...")
        ranking_list = sorted(melhores_por_modelo.values(), key=lambda x: x.preco)
        msg_resumo = formatar_resumo_smartwatches(ranking_list)
        enviar_mensagem(msg_resumo)
        print("✅ Resumo enviado!")

    return todas_ofertas


def disparar_teste_alerta():
    print("Disparando Alerta de Teste para o Telegram...")
    msg_teste = (
        "🧪 <b>[TESTE TÉCNICO DE SISTEMA - NÃO É OFERTA REAL]</b>\n\n"
        "Esta mensagem é apenas um <b>teste de conectividade</b> para verificar se o seu Telegram está recebendo alertas.\n\n"
        "⌚ <b>Amazfit Balance 2 (46mm)</b> [SIMULAÇÃO]\n"
        "💰 Preço Simulado: R$ 1.099,00 (Exemplo)\n"
        "📉 Preço Normal: R$ 1.400,00\n"
        "🟢 Economia Simulada: R$ 301,00\n\n"
        "⚠️ <i>Atenção: Não há cupom nem oferta ativa neste momento. Ofertas reais serão enviadas com links diretos das lojas oficiais assim que o mercado registrar quedas reais de R$ 200.</i>"
    )
    ok = enviar_mensagem(msg_teste)
    if ok:
        print("✅ Alerta de teste enviado com sucesso no Telegram!")
    else:
        print("❌ Falha ao enviar alerta de teste.")


def exibir_status_historico():
    hist = carregar_historico()
    if not hist:
        print("Nenhum histórico registrado ainda.")
        return
    print("\n📊 HISTÓRICO DE SMARTWATCHES OBSERVADOS:")
    print("-" * 80)
    print(f"{'MODELO':<18} {'PREÇO':<12} {'MENOR HIST':<12} {'LOJA':<20} {'ATUALIZADO'}")
    print("-" * 80)
    for k, v in sorted(hist.items()):
        p = f"R$ {v.get('ultimo_preco', 0):,.2f}"
        m = f"R$ {v.get('menor_historico', 0):,.2f}"
        loja = v.get("loja", "-")[:18]
        dt = v.get("ultima_atualizacao", "-")[:16]
        mod = v.get("modelo_id", k)[:16]
        print(f"{mod:<18} {p:<12} {m:<12} {loja:<20} {dt}")
    print("-" * 80)


def main():
    parser = argparse.ArgumentParser(description="Monitor de Preços & Cupons de Smartwatches")
    parser.add_argument("--check", action="store_true", help="Executa varredura silenciosa e alerta sobre oportunidades")
    parser.add_argument("--resumo", action="store_true", help="Executa varredura e envia o resumo completo com ranking no Telegram")
    parser.add_argument("--status", action="store_true", help="Exibe o histórico de preços salvo localmente")
    parser.add_argument("--teste-alerta", action="store_true", help="Dispara um alerta de teste simulado para o Telegram")
    parser.add_argument("--modelo", type=str, default=None, help="Filtra a execução por um modelo específico")

    args = parser.parse_args()

    if args.teste_alerta:
        disparar_teste_alerta()
    elif args.status:
        exibir_status_historico()
    elif args.resumo:
        executar_varredura(enviar_resumo=True, apenas_modelo=args.modelo)
    else:
        executar_varredura(enviar_resumo=False, apenas_modelo=args.modelo)


if __name__ == "__main__":
    main()
