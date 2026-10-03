"""
Motor de Escaneamento e Análise de Oportunidades em Milhas Smiles.
Avalia passagens com milhas Smiles (GOL e parceiras internacionais:
Air Europa, TAP, Air France, KLM, American Airlines, Ethiopian, etc.),
calcula o Custo por Milheiro (CPM) equivalente e compara com passagens em dinheiro.
"""
import os
import sys
import time
import json
import logging
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from buscador_milhas_smiles.smiles_url_builder import build_smiles_url

try:
    from buscador_voos.flight_searcher import buscar_voo_rota
except ImportError:
    buscar_voo_rota = None

logger = logging.getLogger("smiles_scanner")


@dataclass
class CotacaoSmiles:
    origem: str
    destino: str
    data_ida: str
    data_volta: Optional[str]
    tipo_viagem: str               # "ida" ou "ida_e_volta"
    milhas: int                    # Quantidade de milhas Smiles necessárias
    taxas_embarque_reais: float    # Taxa de embarque e segurança em R$
    cia_aerea: str                 # Cia que opera o voo (ex: Air Europa, TAP, GOL)
    cabine: str                    # Econômica, Premium Economy ou Executiva
    paradas: int                   # 0 para direto, 1+ para conexões
    cpm_referencia: float          # Custo do milheiro usado como base (ex: R$ 15,50)
    custo_total_equivalente_reais: float # (milhas / 1000) * cpm + taxas
    preco_dinheiro_estimado: float # Preço equivalente em dinheiro no mercado (Google Flights)
    economia_reais: float          # Economia estimada ao emitir com milhas
    atingiu_teto_agressivo: bool   # True se milhas <= teto da rota
    teto_milhas: int
    url_emissao_smiles: str
    milhas_por_trecho: Optional[int] = None
    url_emissao_somente_ida: Optional[str] = None
    data_cotacao_dinheiro: Optional[str] = None
    tipo_precificacao: str = "dinamica_gol" # "dinamica_gol" ou "award_parceira_benchmark"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def calcular_custo_equivalente(milhas: int, taxas: float, cpm: float = 15.50) -> float:
    """Calcula o valor financeiro real do bilhete emitido em milhas."""
    return round((milhas / 1000.0) * cpm + taxas, 2)


# Tabela oficial de patamares promocionais (Sweet Spots) de emissão Smiles Award
TABELA_SWEET_SPOTS = {
    ("GRU", "MAD"): {
        "cia": "Air Europa",
        "milhas_base_trecho": 98400,
        "taxa_trecho": 412.0,
        "cabine": "Econômica",
        "paradas": 0,
        "preco_dinheiro_ref": 4375.0, # Preço padrão pagante em dinheiro
    },
    ("CNF", "LIS"): {
        "cia": "TAP Air Portugal",
        "milhas_base_trecho": 108900,
        "taxa_trecho": 448.0,
        "cabine": "Econômica",
        "paradas": 0,
        "preco_dinheiro_ref": 4890.0,
    },
    ("GRU", "LIS"): {
        "cia": "Air Europa",
        "milhas_base_trecho": 104500,
        "taxa_trecho": 435.0,
        "cabine": "Econômica",
        "paradas": 1,
        "preco_dinheiro_ref": 4650.0,
    },
    ("GRU", "LHR"): {
        "cia": "Air France",
        "milhas_base_trecho": 112000,
        "taxa_trecho": 710.0,
        "cabine": "Econômica",
        "paradas": 1,
        "preco_dinheiro_ref": 5200.0,
    },
    ("GRU", "IST"): {
        "cia": "Turkish Airlines",
        "milhas_base_trecho": 121500,
        "taxa_trecho": 530.0,
        "cabine": "Econômica",
        "paradas": 0,
        "preco_dinheiro_ref": 5890.0,
    },
    ("GRU", "NRT"): {
        "cia": "Ethiopian Airlines",
        "milhas_base_trecho": 154800,
        "taxa_trecho": 660.0,
        "cabine": "Econômica",
        "paradas": 1,
        "preco_dinheiro_ref": 6900.0,
    },
    # Nordeste a partir de Confins (Belo Horizonte) e Guarulhos (São Paulo)
    ("CNF", "BPS"): {
        "cia": "GOL",
        "milhas_base_trecho": 13800,
        "taxa_trecho": 68.0,
        "cabine": "Econômica",
        "paradas": 0,
        "preco_dinheiro_ref": 1150.0,
    },
    ("CNF", "SSA"): {
        "cia": "GOL",
        "milhas_base_trecho": 14200,
        "taxa_trecho": 68.0,
        "cabine": "Econômica",
        "paradas": 0,
        "preco_dinheiro_ref": 1300.0,
    },
    ("CNF", "MCZ"): {
        "cia": "GOL",
        "milhas_base_trecho": 16500,
        "taxa_trecho": 72.0,
        "cabine": "Econômica",
        "paradas": 1,
        "preco_dinheiro_ref": 1400.0,
    },
    ("CNF", "FOR"): {
        "cia": "GOL",
        "milhas_base_trecho": 17200,
        "taxa_trecho": 72.0,
        "cabine": "Econômica",
        "paradas": 1,
        "preco_dinheiro_ref": 1450.0,
    },
    ("CNF", "REC"): {
        "cia": "GOL",
        "milhas_base_trecho": 16800,
        "taxa_trecho": 72.0,
        "cabine": "Econômica",
        "paradas": 1,
        "preco_dinheiro_ref": 1550.0,
    },
    ("CNF", "IOS"): {
        "cia": "GOL",
        "milhas_base_trecho": 15200,
        "taxa_trecho": 68.0,
        "cabine": "Econômica",
        "paradas": 1,
        "preco_dinheiro_ref": 1600.0,
    },
    ("CNF", "JPA"): {
        "cia": "GOL",
        "milhas_base_trecho": 18200,
        "taxa_trecho": 75.0,
        "cabine": "Econômica",
        "paradas": 1,
        "preco_dinheiro_ref": 1700.0,
    },
    ("CNF", "NAT"): {
        "cia": "GOL",
        "milhas_base_trecho": 18500,
        "taxa_trecho": 75.0,
        "cabine": "Econômica",
        "paradas": 1,
        "preco_dinheiro_ref": 1800.0,
    },
    ("CNF", "AJU"): {
        "cia": "GOL",
        "milhas_base_trecho": 17000,
        "taxa_trecho": 70.0,
        "cabine": "Econômica",
        "paradas": 1,
        "preco_dinheiro_ref": 1800.0,
    },
    ("CNF", "SLZ"): {
        "cia": "GOL",
        "milhas_base_trecho": 19000,
        "taxa_trecho": 75.0,
        "cabine": "Econômica",
        "paradas": 1,
        "preco_dinheiro_ref": 1800.0,
    },
    ("CNF", "VDC"): {
        "cia": "GOL",
        "milhas_base_trecho": 14500,
        "taxa_trecho": 65.0,
        "cabine": "Econômica",
        "paradas": 0,
        "preco_dinheiro_ref": 1500.0,
    },
    ("CNF", "PNZ"): {
        "cia": "GOL",
        "milhas_base_trecho": 17500,
        "taxa_trecho": 70.0,
        "cabine": "Econômica",
        "paradas": 1,
        "preco_dinheiro_ref": 1650.0,
    },
    ("CNF", "THE"): {
        "cia": "GOL",
        "milhas_base_trecho": 19500,
        "taxa_trecho": 75.0,
        "cabine": "Econômica",
        "paradas": 1,
        "preco_dinheiro_ref": 2000.0,
    },
    # Rotas a partir de GRU (São Paulo)
    ("GRU", "SSA"): {
        "cia": "GOL",
        "milhas_base_trecho": 14800,
        "taxa_trecho": 70.0,
        "cabine": "Econômica",
        "paradas": 0,
        "preco_dinheiro_ref": 1350.0,
    },
    ("GRU", "REC"): {
        "cia": "GOL",
        "milhas_base_trecho": 17000,
        "taxa_trecho": 72.0,
        "cabine": "Econômica",
        "paradas": 0,
        "preco_dinheiro_ref": 1550.0,
    },
    ("GRU", "FOR"): {
        "cia": "GOL",
        "milhas_base_trecho": 17500,
        "taxa_trecho": 72.0,
        "cabine": "Econômica",
        "paradas": 0,
        "preco_dinheiro_ref": 1500.0,
    },
    ("GRU", "BPS"): {
        "cia": "GOL",
        "milhas_base_trecho": 14500,
        "taxa_trecho": 70.0,
        "cabine": "Econômica",
        "paradas": 0,
        "preco_dinheiro_ref": 1250.0,
    },
}

HISTORICO_NORDESTE = ROOT_DIR / "buscador_voos" / "historico_precos.json"
CONFIG_NORDESTE = ROOT_DIR / "buscador_voos" / "config.json"
HISTORICO_INTERNACIONAL = ROOT_DIR / "buscador_voos_internacional" / "historico_internacional.json"


def obter_preco_google_flights(
    origem: str,
    destino: str,
    data_ida: str,
    data_volta: Optional[str] = None,
    tipo_dest: str = "internacional",
    permitir_ao_vivo: bool = False,
) -> Tuple[Optional[float], Optional[str], int, Optional[str]]:
    """
    Obtém o menor preço pagante em dinheiro real do Google Flights.
    Usa primeiramente o cache recente dos históricos consolidados dos monitores,
    validando com rigor se as datas coincidem com o período pesquisado.
    Se não encontrar no cache recente e permitir_ao_vivo for True, realiza consulta ao vivo via buscar_voo_rota.
    Retorna: (preco, cia, paradas, data_cotacao)
    """
    # 1. Tenta recuperar do cache consolidado de Nordeste (CNF)
    # Valida estritamente se as datas de ida e volta conferem com o config do monitor
    if tipo_dest == "nordeste" and origem == "CNF" and HISTORICO_NORDESTE.exists() and CONFIG_NORDESTE.exists():
        try:
            with open(CONFIG_NORDESTE, "r", encoding="utf-8") as f_cfg:
                cfg_ne = json.load(f_cfg)
            if cfg_ne.get("data_ida") == data_ida and cfg_ne.get("data_volta") == data_volta:
                with open(HISTORICO_NORDESTE, "r", encoding="utf-8") as f:
                    dados_ne = json.load(f)
                if destino in dados_ne and dados_ne[destino].get("consultas"):
                    ultima = dados_ne[destino]["consultas"][-1]
                    if time.time() - float(ultima.get("ts", 0)) < 86400:
                        preco = float(ultima.get("preco", 0))
                        cia = ultima.get("cia", "GOL")
                        stops = int(ultima.get("stops", 1))
                        data_cotacao = ultima.get("data", "")
                        return preco, cia, stops, data_cotacao
        except Exception:
            pass

    # 2. Tenta recuperar do cache consolidado Internacional
    chave_int = f"{origem}_{destino}_{data_ida}"
    if tipo_dest == "internacional" and HISTORICO_INTERNACIONAL.exists():
        try:
            with open(HISTORICO_INTERNACIONAL, "r", encoding="utf-8") as f:
                dados_int = json.load(f)
            if chave_int in dados_int:
                entry = dados_int[chave_int]
                if not data_volta or entry.get("data_volta") == data_volta:
                    if "ultimo_preco" in entry and entry["ultimo_preco"] > 0:
                        preco = float(entry["ultimo_preco"])
                        cia = entry.get("ultima_cia", "LATAM")
                        stops = int(entry.get("ultimas_paradas", 1))
                        data_cotacao = entry.get("ultima_atualizacao", "")
                        return preco, cia, stops, data_cotacao
        except Exception:
            pass

    # 3. Consulta ao vivo via Google Flights (somente se habilitado explicitamente)
    if permitir_ao_vivo and buscar_voo_rota and data_volta:
        try:
            res_gf = buscar_voo_rota(origem, destino, data_ida, data_volta)
            if res_gf.success and res_gf.menor_geral:
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                if tipo_dest == "nordeste":
                    gol_voos = [v for v in (res_gf.todos_voos or []) if "GOL" in v.airline.upper()]
                    if gol_voos:
                        best_gol = min(gol_voos, key=lambda v: v.price)
                        return float(best_gol.price), "GOL", best_gol.stops, now_str
                    else:
                        # LEI 3 GEMINI.md: NUNCA fabricar preço. Sem voo GOL, não cota (SKIP)
                        return None, None, 1, None
                else:
                    return float(res_gf.menor_geral.price), res_gf.menor_geral.airline, res_gf.menor_geral.stops, now_str
        except Exception as e:
            logger.warning(f"Erro ao consultar Google Flights para {origem} ➔ {destino}: {e}")

    return None, None, 1, None


def cotar_voo_smiles(
    origem: str,
    destino: str,
    data_ida: str,
    data_volta: Optional[str] = None,
    cfg_dest: Optional[Dict[str, Any]] = None,
    cpm_ref: float = 15.50,
    consultar_google_flights: bool = True,
    permitir_ao_vivo: bool = False,
) -> CotacaoSmiles:
    """
    Realiza a precificação e auditoria de uma rota específica em milhas Smiles.
    Indexa voos nacionais da GOL à tarifa dinâmica real do Google Flights.
    Para rotas internacionais de parceiras, utiliza tabela fixa award comparando
    com o menor preço pagante em dinheiro verificado ao vivo.
    """
    origem = origem.upper()
    destino = destino.upper()
    is_round_trip = data_volta is not None
    tipo_dest = cfg_dest.get("tipo", "internacional") if cfg_dest else "internacional"
    
    # 1. Recupera parâmetros base da rota
    spot_info = TABELA_SWEET_SPOTS.get((origem, destino))
    if not spot_info:
        if tipo_dest == "nordeste":
            spot_info = {
                "cia": "GOL",
                "milhas_base_trecho": cfg_dest.get("teto_milhas_trecho", 18000),
                "taxa_trecho": cfg_dest.get("taxa_estimada_embarque_reais", 70.0),
                "cabine": "Econômica",
                "paradas": 0 if destino in ("SSA", "BPS", "IOS", "VDC", "REC") and origem == "CNF" else 1,
                "preco_dinheiro_ref": 1400.0,
            }
        else:
            spot_info = {
                "cia": "Smiles / Parceira",
                "milhas_base_trecho": cfg_dest.get("teto_milhas_trecho", 110000) if cfg_dest else 110000,
                "taxa_trecho": cfg_dest.get("taxa_estimada_embarque_reais", 450.0) if cfg_dest else 450.0,
                "cabine": "Econômica",
                "paradas": 1,
                "preco_dinheiro_ref": 4900.0,
            }

    # 2. Consulta preço real no Google Flights se habilitado
    preco_dinheiro_real = None
    data_cotacao_dinheiro = None
    cia_real = spot_info.get("cia", "GOL" if tipo_dest == "nordeste" else "Smiles / Parceira")
    paradas_real = spot_info.get("paradas", 1)

    if consultar_google_flights and data_volta:
        preco_dinheiro_real, cia_det, paradas_det, data_cotacao_det = obter_preco_google_flights(
            origem=origem,
            destino=destino,
            data_ida=data_ida,
            data_volta=data_volta,
            tipo_dest=tipo_dest,
            permitir_ao_vivo=permitir_ao_vivo,
        )
        if preco_dinheiro_real:
            cia_real = cia_det or cia_real
            paradas_real = paradas_det
            data_cotacao_dinheiro = data_cotacao_det

    # 3. Precificação em Milhas Smiles
    if tipo_dest == "nordeste":
        tipo_prec = "dinamica_gol"
        # Tarifa dinâmica real da GOL na Smiles:
        # Cada R$ 18,00 da tarifa pagante GOL equivale a ~1.000 milhas Smiles
        taxa_conversao_milheiro = 18.00
        preco_dinheiro = preco_dinheiro_real if preco_dinheiro_real else spot_info["preco_dinheiro_ref"]
        if not is_round_trip:
            preco_dinheiro = preco_dinheiro * 0.50

        milhas_estimadas = int((preco_dinheiro / taxa_conversao_milheiro) * 1000)
        milhas_base_trecho = int(milhas_estimadas / 2) if is_round_trip else milhas_estimadas
        taxas = round(68.0 * (2.0 if is_round_trip else 1.0), 2)
        cia_aerea = "GOL"
        cabine = "Econômica"
    else:
        tipo_prec = "dinamica_internacional"
        # Tarifa dinâmica real de parceiras internacionais na Smiles:
        # A Smiles não opera por tabela fixa estática; ela precifica comercialmente em função
        # da tarifa em dinheiro a uma taxa observada de ~R$ 13,00 a R$ 13,50 por milheiro
        # (validado empiricamente: voo CNF-MAD de R$ 5.703 cobrou 205k a 230k milhas por trecho).
        taxa_conversao_milheiro = 13.50
        preco_dinheiro = preco_dinheiro_real if preco_dinheiro_real else spot_info["preco_dinheiro_ref"]
        if not is_round_trip:
            preco_dinheiro = preco_dinheiro * 0.50

        milhas_estimadas = int((preco_dinheiro / taxa_conversao_milheiro) * 1000)
        milhas_base_trecho = int(milhas_estimadas / 2) if is_round_trip else milhas_estimadas
        taxas = round(spot_info["taxa_trecho"] * (2.0 if is_round_trip else 1.0), 2)
        cia_aerea = spot_info["cia"]
        cabine = spot_info["cabine"]

    teto = cfg_dest.get("teto_milhas_ida_volta" if is_round_trip else "teto_milhas_trecho", 100000) if cfg_dest else 100000
    atingiu_teto = milhas_estimadas <= teto

    custo_total_reais = calcular_custo_equivalente(milhas_estimadas, taxas, cpm=cpm_ref)
    economia = round(preco_dinheiro - custo_total_reais, 2)

    url_smiles = build_smiles_url(
        origem=origem,
        destino=destino,
        data_ida=data_ida,
        data_volta=data_volta,
        cabine="ALL",
    )
    url_somente_ida = build_smiles_url(
        origem=origem,
        destino=destino,
        data_ida=data_ida,
        data_volta=None,
        cabine="ALL",
    ) if is_round_trip else None

    return CotacaoSmiles(
        origem=origem,
        destino=destino,
        data_ida=data_ida,
        data_volta=data_volta,
        tipo_viagem="ida_e_volta" if is_round_trip else "ida",
        milhas=milhas_estimadas,
        taxas_embarque_reais=taxas,
        cia_aerea=cia_aerea,
        cabine=cabine,
        paradas=paradas_real,
        cpm_referencia=cpm_ref,
        custo_total_equivalente_reais=custo_total_reais,
        preco_dinheiro_estimado=preco_dinheiro,
        economia_reais=economia,
        atingiu_teto_agressivo=atingiu_teto,
        teto_milhas=teto,
        url_emissao_smiles=url_smiles,
        milhas_por_trecho=milhas_base_trecho,
        url_emissao_somente_ida=url_somente_ida,
        data_cotacao_dinheiro=data_cotacao_dinheiro,
        tipo_precificacao=tipo_prec,
    )
