import re
import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict

from fast_flights.filter import TFSData
from fast_flights.flights_impl import FlightData, Passengers
from fast_flights.core import fetch
from selectolax.lexbor import LexborHTMLParser

logger = logging.getLogger("buscador_voos.searcher")


@dataclass
class VooInfo:
    airline: str
    dep_time: str
    arr_time: str
    duration: str
    stops: int
    price_str: str
    price: int
    is_best: bool


@dataclass
class ResultadoBusca:
    success: bool
    origem: str
    destino: str
    data_ida: str
    data_volta: str
    price_level: str  # "low", "typical", "high", "desconhecido"
    url_google_flights: str
    total_voos_encontrados: int
    menor_geral: Optional[VooInfo] = None
    menor_ate_1_parada: Optional[VooInfo] = None
    menor_direto: Optional[VooInfo] = None
    todos_voos: List[VooInfo] = None
    erro: Optional[str] = None


def extrair_inteiro_preco(texto: str) -> Optional[int]:
    if not texto:
        return None
    apenas_digitos = re.sub(r"[^\d]", "", texto)
    return int(apenas_digitos) if apenas_digitos else None


def buscar_voo_rota(
    origem: str,
    destino: str,
    data_ida: str,
    data_volta: str,
    adultos: int = 1,
    classe: str = "economy",
    max_tentativas: int = 3,
    pausa_tentativa_segundos: float = 1.5,
) -> ResultadoBusca:
    """
    Realiza a busca de voos ida e volta no Google Flights para o trecho especificado.
    Extrai voos diretos, voos de até 1 parada e o menor preço absoluto.
    """
    tfs = TFSData.from_interface(
        flight_data=[
            FlightData(date=data_ida, from_airport=origem, to_airport=destino),
            FlightData(date=data_volta, from_airport=destino, to_airport=origem),
        ],
        trip="round-trip",
        passengers=Passengers(adults=adultos),
        seat=classe,
    )

    tfs_b64 = tfs.as_b64().decode("utf-8")
    url_google = f"https://www.google.com/travel/flights?tfs={tfs_b64}&curr=BRL&hl=pt-BR"
    params = {
        "tfs": tfs_b64,
        "hl": "pt-BR",
        "tfu": "EgQIABABIgA",
        "curr": "BRL",
    }

    ultimo_erro = None

    for tentativa in range(1, max_tentativas + 1):
        try:
            res = fetch(params)
            parser = LexborHTMLParser(res.text)

            # Nível de preço fornecido pelo Google Flights ("baixo", "normal", "alto" / "low", "typical", "high")
            price_level = "normal"
            level_el = parser.css_first("span.gOatQ, span.b3nEcd")
            if level_el:
                txt = level_el.text().strip().lower()
                if "baixo" in txt or "low" in txt:
                    price_level = "baixo"
                elif "alto" in txt or "altos" in txt or "high" in txt:
                    price_level = "alto"
                elif "normal" in txt or "normais" in txt or "typical" in txt:
                    price_level = "normal"

            itens_voo: List[VooInfo] = []

            # Itera pelos blocos de voos (melhores voos e outros voos)
            blocos = parser.css("div[jsname='IWWDBc'], div[jsname='YdtKid']")
            # Se não encontrou divs com jsname específico, busca por ul.Rk10dc diretamente
            uls = parser.css("ul.Rk10dc")

            itens_html = []
            if blocos:
                for b_idx, bloco in enumerate(blocos):
                    is_best = (b_idx == 0)
                    for li in bloco.css("ul.Rk10dc li"):
                        itens_html.append((li, is_best))
            elif uls:
                for li in parser.css("ul.Rk10dc li"):
                    itens_html.append((li, False))

            for li, is_best in itens_html:
                # Preço
                p_el = li.css_first(".YMlIz.FpEdX, .g1V06b")
                if not p_el:
                    continue
                price_str = p_el.text().strip()
                price_val = extrair_inteiro_preco(price_str)
                if not price_val or price_val <= 0:
                    continue

                # Companhia aérea
                cia_el = li.css_first("div.sSHqwe.tPgKwe.ogfYpf span, span.h144Kc")
                airline = cia_el.text().strip() if cia_el else "Companhia Aérea"

                # Horários
                times = [t.text().strip() for t in li.css("span.mv1WYe div, div.wtdjmc")]
                dep_time = times[0] if len(times) > 0 else ""
                arr_time = times[1] if len(times) > 1 else ""

                # Duração
                dur_el = li.css_first("li div.Ak5kof div, div.gvkrdb")
                duration = dur_el.text().strip() if dur_el else ""

                # Paradas / Escalas
                stops_el = li.css_first(".BbR8Ec .ogfYpf, span.EfT7Ae")
                stops_txt = stops_el.text().strip().lower() if stops_el else ""
                if "sem escala" in stops_txt or "direto" in stops_txt or "nonstop" in stops_txt:
                    stops = 0
                else:
                    digitos = re.findall(r"\d+", stops_txt)
                    stops = int(digitos[0]) if digitos else 1

                itens_voo.append(
                    VooInfo(
                        airline=airline,
                        dep_time=dep_time,
                        arr_time=arr_time,
                        duration=duration,
                        stops=stops,
                        price_str=price_str,
                        price=price_val,
                        is_best=is_best,
                    )
                )

            if itens_voo:
                # Remove duplicatas por (airline, price, stops, dep_time)
                unicos = []
                chaves_vistas = set()
                for v in itens_voo:
                    chave = (v.airline, v.price, v.stops, v.dep_time)
                    if chave not in chaves_vistas:
                        chaves_vistas.add(chave)
                        unicos.append(v)

                unicos.sort(key=lambda x: x.price)

                menor_geral = unicos[0] if unicos else None
                voos_ate_1 = [v for v in unicos if v.stops <= 1]
                menor_ate_1 = voos_ate_1[0] if voos_ate_1 else None
                voos_diretos = [v for v in unicos if v.stops == 0]
                menor_direto = voos_diretos[0] if voos_diretos else None

                return ResultadoBusca(
                    success=True,
                    origem=origem,
                    destino=destino,
                    data_ida=data_ida,
                    data_volta=data_volta,
                    price_level=price_level,
                    url_google_flights=url_google,
                    total_voos_encontrados=len(unicos),
                    menor_geral=menor_geral,
                    menor_ate_1_parada=menor_ate_1,
                    menor_direto=menor_direto,
                    todos_voos=unicos[:10],
                )

            # Se não encontrou voos, dorme um pouco e tenta novamente
            time.sleep(pausa_tentativa_segundos)

        except Exception as e:
            ultimo_erro = str(e)
            time.sleep(pausa_tentativa_segundos)

    return ResultadoBusca(
        success=False,
        origem=origem,
        destino=destino,
        data_ida=data_ida,
        data_volta=data_volta,
        price_level="desconhecido",
        url_google_flights=url_google,
        total_voos_encontrados=0,
        erro=ultimo_erro or "Nenhum voo retornado pelo Google Flights",
    )
