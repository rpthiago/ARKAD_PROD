import re
import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from fast_flights.filter import TFSData
from fast_flights.flights_impl import FlightData, Passengers
from fast_flights.core import fetch
from selectolax.lexbor import LexborHTMLParser

logger = logging.getLogger("buscador_internacional.scanner")


@dataclass
class VooInternacional:
    airline: str
    dep_time: str
    arr_time: str
    duration: str
    stops: int
    price_str: str
    price: int
    is_best: bool


@dataclass
class CotacaoRota:
    success: bool
    origem: str
    destino: str
    data_ida: str
    data_volta: str
    price_level: str  # "baixo", "normal", "alto", "desconhecido"
    url_google_flights: str
    menor_voo: Optional[VooInternacional] = None
    menor_direto: Optional[VooInternacional] = None
    todos_voos: List[VooInternacional] = None
    erro: Optional[str] = None


def extrair_inteiro_preco(texto: str) -> Optional[int]:
    if not texto:
        return None
    apenas_digitos = re.sub(r"[^\d]", "", texto)
    return int(apenas_digitos) if apenas_digitos else None


def consultar_voo_internacional(
    origem: str,
    destino: str,
    data_ida: str,
    data_volta: str,
    adultos: int = 1,
    classe: str = "economy",
    max_tentativas: int = 3,
    pausa_segundos: float = 1.5,
) -> CotacaoRota:
    """
    Consulta voos internacionais ida e volta no Google Flights.
    Permite conexões/escalas e identifica tanto o menor preço absoluto quanto o voo direto (se houver).
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

            itens_voo: List[VooInternacional] = []

            blocos = parser.css("div[jsname='IWWDBc'], div[jsname='YdtKid']")
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
                p_el = li.css_first(".YMlIz.FpEdX, .g1V06b")
                if not p_el:
                    continue
                price_str = p_el.text().strip()
                price_val = extrair_inteiro_preco(price_str)
                if not price_val or price_val <= 0:
                    continue

                cia_el = li.css_first("div.sSHqwe.tPgKwe.ogfYpf span, span.h144Kc")
                airline = cia_el.text().strip() if cia_el else "Companhia Aérea"

                times = [t.text().strip() for t in li.css("span.mv1WYe div, div.wtdjmc")]
                dep_time = times[0] if len(times) > 0 else ""
                arr_time = times[1] if len(times) > 1 else ""

                dur_el = li.css_first("li div.Ak5kof div, div.gvkrdb")
                duration = dur_el.text().strip() if dur_el else ""

                stops_el = li.css_first(".BbR8Ec .ogfYpf, span.EfT7Ae")
                stops_txt = stops_el.text().strip().lower() if stops_el else ""
                if "sem escala" in stops_txt or "direto" in stops_txt or "nonstop" in stops_txt:
                    stops = 0
                else:
                    digitos = re.findall(r"\d+", stops_txt)
                    stops = int(digitos[0]) if digitos else 1

                itens_voo.append(
                    VooInternacional(
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
                unicos = []
                chaves_vistas = set()
                for v in itens_voo:
                    chave = (v.airline, v.price, v.stops, v.dep_time)
                    if chave not in chaves_vistas:
                        chaves_vistas.add(chave)
                        unicos.append(v)

                unicos.sort(key=lambda x: x.price)
                menor_voo = unicos[0] if unicos else None
                voos_diretos = [v for v in unicos if v.stops == 0]
                menor_direto = voos_diretos[0] if voos_diretos else None

                return CotacaoRota(
                    success=True,
                    origem=origem,
                    destino=destino,
                    data_ida=data_ida,
                    data_volta=data_volta,
                    price_level=price_level,
                    url_google_flights=url_google,
                    menor_voo=menor_voo,
                    menor_direto=menor_direto,
                    todos_voos=unicos[:5],
                )

            time.sleep(pausa_segundos)

        except Exception as e:
            ultimo_erro = str(e)
            time.sleep(pausa_segundos)

    return CotacaoRota(
        success=False,
        origem=origem,
        destino=destino,
        data_ida=data_ida,
        data_volta=data_volta,
        price_level="desconhecido",
        url_google_flights=url_google,
        erro=ultimo_erro or "Nenhum voo retornado pelo Google Flights",
    )
