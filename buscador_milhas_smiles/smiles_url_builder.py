"""
Gerador de links profundos (Deep Links) de emissão com milhas na Smiles.
Gera o link exato para a interface moderna da Smiles (/mfe/emissao-passagem).
"""
import urllib.parse
from datetime import datetime
from typing import Optional


def date_to_epoch_ms(date_str: str) -> int:
    """
    Converte uma data 'YYYY-MM-DD' para timestamp epoch em milissegundos
    no fuso horário local/UTC respeitado pela Smiles.
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp() * 1000)


def build_smiles_url(
    origem: str,
    destino: str,
    data_ida: str,
    data_volta: Optional[str] = None,
    adultos: int = 1,
    cabine: str = "ALL",
    somente_congeneres: bool = False,
) -> str:
    """
    Gera a URL oficial de pesquisa e emissão com milhas da Smiles.
    Ao clicar neste link, o usuário é direcionado diretamente para a tela
    de seleção de voos da Smiles com a data e rota pré-preenchidas.
    """
    origem = origem.strip().upper()
    destino = destino.strip().upper()
    ts_ida = date_to_epoch_ms(data_ida)
    
    # Na Smiles: tripType "1" = ROUND_TRIP (Ida e Volta), "2" = ONE_WAY (Somente Ida)
    trip_type = "1" if data_volta else "2"
    ts_volta = str(date_to_epoch_ms(data_volta)) if data_volta else ""
    search_type = "congenere" if somente_congeneres else "both"

    params = {
        "adults": str(adultos),
        "cabin": cabine,
        "children": "0",
        "departureDate": str(ts_ida),
        "infants": "0",
        "isElegible": "false",
        "isFlexibleDateChecked": "false",
        "returnDate": ts_volta,
        "searchType": search_type,
        "segments": "2" if data_volta else "1",
        "tripType": trip_type,
        "originAirport": origem,
        "originCity": "",
        "originCountry": "",
        "originAirportIsAny": "false",
        "destinationAirport": destino,
        "destinCity": "",
        "destinCountry": "",
        "destinAirportIsAny": "false",
    }
    
    query = urllib.parse.urlencode(params)
    return f"https://www.smiles.com.br/mfe/emissao-passagem/?{query}"
