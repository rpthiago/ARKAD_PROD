"""
Scanner Comparador Multiloja (Buscapé).
Compara ofertas em tempo real em Amazon, Mercado Livre, KaBuM, Magalu e outras redes.
"""
import re
import logging
from typing import List, Dict, Any, Optional
import primp
from bs4 import BeautifulSoup
from buscador_smartwatch.pelando_scanner import OfertaSmartwatch, e_produto_valido

logger = logging.getLogger("smartwatch.buscape")


def buscar_ofertas_buscape(
    cfg_modelo: Dict[str, Any],
    palavras_negativas: List[str],
    client: Optional[primp.Client] = None
) -> List[OfertaSmartwatch]:
    if client is None:
        client = primp.Client(impersonate="chrome_131")

    query = cfg_modelo.get("query_busca", "")
    termos_obrigatorios = cfg_modelo.get("termos_obrigatorios", [])
    termos_negativos = cfg_modelo.get("termos_negativos", [])
    preco_ref = cfg_modelo.get("preco_normal_referencia", 1200.0)

    url = f"https://www.buscape.com.br/search?q={query.replace(' ', '%20')}"
    ofertas: List[OfertaSmartwatch] = []

    try:
        r = client.get(url)
        if r.status_code != 200:
            logger.warning(f"Buscapé retornou status {r.status_code} para '{query}'")
            return ofertas

        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.find_all("a", href=True):
            title_el = a.find("h2")
            if not title_el:
                continue

            titulo = title_el.get_text(strip=True)
            if not e_produto_valido(titulo, termos_obrigatorios, palavras_negativas, termos_negativos):
                continue

            # Procura preços em R$ ignorando parcelas ("10x de R$ ...")
            precos_candidatos = []
            for s in a.find_all(string=re.compile(r"R\$\s*[\d\.,]+")):
                s_str = s.strip()
                if re.search(r"\d+x\s*de", s_str, re.IGNORECASE) or "x de" in s_str.lower():
                    continue
                m = re.search(r"R\$\s*([\d\.]+,\d{2}|\d+)", s_str)
                if m:
                    clean_p = m.group(1).replace(".", "").replace(",", ".")
                    try:
                        pv = float(clean_p)
                        if pv >= 250.0:
                            precos_candidatos.append(pv)
                    except ValueError:
                        pass

            if not precos_candidatos:
                continue

            preco_val = precos_candidatos[0]
            preco_min = cfg_modelo.get("preco_minimo_filtro", 350.0)
            if preco_val < preco_min:
                continue

            href = a.get("href", "")
            link = href if href.startswith("http") else f"https://www.buscape.com.br{href}"

            economia = round(preco_ref - preco_val, 2)
            desc_pct = round((economia / preco_ref) * 100, 1) if preco_ref > 0 else 0.0

            if not any(o.link == link for o in ofertas):
                ofertas.append(OfertaSmartwatch(
                    modelo_id=cfg_modelo["id"],
                    modelo_nome=cfg_modelo["modelo"],
                    marca=cfg_modelo["marca"],
                    tamanho_caixa=cfg_modelo["tamanho_caixa"],
                    emoji=cfg_modelo.get("emoji", "⌚"),
                    titulo=titulo,
                    preco=preco_val,
                    preco_normal=preco_ref,
                    economia=economia,
                    desconto_pct=desc_pct,
                    loja="Multilojas (Amazon / ML / KaBuM / Magalu)",
                    cupom=None,
                    link=link,
                    origem="Buscapé (Comparador)",
                    ativo=True
                ))

    except Exception as e:
        logger.error(f"Erro ao buscar Buscapé para '{query}': {e}")

    return ofertas
