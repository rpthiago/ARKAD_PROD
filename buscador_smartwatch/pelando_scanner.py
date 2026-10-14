"""
Scanner de Promoções Quentes e Cupons de Smartwatches no Pelando.
Detecta ofertas ativas com cupom de desconto, moedas ou preços promocionais.
"""
import re
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import primp
from bs4 import BeautifulSoup

logger = logging.getLogger("smartwatch.pelando")


@dataclass
class OfertaSmartwatch:
    modelo_id: str
    modelo_nome: str
    marca: str
    tamanho_caixa: str
    emoji: str
    titulo: str
    preco: float
    preco_normal: float
    economia: float
    desconto_pct: float
    loja: str
    cupom: Optional[str]
    link: str
    origem: str
    ativo: bool
    data_postagem: Optional[str] = None


def contem_termo(texto: str, termo: str) -> bool:
    """Verifica se termo está no texto usando limite de palavra para termos curtos/numéricos."""
    t_clean = texto.lower()
    termo_clean = str(termo).strip().lower()
    if not termo_clean:
        return False
    # Termo curto (<= 3 caracteres) ou dígito: exige limite de palavra estrito \b
    if len(termo_clean) <= 3 or termo_clean.isdigit():
        return bool(re.search(rf"\b{re.escape(termo_clean)}\b", t_clean))
    return termo_clean in t_clean


def e_produto_valido(
    titulo: str,
    termos_obrigatorios: List[Any],
    palavras_negativas: List[str],
    termos_negativos_modelo: Optional[List[str]] = None
) -> bool:
    t_lower = titulo.lower()

    # 1. Descarta se tiver qualquer palavra negativa global (acessórios, peças, usados)
    for neg in palavras_negativas:
        if contem_termo(t_lower, neg):
            return False

    # 2. Descarta se tiver qualquer termo negativo específico do modelo (ex: balance 2 para o balance 3)
    if termos_negativos_modelo:
        for n_mod in termos_negativos_modelo:
            if contem_termo(t_lower, n_mod):
                return False

    # 3. Exige os termos do modelo (suporta string ou lista de sinônimos/grafias)
    for ob in termos_obrigatorios:
        if isinstance(ob, (list, tuple)):
            if not any(contem_termo(t_lower, str(sub)) for sub in ob):
                return False
        else:
            if not contem_termo(t_lower, str(ob)):
                return False

    return True



def buscar_ofertas_pelando(
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

    url = f"https://www.pelando.com.br/busca/{query.replace(' ', '%20')}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Accept-Language": "pt-BR,pt;q=0.9",
    }

    ofertas: List[OfertaSmartwatch] = []
    try:
        r = client.get(url, headers=headers)
        if r.status_code != 200:
            logger.warning(f"Pelando retornou status {r.status_code} para '{query}'")
            return ofertas

        soup = BeautifulSoup(r.text, "html.parser")

        vistos_links = set()
        # No Astro Pelando, links de promoções têm /d/<slug>
        for a in soup.find_all("a", href=re.compile(r"/d/[a-zA-Z0-9_-]+")):
            href = a.get("href", "")
            clean_href = href.split("?")[0]
            if clean_href in vistos_links:
                continue

            # Sobe na árvore buscando o container raiz do card (_deal-card_ ou similar)
            curr = a
            card_el = None
            while curr.parent and curr.parent.name not in ["body", "html", "[document]"]:
                curr = curr.parent
                classes = curr.get("class", [])
                if any(c.startswith("_deal-card_") or c.startswith("deal-card") for c in classes):
                    card_el = curr
                    break

            if not card_el:
                card_el = a.find_parent("article") or a.find_parent("div", class_=re.compile(r"sc-|item|feed")) or a.find_parent("div")
            if not card_el:
                continue

            vistos_links.add(clean_href)
            card_text = card_el.get_text(" ", strip=True)
            if not card_text:
                continue

            # Se expirada, pula
            if "Expirada" in card_text or "expirada" in card_text:
                continue

            # Título do anúncio
            h_tag = card_el.find(["h2", "h3", "h4"])
            titulo = h_tag.get_text(strip=True) if h_tag else a.get_text(strip=True)
            if not titulo or len(titulo) < 5:
                titulo = a.get_text(strip=True)
            if not titulo or len(titulo) < 5:
                continue

            if not e_produto_valido(titulo, termos_obrigatorios, palavras_negativas, termos_negativos):
                continue

            # Link completo
            link = f"https://www.pelando.com.br{clean_href}" if not clean_href.startswith("http") else clean_href

            # Preço no card (procura padrão R$ 000,00 ou R$000)
            preco_val = None
            price_matches = re.findall(r"R\$\s*([\d\.]+,\d{2}|\d+)", card_text)
            if not price_matches:
                price_matches = re.findall(r"R\$\s*\|\s*([\d\.]+,\d{2}|\d+)", card_text)

            if price_matches:
                for pm in price_matches:
                    clean = pm.replace(".", "").replace(",", ".")
                    try:
                        pv = float(clean)
                        preco_min = cfg_modelo.get("preco_minimo_filtro", 350.0)
                        if preco_min <= pv <= 4500.0: # Faixa realista de smartwatch novo
                            preco_val = pv
                            break
                    except ValueError:
                        pass

            # Loja
            loja = "Loja Online"
            for l_cand in ["Amazon", "Mercado Livre", "AliExpress", "KaBuM!", "Magalu", "Shopee", "Fast Shop", "Girafa"]:
                if l_cand.lower() in card_text.lower():
                    loja = l_cand
                    break

            # Cupom (se houver menção)
            cupom = None
            coupon_match = re.search(r"(?:cupom|código|code):\s*([A-Za-z0-9_-]{4,20})", card_text, re.IGNORECASE)
            if coupon_match:
                cupom = coupon_match.group(1).upper()
            elif "cupom" in card_text.lower():
                cupom = "Disponível no link"

            if preco_val:
                economia = round(preco_ref - preco_val, 2)
                desc_pct = round((economia / preco_ref) * 100, 1) if preco_ref > 0 else 0.0

                # Evita duplicatas pelo link
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
                        loja=loja,
                        cupom=cupom,
                        link=link,
                        origem="Pelando (Comunidade)",
                        ativo=True
                    ))

    except Exception as e:
        logger.error(f"Erro ao buscar Pelando para '{query}': {e}")

    return ofertas
