# -*- coding: utf-8 -*-
"""
sinais_ko_core.py — as regras do "Portfolio de Metodos em Validacao Forward" avaliadas sobre UMA
captura do coletor perto do KO (odd de LAY real, liquidez visivel). Mesmo codigo no servico da VPS (sinais_ko_vps.py,
ao vivo + Telegram) e no preenchimento historico (sinais_ko_backfill.py).

Portfólio Ativo (1X2):
1. Lay Draw (Fav <= 1.40) — Teto estrito: 4.50 <= Odd_Lay <= 7.00 (Mata-mata somente Mandante)
2. Lay Home/DC X2 (FavVis <= 1.65) — Teto estrito: 2.00 <= Odd_Lay <= 8.00

Filtros de Governanca de Ligas (GEMINI.md):
- Bloqueia Futebol Feminino ((W), Women, Frauen, WSL, etc.)
- Bloqueia Selecoes Nacionais e Torneios Internacionais (Nations League, World Cup, Friendlies, etc.)
- Bloqueia Divisoes Inferiores / Amadoras (3rd/4th Division, MSFL, Tercera, etc.)
- Em Copas / Mata-Mata: Lay Draw aceita SOMENTE Favorito em Casa (Odd_H <= 1.40); Favorito Fora e bloqueado.
"""
import re

JANELA_KO = (4.0, 16.0)          # minutos antes do KO em que a captura vale (coletor passa a cada ~5 min)
LIQ_MIN = 0.0                    # liquidez gravada, nao filtra (igual ao ledger das 06:00)

M_DRAW = "Lay Draw (Fav<=1.40)"
M_HOME = "Lay Home/DC X2 (FavVis<=1.65)"

# Constantes retrocompatíveis mantidas para evitar quebra de imports em scripts históricos
M_0X3, M_0X3_AMPLA, M_2X2, M_O45 = (
    "Lay 0x3 Top 3", "Lay 0x3 (Regra Ampla)", "Lay 2x2 Top 3", "Lay Over 4.5 (Under Pesado)"
)
BLACKLIST_2X2 = ("SERB", "IRISH", "IRELAND", "TURK", "SCOT")

# Regex de filtros de ligas e governança
RE_FEMININO = re.compile(
    r"(\b(WOMEN|FEMININ\w*|FEMENIN\w*|FRAUEN|LADIES|WSL|W-LEAGUE|DAMALLSVENSKAN|PREMIERE LIGUE|LIGA F|DE LA REINA)\b|\(W\))",
    re.IGNORECASE
)

RE_SELECAO = re.compile(
    r"\b(NATIONS LEAGUE|WORLD CUP|EUROCUP|EURO CUP|UEFA EURO|AMERICA CUP|COPA AMERICA|FRIENDLIES|FRIENDLY|INTERNATIONALS?|QUALIF\w*|AFCON|AFRICA CUP|ASIAN CUP|CONCACAF|U23|U21|U20|U19)\b",
    re.IGNORECASE
)

RE_COPA = re.compile(
    r"\b(CUP|COPA|CHAMPIONS LEAGUE|EUROPA LEAGUE|CONFERENCE LEAGUE|LIBERTADORES|SUDAMERICANA|POKAL|COUPE|COPPA|TAÇA|TACA|TROPHY|SHIELD|SUPERCUP|SUPER CUP|PLAYOFFS?)\b",
    re.IGNORECASE
)

RE_DIV_BAIXA = re.compile(
    r"\b(3RD DIVISION|4TH DIVISION|5TH DIVISION|3\. DIVISION|4\. DIVISION|TERCERA|MSFL|LEAGUE 3|LEAGUE THREE|LIGA 3|LIGA III|LIGA BET|LIGA ALEF|SECOND DIVISION B|MIZORAM|SHILLONG|BANGALORE|CHINESE LEAGUE 2|CHINA LEAGUE TWO)\b",
    re.IGNORECASE
)

RE_PERIFERICA = re.compile(
    r"\b(BOLIVIA\w*|PANAMA\w*|JAMAICA\w*|GUATEMALA\w*|SALVADOR\w*|NICARAGUA\w*|COSTA RICA\w*|HONDURAS\w*|PARAGUA\w*|PERU\w*|VENEZUELA\w*|ECUADOR\w*|"
    r"HONG KONG\w*|THAI\w*|INDONESIA\w*|INDIA\w*|SINGAPORE|CAMBODIA|BHUTAN|BANGLADESH|MONGOLIA|PHILIPPINES|VIETNAM|"
    r"KENYA\w*|TANZANIA\w*|RWANDA\w*|UGANDA\w*|NIGERIA\w*|GHANA\w*|ZAMBIA\w*|ZIMBABWE\w*|ETHIOPIA\w*|ALGERI\w*|EGYPT\w*|"
    r"FAROE|ESTONIA\w*|LATVIA\w*|LITHUANIA\w*|ARMENIA\w*|GEORGIA\w*|AZERBAIJAN\w*|KAZAKHSTAN\w*|UZBEKISTAN\w*|JORDAN\w*|KUWAIT\w*|OMAN\w*|LEBAN\w*|QATAR\w*|UAE\w*|SAUDI\w*|BAHRAIN\w*|CYPRUS|CYPRIOT\w*|MALTA|MALTESE\w*|MOLDOVA\w*|BELARUS\w*|KOSOVO\w*|ALBANIA\w*|MONTENEGR\w*|BOSNIA\w*|OTHER COMPETITIONS)\b",
    re.IGNORECASE
)


def _p(mk, mt, runner, lado):
    r = mk.get(mt, {}).get(runner)
    if not r: return None, 0.0
    v, sz = (r[0], r[1]) if lado == "back" else (r[2], r[3])
    return (v if (v is not None and v > 1.0) else None), (sz or 0.0)


def eh_jogo_ignorado(home, away, competicao):
    """Retorna True se o jogo pertencer a ligas/torneios fora do universo de governança."""
    txt = f"{home} {away} {competicao}"
    comp = str(competicao or "")
    if RE_FEMININO.search(txt):
        return True
    if RE_SELECAO.search(comp):
        return True
    if RE_DIV_BAIXA.search(comp):
        return True
    if RE_PERIFERICA.search(comp):
        return True
    return False


def candidatos(mk, home, away, competicao):
    """elegibilidade para o radar do dia."""
    if eh_jogo_ignorado(home, away, competicao):
        return {M_DRAW: None, M_HOME: None}

    a, _ = _p(mk, "MATCH_ODDS", away, "back"); h, _ = _p(mk, "MATCH_ODDS", home, "back")
    dl, _ = _p(mk, "MATCH_ODDS", "The Draw", "lay"); hl, _ = _p(mk, "MATCH_ODDS", home, "lay")
    fav = min(h or 99.0, a or 99.0)
    eh_copa_jogo = bool(RE_COPA.search(str(competicao or "")))

    c = {M_DRAW: None, M_HOME: None}
    # Teto validado: Lay Draw <= 7.00
    if h and a and dl and 4.5 <= dl <= 7.00:
        if (eh_copa_jogo and h <= 1.40) or (not eh_copa_jogo and fav <= 1.40):
            c[M_DRAW] = dl

    # Teto validado: Lay Home <= 8.00
    if a and a <= 1.65 and hl and 2.0 <= hl <= 8.00:
        c[M_HOME] = hl

    return c


def e_top3(odd, odds_dia):
    """odd esta entre as 3 menores do conjunto (que ja inclui a propria odd)?"""
    return odd in sorted(list(odds_dia))[:3]


def avaliar(mk, home, away, competicao, top3_dia=None):
    """Devolve lista de sinais [(metodo, odd_lay, liq, odd_fav)] para este jogo nesta captura.
    Portfólio oficial 1X2 estritamente dentro dos tetos lucrativos homologados."""
    if eh_jogo_ignorado(home, away, competicao):
        return []

    out = []
    h, _ = _p(mk, "MATCH_ODDS", home, "back"); a, _ = _p(mk, "MATCH_ODDS", away, "back")
    dl, dls = _p(mk, "MATCH_ODDS", "The Draw", "lay"); hl, hls = _p(mk, "MATCH_ODDS", home, "lay")
    fav = min(h or 99.0, a or 99.0)

    # 1. Lay Draw: min(H,A) <= 1.40 e 4.5 <= lay empate <= 7.00 (Teto Homologado)
    # Regra Estrutural de Copas (GEMINI.md): em COPAS, entra SOMENTE se o Super Fav for MANDANTE (H <= 1.40).
    eh_copa_jogo = bool(RE_COPA.search(str(competicao or "")))
    draw_ok = False
    if h and a and dl and 4.5 <= dl <= 7.00:
        if eh_copa_jogo:
            draw_ok = (h <= 1.40)
        else:
            draw_ok = (fav <= 1.40)
    if draw_ok:
        out.append((M_DRAW, dl, dls, fav))

    # 2. Lay Home em favorito visitante: A <= 1.65 e 2.0 <= lay mandante <= 8.00 (Teto Homologado)
    if a and a <= 1.65 and hl and 2.0 <= hl <= 8.00:
        out.append((M_HOME, hl, hls, a))

    return out
