# -*- coding: utf-8 -*-
"""
sinais_ko_core.py — as regras do "Portfolio de Metodos em Validacao Forward" avaliadas sobre UMA
captura do coletor perto do KO (odd de LAY real, liquidez visivel). Mesmo codigo no servico da VPS (sinais_ko_vps.py,
ao vivo + Telegram) e no preenchimento historico (sinais_ko_backfill.py).

Filtros de Governanca de Ligas (GEMINI.md):
- Bloqueia Futebol Feminino ((W), Women, Frauen, WSL, etc.)
- Bloqueia Selecoes Nacionais e Torneios Internacionais (Nations League, World Cup, Friendlies, etc.)
- Bloqueia Divisoes Inferiores / Amadoras (3rd/4th Division, MSFL, Tercera, etc.)
- Em Copas / Mata-Mata: Lay Draw aceita SOMENTE Favorito em Casa (Odd_H <= 1.40); Favorito Fora e bloqueado.
"""
import re

JANELA_KO = (4.0, 16.0)          # minutos antes do KO em que a captura vale (coletor passa a cada ~5 min)
LIQ_MIN = 0.0                    # liquidez gravada, nao filtra (igual ao ledger das 06:00)
M_0X3, M_0X3_AMPLA, M_2X2, M_DRAW, M_HOME, M_O45 = (
    "Lay 0x3 Top 3", "Lay 0x3 (Regra Ampla)", "Lay 2x2 Top 3",
    "Lay Draw (Fav<=1.40)", "Lay Home/DC X2 (FavVis<=1.65)", "Lay Over 4.5 (Under Pesado)"
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
    r"\b(3RD DIVISION|4TH DIVISION|5TH DIVISION|3\. DIVISION|4\. DIVISION|TERCERA|MSFL|LEAGUE 3|LEAGUE THREE|LIGA 3|LIGA III|LIGA BET|LIGA ALEF|SECOND DIVISION B|MIZORAM|SHILLONG|BANGALORE)\b",
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
    return False


def candidatos(mk, home, away, competicao):
    """elegibilidade SEM ranking: {M_0X3: odd | None, M_2X2: odd | None} (para o conjunto do dia)."""
    if eh_jogo_ignorado(home, away, competicao):
        return {M_0X3: None, M_2X2: None}

    a, _ = _p(mk, "MATCH_ODDS", away, "back"); h, _ = _p(mk, "MATCH_ODDS", home, "back")
    u25, _ = _p(mk, "OVER_UNDER_25", "Under 2.5 Goals", "back")
    l03, _ = _p(mk, "CORRECT_SCORE", "0 - 3", "lay"); l22, _ = _p(mk, "CORRECT_SCORE", "2 - 2", "lay")
    c = {M_0X3: None, M_2X2: None}
    # 0x3 desativado a pedido do usuario em 03/10/2026
    # if u25 and u25 <= 2.10 and l03 and 14.0 <= l03 <= 35.0 and (a is None or a >= 1.85): c[M_0X3] = l03
    if l22 and 8.0 <= l22 <= 20.0 and ((u25 and u25 <= 2.00) or (h and h <= 1.55) or (a and a <= 1.60)):
        if not any(b in str(competicao or "").upper() for b in BLACKLIST_2X2): c[M_2X2] = l22
    return c


def e_top3(odd, odds_dia):
    """odd esta entre as 3 menores do conjunto (que ja inclui a propria odd)?"""
    return odd in sorted(list(odds_dia))[:3]


def avaliar(mk, home, away, competicao, top3_dia):
    """Devolve lista de sinais [(metodo, odd_lay, liq, odd_fav)] para este jogo nesta captura.
    top3_dia: dict metodo -> lista de odds do CONJUNTO do dia local (avaliados + proximos); a odd deste jogo e
    acrescentada aqui antes do teste."""
    # 1. Filtro estrito de governança (Feminino, Seleções, Divisões periféricas)
    if eh_jogo_ignorado(home, away, competicao):
        return []

    out = []
    h, _ = _p(mk, "MATCH_ODDS", home, "back"); a, _ = _p(mk, "MATCH_ODDS", away, "back")
    dl, dls = _p(mk, "MATCH_ODDS", "The Draw", "lay"); hl, hls = _p(mk, "MATCH_ODDS", home, "lay")
    u25, _ = _p(mk, "OVER_UNDER_25", "Under 2.5 Goals", "back")
    o45l, o45s = _p(mk, "OVER_UNDER_45", "Over 4.5 Goals", "lay")
    l03, l03s = _p(mk, "CORRECT_SCORE", "0 - 3", "lay"); l22, l22s = _p(mk, "CORRECT_SCORE", "2 - 2", "lay")
    fav = min(h or 99.0, a or 99.0)

    # 3. Lay Draw: min(H,A) <= 1.40 e 4.5 <= lay empate <= 10
    # Regra Estrutural de Copas (GEMINI.md): em COPAS, entra SOMENTE se o Super Fav for MANDANTE (H <= 1.40).
    # Se o Super Fav for VISITANTE (A <= 1.40) em Copas, BLOQUEIA (taxa de empate salta para 20.7%, ROI -8.64%).
    eh_copa_jogo = bool(RE_COPA.search(str(competicao or "")))
    draw_ok = False
    if h and a and dl and 4.5 <= dl <= 10.0:
        if eh_copa_jogo:
            draw_ok = (h <= 1.40)
        else:
            draw_ok = (fav <= 1.40)
    if draw_ok:
        out.append((M_DRAW, dl, dls, fav))

    # 4. Lay Home em favorito visitante: A <= 1.65 e 2.0 <= lay mandante <= 10
    if a and a <= 1.65 and hl and 2.0 <= hl <= 10.0:
        out.append((M_HOME, hl, hls, a))

    # 5. Lay Over 4.5 em jogo under: Under 2.5 back <= 1.50 e 4.0 <= lay Over 4.5 <= 20
    if u25 and u25 <= 1.50 and o45l and 4.0 <= o45l <= 20.0:
        out.append((M_O45, o45l, o45s, u25))

    # 1. Lay 0x3: DESATIVADO DEFINITIVAMENTE a pedido do usuario em 03/10/2026 (cauda gorda / risco desnecessario)
    # if u25 and u25 <= 2.10 and l03 and 14.0 <= l03 <= 35.0 and (a is None or a >= 1.85):
    #     out.append((M_0X3_AMPLA, l03, l03s, u25))
    #     lst = top3_dia.setdefault(M_0X3, []); lst.append(l03)
    #     if l03 in sorted(lst)[:3]:
    #         out.append((M_0X3, l03, l03s, u25))

    # 2. Lay 2x2: mantido temporariamente a pedido do usuario ("o metodo pode deixar por enquanto")
    if l22 and 8.0 <= l22 <= 20.0 and ((u25 and u25 <= 2.00) or (h and h <= 1.55) or (a and a <= 1.60)):
        comp = str(competicao or "").upper()
        if not any(b in comp for b in BLACKLIST_2X2):
            lst = top3_dia.setdefault(M_2X2, []); lst.append(l22)
            if l22 in sorted(lst)[:3]:
                out.append((M_2X2, l22, l22s, fav))
    return out
