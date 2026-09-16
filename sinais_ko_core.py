# -*- coding: utf-8 -*-
"""
sinais_ko_core.py — as 5 regras do "Portfolio de Metodos em Validacao Forward" (+ 0x3 Ampla) avaliadas sobre UMA
captura do coletor perto do KO (odd de LAY real, liquidez visivel). Mesmo codigo no servico da VPS (sinais_ko_vps.py,
ao vivo + Telegram) e no preenchimento historico (sinais_ko_backfill.py). Regras copiadas de
relatorio_forward_5metodos.sinais_do_dia / estrategia_lay_0x3 / estrategia_lay_2x2 — nao reinterpretadas.

Entrada: mk[market_type][runner] = (back, back_size, lay, lay_size)  (so o que o coletor gravou; sem default).
TOP 3 (0x3 e 2x2): "3 menores odds do dia" nao existe no KO; aqui = entre os elegiveis do dia ja avaliados ate este
KO (inclusive), a odd deste jogo esta entre as 3 menores. Registrado como definicao do ledger do KO.
"""
JANELA_KO = (4.0, 16.0)          # minutos antes do KO em que a captura vale (coletor passa a cada ~5 min)
LIQ_MIN = 0.0                    # liquidez gravada, nao filtra (igual ao ledger das 06:00)
M_0X3, M_0X3_AMPLA, M_2X2, M_DRAW, M_HOME, M_O45 = ("Lay 0x3 Top 3", "Lay 0x3 (Regra Ampla)", "Lay 2x2 Top 3",
                                                    "Lay Draw (Fav<=1.40)", "Lay Home/DC X2 (FavVis<=1.65)", "Lay Over 4.5 (Under Pesado)")
BLACKLIST_2X2 = ("SERB", "IRISH", "IRELAND", "TURK", "SCOT")   # = ['SERBIA','IRELAND','TURKEY','SCOTLAND'] de estrategia_lay_2x2, nos nomes da Betfair


def _p(mk, mt, runner, lado):
    r = mk.get(mt, {}).get(runner)
    if not r: return None, 0.0
    v, sz = (r[0], r[1]) if lado == "back" else (r[2], r[3])
    return (v if (v is not None and v > 1.0) else None), (sz or 0.0)


def avaliar(mk, home, away, competicao, top3_dia):
    """Devolve lista de sinais [(metodo, odd_lay, liq, odd_fav)] para este jogo nesta captura.
    top3_dia: dict metodo -> lista de odds dos elegiveis do dia ja vistos (mutado aqui)."""
    out = []
    h, _ = _p(mk, "MATCH_ODDS", home, "back"); a, _ = _p(mk, "MATCH_ODDS", away, "back")
    dl, dls = _p(mk, "MATCH_ODDS", "The Draw", "lay"); hl, hls = _p(mk, "MATCH_ODDS", home, "lay")
    u25, _ = _p(mk, "OVER_UNDER_25", "Under 2.5 Goals", "back")
    o45l, o45s = _p(mk, "OVER_UNDER_45", "Over 4.5 Goals", "lay")
    l03, l03s = _p(mk, "CORRECT_SCORE", "0 - 3", "lay"); l22, l22s = _p(mk, "CORRECT_SCORE", "2 - 2", "lay")
    fav = min(h or 99.0, a or 99.0)
    # 3. Lay Draw: min(H,A) <= 1.40 e 4.5 <= lay empate <= 10
    if h and a and fav <= 1.40 and dl and 4.5 <= dl <= 10.0:
        out.append((M_DRAW, dl, dls, fav))
    # 4. Lay Home em favorito visitante: A <= 1.65 e 2.0 <= lay mandante <= 10
    if a and a <= 1.65 and hl and 2.0 <= hl <= 10.0:
        out.append((M_HOME, hl, hls, a))
    # 5. Lay Over 4.5 em jogo under: Under 2.5 back <= 1.50 e 4.0 <= lay Over 4.5 <= 20
    if u25 and u25 <= 1.50 and o45l and 4.0 <= o45l <= 20.0:
        out.append((M_O45, o45l, o45s, u25))
    # 1. Lay 0x3: Under 2.5 back <= 2.10, lay 0-3 em 14-35, visitante >= 1.85 (ou sem odd)
    if u25 and u25 <= 2.10 and l03 and 14.0 <= l03 <= 35.0 and (a is None or a >= 1.85):
        out.append((M_0X3_AMPLA, l03, l03s, u25))
        lst = top3_dia.setdefault(M_0X3, []); lst.append(l03)
        if l03 in sorted(lst)[:3]:
            out.append((M_0X3, l03, l03s, u25))
    # 2. Lay 2x2: lay 2-2 em 8-20, tendencia (Under 2.5 <= 2.00 ou H <= 1.55 ou A <= 1.60), fora da blacklist
    if l22 and 8.0 <= l22 <= 20.0 and ((u25 and u25 <= 2.00) or (h and h <= 1.55) or (a and a <= 1.60)):
        comp = str(competicao or "").upper()
        if not any(b in comp for b in BLACKLIST_2X2):
            lst = top3_dia.setdefault(M_2X2, []); lst.append(l22)
            if l22 in sorted(lst)[:3]:
                out.append((M_2X2, l22, l22s, fav))
    return out
