# -*- coding: utf-8 -*-
"""
trader_inplay_core.py — núcleo dos métodos trader in-play (PREREGISTRO_SUITE_TRADER_INPLAY.md + emenda 16/09).
O MESMO código roda (a) ao vivo, sobre o coletor da VPS (tracker_trader_inplay.py) e (b) no histórico
(trader_inplay_olhar.py). Lei 2: backtest e live são o mesmo objeto.

Entrada: capturas de UM jogo, em ordem de tempo. Cada captura = (ts, minuto, mk) onde
  mk[market_type][runner] = (back, back_size, lay, lay_size)   — só o que o coletor gravou; sem default.
Fatos usados (nunca previsão):
  - gols já saídos: pelas linhas Over batidas (Over L <= 1,02) ou sumidas depois de vistas (gols_por_ou).
  - quem marcou: pelos runners de Correct Score que a Betfair REMOVE quando ficam impossíveis
    (1 gol: "0 - 1" ausente e "1 - 0" presente => mandante marcou; 2 gols: idem com "0 - 2"/"1 - 1"/"2 - 0").
  - odd de saída: a da PRIMEIRA captura depois do evento de saída em que o runner tem preço. Nunca estimada.
P&L (stake-zero, unidade = 1u de risco): lay fechado em back: S_in = 1/(O_in-1); pnl = S_in*(1 - O_in/O_out).
back fechado em lay: pnl = O_in/O_out - 1. Comissão 5% sobre lucro positivo.
"""
COMISSAO = 0.05
LINHAS = {"OVER_UNDER_05": 0.5, "OVER_UNDER_15": 1.5, "OVER_UNDER_25": 2.5, "OVER_UNDER_35": 3.5}

# ---- regras congeladas (emenda 16/09: só o que o coletor mede) ----
M1 = dict(id="M1_LTD", nome="LTD Trader", jan=(15, 25), fav_max=1.45, odd=(3.00, 4.20), liq=200, stop_min=68, obs_zebra_min=5)
M2 = dict(id="M2_SWING_FAV", nome="Swing Fav em Desvantagem", jan=(20, 45), fav_max=1.35, odd=(2.10, 3.20), liq=200, stop_min=70)
M3 = dict(id="M3_SCALP_U25", nome="Scalping Under 2.5 (55-62')", jan=(55, 62), gols_max=1, odd=(1.01, 20.0), liq=200, tp_ticks=3, cap_min=8)
METODOS = (M1, M2, M3)


def tick(o):
    """incremento da escada de preços da Betfair na odd o."""
    if o < 2: return 0.01
    if o < 3: return 0.02
    if o < 4: return 0.05
    if o < 6: return 0.1
    if o < 10: return 0.2
    if o < 20: return 0.5
    if o < 30: return 1.0
    if o < 50: return 2.0
    if o < 100: return 5.0
    return 10.0


def menos_ticks(o, n):
    for _ in range(n):
        o = round(o - tick(o - 1e-9), 2)
    return o


def pnl_lay_fechado(o_in, o_out):
    s_in = 1.0 / (o_in - 1.0); p = s_in * (1.0 - o_in / o_out)
    return p * (1 - COMISSAO) if p > 0 else p


def pnl_back_fechado(o_in, o_out):
    p = o_in / o_out - 1.0
    return p * (1 - COMISSAO) if p > 0 else p


def _preco(mk, mtype, runner, lado):
    r = mk.get(mtype, {}).get(runner)
    if not r: return None, None
    back, bsz, lay, lsz = r
    v, sz = (back, bsz) if lado == "back" else (lay, lsz)
    return (v if (v is not None and v > 1.0) else None), (sz or 0.0)


def _cs_presente(mk, runner):
    r = mk.get("CORRECT_SCORE", {}).get(runner)
    return bool(r) and ((r[0] is not None and r[0] > 1.0) or (r[2] is not None and r[2] > 1.0))


class Jogo:
    """Estado de um jogo alimentado captura a captura. Produz trades fechados em self.fechados."""

    def __init__(self, ko, home, away):
        self.ko, self.home, self.away = ko, home, away
        self.fav_pre = None            # (odd, lado) menor back de MATCH_ODDS na captura mais próxima do KO (mtk >= -5)
        self.fav_mtk = None
        self.vistas = set()            # linhas Over já vistas com preço
        self.gols = None               # gols já saídos (fato) na última captura, None = indefinido
        self.lado_gol = {}             # n_gols -> "casa"/"fora"/"empate2" quando determinável
        self.abertos = {}              # id_metodo -> trade aberto
        self.fechados = []
        self.feito = set()             # métodos já usados neste jogo (1 trade por jogo por método)
        self.ult_min = None

    # ---------- fatos ----------
    def _gols(self, mk):
        bat, nao = [], []
        ativo = any(m in mk for m in ("MATCH_ODDS", "OVER_UNDER_15", "OVER_UNDER_25", "OVER_UNDER_35"))
        for mt, L in LINHAS.items():
            r = mk.get(mt, {}).get("Over %.1f Goals" % L)
            if r and r[0] is not None and r[0] > 1.0:
                self.vistas.add(L); (bat if r[0] <= 1.02 else nao).append(L)
            elif r and r[2] is not None and r[2] > 1.0:
                self.vistas.add(L); nao.append(L)           # só lay: linha viva
            elif L in self.vistas and mt not in mk and ativo:
                bat.append(L)                              # sumiu depois de vista, com o jogo sendo capturado: batida
        if not nao and not bat: return None
        lo = (max(bat) + 0.5) if bat else 0
        hi = (min(nao) - 0.5) if nao else None
        if hi is not None and lo == hi: return int(lo)
        if hi is None and bat and max(bat) == 3.5: return 4
        return None

    def _lado(self, mk, gols):
        if gols == 1:
            a, b = _cs_presente(mk, "1 - 0"), _cs_presente(mk, "0 - 1")
            if a and not b: return "casa"
            if b and not a: return "fora"
        if gols == 2:
            c20, c02, c11 = _cs_presente(mk, "2 - 0"), _cs_presente(mk, "0 - 2"), _cs_presente(mk, "1 - 1")
            if c11 and not c20 and not c02: return "empate2"
            if c20 and not c02 and not c11: return "casa2"
            if c02 and not c20 and not c11: return "fora2"
        return None

    # ---------- alimentação ----------
    def captura(self, ts, mtk, mk):
        minuto = -mtk - 15
        if "MATCH_ODDS" in mk and mtk >= -5 and (self.fav_mtk is None or abs(mtk) < abs(self.fav_mtk)):
            h, _ = _preco(mk, "MATCH_ODDS", self.home, "back"); a, _ = _preco(mk, "MATCH_ODDS", self.away, "back")
            if h and a: self.fav_pre, self.fav_mtk = ((h, "casa") if h <= a else (a, "fora")), mtk
        if minuto <= 0: return
        g = self._gols(mk)
        if g is not None:
            if self.gols is not None and g < self.gols: g = self.gols          # gols não diminuem (linha ausente num passe)
            if self.gols is not None and g > self.gols:
                for n in range(self.gols + 1, g + 1): self.lado_gol.setdefault(n, None)
            self.gols = g
            if g in (1, 2) and self.lado_gol.get(g) is None:
                self.lado_gol[g] = self._lado(mk, g)
        self.ult_min = minuto
        self._saidas(ts, minuto, mk)
        self._entradas(ts, minuto, mk)

    # ---------- entradas ----------
    def _entradas(self, ts, minuto, mk):
        if self.fav_pre is None or self.gols is None: return
        fav, lado_fav = self.fav_pre
        # M1: 0-0, 15-25', mandante favorito <=1.45, lay do empate 3.00-4.20, liquidez >=200
        if M1["id"] not in self.feito and M1["jan"][0] <= minuto <= M1["jan"][1] and self.gols == 0 and lado_fav == "casa" and fav <= M1["fav_max"]:
            o, sz = _preco(mk, "MATCH_ODDS", "The Draw", "lay")
            if o is not None:
                self.feito.add(M1["id"])
                if M1["odd"][0] <= o <= M1["odd"][1] and sz >= M1["liq"]:
                    self.abertos[M1["id"]] = dict(id=M1["id"], nome=M1["nome"], ts_in=ts, min_in=minuto, gols_in=0, odd_in=o, liq_in=sz, lado="lay", mercado="MATCH_ODDS", runner="The Draw", evento=None, evento_min=None)
                else:
                    self.fechados.append(dict(id=M1["id"], nome=M1["nome"], ts_in=ts, min_in=minuto, odd_in=o, liq_in=sz, status="FORA_DA_FAIXA"))
        # M2: 0-1 (visitante marcou), 20-45', mandante favorito <=1.35, back no mandante 2.10-3.20
        if M2["id"] not in self.feito and M2["jan"][0] <= minuto <= M2["jan"][1] and self.gols == 1 and self.lado_gol.get(1) == "fora" and lado_fav == "casa" and fav <= M2["fav_max"]:
            o, sz = _preco(mk, "MATCH_ODDS", self.home, "back")
            if o is not None:
                self.feito.add(M2["id"])
                if M2["odd"][0] <= o <= M2["odd"][1] and sz >= M2["liq"]:
                    self.abertos[M2["id"]] = dict(id=M2["id"], nome=M2["nome"], ts_in=ts, min_in=minuto, gols_in=1, odd_in=o, liq_in=sz, lado="back", mercado="MATCH_ODDS", runner=self.home, evento=None, evento_min=None)
                else:
                    self.fechados.append(dict(id=M2["id"], nome=M2["nome"], ts_in=ts, min_in=minuto, odd_in=o, liq_in=sz, status="FORA_DA_FAIXA"))
        # M3: 55-62', <=1 gol, back Under 2.5
        if M3["id"] not in self.feito and M3["jan"][0] <= minuto <= M3["jan"][1] and self.gols <= M3["gols_max"]:
            o, sz = _preco(mk, "OVER_UNDER_25", "Under 2.5 Goals", "back")
            if o is not None:
                self.feito.add(M3["id"])
                if M3["odd"][0] <= o <= M3["odd"][1] and sz >= M3["liq"]:
                    self.abertos[M3["id"]] = dict(id=M3["id"], nome=M3["nome"], ts_in=ts, min_in=minuto, gols_in=self.gols, odd_in=o, liq_in=sz, lado="back", mercado="OVER_UNDER_25", runner="Under 2.5 Goals", evento=None, evento_min=None, alvo=menos_ticks(o, M3["tp_ticks"]))
                else:
                    self.fechados.append(dict(id=M3["id"], nome=M3["nome"], ts_in=ts, min_in=minuto, odd_in=o, liq_in=sz, status="FORA_DA_FAIXA"))

    # ---------- saídas ----------
    def _fechar(self, t, ts, minuto, o_out, motivo):
        pnl = pnl_lay_fechado(t["odd_in"], o_out) if t["lado"] == "lay" else pnl_back_fechado(t["odd_in"], o_out)
        t.update(ts_out=ts, min_out=minuto, odd_out=o_out, pnl=round(pnl, 5), motivo=motivo, status="FECHADO", gols_out=self.gols)
        self.fechados.append(t); del self.abertos[t["id"]]

    def _saidas(self, ts, minuto, mk):
        for mid in list(self.abertos):
            t = self.abertos[mid]
            # evento de saída ainda não marcado?
            if t["evento"] is None:
                if mid == M1["id"]:
                    if self.gols is not None and self.gols >= 1:
                        lado = self.lado_gol.get(1)
                        if lado == "casa": t["evento"], t["evento_min"] = "GOL_FAV", minuto
                        elif lado == "fora": t["evento"], t["evento_min"] = "GOL_ZEBRA", minuto
                        elif self.gols >= 2: t["evento"], t["evento_min"] = "GOL_2", minuto     # lado indeterminado, 2 gols: fecha
                    elif minuto >= M1["stop_min"]: t["evento"], t["evento_min"] = "TEMPO_68", minuto
                elif mid == M2["id"]:
                    if self.gols is not None and self.gols >= 2:
                        l2 = self.lado_gol.get(2)
                        t["evento"], t["evento_min"] = ("EMPATE_1x1" if l2 == "empate2" else ("GOL_ZEBRA_0x2" if l2 == "fora2" else "GOL_2")), minuto
                    elif minuto >= M2["stop_min"]: t["evento"], t["evento_min"] = "TEMPO_70", minuto
                elif mid == M3["id"]:
                    if self.gols is not None and self.gols > t["gols_in"]: t["evento"], t["evento_min"] = "GOL", minuto
                    else:
                        o_lay, _ = _preco(mk, "OVER_UNDER_25", "Under 2.5 Goals", "lay")
                        if o_lay is not None and o_lay <= t["alvo"]: t["evento"], t["evento_min"] = "TP_TICKS", minuto
                        elif minuto - t["min_in"] >= M3["cap_min"]: t["evento"], t["evento_min"] = "TEMPO_8MIN", minuto
            if t["evento"] is None: continue
            # M1 gol da zebra: espera 5 min (emenda: "reação" não é mensurável) e fecha
            if t["evento"] == "GOL_ZEBRA" and minuto - t["evento_min"] < M1["obs_zebra_min"]:
                if self.gols is not None and self.gols >= 2: t["evento"] = "GOL_2"      # segundo gol antes dos 5 min: fecha já
                else: continue
            lado_out = "back" if t["lado"] == "lay" else "lay"
            o_out, _ = _preco(mk, t["mercado"], t["runner"], lado_out)
            if o_out is None: continue                                              # mercado suspenso: espera a próxima captura com preço
            self._fechar(t, ts, minuto, o_out, t["evento"])

    def encerrar(self, motivo="SEM_ODD_SAIDA"):
        """fim das capturas do jogo: o que ficou aberto não tem odd de saída observada -> fora da conta."""
        for mid in list(self.abertos):
            t = self.abertos[mid]; t.update(status=motivo, ts_out=None, min_out=self.ult_min, odd_out=None, pnl=None)
            self.fechados.append(t); del self.abertos[mid]


def agrupar_capturas(linhas, gap_s=90):
    """linhas de UM jogo ordenadas por ts: (ts_epoch, ts_str, mtk, mtype, runner, back, bsz, lay, lsz) ->
    lista de passes [(ts_str, mtk, mk)], juntando linhas a menos de gap_s segundos."""
    passes, cur, t0 = [], None, None
    for te, ts, mtk, mt, rn, b, bs, l, ls in linhas:
        if cur is None or te - t0 > gap_s:
            if cur is not None: passes.append(cur)
            cur = (ts, mtk, {}); t0 = te
        cur[2].setdefault(mt, {})[rn] = (b, bs, l, ls)
    if cur is not None: passes.append(cur)
    return passes
