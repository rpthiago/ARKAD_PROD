# Stop Red aplicado por um OPERADOR HUMANO, isolado por metodo.
# Pergunta: em qual metodo parar depois de um red ajuda, e em qual custa dinheiro?
import csv, sys, math, random, collections, datetime
sys.path.insert(0, r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD")
from sinais_ko_core import eh_jogo_ignorado, LIQ_MIN

LED = r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD\metodos_aprovados\forward_ko_ledger.csv"
C = 0.05
TETO = {
    "Lay Draw (Fav<=1.40)": (4.50, 7.00), "Lay Home/DC X2 (FavVis<=1.65)": (2.00, 8.00),
    "Lay Over 4.5 (Under Pesado)": (4.00, 20.00), "Lay 0x3 Top 3": (14.00, 30.00),
    "Lay 0x3 (Regra Ampla)": (14.00, 30.00), "Lay 2x2 Top 3": (8.00, 14.00),
}
FAV_MAX = {"Lay Draw (Fav<=1.40)": 1.40, "Lay Home/DC X2 (FavVis<=1.65)": 1.65}


def f(x):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None


def be(o, c=C):
    return (o - 1) / (o - c)


def pnl(o, g, c=C):
    return (1 - c) / (o - 1) if g else -1.0


rows = []
for r in csv.DictReader(open(LED, encoding="utf-8-sig")):
    res = (r.get("resultado") or "").strip().upper()[:1]
    if res not in ("G", "R"):
        continue
    odd, liq, fav, m = f(r.get("Odd_Lay")), f(r.get("liq_lay")), f(r.get("Odd_Fav")), r["Metodo"]
    if odd is None:
        continue
    lo, hi = TETO.get(m, (0.0, 1e9))
    ok = (not eh_jogo_ignorado(r.get("Home", ""), r.get("Away", ""), r.get("Liga", ""))
          and lo <= odd <= hi and liq is not None and liq >= LIQ_MIN
          and not (m in FAV_MAX and (fav is None or fav > FAV_MAX[m])))
    if not ok:
        continue
    rows.append(dict(data=r["Data"], hora=(r.get("Hora") or "00:00").strip()[:5],
                     metodo=m, odd=odd, green=(res == "G")))


def ko(x):
    hh, mm = (x["hora"].split(":") + ["0"])[:2]
    try:
        return datetime.datetime.fromisoformat(x["data"]) + datetime.timedelta(hours=int(hh), minutes=int(mm))
    except Exception:
        return datetime.datetime.fromisoformat(x["data"])


def simular(sel, max_reds, greens=None, mins=115):
    if greens is None:
        greens = [x["green"] for x in sel]
    pd = collections.defaultdict(list)
    for x, g in zip(sel, greens):
        pd[x["data"]].append((x, g))
    tot = 0.0; pg = pr = 0
    for _, itens in pd.items():
        itens.sort(key=lambda t: ko(t[0]))
        liq_reds = []
        for x, g in itens:
            k = ko(x)
            if max_reds is not None and sum(1 for t in liq_reds if t <= k) >= max_reds:
                pg += 1 if g else 0; pr += 0 if g else 1
                continue
            tot += pnl(x["odd"], g)
            if not g:
                liq_reds.append(k + datetime.timedelta(minutes=mins))
    return tot, pg, pr


def perm_p(sel, nperm=4000, seed=5):
    obs = simular(sel, 1)[0] - simular(sel, None)[0]
    lab = [x["green"] for x in sel]
    rnd = random.Random(seed)
    ge = 0
    for _ in range(nperm):
        p = lab[:]; rnd.shuffle(p)
        if (simular(sel, 1, p)[0] - simular(sel, None, p)[0]) >= obs:
            ge += 1
    return obs, ge / nperm


print(f"portfolio limpo: {len(rows)} entradas\n")
print("=" * 118)
print("STOP 1 RED ISOLADO POR METODO — operador humano, sabe do red no fim do jogo")
print("=" * 118)
print(f"  {'metodo':<32} {'N':>4} {'gap vs BE':>10} {'sem stop':>10} {'com stop':>10} "
      f"{'delta':>8} {'pulou':>9} {'P perm':>8}")
ordem = sorted(set(x["metodo"] for x in rows))
for m in ordem:
    sel = [x for x in rows if x["metodo"] == m]
    if len(sel) < 5:
        print(f"  {m:<32} {len(sel):>4}   amostra pequena demais")
        continue
    g = sum(1 for x in sel if x["green"]); n = len(sel)
    gap = 100 * (g / n - sum(be(x["odd"]) for x in sel) / n)
    s0 = simular(sel, None)[0]
    s1, pg, pr = simular(sel, 1)
    obs, p = perm_p(sel)
    print(f"  {m:<32} {n:>4} {gap:>+9.1f}pp {s0:>+9.2f}u {s1:>+9.2f}u "
          f"{s1-s0:>+7.2f}u {pg:>4}G/{pr:<3}R {p:>8.3f}")

print()
print("=" * 118)
print("O QUE CUSTA APLICAR O STOP NO METODO QUE TEM EDGE (Lay Home)")
print("=" * 118)
lh = [x for x in rows if "Home" in x["metodo"]]
if lh:
    n = len(lh); g = sum(1 for x in lh if x["green"])
    bem = sum(be(x["odd"]) for x in lh) / n
    ev = sum(pnl(x["odd"], True) * (g / n) - 1 * (1 - g / n) for x in lh) / n
    _, pg, pr = simular(lh, 1)
    print(f"  Lay Home limpo: N={n}  WR={100*g/n:.1f}%  BE={100*bem:.1f}%  "
          f"gap={100*(g/n-bem):+.1f}pp  ROI={100*simular(lh,None)[0]/n:+.2f}%")
    print(f"  o stop pularia {pg} greens e {pr} reds")
    print(f"  EV por entrada medido = {100*simular(lh,None)[0]/n:+.2f}% da liability")
    print(f"  pular {pg+pr} entradas de EV positivo custa, em expectativa, "
          f"{(pg+pr)*simular(lh,None)[0]/n:+.2f}u")
    print()
    print("  >>> parar um metodo de EV POSITIVO depois de um red joga fora lucro esperado.")
    print("  >>> parar um metodo de EV ZERO/NEGATIVO nao custa nada — e tambem nao ganha nada.")
