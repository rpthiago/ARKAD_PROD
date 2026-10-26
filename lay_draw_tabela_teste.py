"""Teste de tendencia: o mercado precifica o empate igual, mas a taxa de empate muda
com a posicao do favorito na tabela? E se muda, qual o lado acionavel?
"""
import csv, sys, math, random, collections
sys.path.insert(0, r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD")
from sinais_ko_core import eh_jogo_ignorado, RE_COPA

BASE = r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD\Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv"
C = 0.05
MIN_JOGOS = 5


def f(x):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None


def be_lay(o, c=C):
    return (o - 1) / (o - c)


reg = []
for r in csv.DictReader(open(BASE, encoding="utf-8-sig")):
    d = (r.get("Date") or "")[:10]
    gh, ga = f(r.get("Goals_H_FT")), f(r.get("Goals_A_FT"))
    if not d or gh is None or ga is None:
        continue
    reg.append(dict(d=d, liga=r.get("League", ""), temp=r.get("Season", ""),
                    home=r.get("Home", ""), away=r.get("Away", ""), gh=int(gh), ga=int(ga),
                    h=f(r.get("Odd_H_Back")), a=f(r.get("Odd_A_Back")),
                    dl=f(r.get("Odd_D_Lay")), db=f(r.get("Odd_D_Back"))))
reg.sort(key=lambda x: x["d"])

acum = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0]))
for x in reg:
    t = acum[(x["liga"], x["temp"])]
    x["snap"] = {n: (t[n][0] / t[n][1] if t[n][1] else None, t[n][1]) for n in (x["home"], x["away"])}
    elig = [p / j for _, (p, j) in t.items() if j >= MIN_JOGOS]
    x["elig"] = elig
    if x["gh"] > x["ga"]:
        t[x["home"]][0] += 3
    elif x["gh"] < x["ga"]:
        t[x["away"]][0] += 3
    else:
        t[x["home"]][0] += 1; t[x["away"]][0] += 1
    t[x["home"]][1] += 1; t[x["away"]][1] += 1

uni = []
for x in reg:
    if eh_jogo_ignorado(x["home"], x["away"], x["liga"]):
        continue
    h, a, dl, db = x["h"], x["a"], x["dl"], x["db"]
    if None in (h, a, dl, db) or db <= 1.0:
        continue
    if not (4.50 <= dl <= 7.00) or dl / db > 1.15:
        continue
    fav = min(h, a)
    if RE_COPA.search(str(x["liga"] or "")):
        if not (h <= 1.40):
            continue
    elif not (fav <= 1.40):
        continue
    fav_casa = h <= a
    nf = x["home"] if fav_casa else x["away"]
    ppg_f, j_f = x["snap"][nf]
    if ppg_f is None or j_f < MIN_JOGOS or len(x["elig"]) < 6:
        continue
    pct = sum(1 for v in x["elig"] if v > ppg_f) / max(1, len(x["elig"]) - 1)
    uni.append(dict(d=x["d"], ano=x["d"][:4], pct=pct, dl=dl, db=db,
                    empate=(x["gh"] == x["ga"])))

print(f"universo com tabela: {len(uni)} jogos\n")

print("=" * 116)
print("1. O MERCADO PRECIFICA A DIFERENCA? (se a odd for igual nas faixas, nao precifica)")
print("=" * 116)
faixas = [(0, .2), (.2, .4), (.4, .6), (.6, 1.01)]
print(f"  {'faixa':<14} {'N':>5} {'odd lay media':>14} {'implicito':>10} {'empate real':>12} {'dif':>8}")
for lo, hi in faixas:
    s = [u for u in uni if lo <= u["pct"] < hi]
    if not s:
        continue
    odd = sum(u["dl"] for u in s) / len(s)
    imp = 100 * (1 - sum(be_lay(u["dl"]) for u in s) / len(s))
    real = 100 * sum(1 for u in s if u["empate"]) / len(s)
    print(f"  {lo:.1f}-{hi:.1f}       {len(s):>5} {odd:>14.2f} {imp:>9.1f}% {real:>11.1f}% {real-imp:>+7.1f}pp")
print()

print("=" * 116)
print("2. TESTE DE TENDENCIA — permutacao sobre a correlacao entre percentil e empate")
print("    H0: a posicao do favorito na tabela nao tem relacao com o empate")
print("=" * 116)


def corr(xs, ys):
    n = len(xs); mx = sum(xs) / n; my = sum(ys) / n
    num = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
    dx = math.sqrt(sum((a - mx) ** 2 for a in xs)); dy = math.sqrt(sum((b - my) ** 2 for b in ys))
    return num / (dx * dy) if dx and dy else 0.0


for rot, sel in (("todos os anos", uni),
                 ("so 2026", [u for u in uni if u["ano"] == "2026"]),
                 ("so 2024-25", [u for u in uni if u["ano"] in ("2024", "2025")])):
    xs = [u["pct"] for u in sel]; ys = [1.0 if u["empate"] else 0.0 for u in sel]
    obs = corr(xs, ys)
    rnd = random.Random(9); ge = 0; N = 8000
    for _ in range(N):
        p = ys[:]; rnd.shuffle(p)
        if corr(xs, p) >= obs:
            ge += 1
    print(f"  {rot:<14} N={len(sel):<5} correlacao percentil x empate = {obs:+.4f}   "
          f"P(>= obs | H0) = {ge/N:.4f}  {'SIGNIFICATIVO' if ge/N < 0.05 else 'nao significativo'}")
print()

print("=" * 116)
print("3. TERCO DE CIMA vs RESTO, agregado (a comparacao com mais poder)")
print("=" * 116)


def cel(sel, rot):
    if not sel:
        return
    n = len(sel); emp = sum(1 for u in sel if u["empate"]); g = n - emp
    bem = sum(be_lay(u["dl"]) for u in sel) / n
    se = math.sqrt(bem * (1 - bem) / n)
    wr = g / n
    pl = sum((1 - C) / (u["dl"] - 1) if not u["empate"] else -1.0 for u in sel)
    print(f"  {rot:<34} N={n:<5} WR={100*wr:5.2f}% BE={100*bem:5.2f}% gap={100*(wr-bem):+5.2f}pp "
          f"z={(wr-bem)/se:+5.2f} ROI={100*pl/n:+6.2f}% reds={emp}")


cel([u for u in uni if u["pct"] <= 1 / 3], "fav no TERCO DE CIMA")
cel([u for u in uni if u["pct"] > 1 / 3], "fav FORA do terco de cima")
print()
for ano in ("2026", "2025", "2024"):
    s = [u for u in uni if u["ano"] == ano]
    cel([u for u in s if u["pct"] <= 1 / 3], f"  {ano} terco de cima")
    cel([u for u in s if u["pct"] > 1 / 3], f"  {ano} fora do terco de cima")
print()

print("=" * 116)
print("4. O LADO ACIONAVEL — se o empate acontece mais que o precificado, o lado e BACK no empate")
print("=" * 116)
print(f"  {'faixa':<22} {'N':>5} {'empate real':>12} {'odd back media':>15} {'BE back':>9} {'ROI back':>10} {'reds':>6}")
for rot, sel in (("fav fora terco cima", [u for u in uni if u["pct"] > 1 / 3]),
                 ("fav percentil > 0,5", [u for u in uni if u["pct"] > 0.5]),
                 ("fav percentil > 0,6", [u for u in uni if u["pct"] > 0.6]),
                 ("fav terco de cima", [u for u in uni if u["pct"] <= 1 / 3])):
    if len(sel) < 10:
        print(f"  {rot:<22} {len(sel):>5}  amostra pequena")
        continue
    n = len(sel); emp = sum(1 for u in sel if u["empate"])
    oddb = sum(u["db"] for u in sel) / n
    beb = 100 / oddb
    # back no empate: stake 1u, green = (odd-1)*(1-c)... exchange back nao paga comissao sobre stake
    pl = sum(((u["db"] - 1) * (1 - C)) if u["empate"] else -1.0 for u in sel)
    print(f"  {rot:<22} {n:>5} {100*emp/n:>11.1f}% {oddb:>15.2f} {beb:>8.1f}% "
          f"{100*pl/n:>+9.2f}% {n-emp:>6}")
print()
print("  obs: ROI do back e por STAKE (stake 1u, green = (odd-1)*0,95). Nao comparar com ROI de lay.")
