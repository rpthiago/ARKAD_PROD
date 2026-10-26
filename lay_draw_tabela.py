"""O favorito por odd (<=1.40) tambem e favorito NA TABELA?

Tabela derivada da propria base, usando SOMENTE jogos com Date < Date do jogo avaliado
(mesma League+Season). Zero vazamento. Metrica principal = pontos por jogo (PPG), que e
robusta a cobertura parcial e a ligas com grupos (ITALY 3 etc.).
"""
import csv, sys, math, random, collections
sys.path.insert(0, r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD")
from sinais_ko_core import eh_jogo_ignorado, RE_COPA

BASE = r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD\Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv"
C = 0.05
MIN_JOGOS = 5          # historico minimo na temporada para a tabela significar algo


def f(x):
    try:
        v = float(str(x).replace(",", "."))
        return v
    except Exception:
        return None


def be(o, c=C):
    return (o - 1) / (o - c)


def pnl(o, g, c=C):
    return (1 - c) / (o - 1) if g else -1.0


# ---------- 1. le a base ----------
reg = []
for r in csv.DictReader(open(BASE, encoding="utf-8-sig")):
    d = (r.get("Date") or "")[:10]
    gh, ga = f(r.get("Goals_H_FT")), f(r.get("Goals_A_FT"))
    if not d or gh is None or ga is None:
        continue
    reg.append(dict(
        d=d, liga=r.get("League", ""), temp=r.get("Season", ""),
        home=r.get("Home", ""), away=r.get("Away", ""),
        gh=int(gh), ga=int(ga),
        h=f(r.get("Odd_H_Back")), a=f(r.get("Odd_A_Back")),
        dl=f(r.get("Odd_D_Lay")), db=f(r.get("Odd_D_Back")),
    ))
reg.sort(key=lambda x: x["d"])
print(f"base: {len(reg)} jogos com placar\n")

# ---------- 2. tabela incremental, sem vazamento ----------
# acum[(liga,temp)][time] = [pontos, jogos]
acum = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0]))
for x in reg:
    k = (x["liga"], x["temp"])
    t = acum[k]
    # snapshot ANTES de creditar este jogo
    x["tab"] = {}
    for nome in (x["home"], x["away"]):
        p, j = t[nome]
        x["tab"][nome] = (p / j if j else None, j)
    # ranking percentil do time dentro da liga/temporada, entre quem tem >= MIN_JOGOS
    elig = [(p / j) for nome, (p, j) in t.items() if j >= MIN_JOGOS]
    x["n_elig"] = len(elig)
    for nome in (x["home"], x["away"]):
        ppg, j = x["tab"][nome]
        if ppg is None or j < MIN_JOGOS or len(elig) < 6:
            x[f"pct_{nome == x['away']}"] = None
        else:
            melhores = sum(1 for v in elig if v > ppg)
            x[f"pct_{nome == x['away']}"] = melhores / max(1, len(elig) - 1)
    # credita
    if x["gh"] > x["ga"]:
        t[x["home"]][0] += 3
    elif x["gh"] < x["ga"]:
        t[x["away"]][0] += 3
    else:
        t[x["home"]][0] += 1; t[x["away"]][0] += 1
    t[x["home"]][1] += 1; t[x["away"]][1] += 1

# ---------- 3. universo Lay Draw de producao ----------
uni = []
for x in reg:
    if eh_jogo_ignorado(x["home"], x["away"], x["liga"]):
        continue
    h, a, dl, db = x["h"], x["a"], x["dl"], x["db"]
    if None in (h, a, dl, db) or min(h, a) <= 0:
        continue
    if not (4.50 <= dl <= 7.00):
        continue
    if db <= 1.0 or dl / db > 1.15:
        continue
    fav = min(h, a)
    if RE_COPA.search(str(x["liga"] or "")):
        if not (h <= 1.40):
            continue
    else:
        if not (fav <= 1.40):
            continue
    fav_casa = h <= a
    nf = x["home"] if fav_casa else x["away"]
    no = x["away"] if fav_casa else x["home"]
    ppg_f, j_f = x["tab"][nf]
    ppg_o, j_o = x["tab"][no]
    pct_f = x.get(f"pct_{not fav_casa}")
    uni.append(dict(
        d=x["d"], ano=x["d"][:4], liga=x["liga"], dl=dl,
        empate=(x["gh"] == x["ga"]), fav_casa=fav_casa,
        ppg_f=ppg_f, ppg_o=ppg_o, j_f=j_f, j_o=j_o, pct_f=pct_f,
        n_elig=x["n_elig"],
    ))

print(f"universo Lay Draw (regra de producao, sem liquidez): {len(uni)} jogos")
print("  por ano:", dict(sorted(collections.Counter(u["ano"] for u in uni).items())))
com = [u for u in uni if u["pct_f"] is not None and u["ppg_o"] is not None and u["j_o"] >= MIN_JOGOS]
print(f"  com tabela utilizavel (ambos >= {MIN_JOGOS} jogos, liga >= 6 times): {len(com)}")
print("  por ano:", dict(sorted(collections.Counter(u["ano"] for u in com).items())))
print()


def boot(pd, n=4000, seed=3):
    dias = list(pd)
    if len(dias) < 2:
        return float("nan"), float("nan")
    rnd = random.Random(seed); out = []
    for _ in range(n):
        s = 0.0; k = 0
        for _ in range(len(dias)):
            for v in pd[rnd.choice(dias)]:
                s += v; k += 1
        if k:
            out.append(100 * s / k)
    out.sort()
    return out[int(.025 * len(out))], out[int(.975 * len(out))]


def mostra(sel, rot):
    if not sel:
        print(f"  {rot:<40} N=0")
        return
    g = sum(1 for u in sel if not u["empate"]); n = len(sel); r_ = n - g
    s = sum(pnl(u["dl"], not u["empate"]) for u in sel)
    bem = 100 * sum(be(u["dl"]) for u in sel) / n
    pd = collections.defaultdict(list)
    for u in sel:
        pd[u["d"]].append(pnl(u["dl"], not u["empate"]))
    lo, hi = boot(pd)
    flag = "  <<5 reds" if r_ < 5 else ""
    print(f"  {rot:<40} N={n:<5} WR={100*g/n:5.1f}% BE={bem:5.1f}% gap={100*g/n-bem:+5.1f}pp "
          f"ROI={100*s/n:+5.2f}% IC95=[{lo:+5.1f},{hi:+5.1f}] reds={r_}{flag}")


print("=" * 120)
print("1. O FAVORITO POR ODD E FAVORITO NA TABELA? (so jogos com tabela utilizavel)")
print("=" * 120)
for ano in ("2026", "2025", "2024"):
    sel = [u for u in com if u["ano"] == ano]
    if not sel:
        continue
    melhor = sum(1 for u in sel if u["ppg_f"] > u["ppg_o"])
    print(f"  {ano}: em {100*melhor/len(sel):.1f}% dos jogos o favorito por odd tambem tem PPG maior "
          f"({melhor}/{len(sel)})")
    pcts = sorted(u["pct_f"] for u in sel)
    med = pcts[len(pcts) // 2]
    meio = sum(1 for u in sel if u["pct_f"] > 1 / 3)
    print(f"        percentil do favorito na tabela: mediana {med:.2f} "
          f"(0=lider, 1=lanterna) | fora do terco de cima: {100*meio/len(sel):.1f}%")
print()

print("=" * 120)
print("2. HIPOTESE DO THIAGO — favorito meio/baixo de tabela e pior? (2026)")
print("=" * 120)
s26 = [u for u in com if u["ano"] == "2026"]
mostra(s26, "2026 TODOS com tabela")
print()
mostra([u for u in s26 if u["pct_f"] <= 1 / 3], "  fav no TERCO DE CIMA da tabela")
mostra([u for u in s26 if 1 / 3 < u["pct_f"] <= 2 / 3], "  fav no TERCO DO MEIO")
mostra([u for u in s26 if u["pct_f"] > 2 / 3], "  fav no TERCO DE BAIXO")
print()
mostra([u for u in s26 if u["ppg_f"] > u["ppg_o"]], "  fav por odd TAMBEM melhor no PPG")
mostra([u for u in s26 if u["ppg_f"] <= u["ppg_o"]], "  fav por odd PIOR/IGUAL no PPG (!)")
print()

print("=" * 120)
print("3. O MESMO CORTE EM 2024-2025 (o filtro se repete fora de 2026?)")
print("=" * 120)
s45 = [u for u in com if u["ano"] in ("2024", "2025")]
mostra(s45, "2024-25 TODOS com tabela")
mostra([u for u in s45 if u["pct_f"] <= 1 / 3], "  fav no TERCO DE CIMA")
mostra([u for u in s45 if 1 / 3 < u["pct_f"] <= 2 / 3], "  fav no TERCO DO MEIO")
mostra([u for u in s45 if u["pct_f"] > 2 / 3], "  fav no TERCO DE BAIXO")
mostra([u for u in s45 if u["ppg_f"] > u["ppg_o"]], "  fav TAMBEM melhor no PPG")
mostra([u for u in s45 if u["ppg_f"] <= u["ppg_o"]], "  fav PIOR/IGUAL no PPG")
print()

print("=" * 120)
print("4. RELACAO CONTINUA — taxa de EMPATE por percentil do favorito (todos os anos)")
print("    (se a hipotese vale, a taxa de empate sobe conforme o fav desce na tabela)")
print("=" * 120)
faixas = [(0, .2), (.2, .4), (.4, .6), (.6, .8), (.8, 1.01)]
for lo_, hi_ in faixas:
    sel = [u for u in com if lo_ <= u["pct_f"] < hi_]
    if len(sel) < 20:
        print(f"  percentil {lo_:.1f}-{hi_:.1f}: N={len(sel)} (pequeno)")
        continue
    emp = sum(1 for u in sel if u["empate"])
    bem = 100 * sum(be(u["dl"]) for u in sel) / len(sel)
    s = sum(pnl(u["dl"], not u["empate"]) for u in sel)
    print(f"  percentil {lo_:.1f}-{hi_:.1f}: N={len(sel):<5} empates={100*emp/len(sel):5.1f}% "
          f"(BE pede <= {100-bem:4.1f}%)  ROI={100*s/len(sel):+6.2f}%")
