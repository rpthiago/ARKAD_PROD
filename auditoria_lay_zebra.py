# Auditoria Lay Zebra vs Lay Draw — base FRESH3, governanca de producao
import csv, sys, math, random, collections
sys.path.insert(0, r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD")
from sinais_ko_core import eh_jogo_ignorado, RE_COPA

BASE = r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD\Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv"
C = 0.05

def f(x):
    try:
        v = float(str(x).replace(",", "."))
        return v if v > 0 else None
    except Exception:
        return None

def be(o, c=C):
    return (o - 1) / (o - c)

def pnl_lay(odd, green, c=C):
    """ROI por liability: green = (1-c)/(odd-1), red = -1"""
    return (1 - c) / (odd - 1) if green else -1.0

def boot_ic(por_dia, n=4000, seed=7):
    """IC95 bootstrap de blocos-dia sobre ROI medio por liability"""
    dias = list(por_dia.keys())
    if len(dias) < 2:
        return (float("nan"), float("nan"))
    rnd = random.Random(seed)
    out = []
    for _ in range(n):
        s = 0.0; k = 0
        for _ in range(len(dias)):
            d = dias[rnd.randrange(len(dias))]
            for v in por_dia[d]:
                s += v; k += 1
        if k:
            out.append(100 * s / k)
    out.sort()
    return (out[int(0.025 * len(out))], out[int(0.975 * len(out))])

# ---------- carrega e monta o universo Lay Draw de producao ----------
jogos = []
with open(BASE, encoding="utf-8-sig") as fh:
    for r in csv.DictReader(fh):
        data = (r.get("Date") or "").strip()[:10]
        if not data:
            continue
        home, away, liga = r.get("Home", ""), r.get("Away", ""), r.get("League", "")
        if eh_jogo_ignorado(home, away, liga):
            continue
        h, a = f(r.get("Odd_H_Back")), f(r.get("Odd_A_Back"))
        dl, db = f(r.get("Odd_D_Lay")), f(r.get("Odd_D_Back"))
        hl, al = f(r.get("Odd_H_Lay")), f(r.get("Odd_A_Lay"))
        gh, ga = f(r.get("Goals_H_FT")), f(r.get("Goals_A_FT"))
        gh = 0.0 if r.get("Goals_H_FT") in ("0", "0.0", 0) else gh
        ga = 0.0 if r.get("Goals_A_FT") in ("0", "0.0", 0) else ga
        if None in (h, a, dl, db, hl, al) or gh is None or ga is None:
            continue
        fav = min(h, a)
        eh_copa = bool(RE_COPA.search(str(liga or "")))
        # REGRA LAY DRAW DE PRODUCAO
        if not (4.50 <= dl <= 7.00):
            continue
        if db and dl / db > 1.15:      # trava de spread
            continue
        if eh_copa:
            if not (h <= 1.40):
                continue
        else:
            if not (fav <= 1.40):
                continue
        fav_eh_casa = h <= a
        zebra_lay = al if fav_eh_casa else hl
        zebra_back = f(r.get("Odd_A_Back")) if fav_eh_casa else f(r.get("Odd_H_Back"))
        if not zebra_lay or not zebra_back:
            continue
        if zebra_lay / zebra_back > 1.15:   # mesma trava de spread
            continue
        jogos.append(dict(
            data=data, ano=data[:4], liga=liga, home=home, away=away,
            dl=dl, zl=zebra_lay, fav_casa=fav_eh_casa, copa=eh_copa,
            gh=int(gh), ga=int(ga),
            empate=(gh == ga),
            zebra_ganhou=(ga > gh) if fav_eh_casa else (gh > ga),
        ))

print(f"universo Lay Draw de producao (FRESH3, com governanca): {len(jogos)} jogos")
anos = collections.Counter(j["ano"] for j in jogos)
print("  por ano:", dict(sorted(anos.items())))
print()

def avalia(sel, metodo, titulo, mostrar=True):
    """metodo: 'draw' = lay empate | 'zebra' = lay zebra"""
    if not sel:
        return None
    por_dia = collections.defaultdict(list)
    g = r_ = 0; soma = 0.0; bes = []
    for j in sel:
        odd = j["dl"] if metodo == "draw" else j["zl"]
        green = (not j["empate"]) if metodo == "draw" else (not j["zebra_ganhou"])
        v = pnl_lay(odd, green)
        soma += v; por_dia[j["data"]].append(v)
        bes.append(be(odd))
        if green: g += 1
        else: r_ += 1
    n = g + r_
    wr = 100 * g / n
    bem = 100 * sum(bes) / n
    roi = 100 * soma / n
    lo, hi = boot_ic(por_dia)
    if mostrar:
        flag = "" if r_ >= 5 else "  <<< MENOS DE 5 REDS: nao pode passar"
        print(f"  {titulo:<34} N={n:<5} WR={wr:5.2f}%  BE={bem:5.2f}%  "
              f"gap={wr-bem:+5.2f}pp  ROI={roi:+5.2f}%  IC95=[{lo:+5.2f}%,{hi:+5.2f}%]  reds={r_}{flag}")
    return dict(n=n, wr=wr, be=bem, roi=roi, lo=lo, hi=hi, reds=r_, pnl=soma)

print("=" * 118)
print("1. MESMOS JOGOS, OS DOIS METODOS — base inteira 2024-2026")
print("=" * 118)
avalia(jogos, "draw", "Lay Draw (empate)")
avalia(jogos, "zebra", "Lay Zebra (sem teto)")
print()

print("=" * 118)
print("2. ANO A ANO")
print("=" * 118)
for ano in sorted(anos):
    sel = [j for j in jogos if j["ano"] == ano]
    avalia(sel, "draw", f"Lay Draw {ano}")
    avalia(sel, "zebra", f"Lay Zebra {ano}")
    print()

print("=" * 118)
print("3. SENSIBILIDADE DO TETO (o teto 10.00 foi escolhido vendo o resultado)")
print("=" * 118)
for teto in (6, 7, 8, 9, 10, 11, 12, 14, 16, 20, 99):
    sel = [j for j in jogos if j["zl"] <= teto]
    sel26 = [j for j in sel if j["ano"] == "2026"]
    a_all = avalia(sel, "zebra", f"Lay Zebra teto<={teto}  TODOS", mostrar=False)
    a_26 = avalia(sel26, "zebra", f"teto<={teto} 2026", mostrar=False)
    if a_all:
        s26 = (f"2026: N={a_26['n']:<4} ROI={a_26['roi']:+6.2f}% reds={a_26['reds']:<3}"
               if a_26 else "2026: sem jogos")
        print(f"  teto<={teto:<3} TODOS: N={a_all['n']:<5} WR={a_all['wr']:5.2f}% "
              f"gap={a_all['wr']-a_all['be']:+5.2f}pp ROI={a_all['roi']:+6.2f}% "
              f"IC95=[{a_all['lo']:+6.2f},{a_all['hi']:+6.2f}] reds={a_all['reds']:<4} | {s26}")
print()

print("=" * 118)
print("4. O QUE E REALMENTE NOVO: separando fav mandante x fav visitante")
print("    (fav visitante JA E o metodo Lay Home/DC X2, que esta no robo e da lucro)")
print("=" * 118)
for teto, rot in ((10, "teto<=10"), (99, "sem teto")):
    base_t = [j for j in jogos if j["zl"] <= teto]
    print(f"  --- {rot} ---")
    avalia([j for j in base_t if j["fav_casa"]], "zebra", "Lay Away (fav MANDANTE) = NOVO")
    avalia([j for j in base_t if not j["fav_casa"]], "zebra", "Lay Home (fav VISITANTE) = ja existe")
    sel26 = [j for j in base_t if j["ano"] == "2026"]
    avalia([j for j in sel26 if j["fav_casa"]], "zebra", "  so 2026: Lay Away (NOVO)")
    avalia([j for j in sel26 if not j["fav_casa"]], "zebra", "  so 2026: Lay Home (ja existe)")
    print()

print("=" * 118)
print("5. 2026 PARTIDO EM DUAS METADES (teste de decaimento), teto<=10")
print("=" * 118)
s26 = sorted([j for j in jogos if j["ano"] == "2026" and j["zl"] <= 10], key=lambda x: x["data"])
if s26:
    meio = len(s26) // 2
    avalia(s26[:meio], "zebra", f"1a metade 2026 (ate {s26[meio-1]['data']})")
    avalia(s26[meio:], "zebra", f"2a metade 2026 (de {s26[meio]['data']})")
