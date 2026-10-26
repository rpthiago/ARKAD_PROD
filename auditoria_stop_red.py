# Auditoria adversarial: filtros de producao + Stop Red no forward_ko_ledger
import csv, sys, math, random, collections, datetime
sys.path.insert(0, r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD")
from sinais_ko_core import eh_jogo_ignorado, LIQ_MIN

LED = r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD\metodos_aprovados\forward_ko_ledger.csv"
C = 0.05

TETO = {  # (min, max) do lay por metodo, conforme sinais_ko_core.avaliar
    "Lay Draw (Fav<=1.40)":            (4.50, 7.00),
    "Lay Home/DC X2 (FavVis<=1.65)":   (2.00, 8.00),
    "Lay Over 4.5 (Under Pesado)":     (4.00, 20.00),
    "Lay 0x3 Top 3":                   (14.00, 30.00),
    "Lay 0x3 (Regra Ampla)":           (14.00, 30.00),
    "Lay 2x2 Top 3":                   (8.00, 14.00),
}
FAV_MAX = {"Lay Draw (Fav<=1.40)": 1.40, "Lay Home/DC X2 (FavVis<=1.65)": 1.65}


def f(x):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None


def be(o, c=C):
    return (o - 1) / (o - c)


def pnl(odd, green, c=C):
    return (1 - c) / (odd - 1) if green else -1.0


rows = []
with open(LED, encoding="utf-8-sig") as fh:
    for r in csv.DictReader(fh):
        res = (r.get("resultado") or "").strip().upper()[:1]
        if res not in ("G", "R"):
            continue
        odd, liq, fav = f(r.get("Odd_Lay")), f(r.get("liq_lay")), f(r.get("Odd_Fav"))
        if odd is None:
            continue
        m = r["Metodo"]
        lo, hi = TETO.get(m, (0.0, 1e9))
        motivos = []
        if eh_jogo_ignorado(r.get("Home", ""), r.get("Away", ""), r.get("Liga", "")):
            motivos.append("governanca")
        if not (lo <= odd <= hi):
            motivos.append("teto")
        if liq is None or liq < LIQ_MIN:
            motivos.append("liquidez")
        if m in FAV_MAX and (fav is None or fav > FAV_MAX[m]):
            motivos.append("fav")
        rows.append(dict(
            data=r["Data"], hora=(r.get("Hora") or "00:00").strip()[:5], metodo=m,
            liga=r.get("Liga", ""), home=r.get("Home", ""), away=r.get("Away", ""),
            odd=odd, liq=liq, fav=fav, green=(res == "G"),
            v=pnl(odd, res == "G"), limpo=(len(motivos) == 0), motivos=motivos,
        ))

print(f"ledger liquidado: {len(rows)} linhas\n")


def boot(por_dia, n=4000, seed=7):
    dias = list(por_dia)
    if len(dias) < 2:
        return (float("nan"), float("nan"))
    rnd = random.Random(seed)
    out = []
    for _ in range(n):
        s = 0.0; k = 0
        for _ in range(len(dias)):
            for v in por_dia[rnd.choice(dias)]:
                s += v; k += 1
        if k:
            out.append(100 * s / k)
    out.sort()
    return out[int(.025 * len(out))], out[int(.975 * len(out))]


def resumo(sel, rot):
    if not sel:
        print(f"  {rot:<40} N=0")
        return None
    g = sum(1 for x in sel if x["green"]); r_ = len(sel) - g
    s = sum(x["v"] for x in sel)
    bem = 100 * sum(be(x["odd"]) for x in sel) / len(sel)
    wr = 100 * g / len(sel)
    pd = collections.defaultdict(list)
    for x in sel:
        pd[x["data"]].append(x["v"])
    lo, hi = boot(pd)
    flag = "  <<5 reds" if r_ < 5 else ""
    print(f"  {rot:<40} N={len(sel):<4} WR={wr:5.1f}% BE={bem:5.1f}% gap={wr-bem:+5.1f}pp "
          f"PL={s:+6.2f}u ROI={100*s/len(sel):+5.2f}% IC95=[{lo:+5.1f},{hi:+5.1f}]{flag}")
    return dict(n=len(sel), g=g, r=r_, pl=s, wr=wr, be=bem)


print("=" * 122)
print("1. RECLASSIFICACAO — reproduzindo o achado do Antigravity")
print("=" * 122)
ld = [x for x in rows if "Draw" in x["metodo"]]
print(f"  Lay Draw no ledger: {len(ld)}")
resumo(ld, "Lay Draw TOTAL BRUTO")
resumo([x for x in ld if not x["limpo"]], "Lay Draw FORA dos filtros")
resumo([x for x in ld if x["limpo"]], "Lay Draw DENTRO dos filtros")
print()
mot = collections.Counter()
for x in ld:
    for m in x["motivos"]:
        mot[m] += 1
print("  motivos de exclusao (com sobreposicao):", dict(mot))
print()

print("=" * 122)
print("2. PODER ESTATISTICO DOS 81 JOGOS LIMPOS")
print("=" * 122)
lim = [x for x in ld if x["limpo"]]
if lim:
    g = sum(1 for x in lim if x["green"]); n = len(lim)
    bem = sum(be(x["odd"]) for x in lim) / n
    se = math.sqrt(bem * (1 - bem) / n)
    gap = g / n - bem
    print(f"  N={n}  WR={100*g/n:.2f}%  BE={100*bem:.2f}%  gap={100*gap:+.2f}pp")
    print(f"  erro-padrao do WR = {100*se:.2f}pp  ->  z = {gap/se:+.2f}")
    print(f"  o IC95 do WR vai de {100*(g/n-1.96*se):.1f}% a {100*(g/n+1.96*se):.1f}% "
          f"— ou seja, nao distingue um metodo de +4% de um de -4%")
print()

print("=" * 122)
print("3. OUT-OF-SAMPLE: os filtros foram deployados em 08/10 e 13/10")
print("=" * 122)
for corte, rot in (("2026-10-08", "deploy governanca"), ("2026-10-13", "deploy teto/spread")):
    resumo([x for x in lim if x["data"] >= corte], f"Lay Draw limpo, KO >= {corte} ({rot})")
    resumo([x for x in lim if x["data"] < corte], f"Lay Draw limpo, KO <  {corte} (in-sample)")
print()


# ---------------- Stop Red cronologico ----------------
def liquida(x, mins=115):
    hh, mm = (x["hora"].split(":") + ["0"])[:2]
    try:
        t = datetime.datetime.fromisoformat(x["data"]) + datetime.timedelta(
            hours=int(hh), minutes=int(mm) + mins)
    except Exception:
        t = datetime.datetime.fromisoformat(x["data"]) + datetime.timedelta(minutes=mins)
    return t


def ko(x):
    hh, mm = (x["hora"].split(":") + ["0"])[:2]
    try:
        return datetime.datetime.fromisoformat(x["data"]) + datetime.timedelta(
            hours=int(hh), minutes=int(mm))
    except Exception:
        return datetime.datetime.fromisoformat(x["data"])


def simular(sel, max_reds, greens=None):
    """Stop por METODO e por DIA. greens=None usa o desfecho real;
    senao usa a lista booleana fornecida (para permutacao)."""
    if greens is None:
        greens = [x["green"] for x in sel]
    por_dia = collections.defaultdict(list)
    for x, gr in zip(sel, greens):
        por_dia[x["data"]].append((x, gr))
    total = 0.0; entrou = 0; pulou_g = 0; pulou_r = 0
    for dia, itens in por_dia.items():
        itens.sort(key=lambda t: ko(t[0]))
        liq_reds = []   # instantes em que reds liquidaram
        for x, gr in itens:
            k = ko(x)
            if max_reds is not None and sum(1 for t in liq_reds if t <= k) >= max_reds:
                if gr: pulou_g += 1
                else:  pulou_r += 1
                continue
            entrou += 1
            total += pnl(x["odd"], gr)
            if not gr:
                liq_reds.append(liquida(x))
    return total, entrou, pulou_g, pulou_r


print("=" * 122)
print("4. STOP RED — RESPOSTA A DOSE (um mecanismo real e monotono)")
print("=" * 122)
for rot, sel in (("Lay Draw limpo", lim),
                 ("Portfolio limpo", [x for x in rows if x["limpo"]])):
    print(f"  --- {rot} (N={len(sel)}) ---")
    base = None
    for mr in (None, 1, 2, 3):
        t, e, pg, pr = simular(sel, mr)
        if mr is None:
            base = t
        d = t - base
        nome = "sem stop" if mr is None else f"stop {mr} red(s)"
        print(f"    {nome:<14} PL={t:+6.2f}u  entrou={e:<4} pulou {pg}G/{pr}R  delta vs sem stop={d:+6.2f}u")
    print()

print("=" * 122)
print("5. TESTE DE PERMUTACAO — os reds se agrupam no dia alem do azar?")
print("    H0: mesmos jogos, mesmas odds, mesmo numero de reds, posicoes sorteadas.")
print("=" * 122)
for rot, sel in (("Lay Draw limpo", lim),
                 ("Portfolio limpo", [x for x in rows if x["limpo"]])):
    if len(sel) < 10:
        continue
    obs_stop, _, _, _ = simular(sel, 1)
    obs_base, _, _, _ = simular(sel, None)
    obs_d = obs_stop - obs_base
    rnd = random.Random(11)
    labels = [x["green"] for x in sel]
    maiores = 0; N = 3000; ds = []
    for _ in range(N):
        p = labels[:]
        rnd.shuffle(p)
        s, _, _, _ = simular(sel, 1, greens=p)
        b, _, _, _ = simular(sel, None, greens=p)
        ds.append(s - b)
        if (s - b) >= obs_d:
            maiores += 1
    ds.sort()
    med = ds[len(ds) // 2]
    print(f"  {rot}: ganho observado do stop = {obs_d:+.2f}u")
    print(f"    sob H0: mediana {med:+.2f}u   IC90 [{ds[int(.05*N)]:+.2f}, {ds[int(.95*N)]:+.2f}]")
    print(f"    P(ganho >= observado | H0) = {maiores/N:.3f}"
          f"   -> {'NAO distinguivel de azar' if maiores/N > 0.05 else 'agrupamento REAL'}")
    print()

print("=" * 122)
print("6. DEIXA-UM-DIA-DE-FORA: o ganho do stop depende de um unico dia?")
print("=" * 122)
dias = sorted(set(x["data"] for x in lim))
obs_d = simular(lim, 1)[0] - simular(lim, None)[0]
print(f"  ganho do stop com todos os dias: {obs_d:+.2f}u")
piores = []
for d in dias:
    s2 = [x for x in lim if x["data"] != d]
    if not s2:
        continue
    dd = simular(s2, 1)[0] - simular(s2, None)[0]
    piores.append((dd, d))
piores.sort()
for dd, d in piores[:4]:
    nr = sum(1 for x in lim if x["data"] == d and not x["green"])
    print(f"    sem {d} (tinha {nr} reds): ganho do stop cai para {dd:+.2f}u")
