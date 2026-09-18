import pandas as pd, numpy as np
src = open("pages/02_📊_Resultados_Metodos_Aprovados.py", encoding="utf-8").read()
cut = src.index("df_raw = carregar_dados_aprovados")
ns = {"__file__": r"C:\Users\thiag\OneDrive\Documentos\GitHub\ARKAD_PROD\pages\02.py"}; exec(compile(src[:cut], "p02", "exec"), ns)
df = ns["carregar_dados_aprovados"]("👑 Portfólio em Validação Forward (Lay 0x0 XGBoost, 0x3, 2x2, Tríade + Zebras)")
d = df[df.Status.isin(["🟢 GREEN","🔴 RED"]) & ~df["Método"].astype(str).str.contains("Paralelo|Zebra|Micro|0x0")].copy()
d["red"]=(d.Status=="🔴 RED").astype(int); d["odd"]=pd.to_numeric(d.Odd_Entrada,errors="coerce"); d=d.dropna(subset=["odd"])
d["met"]=d["Método"].astype(str).str.replace(r" \(.*","",regex=True).str.replace("Lay ","").str.replace(" / DC X2","").str.replace(" FT","").str.replace(" Top 3","")
d["pct"]=np.where(d.met.str.contains("Draw|Home"),0.05,0.15)
d["t"]=pd.to_datetime(d.Data.dt.strftime("%Y-%m-%d")+" "+d.Hora.astype(str).str[:5].replace("nan","15:00"),errors="coerce"); d=d.dropna(subset=["t"]).sort_values("t").reset_index(drop=True)
d["dstr"]=d.t.dt.strftime("%Y-%m-%d")
print("PAGINA 02 (jogos reais): N=%d | reds=%d (%.1f%%) | %s -> %s" % (len(d), d.red.sum(), 100*d.red.mean(), d.dstr.min(), d.dstr.max()))
print(d.groupby("met").agg(N=("red","size"), reds=("red","sum"), pnl_u=("PnL_u","sum")).round(2).to_string())
d["red_antes"]=d.groupby("dstr").red.cumsum()-d.red
ra=d[d.red_antes>0].red.mean(); rs=d[d.red_antes==0].red.mean()
rng=np.random.default_rng(3); difs=[]
for _ in range(3000):
    rr=rng.permutation(d.red.values); antes=pd.Series(rr).groupby(d.dstr.values).cumsum().values-rr; difs.append(rr[antes>0].mean()-rr[antes==0].mean())
print("\nred depois de red no dia: %.1f%% (N=%d) vs sem red antes: %.1f%% (N=%d) | dif %+.1fpp | P(acaso) = %.3f" % (100*ra,(d.red_antes>0).sum(),100*rs,(d.red_antes==0).sum(),100*(ra-rs),(np.array(difs)>=(ra-rs)).mean()))
def simular(df, stop=None, pct_alto=0.15, teto=None):
    banca=2000.0; pico=banca; mdd=0; n=0; pulados=0; abertos=[]
    for dia in df.dstr.unique():
        g=df[df.dstr==dia]; ini=banca; bloq=None
        for r in g.itertuples():
            abertos=[(t,l) for t,l in abertos if t>r.t]        # ainda nao liquidados
            if bloq is not None and r.t>bloq: pulados+=1; continue
            pct=0.05 if r.pct==0.05 else pct_alto; liab=pct*banca
            if teto is not None and sum(l for _,l in abertos)+liab > teto*banca: pulados+=1; continue
            n+=1; abertos.append((r.t+pd.Timedelta(minutes=115), liab))
            banca += (-liab if r.red else liab*0.95/(r.odd-1)); pico=max(pico,banca); mdd=max(mdd,(pico-banca)/pico)
            if stop is not None and bloq is None and (banca-ini)/ini <= -stop: bloq=r.t+pd.Timedelta(minutes=115)
    return banca,mdd,n,pulados
print()
for pa in (0.05,0.075,0.10,0.125,0.15):
    b,m,n,p=simular(d,pct_alto=pa); b2,m2,n2,p2=simular(d,pct_alto=pa,stop=0.10)
    print("2x2/0x3/Over a %4.1f%% | sem stop: R$%5.0f (%+4.0f%%) maxDD %4.1f%% | com stop -10%%: R$%5.0f (%+4.0f%%) maxDD %4.1f%% (pulou %d)" % (100*pa,b,100*(b/2000-1),100*m,b2,100*(b2/2000-1),100*m2,p2))
# pior sequencia possivel: quantos reds seguidos nos de 15%/10% para cair 30% e 50% da banca?
import math
for pa in (0.05,0.075,0.10,0.15):
    print("  a %4.1f%%: %d reds seguidos = -30%% da banca | %d reds seguidos = -50%%" % (100*pa, math.ceil(math.log(0.7)/math.log(1-pa)), math.ceil(math.log(0.5)/math.log(1-pa))))
# maxDD sob o nulo (resultados embaralhados) por sizing: mediana e p95 do maxDD
for pa in (0.05,0.10,0.15):
    m=[]
    for i in range(150):
        dd=d.copy(); dd["red"]=rng.permutation(d.red.values); m.append(simular(dd,pct_alto=pa)[1])
    m=np.array(m); print("  a %4.1f%% com resultados embaralhados: maxDD mediano %.1f%% | p95 %.1f%%" % (100*pa,100*np.median(m),100*np.percentile(m,95)))
ganhos=[]; dds=[]
for i in range(300):
    dd=d.copy(); dd["red"]=rng.permutation(d.red.values); b0,m0,_,_=simular(dd); b1,m1,_,_=simular(dd,stop=0.10); ganhos.append(b1-b0); dds.append(m0-m1)
b0,m0,_,_=simular(d); b1,m1,_,_=simular(d,stop=0.10); obs=b1-b0
print("\nstop -10%% sob o nulo (resultados embaralhados, 300x): ganho mediano R$%+.0f | IC90 [%+.0f, %+.0f] | P(ganho >= observado R$%+.0f) = %.2f | reducao maxDD mediana %+.1fpp (observada %+.1fpp)" % (np.median(ganhos),np.percentile(ganhos,5),np.percentile(ganhos,95),obs,(np.array(ganhos)>=obs).mean(),100*np.median(dds),100*(m0-m1)))

