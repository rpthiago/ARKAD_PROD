# roda NA VPS: streaming, O(1) por jogo, saida pequena
import csv, sys, os
path='/home/ubuntu/betfair-collector/betfair_live_odds.csv'
sz=os.path.getsize(path); seek=max(0, sz-2600*1024*1024)
HT={}; FT={}; MO={}
def put(d,g,mtk,key,val):
    r=d.setdefault(g,{})
    # 1a captura elegivel da janela = mtk MAIS ALTO (tempo cresce quando mtk cai)
    if r.get('_m') is None or mtk>r['_m']:
        if r.get('_m') is not None and mtk>r['_m']: r.clear()
        r['_m']=mtk
    if mtk==r['_m']: r[key]=val
def fl(x):
    try: return float(x)
    except: return None
with open(path,'r',encoding='utf-8',errors='ignore') as f:
    f.seek(seek); f.readline()
    for row in csv.reader(f):
        if len(row)<15: continue
        mt=row[1]
        if mt not in ('FIRST_HALF_GOALS_05','OVER_UNDER_05','MATCH_ODDS'): continue
        ko=row[5]
        if not (ko>='2026-09-27' and ko<'2026-10-27'): continue
        mtk=fl(row[6])
        if mtk is None: continue
        g=ko+'|'+row[3].strip()+'|'+row[4].strip()+'|'+row[2].strip()
        rn=row[7].strip(); bk=fl(row[9]); bs=fl(row[10]); ly=fl(row[11]); ls=fl(row[12])
        if mt=='FIRST_HALF_GOALS_05' and -18.0<=mtk<=-7.0:
            if 'Over' in rn:  put(HT,g,mtk,'ob',bk); put(HT,g,mtk,'obs',bs); put(HT,g,mtk,'ol',ly); put(HT,g,mtk,'ols',ls)
            elif 'Under' in rn: put(HT,g,mtk,'ub',bk); put(HT,g,mtk,'ubs',bs)
        elif mt=='OVER_UNDER_05' and -18.0<=mtk<=-7.0:
            if 'Over' in rn: put(FT,g,mtk,'fob',bk)
        elif mt=='MATCH_ODDS' and 8.0<=mtk<=16.0:
            if rn==g.split('|')[1]: put(MO,g,mtk,'h',bk)
            elif rn==g.split('|')[2]: put(MO,g,mtk,'a',bk)
            elif rn.lower()=='the draw': put(MO,g,mtk,'d',bk)
w=csv.writer(sys.stdout, lineterminator='\n')
w.writerow(['ko','home','away','competition','mtk','ht_ob','ht_obs','ht_ol','ht_ols','ht_ub','ft_ob','pre_h','pre_d','pre_a'])
for g,r in HT.items():
    ko,h,a,c=g.split('|')
    m=MO.get(g,{}); fo=FT.get(g,{})
    w.writerow([ko,h,a,c,r.get('_m'),r.get('ob'),r.get('obs'),r.get('ol'),r.get('ols'),r.get('ub'),
                fo.get('fob'),m.get('h'),m.get('d'),m.get('a')])
