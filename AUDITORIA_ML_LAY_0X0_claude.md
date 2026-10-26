# Auditoria — método ML para Lay Under 0.5 FT / Lay 0x0

**Auditor:** Claude (Opus 5) · **Data:** 26/10/2026 · **Objeto:** proposta Antigravity/Gemini, LightGBM
cross-market, OOS declarado Mai–Out/2026.

## Veredito: (C) ARQUIVADO / REPROVADO

Não por ser trabalho ruim — a parte de Mai–Ago é tecnicamente competente e honesta em pontos que
importam. Reprovado porque **o mês que sustenta 95% do P&L (outubro) não é pré-jogo: é odd AO VIVO
dos minutos 20–30**, e porque, quando o resultado é medido na convenção correta, o que resta é zero.

| | reportado | medido por mim | o que explica a diferença |
|---|---:|---:|---|
| Mai–Ago (a regra completa, OOS) | +33,55u, 4 meses positivos | **ROI/liability +0,19%, IC95 [−3,58, +3,76], z=+0,07** | convenção de stake; o agregado real é −0,23% nominal |
| Outubro | +678,80u, ROI +73,86% | **inválido** | janela `min_to_ko ∈ [−45,−2]` = minuto −13 a +30 do jogo |
| A regra em base independente (FRESH3, 2026) | — | **ROI/liability +1,19%, IC95 [−0,36, +2,71]** | é o número honesto |

Os três cruzam zero. Reproduzi o split, o modelo e os hiperparâmetros de vocês: `N=235` contra os 236
reportados, então estamos olhando o mesmo objeto.

---

## 1. A falha fatal: outubro é ao vivo, não pré-jogo

`scratch/extract_october_from_vps.py` filtra assim, sob o comentário `# Pre-jogo perto do KO`:

```python
if mtk < -45.0 or mtk > -2.0:
    continue
```

Isso mantém `min_to_ko` **negativo**. E `min_to_ko` negativo é o relógio já corrido desde o apito:

- `coletar_betfair_direto.py:192` — *"pega jogos já em andamento … → min_to_ko fica **NEGATIVO
  (minuto do jogo)**. E isso que permite medir a odd **AO VIVO nos minutos 20-30** (Lay 0x0 / Over
  0.5 in-play)."*
- `auditar_filtro_spread.py:261` — a janela pré-jogo é `4.0 <= min_to_ko <= 16.0`, **positiva**.
- `forward_ko_ledger.csv`, captura de produção no KO−10: `min_to_ko` mediano **+14,8** (N=894).
- Medido direto no coletor: `capture_ts=2026-10-25 20:30:36`, `ko=20:00`, `min_to_ko=-31.0` — ou
  seja, 30,6 minutos de relógio após o apito, sem offset.

Logo a janela `[−45, −2]` vai do **minuto 2 ao minuto 45** do jogo. E como o extrator percorre o
arquivo em ordem cronológica e sobrescreve o campo a cada linha que casa, o valor que sobra é o
**último** da janela — ou seja, a odd mais profunda disponível, perto do **fim do primeiro tempo**.

> **Correção de uma versão anterior deste parecer.** Eu havia escrito que a janela ia de "13 min
> antes do apito ao minuto 30", aplicando a fórmula `minuto = -min_to_ko - 15` de
> `trader_inplay_engine.py:10`. Aquele `−15` desconta a pausa do intervalo e **só vale do segundo
> tempo em diante**; no primeiro tempo o minuto é simplesmente `-min_to_ko`. A conclusão não muda —
> continua sendo dado ao vivo, e na verdade mais profundo do que eu disse.

O arquivo chama-se `october_pregame_odds.csv`. Ele não contém nenhuma linha de pré-jogo: o teste
`mtk > -2.0 → continue` descarta **100%** das capturas pré-jogo verdadeiras.

**A assinatura no preço confirma, sem depender da leitura do código.** Preço de lay de Under 0.5:

| | coletor "outubro" | FRESH3 (API, pré-jogo real) |
|---|---:|---:|
| mediana geral | **10,50** | — |
| fav 1,0–1,3 | — | 55,00 |
| fav 1,3–1,5 | — | 26,00 |
| fav 1,5–1,8 | — | 19,00 |

Um jogo 0-0 no minuto 25 tem o Under 0.5 despencando, porque o 0-0 ficou muito mais provável. É
exatamente o deslocamento observado. E o teto `≤ 16,50` passa a capturar 2.284 de 2.974 jogos.

Daí também vem o WR de 97,5%: **um favorito forte ainda em 0-0 no minuto 25 marca nos 65 minutos que
sobram quase sempre.** A taxa final de 0-0 nessa célula é 2,79%, contra 6,32% da mesma regra no
pré-jogo e 7,5% da base. O modelo não previu nada disso — o preço de entrada já carregava 25 minutos
de jogo sem gol.

Violação da Lei 2 na forma mais direta: **o objeto do backtest e o objeto da operação não são o
mesmo**. Não é "mudança de base" (Lei 7) — é mudança de instante.

## 2. Em outubro, 16 das 37 features são constantes escritas na mão

`scratch/evaluate_full_october_ml.py`, linhas 122–140:

```python
valid_oct['H_GF_r5'] = 1.3
valid_oct['total_exp_goals'] = 2.50
valid_oct['total_SoTF_r5'] = 8.5
valid_oct['p_ht05'] = 1.0 / np.maximum(1.01, 1.40)   # default se faltar ht
...
```

Todo o grupo de métricas rolantes (6) e de relação estatística-vs-preço (7) é constante para os 3.640
jogos. O modelo foi **treinado** com esses valores variando de verdade e **previu** com eles
congelados: toda árvore que divide em `total_exp_goals` cai no mesmo galho para todo jogo. E
`goals_per_implied_p00 = 2,50 × p_implied_0x0_lay` passa a ser só um reescalonamento do preço.

Consequência: em outubro o mecanismo anunciado — **discrepância cross-market** — não estava rodando.
O que rodou foi uma função das odds 1X2/Over/BTTS, com `ratio_ht05_o15` também fixo num default.

## 3. O filtro de governança: duas implementações, uma delas inoperante

**Em Mai–Ago a governança é legítima, e isso é crédito de vocês.** Verifiquei
`dataset_leak_free_features.parquet`: `liga_0x0_rate` é incremental no tempo (ARGENTINA 1 sai de
0,075 e chega a 0,144 em ago/2026), 133 das 142 ligas variam. É leak-free por construção. Eu esperava
encontrar vazamento aqui e não encontrei.

**Em outubro ela foi substituída por isto:**

```python
def check_defensive_league(comp):
    if any(k in c for k in ['(W)','WOMEN',...,'COLOMBIA 2','COLOMBIA B','ARGENTINA 2',
                            'URUGUAY 2','BRAZIL 3','BRAZIL C','PARAGUAY 2']):
        return 0.12
    return 0.07
```

Medi o que esse filtro remove dos 3.640 jogos de outubro: **87 jogos, e todos os 87 são futebol
feminino. Zero segundas divisões latinas.** Porque os nomes reais no coletor são `Colombian Primera
B`, `Uruguayan Segunda Division`, `Brazilian Serie C` — nenhum contém `COLOMBIA B`, `URUGUAY 2` nem
`BRAZIL C`.

O mecanismo que o relatório apresenta como a razão de junho e julho terem virado positivos
**não dispara em outubro**. E é isso, não coincidência, que explica a contradição interna que vocês
mesmos registraram: 2 dos 3 reds em COLÔMBIA 2. Colombian Primera B entra com rate 0,07 e passa.
Além disso há *train/serve skew*: a feature foi treinada com valores reais e servida com 0,07/0,12.

Sobre o limiar: a regra congelada e **pré-registrada em 02/08/2026** era `liga_rate < 0.08`. Mover
para 0,10 depois de ver junho e julho negativos é reajustar parâmetro pré-registrado — e o limiar foi
escolhido olhando exatamente os meses que ele conserta. Junho e julho são in-sample para essa escolha.

## 4. A convenção de unidade infla tudo por ~10x

O P&L de vocês é `green = +0,95` / `red = −(odd−1)` sobre stake nominal de 1u. Com odd 8–16, cada red
custa 7u a 15u, então o denominador correto é a **liability**, não o stake. O próprio script calcula a
coluna certa (`roi_risk`) e ela não foi reportada. Fator de conversão ≈ 10:

| mês | ROI reportado | ROI por liability |
|---|---:|---:|
| Maio | +5,23% | ~+0,5% |
| Setembro | +51,90% | ~+5,2% |
| Outubro | +73,86% | ~+9,7% |

Isto já está no Hall of Shame com nome e sobrenome: a convenção de stake foi o que transformou um edge
de 1,1pp em "34%". O estudo reencontrou o mesmo 1,1pp e reinflou do mesmo jeito.

## 5. Células aprovando com menos de 5 reds

- **Agosto: N=4, 0 reds, +95,00%.** Não é um mês, é um buraco de dados. Reproduzi exatamente.
- **A célula `Fav ≤ 1,80` em Mai–Ago: N=52, 1 red.** É onde está toda a mágica aparente (+6,50% por
  liability) e ela tem **um** red. A regra de guarda a barra sozinha.
- Setembro: N=42, 2 reds. Também barrada.

## 6. Nenhum piso de liquidez, em nenhum lugar

O extrator lê `row[11]` (lay) e nunca `row[12]` (`lay_size`). Não há piso de liquidez em nenhuma etapa
do estudo. É a lição mais caras do projeto inteiro — e a que separou método que funciona de método que
não funciona em todos os casos anteriores.

Qualidade do livro em outubro, no universo usado: **362 de 3.640 jogos com back do empate > 10,00**
(livro vazio), lay de Under 0.5 chegando a 1.000, overround 1X2 acima de 1,30 em 210 jogos. A primeira
linha do arquivo é `Argentina x Bolivia, Friendlies International, 1.02 / 220.0 / 150.0`.

Retirar os livros obviamente vazios derruba o gap de +3,77pp para +3,30pp — pouco, mas o teste certo
(piso em `lay_size` no instante da decisão) não é computável com os dados extraídos.

## 7. Grade sem FDR

`evaluate_full_october_ml.py` varre teto × EV (4 × 3) e governança on/off;
`eval_october_pure_market.py` varre 4 × 3. Com o filtro de favorito, são dezenas de configurações
sobre a mesma amostra. É grade, e o FDR se aplica. Nenhum p-valor foi corrigido.

---

## O que o modelo realmente adiciona

Essa é a pergunta que vale, e a resposta é: **cerca de +1pp, em cima de um mercado que está em zero.**
Mesmo split, mesmo modelo, OOS Mai–Ago, ROI por liability:

| | N | gap vs BE | ROI/liab | IC95 | reds |
|---|---:|---:|---:|---|---:|
| com modelo (EV≥2%) + governança | 235 | +0,13pp | **+0,19%** | [−3,58, +3,76] | 20 |
| só governança + teto, sem modelo | 1032 | −0,81pp | −0,88% | [−2,64, +0,81] | 89 |
| com modelo, sem governança | 420 | −0,16pp | −0,15% | [−2,90, +2,35] | 40 |
| mercado puro (só teto) | 1591 | −1,34pp | −1,48% | [−2,97, +0,04] | 161 |

O ML e a governança juntos levam de −1,48% a +0,19%. É ganho real de calibração — o AUC de 0,62 e o
log-loss melhor que a closing line não são mentira. Mas o destino é zero, não lucro.

E no universo de outubro o efeito aparece **inteiro já com EV ≥ 0,00**, isto é, sem seleção nenhuma do
modelo: +5,29pp contra +6,15pp com EV ≥ 2%. Lá o ML contribui com quase nada; o que carrega é a janela
ao vivo.

---

## Respostas pontuais

**1. Risco de cauda com odds 10–16.** É a mesma miragem de cauda, por dois caminhos. O primeiro é a
convenção: a margem anunciada de "+1,4 a +4,0pp" com ROI de dezenas de por cento só coexiste porque o
ROI está por stake. O segundo é mais sério: a margem honesta medida é **+0,13pp com IC de ±3,7pp**. A
pergunta "o edge absorve agrupamentos de red?" pressupõe um edge; não há edge para absorver nada. Com
liability de 9u a 15u por red e gap indistinguível de zero, o drawdown é governado pelo clustering e
não pela média.

**2. Governança de ligas: fundamentada ou ad-hoc?** As duas coisas, em implementações diferentes. A
feature do parquet é fundamentada e leak-free — isso eu confirmei e é mérito. O **limiar 0,10** é
ad-hoc: escolhido depois de ver junho e julho negativos, substituindo um 0,08 pré-registrado. E a
implementação de outubro é um blacklist de strings que não casa com os nomes reais e remove só futebol
feminino. *Garden of forking paths* no limiar, e inoperante no mês que decide o resultado.

**3. Lei 7 — API vs coletor VPS.** A validação na odd de lay real do coletor **é** a autoridade máxima
do forward, e nisso vocês têm razão. Mas não é o que aconteceu. O que foi validado não é a odd de lay
do coletor no pré-jogo — é a odd do coletor **ao vivo no minuto 20–30**. A autoridade do coletor vale
quando o instante de medição é o instante da decisão. Aqui não é, e a janela foi lida com o sinal
invertido.

**4. Viabilidade operacional no KO−10.** Independente do resto, não está demonstrada e há razão para
duvidar: (a) nenhum `lay_size` foi lido, então nenhuma das odds usadas é comprovadamente executável;
(b) as features cross-market exigem Over 1.5 / Over 2.5 / BTTS no instante da decisão, e justamente
essas foram substituídas por constantes em outubro, então nunca se mediu se existem lá; (c) 10% do
universo tem livro vazio pelo próprio 1X2.

**5. Veredito.** **(C) Arquivado.** Falhas do Hall of Shame, em ordem de gravidade:

1. Dado **ao vivo** apresentado como pré-jogo (Lei 2 — backtest ≠ operação).
2. Convenção de **stake** inflando o ROI por ~10x.
3. Célula aprovando com **menos de 5 reds** (ago N=4/0 reds; `Fav≤1,80` N=52/1 red).
4. Filtro escolhido **depois** de ver quais meses perdiam, sobre parâmetro **pré-registrado**.
5. **Sem piso de liquidez**.
6. Grade varrida **sem FDR**.

---

## O que eu faria com isto

Não vai para a Página 03 como está, porque uma watchlist stake-zero de método pré-jogo não observaria
o que o estudo mediu. Duas rotas, e são projetos diferentes:

**Rota A — medir a regra no pré-jogo de verdade.** Já medi: FRESH3 2026, lay U0.5 teto ≤16,5,
Fav ≤1,80 → **+1,19%, IC95 [−0,36, +2,71], N=1171, 74 reds**. Cruza zero, e o universo amplo é
negativo (−1,16%, z=−3,10). Consistente com o 1,1pp que já está registrado. Conclusão: não medido,
não morto. Se quiser perseguir, é pré-registro com N-alvo e nada de reajustar o 0,08.

**Rota B — assumir que é in-play e tratar como tal.** O achado de outubro pode ser real *como método
ao vivo*: "favorito forte, 0-0 no minuto 20–30, lay Under 0.5 com odd ≤16,5". Isso bate com o que o
projeto já sabe — o viés recreativo vive ao vivo, o pré-jogo é eficiente. Mas aí precisa de
pré-registro novo, `lay_size` gravado no instante da decisão, e reconhecimento de que a liquidez do
Under 0.5 ao vivo em liga pequena no minuto 25 é a pergunta inteira. Nada de Mai–Set entra nessa
amostra: são outro método.

**O que não fazer:** colar as duas numa tabela só. Foi isso que produziu os 919 jogos e os +678u.

---

### Reprodução

- `auditoria_ml_0x0.py` — outubro: ROI por liability, efeito sem modelo, qualidade do livro
- `auditoria_0x0_fresh3.py` — a regra na base independente, mês a mês, espelho back/lay verdadeiro
- `honesto_maio_ago.py` — reprodução do split Mai–Ago com ROI por liability e bootstrap

Comissão 5%. ROI sempre por liability (green `(1−c)/(odd−1)`, red `−1`). IC95 por bootstrap de
blocos-dia (4.000 reamostragens). Regra de guarda de 5 reds por célula.

### Uma retratação minha, para o registro

No meio desta auditoria montei um "teste de espelho" comparando **Lay Under 0.5** com **Back Over
0.5** e tratei o fato de os dois darem +3,5% como prova de livro incoerente. Está errado: os dois
ganham no mesmo desfecho (houve gol) — são a **mesma posição** medida de dois jeitos, não lados
opostos. Os dois concordarem é checagem de consistência a favor do dado, não contra. O espelho
verdadeiro é Back Under 0.5, que rodei na FRESH3: lado lay +1,19%, lado back −35,57%, soma dos
implícitos 0,1576 com overround normal — livro coerente. Retiro o teste e a conclusão que tirei dele.
