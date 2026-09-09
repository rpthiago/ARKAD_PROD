# AUDITORIA — Radar Híbrido HT (Back Favorito 0-0 no intervalo)

> Cole tudo abaixo da linha no Gemini.

---

O Radar HT foi **implementado e está no ar** na VPS (systemd `radar-ht`, stake-zero, liquidação
oficial Betfair). Antes de congelar o pré-registro, rodei a auditoria de sempre. Seguem os números
medidos, o método de cada medição e o código para você reproduzir. **Não estou pedindo concordância —
estou pedindo que você confira e diga o que lê nisso.**

## 0. O que reproduz — confirmado

Rodei o seu recorte na `Bases_de_Dados_API_FutPythonTrader_Bet365.csv` (242.536 jogos):

```python
b = df[(df.Odd_H_FT <= 1.45) & (df.Goals_H_HT == 0) & (df.Goals_A_HT == 0)]
b['win'] = (b.Goals_H_FT > b.Goals_A_FT).astype(int)
p = b[(b.Shots_On_Target_H_HT >= 3) | (b.Corners_H_HT >= 4)]
```

| | N | WR |
|---|---|---|
| cego | 14.865 | **50,92%** |
| com pressão | 2.916 | **63,17%** |

**Seus dois números batem exatamente.** N=2.916 e WR=63,17% conferem. (Observação de reconciliação:
o seu prompt reporta N=4.810 para o caso cego; meu N cego é 14.865 e o 4.810 fica muito próximo do
subgrupo "jogos que têm estatística coletada", N=4.785 — vale conferir de onde veio.)

## 1. Medição: 67,8% dos jogos têm as estatísticas do HT gravadas como `0`

As colunas de stats do 1º tempo não vêm vazias quando não foram coletadas — vêm **zero**. `isna()`
não pega nada; a coluna parece 100% preenchida.

```python
sem_dado = (b.Total_Shots_H_HT == 0) & (b.Corners_H_HT == 0) & (b.Possession_H_HT == 0)
```

Dentro do recorte (fav ≤1,45 e 0-0 no HT, N=14.865):

```
Shots_On_Target_H_HT == 0 : 10.861  (73,1%)
Corners_H_HT         == 0 : 10.321  (69,4%)
Total_Shots_H_HT     == 0 : 10.116  (68,1%)
Possession_H_HT      == 0 : 10.090  (67,9%)
TODAS as stats == 0       : 10.080  (67,8%)
```

Esses 10.080 jogos são classificados como "sem pressão" por construção, já que `0 >= 3` é falso.

**Comparando apenas jogos que têm estatística coletada:**

| | N | WR |
|---|---|---|
| cego (só jogos com stats) | 4.785 | **59,90%** |
| com pressão | 2.916 | 63,17% |
| sem pressão (com stats) | 1.869 | 54,79% |

Gap do filtro: **+3,27pp** contra a base correta, em vez de +12,24pp contra a base misturada.

**Estabilidade por ano** (com pressão): 2021 59,3% · 2022 63,1% · 2023 63,2% · 2024 65,3% ·
2025 63,2% · 2026 62,0% — o 63% é estável, não é ruído de um ano.

## 2. Medição: break-even vs. preço justo implícito

Comissão 4,5%, back: `BE = 1 / (1 + (odd−1)·0,955)`.

```
odd 1,75 -> BE 58,27%      odd 2,00 -> BE 51,15%
odd 1,80 -> BE 56,69%      odd 2,05 -> BE 49,93%
odd 1,90 -> BE 53,78%      odd 2,40 -> BE 42,79%
```

Se a taxa de vitória do fav a 0-0 no HT é 59,90% (cego, com stats), o preço justo é **1/0,599 = 1,67**;
com pressão (63,17%), **1,58**. A regra exige `odd_back ≥ 1,75`, que fica **acima** desses dois valores.

## 3. Medição: fluxo real do gatilho no coletor da Betfair

21 dias de coletor 24/7 na VPS (2026-08-17 a 2026-09-08, ~197 jogos/dia, 3.534 jogos observados no
intervalo). Placar inferido pelo runner de menor lay do CORRECT_SCORE; odd pré-jogo = captura de
MATCH_ODDS mais próxima do KO; odd de HT = captura mais próxima de `min_to_ko = −65`.

```
jogos com CS no intervalo               3.534
-> 0-0 no intervalo                       636  (18%)
-> + odd pré do mandante <= 1,45            7
-> + odd de HT entre 1,75 e 2,40            1
-> + liquidez no back >= R$ 300             0
```

Afrouxando o teto pré-jogo:

```
pre <= 1,50 :  8  ->  odd 1,75-2,40:  2  ->  liq >= 300:  0
pre <= 1,60 : 23  ->  odd 1,75-2,40: 13  ->  liq >= 300:  8   (~0,4/dia)
```

**Odd real do mandante no intervalo, jogos 0-0** (medida, não assumida):

| Faixa pré-jogo | N | mediana da odd no HT |
|---|---|---|
| ≤ 1,45 | 7 | 1,23 *(N pequeno demais para concluir)* |
| 1,45-1,60 | 16 | **2,19** |
| 1,60-2,00 | 86 | **2,32** |

## 4. O que a VPS não consegue fazer

Nenhum feed da infraestrutura tem **chutes no alvo ou escanteios ao vivo**. O filtro de pressão só
existe se for conferido a mão e registrado — foi implementado assim (`radar_ht_marcar.py`, coluna
`pressao` S/N, marcada no intervalo). Sinal sem marcação mede "back no fav 0-0 no HT", não o método.

## 5. Contexto: o mesmo teste feito no seu método anterior

No Late Goal, o mesmo tipo de checagem deu: Back Under **−4,64%** e Back Over **−4,63%** no mesmo
cohort de N=371 liquidado oficialmente — overround medido 2,36%. E a odd do "Over limite" no min
75-85 tem mediana **2,00**, não 3,20-5,00 (3 de 360 sinais na faixa 3,00-5,50). Ambos estão rodando
stake-zero com pré-registro congelado.

---

## O que eu quero de você

1. **Confirme ou derrube a medição do item 1.** Se os 67,8% de zeros forem dado ausente, qual é a
   comparação correta para medir o filtro de pressão? Se você discordar de que sejam ausentes,
   mostre por quê.
2. **Item 2:** com WR 59,90% cego e 63,17% com pressão, qual é a sua leitura sobre exigir `odd ≥ 1,75`?
   O que você espera que a odd de HT faça num jogo em que o favorito acabou de fazer 3 chutes no
   alvo e 4 escanteios, e como isso interage com o filtro?
3. **Item 3:** o fluxo medido é 0 na regra exata. Você mantém a regra, muda os filtros, ou considera
   o método inviável nessa infraestrutura? Se mudar, qual mecanismo justifica o novo corte — e não
   o resultado que ele produz nesses mesmos dados?
4. **Se houver erro na minha medição, aponte** — com o código, que a base é a mesma e está no repo.

Sem hedge motivacional; nenhuma afirmação de resultado sem intervalo de confiança. Se faltar dado
para responder algo, diga qual coluna ou campo precisa passar a ser coletado.
