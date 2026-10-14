# Pré-registro — Lay 0x3 com teto de odd 30

**Data do congelamento: 13/10/2026.** Escrito antes de qualquer alteração no robô.
A partir daqui, nada nesta regra pode ser ajustado olhando o resultado. Se algum parâmetro mudar,
o relógio zera e este documento é substituído por outro, com nova data.

## A regra, congelada

```
GOVERNANÇA (sinais_ko_core.eh_jogo_ignorado): bloqueia feminino, seleções,
                                              divisão baixa e ligas periféricas
ENTRADA:   Odd_Under25_FT_Back  <= 2,10
           Odd_A_Back           >= 1,85     (visitante não é favorito)
           14,00 <= Odd_CS_0x3_Lay <= 30,00  ← teto 30, não 35
EXECUÇÃO:  liquidez no lay >= R$ 200        (mesma trava do Lay Draw/Home)
MERCADO:   Correct Score, lay no placar 0-3
GREEN:     qualquer placar que não seja exatamente 0-3
```

Sem filtro de Top 3 — a evidência não separa o suficiente para justificar cortar 75% do volume.

## O que foi medido antes de congelar

Base da API (`.cache_base_betfair.csv`), **só 2026**, com a governança aplicada:

| cenário | N | WR | break-even | ROI/liability | IC95 bootstrap-dia |
|---|---:|---:|---:|---:|---|
| ampla (teto 35) | 2.084 | 98,94% | 95,98% | +3,11% | [+2,61%, +3,58%] |
| **ampla, teto 30** | **1.650** | **99,33%** | **95,69%** | **+3,82%** | **[+3,39%, +4,19%]** |
| Top 3 do dia | 512 | 99,22% | 95,27% | +4,17% | [+3,24%, +4,90%] |
| fora do Top 3 | 1.572 | 98,85% | 96,21% | +2,76% | [+2,08%, +3,32%] |

A odd **transfere** da base para a execução: pareando 1.247 jogos entre a base e o coletor no KO−10,
a correlação é **0,643**, a razão mediana KO/base é **1,000**, e **90% dos jogos que a base coloca na
janela continuam na janela** no momento da entrada. (Contraste: o filtro de spread tinha correlação
0,001 entre base e KO−10 — por isso este é aplicável e aquele não era.)

O teto 30 vem de a faixa 30–35 dar **−1,37%** no coletor (71 jogos) e o ganho se confirmar na base
(+3,11% → +3,82%). O teto foi escolhido vendo o dado — por isso este pré-registro existe.

## O que me preocupa, registrado antes

**O edge está decaindo dentro de 2026.** Primeira metade: +3,38% em 1.825 jogos. Segunda metade:
**+1,14% em 259**, com IC incluindo zero. O fluxo caiu para um sétimo e o edge para um terço.
É o mesmo padrão do Lay Away Fortaleza, que existiu em 2024-25 e virou zero em 2026.

**O break-even é 95,7%.** Um red a mais a cada 23 jogos zera o ganho. É risco de cauda, e foi
exatamente o motivo pelo qual o método foi desativado em 03/10/2026.

**A convenção importa.** Com odd média ~25, cada green rende ~0,04 u e cada red custa 1 u.
Não existe "sequência boa" que compense dois reds seguidos fora da hora.

## Critérios declarados agora

| | |
|---|---|
| **N-alvo** | 600 entradas liquidadas |
| **Tempo esperado** | no ritmo da 2ª metade de 2026 (~57 sinais/mês), cerca de 10 meses |
| **Stake** | zero / micro. Não entra em banca real antes do N-alvo |
| **Parada por perda** | acumulado abaixo de −3% por liability a qualquer momento após 150 entradas |
| **Parada por inviabilidade** | menos de 20 sinais por mês durante 3 meses seguidos |
| **Sucesso** | ROI por liability com IC95 inteiramente acima de zero em N >= 600 |
| **Métrica** | ROI sobre liability: green `(1−c)/(odd−1)`, red `−1`. Nunca por stake |
| **Comissão** | 5%. Se a comissão real do OrbitX for confirmada em 3%, recalcular — não re-congelar |

## O que NÃO pode acontecer

- Mexer no teto de 30, na janela de 14, no piso de liquidez ou no filtro de Under 2.5 durante o teste.
- Adicionar Top 3, blacklist de liga ou qualquer corte novo depois de ver reds.
- Comparar com o desempenho antes de 13/10/2026 como se fosse continuidade: aquilo é amostra de
  treino para o teto de 30.

## Por que mesmo assim vale testar

É o método com a melhor evidência estatística do portfólio hoje: 1.650 jogos, IC95 que exclui zero com
folga, e odd confirmada como executável. O que falta não é evidência histórica — é saber se o edge
ainda existe depois de o mercado ter passado nove meses olhando para ele.
