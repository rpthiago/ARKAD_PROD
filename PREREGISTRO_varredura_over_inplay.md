# Pré-registro — Varredura Back Over in-play (grade congelada)

**Congelado em:** 2026-09-12, antes de qualquer resultado ser visto.
**Pedido:** "Sem dado olhar por exemplo xG ou outra feature, podemos montar mais de 100." /
"Não quero que você ache antes de testar."

## A grade (não muda depois de congelada)

Cada célula é uma hipótese: *backar Over L quando o jogo está no estado S, na janela de minuto J,
com favorito pré-jogo F, tem WR acima do break-even da odd de back real.*

| dimensão | valores |
|---|---|
| **linha L** | Over 0.5 · 1.5 · 2.5 · 3.5 |
| **placar S** | `0-0` · `1 gol` (1-0/0-1) · `1-1` · `diff>=2` · `empate 2-2+` |
| **janela J** (minuto) | 10-25 · 25-40 · 46-60 · 60-75 · 75-85 |
| **favorito pré-jogo F** | `<=1.40` · `1.40-1.80` · `>1.80` (menor entre back mandante/visitante no MO, última captura pré-KO) |

Células impossíveis (linha já batida pelo placar) são descartadas. Célula só entra na tabela com
**N >= 30**. Não há filtro de liga, xG, ou qualquer feature além de odd e estado.

## Como cada aposta é avaliada

- **Odd:** odd de **back** do runner "Over L" no coletor, na primeira captura dentro da janela J.
  Não se assume odd; sem captura na janela, o jogo não entra na célula.
- **Estado S:** runner de Correct Score com menor lay na mesma captura (placar corrente).
- **Resultado:** total de gols no FT > L. Placar FT de fonte externa (liquidação oficial Betfair
  quando existir; senão base Betfair/b365 com placar exato). **Nunca** a reconstrução pelo coletor.
- **P&L:** stake = 1u, comissão 5%: GREEN `(odd−1)×0,95`, RED `−1`. Break-even `1/(1+(odd−1)×0,95)`.

## Estatística (Lei nº 4 + protocolo honesto)

- Por célula: N, WR, break-even médio, gap (pp), ROI, IC95 por **bootstrap de bloco por dia**
  (mínimo 6 dias), p unilateral (ROI ≤ 0).
- **Correção:** Benjamini-Hochberg sobre **M = número de células com N >= 30** (todas, de uma vez).
  Guarda direcional: célula com ROI negativo entra com p = 1.
- **Decisão de 3 vias por célula:** PASSA (sobrevive ao BH e piso do IC95 > 0) · REPROVA
  (teto do IC95 < 0) · INCONCLUSIVO (resto).

## Dados

- Retroativo: coletor 16/08 → 05/09 casado com placar externo. Primeiro snapshot.
- Daí em diante: liquidação oficial da VPS (`placares_ft.csv`) alimenta o mesmo harness.
- Snapshots datados em `varredura_over/varredura_over_<data>.csv`. A grade é a mesma em todos.

## O que NÃO fazer

- Não adicionar célula depois de ver resultado. Não mudar janelas nem faixas.
- Não ler "PASSA" em célula que passa hoje e não passa no próximo snapshot — o critério é
  sobreviver em snapshots sucessivos com N crescente.
