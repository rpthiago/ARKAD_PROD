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

---

## Correção de MEDIÇÃO — 2026-09-12 (auditoria B4; a grade não mudou)

O "placar no momento" do snapshot 1 vinha do runner de **menor lay do Correct Score**. Medido contra
as linhas de Over/Under na mesma captura: aos 10-25 min ele bate com o total real de gols em **27,5%**
dos casos (diz *mais* gols em 70%) — é o placar **final mais provável**, não o atual. Quando o jogo
está 0-0 de verdade, o CS diz 0-0 em 28%. Aos 75-85 min ainda erra 13%.

**Estado corrigido:** gols já saídos = linhas de O/U **batidas** (Over L a ≤1,02) ou que **sumiram e
não voltaram** (mercado liquidado após o gol — o Over 0.5 some no 1º gol). É fato, não previsão.
A divisão casa/fora vem do CS **só quando** o total do CS coincide com o do O/U; com 2+ gols sem
divisão confiável a captura não é classificada. Nenhuma informação de resultado entra no estado.

Também testado e **rejeitado** um guard "FT < parcial → fora": removia 9,7% das apostas, todas as
que terminaram com poucos gols — seleção pelo resultado. Não usar.

## Snapshot 1 (REFEITO com o estado corrigido) — 2026-09-12, retroativo 16/08 → 05/09

1.181 jogos, 19 dias, **15.018 apostas**, **101 células** com N ≥ 30. BH sobre M = 101:
**0 PASSA, 7 REPROVA, 94 INCONCLUSIVO**. Melhor p = 0,0027 (Over 3.5 · 25-40 · 1-1 · >1,80, N=45;
limiar 0,0005). Total: gap −1,2pp, ROI −4,0%. Por linha, só Over 1.5 positiva (+1,4pp / +2,3%).
Por estado: 0-0 −1,5pp · 1 gol −0,1pp · 1-1 +0,1pp · diff≥2 −6,4pp.
Tabela: `varredura_over/varredura_over_2026-09-12.csv` (sobrescreve a versão com estado errado).

Família Over 1.5 · 1 gol · fav >1,80, com o estado corrigido: 10-25 −1,0pp (N=258) · 25-40 +3,0pp
(314) · 46-60 +3,7pp (296) · 60-75 +4,3pp (266) · 75-85 +5,8pp (227). Mesmo padrão, N maior. A
H-extra abaixo fica como registrada (janela 25-85), agora medida com o estado correto.

## H-extra — pré-registrada em 2026-09-12, DEPOIS do snapshot 1 (declarado)

**Hipótese única:** *backar Over 1.5 quando já saiu exatamente 1 gol, sem favorito claro pré-jogo
(menor back no Match Odds > 1,80), entre o minuto 25 e 85.*

**De onde veio:** no snapshot 1 essa regra apareceu como cinco células separadas da grade e foi
positiva em quatro (25-40 +2,4pp · 46-60 +5,1pp · 60-75 +3,5pp · 75-85 +3,9pp; 10-25 −0,3pp), N≈200-250
cada, nenhuma sobrevivendo ao BH, todos os ICs cruzando zero. **Somar as janelas agora seria decisão
pós-resultado** — por isso ela entra como hipótese separada, a ser julgada só com dados posteriores a
esta data (liquidação oficial da VPS).

**Regra congelada:** linha Over 1.5 · estado `1 gol` (1-0 ou 0-1) · fav pré-jogo > 1,80 · janela
25 ≤ minuto < 85 · uma aposta por jogo (a primeira captura elegível) · odd de back real · stake 1u ·
comissão 5%.

**Critério (3 vias, no snapshot 2 em diante):** APROVA se N ≥ 300 **e** piso do IC95 (bloco-dia,
≥ 6 dias) > 0 **e** p ≤ 0,05 sem correção (é UMA hipótese, não 98 — mas fica registrado que nasceu
de uma varredura, então o snapshot 3 precisa confirmar de novo com N novo). REPROVA se teto do IC95
< 0. INCONCLUSIVO no resto.

**Dado de julgamento:** somente jogos com KO ≥ 2026-09-13, placar da liquidação oficial. Os 1.193
jogos do snapshot 1 **não contam** para a H-extra.

**Próximo snapshot da grade e 1º julgamento da H-extra:** 2026-10-12.
