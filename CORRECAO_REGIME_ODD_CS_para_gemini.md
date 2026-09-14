# CORREÇÃO — a base tem três regimes de odd de lay em Correct Score; as auditorias de 12-13/09 precisam ser relidas

> Descoberto em 13/09 ao varrer os 43 mercados de lay. Afeta `AUDITORIA_TOP3_CS_para_gemini.md`,
> `AUDITORIA_RANKING_CS_7MERCADOS_para_gemini.md` e `RESUMO_RANKING_CS_ago2025_para_gemini.md`.
> Reproduzível com os scripts já commitados (basta agrupar por mês).

## O fato

Na base Betfair (apicomunidade, 52.963 jogos), a odd de **lay** dos placares exatos muda de regime ao longo do
tempo, enquanto o Match Odds não muda e o placar final não muda (gols/jogo 2,68 / 2,70 / 2,73; P(0-3) 2,2% em
todos os anos; base bate 100% com a b365).

| meses | lay 0x3 mediana | back 0x3 mediana | spread CS | spread Match Odds | % jogos com lay 0x3 ≤ 40 |
|---|---|---|---|---|---|
| 2024-03 → 2025-12 | 85-280 | 5-18 | **500-4.000%** | 3-6% | 13-24% |
| **2026-01 → 2026-04** | **38-42** | 17-23 | **20-38%** | 2-3% | **50-54%** |
| 2026-05 → 2026-06 | 110-130 | 14-16 | 450-620% | 3-4% | 20% |
| 2026-07 → 2026-09 | 72-90 | 25-28 | 60-70% | 2-3% | 23-28% |

Leitura: em 2024-25 o lado de lay do CS na base é **livro vazio** (odd 100-1000 = ninguém oferecendo). Um jogo que
"qualifica" com lay ≤ 35 nesse regime é um jogo em que alguém já tinha oferecido lay horas antes — uma população
enviesada. Em jan-abr/2026 o snapshot é de **livro cheio** (spread 20-38%, metade dos jogos qualifica). Jul-set é
intermediário. As colunas de lay de CS **não são comparáveis entre regimes**, e "multi-ano" não significa o que eu
disse que significava.

## O efeito nos métodos (funções oficiais dos módulos; liability 1u, comissão 5%)

| método | A · 2024-25 | **B · jan-abr/26** | C · mai-jun/26 | D · jul-set/26 |
|---|---|---|---|---|
| Lay 0x3 TOP 3 | N=1.025, 40 reds, **−0,28%** | N=338, **0 reds**, **+5,64%** | N=71, 1 red, +2,30% | N=99, 3 reds, **+0,57%** |
| Lay 0x3 amplo (sem ranking) | N=1.666, 58 reds, **−0,06%** | N=1.861, 15 reds, **+3,47%** | N=105, 2 reds, +1,59% | N=168, 4 reds, **+1,07%** |
| Lay 2x2 TOP 3 | N=1.277, 77 reds, **−0,78%** | N=337, 12 reds, **+5,96%** | N=108, 4 reds, +1,75% | N=154, 6 reds, **+1,55%** |
| Lay 3x3 (faixa 5-40) | N=97, 3 reds, −0,55% | N=1.760, 23 reds, **+3,10%** | N=38, 1 red, +0,04% | N=60, 1 red, +1,02% |
| Lay 0x2 Zebra | N=18, 0 reds | N=122, 2 reds, +2,71% | — | — |
| Lay 2x0 Zebra | N=32, 2 reds, +1,24% | N=133, 2 reds, +5,64% | N=1 | — |

**Toda a força estatística reportada** (0x3 amplo +1,76% com P(≤0)=0,000; 2x2 TOP 3 2026 +4,06%; 3x3 passando BH;
0x2/2x0 com IC descolado) **vem do regime B**. Fora dele, os métodos são ≈ 0 (regime A) ou +0,6 a +1,6% com N
pequeno (regime D). O regime B é também o mais próximo de uma odd executável (livro cheio), então ele não é "errado"
— mas são 4 meses, e o forward (regime D + feed ≈ odd no KO) é o único dado que mede a mesma coisa que se aposta.

## O que fica retificado

1. **"Sobrevive em 30 meses" (auditoria TOP 3, item 6) — retirado.** Os 30 meses somam dois regimes incomparáveis; a
   afirmação de robustez temporal não se sustenta.
2. **"A regra ampla do 0x3 é o resultado mais forte da auditoria" — condicionado.** É o mais forte **dentro do regime
   B**. Em D é +1,07% com 168 apostas e 4 reds.
3. **"0x2 e 2x0 sem multi-ano" — reforçado.** E o mesmo vale para 3x3 (1.760 de 1.955 em B).
4. **A comparação "kept vs discarded" continua válida** — é dentro do mesmo regime — mas o tamanho absoluto dos
   ROIs deve ser lido por regime, não agregado.
5. **Nenhuma classificação muda** (continuam 🟡/🔴): o gatilho sempre foi o forward, e o forward está no regime D
   com N < 100 em todos.

## O que isso NÃO afeta

- Match Odds, Over/Under, BTTS: spread estável (3-6%) em todos os meses. As reprovações desses mercados (Lay Home,
  Lay Away, Lay Under 1.5 FT, etc.) não dependem do regime.
- O Lay 0x0 XGB pré-registrado usa `Odd_CS_0x0_Lay`, também sujeita a regime (mediana 17-22 em A, 14,5-17,5 em B).
  Fica como pergunta aberta abaixo.
- Liquidação oficial (VPS) e o feed diário: independem da base histórica.

## Perguntas abertas

1. O que mudou na captura da apicomunidade em jan/2026 e reverteu em mai/2026? (hora do snapshot? fonte?) Sem essa
   resposta, qualquer backtest de CS na base precisa ser reportado **por regime**.
2. O Lay 0x0 XGB (+1,17% em 30 meses) — quanto desse número é regime B? Precisa da mesma decomposição antes de o
   próximo relatório citá-lo como multi-ano.
3. Existe alguma fonte de odd de lay de CS pré-KO consistente para 2024-25 (o coletor da VPS só existe desde
   ago/2026)? Sem ela, o histórico de CS se resume a jan-set/2026.
