# Pré-registro — Lay Draw: saída no gol da zebra (regra de saída, não de entrada)

**Congelado em:** 2026-09-16, antes de qualquer resultado forward. **Proposta:** Thiago ("zebra jogando fora, odd do
favorito a 1,36, a zebra faz um gol, não é interessante sair?"). Serviço: `saida_zebra_vps.py` (VPS, alerta no
Telegram no momento do gol). Ledger: `saida_zebra_ledger.csv`. Comparação **pareada** com o mesmo sinal segurado até o fim
(ledger do KO−10).

## 1. Mecanismo e o que a base diz

Nos sinais de Lay Draw (fav ≤ 1,40, lay do empate 4,5–10), quando a **zebra marca o 1º gol** o jogo termina empatado em
**28,0%** (base 2024–26, N=715; favorito em casa 29,4%, N=562; favorito em casa a 1,33–1,40: **32,8%**, N=241) contra 7,8%
quando o favorito marca primeiro. A odd de lay do empate logo após o gol da zebra cai para **mediana 3,80** (coletor,
N=21; pré-jogo 6,40), cujo break-even é 24,6%. Hipótese falsificável: **sair (back no empate) no gol da zebra perde
menos do que segurar**, porque o mercado cobra ~25% para um evento de ~28–33%. Contra-hipótese: o mercado ajusta a
odd por favoritismo/lado e a diferença desaparece na comissão.

**Primeiro olhar (declarado):** 21 casos do coletor (16/08→16/09): saída −0,124u vs segurar −0,059u por 1u de
responsabilidade (só 4 empates em 21 = 19%, abaixo dos 28% da base). N pequeno; não decide.

## 2. A regra (congelada)

| campo | valor |
|---|---|
| sinais elegíveis | todos os Lay Draw do ledger do KO−10 (`forward_ko_ledger.csv`), qualquer lado do favorito |
| evento | 1º gol do jogo marcado pela **zebra** (gols pelas linhas O/U; lado pela direção da odd do favorito no Match Odds, regra de `trader_inplay_core`) |
| ação | back no The Draw na **primeira captura com preço** depois do gol (mercado suspenso ⇒ espera); stake de fechamento = S_in·O_in/O_out (P&L igual em qualquer resultado) |
| se o favorito marca 1º | nada (segura); registrado como informação |
| se o lado do 1º gol for indeterminável | sem ação, registrado `LADO_INDETERMINADO` |
| gravado por evento | odd_in (do KO), odd_out, minuto, lado do favorito, faixa do favorito, pnl_saida (fechado na hora), pnl_hold (no FT) |

P&L por 1u de responsabilidade: saída `S_in·(1 − O_in/O_out)`, `S_in = 1/(O_in − 1)`, comissão 5% só sobre lucro
(aqui é sempre perda parcial, sem comissão); segurar: `+0,95/(O_in − 1)` se não empatar, `−1` se empatar.

## 3. Critério (3 vias, sobre a DIFERENÇA pareada saída − hold)

- APROVA a saída: N ≥ 100 eventos "zebra marcou 1º" com odd de saída observada **e** piso do IC95 (bloco-dia, ≥ 6
  dias) da diferença > 0.
- REPROVA: teto do IC95 da diferença < 0.
- INCONCLUSIVO: resto. Sub-dimensões (lado do favorito, faixa ≤1,25 / 1,25–1,33 / 1,33–1,40) reportadas, não decidem.
- **Julgamento só em KO ≥ 2026-09-17.** Snapshot 12/10 com as outras grades.

## 4. O que NÃO fazer

- Não mudar o gatilho (só o 1º gol, só zebra), não estimar odd de saída, não somar com a saída no gol do favorito.
- Não transformar em regra de entrada ("entrar depois do gol da zebra"): −3,6% pela base (28% real vs 24,6% BE).

---

## Emenda 16/09 (antes de qualquer julgamento) — alternativa "sair no intervalo", só medida

Pergunta do Thiago: é melhor sair no intervalo (esperando o empate/virada no 2º tempo) do que no gol? Base (N=565, zebra
abriu no 1º tempo): no HT a zebra ainda está na frente em 56% (empate FT 36,3%), empatou em 30% (22,8%), favorito virou
em 14% (7,8%). Coletor (N=21): odd do empate no intervalo 3,28 / 4,20 / 4,60 nesses estados (no gol: 3,75).
EV por 1u de risco (entrada 6,6): sair no gol −0,136 · sair no intervalo −0,143 · segurar −0,161 — diferenças menores
que a incerteza da taxa de empate. **Sem alerta novo.** O serviço passa a gravar `HT_ODD` (odd do empate na 1ª captura
de 47–58 min de relógio + estado no HT) nos mesmos jogos, para a comparação pareada gol × intervalo × segurar em 12/10.
