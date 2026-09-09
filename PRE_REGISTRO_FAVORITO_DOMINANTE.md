# PRÉ-REGISTRO: Back Favorito Dominante In-Play (Stake Zero / Observação)

> **Data de Congelamento:** 08/09/2026  
> **Autoridade:** GEMINI.md (Seção 2 — Pré-Registro Obrigatório & Stake-Zero)  
> **Status:** `OBSERVACAO_STAKE_ZERO` (stake: 0.0)

---

## 1. A Tese e o Mecanismo de Mercado
- **O Problema Comum:** Dar Back em favorito empatando em 0x0 a odd 1.30–1.40 é perdedor a longo prazo porque a odd esmagada exige taxas de acerto surreais (>75%) e o mercado já sabe que ele é forte.
- **A Tese de Edge:** O mercado comete erros comportamentais (pânico ou desconfiança excessiva) em duas situações:
  1. **Favorito em desvantagem (Trailing Favorite):** O favorito toma um gol isolado/acidental no 1º tempo (0x1 ou 1x2). A odd de Back sobe bruscamente para a faixa de **1.80 a 2.60**.
  2. **Visitante dominante (Dominant Away):** O favorito joga fora de casa, o mercado desconta por causa do mando de campo, mas as métricas in-play mostram amassamento total.
- **O Filtro de Realidade (Opta):** Entramos apenas quando as estatísticas avançadas ao vivo comprovam que o placar adverso ou o empate é mentiroso — o time favorito está criando volume massivo de chances e cercando a área adversária.

---

## 2. Regras Estritas de Entrada (Gatilho)

Uma partida gera sinal com status `PENDENTE` somente se atender a **TODAS** as condições abaixo simultaneamente:

1. **Universo de Favoritismo:**
   - O time deve ser o favorito pré-jogo (Odd pré-jogo $\le 1.65$).
2. **Momento do Jogo (In-Play):**
   - Minuto de jogo entre **30' e 70'**.
3. **Situação do Placar:**
   - **Cenário A (Desvantagem):** Favorito perdendo por exatamente 1 gol (0x1, 1x2).
   - **Cenário B (Empate tardio):** Jogo empatado (0x0 ou 1x1) entre o minuto **40' e 65'**.
4. **Dominância Estatística Comprovada (Opta Live):**
   - $\text{xG}_{\text{fav}} \ge 1.00$
   - $\text{xG}_{\text{fav}} \ge 2.5 \times \text{xG}_{\text{zebra}}$
   - $\text{Chutes no Alvo}_{\text{fav}} \ge 3$
   - $\text{Toques na Área}_{\text{fav}} \ge 15$
5. **Filtro de Odd Mínima:**
   - Odd de Back in-play estimada/registrada $\ge 1.70$ (proibido entrar em odds esmagadas).

---

## 3. Matemática e Avaliação (Lei nº 4)
- **Operação:** Back a favor do Favorito (Home ou Away).
- **Stake:** `0.0` (estritamente observacional).
- **P&L Nominal de Teste (Comissão 5%):**
  - GREEN (Favorito vence o jogo FT): $+0.95 \times (\text{odd} - 1.0)$
  - RED (Empate ou vitória da zebra): $-1.00$
- **Break-even Win Rate (comissão 5%):** $BE = \frac{1}{1 + 0.95 \times (\text{odd} - 1.0)}$. (Ex.: odd 2.30 → BE real é 44,74%, e não 43,48% ingênuo).
- **Odd In-Play:** Medida em tempo real via coletor Betfair (`odd_inplay_real`). Proibido estimar odd fixa.
- **Portão de Validação:**
  - Amostra mínima: $N \ge 200$ sinais.
  - Teste de hipótese: Bootstrap IC95 do ROI excluindo zero.
  - Não alterar parâmetros enquanto a amostragem estiver ativa (proibido overfitting retroativo).
