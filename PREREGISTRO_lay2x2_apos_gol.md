# Pré-registro — Lay 2x2 in-play depois do primeiro gol (grade congelada)

**Congelado em:** 2026-09-13, antes de qualquer resultado. **Proposta:** Thiago ("quero fazer um estudo no lay 2x2,
quando sai um gol a odd baixa"). Harness: `varredura_lay2x2_apos_gol.py`. Dados: coletor da VPS (runner "2 - 2" do
Correct Score minuto a minuto + linhas O/U para o estado) e placar oficial (`placares_ft.csv`; bases só antes de 12/09).

## 1. Mecanismo

**Por que o mercado ao vivo erraria aqui?** Depois do primeiro gol, a odd de lay do 2-2 cai (o placar fica
"alcançável") e o 2-2 é o placar-símbolo de virada/empate dramático — hipótese: dinheiro recreativo backa o 2-2
após o 1º gol além do que a probabilidade justifica, deixando a odd de lay abaixo do preço justo.

**Falsificável por:** a taxa real de 2-2 final, dado 1 gol no minuto M, comparada ao break-even da odd de lay do
2-2 naquele instante. Se o mercado precifica certo, WR ≈ BE e o lay perde a comissão.

**O que já morreu nessa direção:** Lay 2x2 pré-jogo genérico (≈0, cauda 16,8u); TOP 3 pré-jogo (+1,55% no forward,
IC cruza zero); Lay Draw 60' em favorito empatando (−4,9%, "P(evento) ≠ edge"); under-limite (−3,11%). Nenhum
testou o 2-2 **in-play após o 1º gol** com odd real do instante.

## 2. A regra (congelada)

| campo | valor |
|---|---|
| mercado / runner | Correct Score · **"2 - 2"** |
| lado | **lay** |
| estado do jogo | **exatamente 1 gol** (1-0 ou 0-1), determinado pelas linhas O/U (Over 0.5 batida, Over 1.5 não); divisão casa/fora pelo CS só quando o total coincide |
| janela de minuto (do 1º gol, aproximada pela 1ª captura com 1 gol na janela) | 10-25 · 25-40 · 46-60 · 60-75 |
| favoritismo pré-jogo (menor back do Match Odds ≥3h antes do KO) | ≤1,40 · 1,40-1,80 · >1,80 |
| faixa de odd de lay do 2-2 no instante | **5-30** (fixa) |
| uma aposta por jogo por janela | sim (primeira captura elegível da janela) |
| sub-dimensão gravada mas NÃO célula | quem marcou (mandante/visitante), odd do 2-2 pré-KO (para medir a queda) |

Células = 4 janelas × 3 favoritismos = **12** (+ a mesma grade com estado "1-0" e "0-1" separados = 24, reportadas
mas com BH sobre o total de 36). Célula só entra com **N ≥ 50**.

## 3. Critério (3 vias)

- APROVA: N ≥ 300 **e** piso do IC95 (bloco-dia, ≥6 dias) > 0 **e** reds observados ≥ 5 **e** sobrevive ao BH.
- REPROVA: teto do IC95 < 0.
- INCONCLUSIVO: resto.

P&L: liability 1u, comissão 5%. GREEN 0,95/(odd−1), RED −1. RED = FT exatamente 2-2.

## 4. Dado de julgamento

- **Primeiro olhar (declarado como tal):** coletor 16/08 → 13/09.
- **Julgamento:** só jogos com KO ≥ 2026-09-14. Snapshot 2: **2026-10-12**. Snapshot 3: **2026-11-12**.

## 5. O que NÃO fazer

- Não mudar janelas, faixa, estado ou favoritismo depois de ver resultado. Não somar células.
- Estado nunca pelo CS de menor lay. Nenhum guard dependente do resultado.
