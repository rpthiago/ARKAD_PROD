# Pré-registro — Forward OCULTO 3 métodos (odd lay real Betfair)

> **OCULTO.** Nenhuma página do Streamlit importa esta pasta. NÃO mexer no
> `pages/01_🏆_Portfolio_Metodos_Aprovados.py`. Isto é observação stake-zero,
> pré-registrada, separada do portfólio oficial.

**Início:** 2026-09-02 (congelado no fechamento da base 20/08, antes de olhar qualquer resultado novo).
**Fonte da odd:** feed diário Betfair (`get_daily_dataframe("betfair")`) — odd LAY real executável.
**Liquidação:** placar final (`Goals_*_FT`) da base histórica da API quando preencher (~2 semanas).
Para Draw/Home/Over o placar final é autoritativo — NÃO há o bug de false-green do CS
(aquele era exclusivo do UNDER liquidado por reconstrução de CS). Ver [[alerta-under-limite-vps-telegram]].

## Regras CONGELADAS (não re-ajustar)

Captura registra o método BASE + flags de filtro (nada se perde; fatia base vs filtrado na análise).

### 🏠 Lay Home
- **Base:** `Odd_A_Back <= 1.65` (favorito visitante) **E** `Odd_H_Lay ∈ [2, 10]`.
- **Filtro refinado (o achado do scan, 18/100 holders):** `Odd_A_Back ∈ [1.54, 1.65]` (favorito visitante MODERADO).
- **Green (lay ganha):** `Goals_A_FT >= Goals_H_FT` (visitante não perde).

### ✈️ Lay Away (OBSERVAÇÃO — não é candidato)
- **Base:** `Odd_H_Back <= 1.45` (mandante super-favorito) **E** `Odd_A_Lay ∈ [2, 15]`.
- **Green (lay ganha):** `Goals_H_FT >= Goals_A_FT` (mandante não perde).
- **Status:** o scan amplo (600 testes) REPROVOU o Away (1/100 holders = acaso). Entrou só para
  vigiar se o +3,8% da janela 29-30/08 (N=34, real lay) segura fora dela. Não é candidato a
  produção; peso de prova é contra. Ver [[veredito-governanca-triade-forward]].

### 🥇 Lay Over 4.5
- **Base = método:** `Odd_Under25_FT_Back <= 1.50` (jogo de under pesado) **E** `Odd_Over45_FT_Lay ∈ [4, 20]`.
- **Green:** `Goals_H_FT + Goals_A_FT <= 4`.

### 🥉 Lay Draw
- **Base:** `min(Odd_H_Back, Odd_A_Back) <= 1.40` (favorito forte) **E** `Odd_D_Lay ∈ [4.5, 10]`.
- **Filtro refinado (marginal):** `Odd_Over35_FT_Back >= 2.54`.
- **Green:** `Goals_H_FT != Goals_A_FT` (jogo decisivo).

## Unidade de P&L (LAY, comissão 5%)
- GREEN = `+(1 - 0.05)` por stake; RED = `-(odd_lay - 1)` por stake.
- **ROI/liab = soma(pnl) / soma(odd_lay - 1)** (mesma unidade do OOS honesto).

## Critério de veredito (não olhar antes de bater N)
- N ≥ 400 por método OU 5 fins de semana com dado limpo.
- Bootstrap por semana + FDR contra o histórico. IC95 exclui zero → candidato a produção.
- Estável nas 2 metades. Mecanismo plausível. Senão: watchlist / morto.

## Frequência esperada (base FRESH jul-ago, /dia)
- Lay Home base ~1.4/dia | filtrado ~0.9/dia
- Lay Over 4.5 ~0.43/dia (≈1 a cada 2-3 dias — método intrinsecamente raro)
- Lay Draw (fav≤1.40) ~2-3/dia
