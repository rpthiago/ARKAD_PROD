# PROMPT PARA O CLAUDE — Auditoria da Bateria de 4 Modelos com Dados do FotMob/Opta

> **Instruções para o Claude:** Você é o engenheiro responsável pela governança estatística e infraestrutura do ARKAD. A pedido do usuário, o Antigravity (Gemini) explorou 4 hipóteses/modelos usando os dados históricos limpos que você baixou hoje (`hist_ht_stats.csv`, `hist_time_stats.csv` e o cache `hist_stats_ft/`). Queremos sua auditoria forense sobre o código, os números e a viabilidade prática.

---

## 1. O que foi Testado & Os Scripts no Repositório

Todos os testes foram executados com código reproduzível salvo em:
- `scratch/testar_todos_os_modelos.py` (M1, M2, M3 e visão geral do M4)
- `scratch/test_veto_lay0x0.py` (M4 na amostra inteira)
- `scratch/test_oos_veto.py` (M4 em Split Temporal estrito Out-of-Sample com Bootstrap 10.000)

Três modelos foram **empiricamente refutados** e um modelo apresentou **sobrevida no Out-of-Sample**.

---

## 2. Os 3 Modelos Refutados (Confirmação de Hipóteses Mortas)

### Modelo 1: "Ghost Game" In-Play (Falso 0-0 no Intervalo)
- **Tese:** Jogo 0-0 no HT com $xG_{1T} \ge 1.0$ produz mais gols no 2º tempo (Over 1.5 / Over 2.5 FT) do que jogo 0-0 morno ($xG_{1T} < 0.5$).
- **Base:** `hist_ht_stats.csv` (N=273 jogos limpos de 1º tempo que chegaram 0-0).
- **Medição:**
  - $xG_{1T} \ge 1.0$ (N=116): média de **1,53 gols** no 2º tempo.
  - $xG_{1T} < 0.5$ (N=46): média de **1,59 gols** no 2º tempo.
  - Teste t de Welch: $t = -0,22, \mathbf{p = 0,8278}$.
  - Taxa de 0-0 FT mantido: 20,7% vs 19,6%.
- **Veredito:** Confirmado o seu achado de $r = -0,026$. Ter tido xG alto no 1º tempo não aumenta a taxa de gols do 2º tempo. O futebol tem independência estocástica entre os tempos após os ajustes táticos do intervalo.

### Modelo 2: Assimetria de Ligas (Elite vs Secundárias)
- **Tese:** O mercado pré-jogo erra mais na precificação do Over 2.5 em ligas secundárias (Brasil Série B, Colômbia, League One) onde há menos volume institucional.
- **Base:** `hist_time_stats.csv` (N=5.567 jogos com xG limpo: 1.194 Elite vs 4.373 Secundárias).
- **Medição:**
  - Ligas Elite: correlação Preço Mercado x Over 2.5 $= \mathbf{0,173}$.
  - Ligas Secundárias: correlação Preço Mercado x Over 2.5 $= \mathbf{0,191}$.
- **Veredito:** O mercado da Betfair precifica o Over/Under com a mesma eficiência estrutural na Série B do Brasileirão ou na Colômbia. Não há assimetria de liquidez explorável.

### Modelo 3: O "Goleiro Herói" no 1º Tempo
- **Tese:** A equipe cuja zaga segurou um bombardeio no 1º tempo ($xG \ge 1.2$ e $SOT \ge 4$ sem sofrer gol) colapsa fisicamente no 2º tempo, sofrendo mais gols.
- **Base:** `hist_ht_stats.csv` (N=49 jogos no perfil "Goleiro Segurou" vs N=224 normais).
- **Medição:**
  - Goleiro Segurou no 1ºT: média de gols no 2ºT $= \mathbf{1,55}$ | Over 1.5 FT $= \mathbf{44,9\%}$ | 0-0 final $= \mathbf{18,4\%}$.
  - Jogo Normal no 1ºT: média de gols no 2ºT $= \mathbf{1,61}$ | Over 1.5 FT $= \mathbf{46,0\%}$ | 0-0 final $= \mathbf{20,1\%}$.
  - Teste t: $t = -0,29, \mathbf{p = 0,7741}$.
- **Veredito:** A zaga não colapsa. O técnico fecha as linhas no intervalo e a taxa de gols do 2º tempo segue a média histórica.

---

## 3. O Único Sobrevivente: Modelo 4 (Filtro de Veto no Lay 0x0 via xG Open)

Em vez de criar um método novo para apostar, testamos o uso do **$xG_{\text{open}}$ (xG de bola rolando)** como um **Gatekeeper / Filtro de Veto** para a nossa principal estratégia ativa: o **Lay 0x0**.

### Racional do Mecanismo
O Lay 0x0 morre quando o jogo termina 0-0 (RED). Times que criam perigo real em bola rolando raramente empatam em 0-0, enquanto times anêmicos que dependem de bola parada sofrem apagões ofensivos frequentes. O mercado da Betfair na faixa de odds 10 a 20 precifica o 0-0 por favoritismo genérico, sem penalizar adequadamente a anemia em bola rolando.

### Teste na Amostra Completa (N=2.169 jogos na faixa do Lay 0x0, odds 10-20)
- **Base sem filtro:** N=2.169 | 123 Reds (5,67%) | ROI sobre liability $= \mathbf{+0,82\%}$.
- **Vetados ($r\_xg\_open < 1.80$):** N=844 | 64 Reds (**7,58%**) | **ROI $= \mathbf{-1,04\%}$ (Prejuízo!)**.
- **Aprovados ($r\_xg\_open \ge 1.80$):** N=1.325 | 59 Reds (**4,45%**) | **ROI $= \mathbf{+1,87\%}$ (Mais que dobrou o ROI)**.
- **Impacto:** Cortou **64 de 123 REDS (52,0% dos reds eliminados)**.

### Validação Out-of-Sample Estrita (Split Temporal 50/50)
- **Treino:** Jogos até 2026-03-22 (N=1.072). Corte de $r\_xg\_open \ge 1.80$ congelado no treino.
- **Validação:** Jogos posteriores a 2026-03-22 (N=1.097 jogos reais cronologicamente posteriores).

```
======================================================================
RESULTADO NA VALIDAÇÃO OUT-OF-SAMPLE (N = 1.097)
======================================================================
Base Sem Filtro (OOS):     N = 1.097 | Reds = 60 (5,47%) | ROI = +0,77%

Com Filtro de Veto (>= 1.80):
  Aprovados:               N =   658 | Reds = 31 (4,71%) | ROI = +1,43%
  Vetados pelo Filtro:     N =   439 | Reds = 29 (6,61%) | ROI = -0,36% (Prejuízo evitado!)

  Reds Cortados:           29 de 60 REDS (48,3% dos reds eliminados no OOS)
  Delta de ROI:            +0,66 pp
  Bootstrap 10k IC95:      [-0,45 pp, +1,87 pp] | P(<= 0) = 0,1275
```

---

## 4. O que Queremos da sua Auditoria

1. **Audite o código e a replicação:** Rode `scratch/test_oos_veto.py` e confira se há qualquer vazamento temporal ou viés de look-ahead na construção do `r_xg_open` (`shift(1)` com rolling de 8 partidas).
2. **Avaliação Estatística do M4:**
   - O ganho de ROI fora da amostra foi de $+0,66\text{ pp}$ (de $+0,77\%$ para $+1,43\%$), cortando quase metade dos reds (29/60).
   - Porém, o IC95 no bootstrap OOS é $[-0,45\text{ pp}, +1,87\text{ pp}]$ e o $P(\le 0) = 0,1275$ (não bate o limiar tradicional de $p < 0,05$).
   - Na sua leitura: isso é um **efeito mecânico genuíno** que merece rodar como sombra no `forward_0x0/`, ou você considera que o corte de $1.80$ ainda pode ser ruído de amostra?
3. **Decisão sobre a API:**
   - Se o M4 for viável, a assinatura da RapidAPI (US$ 9,99/mês) ganha um propósito concreto: servir de validador de xG para os 2-3 picks diários do Lay 0x0 gerados pelo `forward_0x0/gerar_picks_dia.py` (consumindo menos de 100 requisições/mês!).
   - Você concorda em plugar essa checagem em modo observação (`xg_veto: PASS/VETO`) no logger diário do 0x0?
