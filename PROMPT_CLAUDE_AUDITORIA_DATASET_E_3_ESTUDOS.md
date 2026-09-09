Segue o relatório e os scripts do que foi executado e medido no ARKAD hoje (08/09/2026).
Todos os scripts e bases estão fisicamente no repositório para reprodução imediata:
- Base consolidada: `hist_time_stats_expandido.csv`
- Ranking gerado: `ranking_eficiencia_ligas.csv`
- Scripts de teste:
  - `scratch/estudo1_ranking_eficiencia_ligas.py`
  - `scratch/estudo2_regressao_media_xg.py`
  - `scratch/estudo3_arame_liso_lay0x0.py`
  - `baixar_expansao_ligas_xg.py`

Não estou pedindo concordância — quero a sua auditoria implacável, especialmente checando a **Lei nº 1 (odds executáveis)**, **overfitting de threshold** e **controle pelo preço de fechamento**.

---

## 1. O Novo Master Dataset Consolidado (Data Harvesting)

Executamos o `baixar_expansao_ligas_xg.py` focando exclusivamente no Top 20 ligas profissionais de 1ª e 2ª divisões (expurgando divisões amadoras sem cobertura Opta, conforme a Lei nº 6):
- **Total de partidas consolidadas:** **11.500 partidas** (cobrindo 48 ligas profissionais ao longo de 300 dias contínuos, de Nov/2025 a Ago/2026).
- **Com estatísticas profundas completas:** **10.305 partidas (89,6%)**.
- **Com xG oficial Opta:** **7.671 partidas (66,7%)**.
- **Gestão de cota:** Consumidas 3.419 requisições (0 gastas com calendário, 100% lidas do cache `hist_agenda/`). Restam **7.650 requisições** intactas na conta RapidAPI para o logger da VPS `xg-ht` operar o mês inteiro.

---

## 2. Estudo 1: Ranking de Eficiência de Mercado das 48 Ligas
**Script:** `scratch/estudo1_ranking_eficiencia_ligas.py` | **Saída:** `ranking_eficiencia_ligas.csv`

Medimos o Multiclass Log-Loss e o Brier Score do 1X2 e Over/Under 2.5 des-viggados proporcionalmente nas ligas com $N \ge 80$ ($N=11.421$ jogos).
- **Ligas Hiper-Eficientes (Log-Loss < 0.96):**
  `SAUDI ARABIA 1` (0.873), `TURKEY 2` (0.915), `PORTUGAL 1` (0.917), `CHAMPIONS LEAGUE` (0.922), `GREECE 1` (0.937), `BULGARIA 1` (0.941), `ITALY 1` (0.959), `SPAIN 1` (0.959).
  *Leitura:* Ligas com forte dominância de 2 ou 3 clubes têm linhas de fechamento quase perfeitas.
- **Ligas Ineficientes e Paritárias (Log-Loss > 1.04):**
  `PORTUGAL 2` (1.072), `POLAND 1` (1.068), `ARGENTINA 1` (1.058), `URUGUAY 1` (1.054), `FRANCE 2` (1.053), `USA 2` (1.042), `NETHERLANDS 2` (1.042), `GERMANY 2` (1.041), `BRAZIL 2` (1.040).
  *Leitura:* As segundas divisões concentram os maiores erros de calibração do bookmaker.
- **Draw Gaps Sistemáticos:** `SERBIA 1` (+6,01 pp) e `EGYPT 1` (+6,02 pp) apresentaram empates reais substancialmente acima do preço de mercado.

---

## 3. Estudo 2: Regressão à Média (Gols vs xG Residual / Fade da Sorte)
**Script:** `scratch/estudo2_regressao_media_xg.py`

Testamos a hipótese de que o mercado sobre-reage à forma de gols de curto prazo e que comprar o time com azar de finalização ($\text{gols} < \text{xG}$) geraria edge.
- **Metodologia:** Rolling $K=5$ jogos passados com `shift(1)` por time ($N=6.451$ jogos com histórico completo).
- **Resultados:**
  1. Regressão Logística $\text{Vitória Mandante} \sim \ln(\text{Odd\_H}) + \Delta \text{Luck}$:
     Coeficiente $\Delta \text{Luck} = -0.0060$, **$p = 0.841$** (completamente não-significante).
  2. Apenas finalização ofensiva ($\Delta \text{Res\_Off}$): $\beta = -0.0502$, **$p = 0.233$**.
  3. Simulação de compra do time azarado contra a closing line:
     - Limiar de desvio $\ge 0.50$: $N=294$, ROI **$-9.52\%$**.
     - Limiar de desvio $\ge 0.75$: $N=90$, ROI **$-20.92\%$**.
     - Só em ligas ineficientes: $N=29$, ROI **$-39.69\%$**.
- **Veredito:** 🔴 **Falsificada.** O mercado de fechamento já absorve a regressão à média. Apostar a favor da regressão apenas queima o overround.

---

## 4. Estudo 3: Perfilamento Tático "Arame Liso" no Lay 0x0
**Script:** `scratch/estudo3_arame_liso_lay0x0.py`

Testamos se a relação entre Posse de Bola e Toques na Área Adversária (`tbox`) identifica favoritos estéreis propensos a 0-0.
- **Métrica (Rolling $K=5$, `shift(1)`):**
  $$\text{Índice Arame Liso} = \frac{\text{Posse de Bola}}{\text{Toques na Área} + 1.0}$$
- **Universo:** Favoritos Mandantes ($\text{Odd\_H} \le 1.65$) com histórico tático completo ($N = 1.763$).

### 1. Comportamento por Quartil de Arame Liso:
| Quartil | Perfil | Posse Média | Toques Área (`tbox`) | Big Chances | Odd Média | Taxa 0-0 FT | Taxa Under 2.5 |
|---|---|---|---|---|---|---|---|
| **Q1** | Vertical / Incisivo | 58.1% | **34.0** | **3.31** | 1.40 | **4.31%** | 37.4% |
| **Q2** | Moderado | 58.3% | 27.5 | 2.74 | 1.42 | **4.29%** | 40.4% |
| **Q3** | Controle | 59.0% | 23.4 | 2.53 | 1.45 | **4.11%** | 44.1% |
| **Q4** | **Arame Liso** | **59.9%** | **18.4** | **2.01** | 1.47 | **7.26%** | **46.3%** |

*Achado qualitativo:* A posse é idêntica (~58-60%), mas o Q4 infiltra metade na área e cria 40% menos chances. A taxa de 0-0 salta de 4.1% para **7.26% (+76% de aumento)**.

### 2. Backtest Completo ($N=1.763$, spread Lay estimado $1.50\times$):
- Base Total: $N=1.763$ · WR: 95.01% · ROI Lay: **$-6.87\%$**
- Aprovados (Sem Q4): $N=1.322$ · WR: 95.76% · ROI Lay: **$+3.48\%$**
- Vetados (Q4 Arame Liso): $N=441$ · WR: 92.74% · ROI Lay: **$-37.89\%$**

### 3. Validação Temporal Out-of-Sample (OOS):
- **Treino (< 01/04/2026, $N=891$):** Corte calibrado em `arame_liso >= 2.383`.
- **Teste OOS (01/04/2026 a 28/08/2026, $N=872$):**
  - OOS Base Total: $N=872$ · WR: 95.64% · ROI Lay: $+7.10\%$
  - OOS Aprovados: $N=636$ · WR: **96.54%** · ROI Lay: **$+19.70\%$**
  - OOS Vetados (Q4): $N=236$ · WR: 93.22% · ROI Lay: **$-26.85\%$**
  - **Delta de ROI OOS:** **$+46.44\text{ pp}$** ($p = 0.0980$, IC95 $[-19.27\text{ pp}, +121.14\text{ pp}]$).

---

## Perguntas para sua Auditoria:

1. **Lei nº 1 e Sensibilidade de Spread no Estudo 3:**
   Usamos spread de $1.50\times$ sobre a odd de back de 0-0. Na sua medição anterior, a mediana foi $1.56\times$. Se estressarmos para $1.56\times$ ou $1.65\times$, o corte de Arame Liso ainda preserva o delta de ROI positivo no OOS?
2. **Robustez do Limiar (Garden of Forking Paths):**
   O corte no percentil 75 (Q4) foi natural ou há fragilidade? Se testarmos o percentil 70 ou 80, ou janelas de $K=3$ e $K=8$, a taxa de 0-0 continua monotonicamente maior nos favoritos estéreis?
3. **Controle de Preço no 0-0:**
   Na regressão logística $\text{is\_0x0} \sim \ln(\text{odd\_h}) + \text{arame\_liso} + \text{bigch}$, o coeficiente de `arame_liso` deu $\beta = +0.3777$ com $p = 0.105$. Isso indica ruído ou apenas uma amostra de eventos raros que precisa de maior poder estatístico?
4. **Veredito Operacional:**
   Diante dos números, qual deve ser a postura do ARKAD com relação ao "Arame Liso"? Recomenda plugar como flag puramente passiva (`arame_veto: PASS/VETO`) em stake zero no forward do Lay 0x0, ou há algum vício oculto que passou batido?
