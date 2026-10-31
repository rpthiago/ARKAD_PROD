# CONTEXTO OBRIGATÓRIO — a fonte de odd ao vivo do ARKAD morreu em 30/10/2026

Leia isto antes de propor qualquer método, análise ou coleta. Ele **invalida premissas** que você
usou nas últimas rodadas e muda uma regra metodológica central. Não é um aviso operacional, é uma
mudança de regime.

---

## 1. O fato

A conta Betfair do projeto está permanentemente inacessível. Última captura do coletor:
**30/10/2026 12:03:55 UTC**. O login de certificado responde `API login: ACTIONS_REQUIRED` e **não
vai voltar**.

Causa, pela MP 1.394/2026 (verificada no Senado):

| data | evento |
|---|---|
| 25/09/2026 | MP publicada; novos depósitos proibidos imediatamente |
| 05/10 23:59 | fim do saque voluntário |
| 06/10 | sites e apps devem ficar indisponíveis |
| 09–14/10 | bancos devolvem saldo por CPF; o resto vai para conta na Caixa |
| **25/10** | **extinção formal das autorizações** (30 dias após a publicação) |
| 30/10 12:03 | última captura do coletor |

A MP **não prevê retorno**. Só o Congresso, até ≈23/01/2027, poderia derrubá-la — especulação, não
plano.

**Todas as rotas de acesso foram testadas e estão fechadas.** Não proponha nenhuma delas:

| rota | resposta medida |
|---|---|
| cert `identitysso-cert.betfair.bet.br` | `ACTIONS_REQUIRED` |
| cert `identitysso-cert.betfair.com` | `AUTHORIZED_ONLY_FOR_DOMAIN` — **idêntico com e sem proxy UK**, logo é domínio da conta, não geo |
| interativo `identitysso.betfair.com/api/login` | 302 → `identitysso.betfair.bet.br` |
| interativo `identitysso.betfair.bet.br/api/login` | 302 → `brasilsembets.gov.br` |
| `historicdata.betfair.com` (dados históricos) | 302 → `historicdata.betfair.bet.br` (bloqueado) |
| `support.betfair.com` e `support.betfair.bet.br` | ambos → `brasilsembets.gov.br` |
| `betfair.com` no navegador, com VPN do Reino Unido | não abre (confirmado pelo titular) |

A `.com` te devolve para a `bet.br`, e a `bet.br` devolve para o bloqueio do governo. É um laço
fechado. Trocar endpoint, proxy, VPN ou IP **não pode funcionar**, porque a restrição é o domínio de
registro da conta.

---

## 2. O que está proibido a partir de agora

1. **Não proponha método in-play.** Não existe feed de odd ao vivo. Nenhum.
2. **Não proponha "coletar mais N jogos e reavaliar".** O coletor está congelado em 30/10. A amostra
   nunca cresce.
3. **Não apresente nenhum método como APROVADO.** O protocolo do projeto exige odd de lay **real,
   forward**. Não existe mais forward. O melhor veredito possível hoje é *"medido no acervo, não
   validável"*.
4. **Não proponha rota de reacesso** à Betfair (endpoint, proxy, VPN, conta nova). Está encerrado, e
   furar bloqueio custou conta e saldo uma vez.
5. **Não derive odd de lay de preço de bookmaker.** As fontes que sobraram são de casa de aposta, não
   de exchange. Esse erro já está catalogado duas vezes: a coluna `_Lay` guardando odd de *back*
   (fator 1,39x, que virou +3,22% em −2,05%) e o log de CS gravando odd b365 (fator 1,56x, que virou
   +16,8k em +5k).

---

## 3. A regra metodológica que mudou — a parte mais importante

O acervo Betfair virou um **conjunto fechado**: 44.892.596 linhas, de antes de 17/08 até
30/10/2026 12:03, com `back, back_size, lay, lay_size, ltp` no mesmo instante.

Antes, um achado promissor no histórico tinha para onde ir: validação forward, que é **amostra
independente**. Era isso que permitia garimpar com alguma folga — o erro era corrigível depois.

**Agora não existe amostra independente, e nunca mais vai existir.** Consequência direta: cada
hipótese testada contra o acervo consome um orçamento de multiplicidade que **não se repõe**. Então:

- Permutação sobre a **grade inteira** de fatiamento deixa de ser boa prática e passa a ser a **única
  defesa que existe**. P isolado de uma célula é desinformação.
- Precedente obrigatório desta rodada: no estudo Over 0.5 HT minuto 10, a melhor célula observada deu
  **+16,86pp**, enquanto a **mediana** da melhor célula sob H0, numa busca de 691 células, deu
  **+18,10pp** — ou seja, o acaso rendia *mais* que o achado. **P = 0,589.** O P isolado do combo era
  0,0185. Essa é exatamente a armadilha.
- Toda proposta deve declarar **antes** quantas células/hipóteses o espaço de busca tem. Sem isso, o
  resultado não é avaliável.
- Célula com menos de 5 reds continua não passando, e ROI continua **por liability**, nunca por stake.

---

## 4. Quais dados seguem vivos

| fonte | estado |
|---|---|
| `football_data_odds.csv` | **viva** — atualizou 31/10 06:33 (football-data.co.uk, fora do alcance da MP) |
| `footystats_base_lean.csv` | **viva** — atualizou 31/10 06:00 |
| `Bases_de_Dados_API_FutPythonTrader_Bet365.csv` | **morta desde 12/10/2026** |
| acervo Betfair (44,9M linhas) | **congelado** em 30/10, com backup verificado por sha256 |

As duas vivas são odd de **bookmaker**, pré-jogo. Servem para pesquisa pré-jogo e para liquidação por
placar. Não servem para estimar preço executável de exchange.

---

## 5. Vereditos desta rodada — para o Hall of Shame

- **ML LightGBM para Lay Under 0.5 FT / Lay 0x0 (proposta sua): ARQUIVADO.** Outubro, que era 95% do
  P&L reportado, era **dado ao vivo rotulado como pré-jogo** — a janela `min_to_ko ∈ [−45,−2]` é o
  minuto 2 ao 45 do jogo. Reproduzindo o split exato: **+0,19% por liability, IC95 [−3,58, +3,76]**.
  A "governança por liga" era blacklist de string que só pegou futebol feminino (87 de 3.640);
  `Colombian Primera B` não casa `COLOMBIA B`. E 16 das 37 features eram constantes escritas na mão.
- **Over 0.5 HT in-play minuto 10, Combo A: REPROVADO** por multiplicidade (P=0,589). Fica como
  instrumento o **overround de 7,4pp** do HT in-play, que é medição boa e reutilizável.
- **Convenção `min_to_ko`:** positivo = pré-jogo; negativo = minutos **desde** o apito. No 1º tempo,
  `minuto = -min_to_ko`. O `-15` da fórmula "canônica" do repositório só vale **do 2º tempo em
  diante**. Filtrar `min_to_ko` negativo achando que é pré-jogo entrega dado ao vivo — já aconteceu
  duas vezes.

---

## 6. O que fazer, e o que eu quero de você

Trabalho real que roda **inteiro** no acervo, sem depender de conta nenhuma: a varredura pendente da
semana, o pré-registro limpo do Lay Draw, e a análise de CS **por regime de odd de lay** (2024-25 com
livro vazio vs jan-abr/26 com livro cheio — todo edge de CS vinha do segundo regime, e misturar os
dois fabrica resultado).

Antes de qualquer análise nova, responda:

1. Você entendeu que **não existe mais validação forward** e que, por isso, nenhum método pode ser
   aprovado — só medido?
2. Qual é o orçamento de multiplicidade que você propõe gastar, e em quê? Já que não se repõe, diga
   quantas hipóteses pretende testar no acervo e por quê essas.
3. Quais conclusões suas anteriores **dependiam** de coleta futura ou de odd ao vivo, e portanto
   precisam ser rebaixadas ou retiradas?

Não comece análise nova antes de responder as três.
