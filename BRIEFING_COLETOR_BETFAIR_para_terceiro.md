# Briefing — construir um coletor de odds executáveis da Betfair

Para o Claude de outra pessoa. Este documento é a destilação de um coletor que roda em produção há
meses, com os erros já pagos. Não é teoria: cada seção de "armadilhas" custou dias ou um método
inteiro jogado no lixo.

**Leia a seção 0 antes de escrever código.**

---

## 0. Avisos que precisam ser ditos ao usuário antes de começar

1. **Isto coleta dados. Não aposta.** O serviço deve ser incapaz de mandar ordem: sem função de
   `place_order` no código, sem permissão de aposta na app key se der para separar. Isso não é
   preciosismo — é o que impede um bug de virar prejuízo.
2. **A Betfair encerrou operação no Brasil** (Medida Provisória 1.394/2026). O site brasileiro não
   existe mais e o `.com` recusa acesso do Brasil. Acessar a API por uma saída de internet de outro
   país é **contrário aos termos de uso da Betfair** e está em zona cinzenta regulatória. Diga isso
   ao usuário de forma clara e deixe a decisão com ele; não é algo para enterrar numa nota de
   rodapé.
3. **Risco concreto de conta:** tentativas repetidas de login quando a conta está travada é o jeito
   mais rápido de perdê-la de vez. O disjuntor da seção 5 não é opcional.
4. **Confirme a documentação atual da API antes de implementar.** O que está aqui funcionava na data
   deste documento (outubro/2026); endpoints e limites mudam.

---

## 1. O que o usuário precisa ter antes

| item | observação |
|---|---|
| conta Betfair Exchange | com acesso à Exchange, não só sportsbook |
| **application key** | criada no painel de desenvolvedor da Betfair; existe versão "delayed" (grátis) e "live" (paga). Delayed serve para estudo, mas a odd vem atrasada — para preço executável você quer a live |
| **certificado de cliente** (`.crt` + `.key`) | é o que permite login sem interação humana. Gerado pelo usuário e registrado na conta Betfair |
| um VPS | 1 GB de RAM basta, mas veja a seção 6 sobre isso |
| uma saída de internet onde a Betfair opera | proxy HTTP ou um segundo VPS na região; o custo costuma ser por GB de tráfego, e é esse o gargalo real do projeto |
| um bot do Telegram | para os alertas da seção 5. Resolve o problema de descobrir uma falha três dias depois |

---

## 2. Arquitetura

```
[VPS, roda 24/7]  --(saída de internet de região onde a Betfair opera)-->  [api.betfair.com]
       |
       +--> grava uma linha por cotação num CSV (append-only)
       +--> manda alerta no Telegram quando algo trava
```

Um processo só, em laço, sob `systemd` com `Restart=always`. Sem banco de dados no começo: CSV
append-only é mais difícil de corromper e trivial de inspecionar com `awk`. Migre para banco quando
o arquivo incomodar, não antes.

---

## 3. Biblioteca e chamadas

Use **`betfairlightweight`** (Python). Ela encapsula o login por certificado, que é a parte chata.

```python
import betfairlightweight

trading = betfairlightweight.APIClient(
    username, password, app_key=app_key, certs="/caminho/para/certs"
)
trading.login()           # usa o certificado; não abre navegador
```

As três chamadas que importam:

| chamada | para quê |
|---|---|
| `betting.list_market_catalogue(...)` | descobrir quais mercados existem numa janela de tempo, com nomes de time, horário de início e nomes dos selecionáveis |
| `betting.list_market_book(...)` | o preço. É aqui que vem back/lay com os tamanhos |
| `betting.list_event_types()` | útil só como teste de "a API responde daqui?" |

No catálogo, peça `market_projection` com `COMPETITION`, `EVENT`, `MARKET_START_TIME` e
`RUNNER_DESCRIPTION` — sem isso você recebe IDs numéricos e não sabe de que jogo se trata.

No book, peça `price_data: ["EX_BEST_OFFERS"]`. **`EX_BEST_OFFERS` traz preço com o tamanho
disponível, que é o ponto inteiro do projeto** (seção 7).

Há limite de quantos market ids cabem por chamada de `list_market_book` — consulte a documentação
atual e trabalhe em lotes. E atenção ao **data charge**: a Betfair cobra de quem puxa muito dado
sem apostar na proporção. Monitore isso desde o primeiro dia; não é uma surpresa agradável.

---

## 4. O schema — grave isto, nesta granularidade

Uma linha por **(captura, mercado, selecionável)**:

```
capture_ts, market_type, competition, home, away, ko, min_to_ko,
runner, selection_id, back, back_size, lay, lay_size, ltp, matched
```

| campo | por que existe |
|---|---|
| `capture_ts` | sem o instante da medição você não tem nada. É o campo mais importante do arquivo |
| `min_to_ko` | minutos até o início. Permite reconstruir "qual era o preço 10 minutos antes da bola rolar", que é o momento de decisão |
| `back` / `lay` | os dois lados. Nunca grave só um: o spread entre eles é informação |
| **`back_size` / `lay_size`** | **o dinheiro disponível naquele preço.** Veja a seção 7 |
| `ltp` | último preço negociado. Serve de sanity check contra o book |
| `matched` | volume casado no mercado; proxy de liquidez real |

Mercados que vale coletar desde o início: `MATCH_ODDS`, `CORRECT_SCORE`, `OVER_UNDER_05` até
`OVER_UNDER_45`, `BOTH_TEAMS_TO_SCORE`. Começar amplo e depois cortar é fácil; descobrir seis meses
depois que faltou um mercado é irrecuperável, porque o passado não volta.

---

## 5. As lições de engenharia (cada uma custou caro)

### 5.1 Cache do catálogo
O catálogo muda devagar (nomes de time e horário de jogo não mudam a cada minuto); o preço muda
rápido. Buscar o catálogo em todo ciclo desperdiça a maior parte do tráfego. **Cachear o catálogo e
revalidar a cada N ciclos cortou 41% do tráfego** e o ciclo caiu de 12s para 7s. Como o tráfego é
pago por GB, isso é dinheiro direto.

### 5.2 Disjuntor com espera crescente
Quando a API responde bloqueio, **não tente de novo em seguida, e não faça login de novo**. Espere
5 → 15 → 30 → 60 → 120 minutos. Distinga dois casos:

- **sessão expirada** → re-login é a resposta certa
- **bloqueio da API / geo / conta travada** → re-login é a pior resposta possível

Tratar os dois igual é o caminho mais curto para perder a conta. Já tivemos 48 logins por dia saindo
de um cron antes de alguém perceber.

### 5.3 Repetir o ciclo depois do re-login
Se a sessão caiu no meio de um ciclo, re-logar e **ir dormir** perde a janela inteira. Re-logue e
**repita o ciclo imediatamente**. No nosso caso isso recuperava um buraco de 5 minutos a cada queda.

### 5.4 Falha transitória não é alarme
Rede pisca. Avisar no Telegram a cada piscada treina o usuário a ignorar o Telegram. Só alarme
depois de **3 falhas consecutivas confirmadas**, e no máximo 1 aviso a cada 6 horas. Avise também a
recuperação — saber que voltou vale tanto quanto saber que caiu.

### 5.5 Detecte o erro do intermediário separadamente
Se você usa proxy, as falhas dele (`407`, `tunnel connection failed`, `proxy authentication`) são
diagnóstico completamente diferente de a Betfair ter recusado. Classifique por string de erro e
mande mensagens distintas. Economiza horas de investigação no escuro.

---

## 6. Armadilhas de infraestrutura

- **VPS de 1 GB de RAM:** nunca leia o arquivo inteiro. O nosso tem 5,2 GB e já travou a máquina por
  3 horas quando um `awk` tentou montar uma estrutura de 2 GB junto com duas transferências de
  arquivo. Use processamento em fluxo, `nice`, limite de banda no `scp`, e uma transferência por vez.
- **Serviço do systemd não usa o seu virtualenv.** Confira qual interpretador está no `ExecStart` e
  valide o import com *esse* interpretador. A gente deixou um serviço 33 horas em loop de crash
  porque o `python` do sistema não tinha a biblioteca.
- **Depois de todo deploy:** compile o arquivo, confirme `is-active`, e leia o log. Três comandos.
  Pular isso é como a gente descobriu as 33 horas.
- **O arquivo cresce rápido:** entre 420 e 500 mil linhas por dia no nosso caso. Planeje rotação ou
  compressão antes de o disco encher, não depois.
- **Nunca ponha credencial em arquivo de repositório.** Só em `.env` no servidor, fora do git. A
  gente vazou uma credencial de proxy num `README` commitado e ela ficou sete dias no GitHub.

---

## 7. A lição que justifica o projeto inteiro

**Grave o tamanho disponível junto com o preço. Esse é o projeto.**

A odd que aparece na tela é vitrine. A odd com dinheiro atrás é a que se consegue. A diferença não é
detalhe:

- Um método que parecia render **+4,31%** com preço estimado virou **+0,14%** com preço medido. O
  lucro inteiro era de um preço que não existia.
- Em outro caso, um log gravava a odd de uma casa de aposta em vez da odd executável da exchange —
  fator de **1,56x**. Um resultado de +16,8 mil virou +5 mil.
- Descobrimos que o **piso de liquidez** (ter pelo menos um valor mínimo disponível no lado em que se
  aposta) separava método que funciona de método que não funciona — e que o **spread** não separava
  nada. Sem gravar o tamanho, nenhuma dessas duas coisas é descobrível.

Três regras que seguem disso:

1. **Se o preço não foi medido, o estudo não existe.** Nunca estime o preço de execução por
   multiplicador sobre a odd de outra fonte.
2. **Fixe o instante de decisão antes de olhar resultado.** "Dez minutos antes do início" é uma
   escolha defensável; "o melhor preço do dia" é garimpo.
3. **Teste se o preço transfere.** Pegue a odd na sua base de referência e a odd no instante de
   decisão e correlacione. No nosso caso: odds de placar exato transferem (correlação 0,64), o spread
   não transfere nada (correlação 0,001). Uma dessas duas coisas virou filtro; a outra foi descartada.

---

## 8. Ordem de implementação sugerida

1. **Teste se a API responde** da saída de internet escolhida, antes de qualquer código de coleta. Um
   `curl` em `listEventTypes` já diz se o caminho funciona ou se há bloqueio no meio. Distinga as
   respostas: bloqueio de CDN, bloqueio regulatório e resposta real do servidor são três coisas.
2. Login por certificado funcionando, sem navegador.
3. Um ciclo só: catálogo de uma janela curta → book → imprime na tela. Nada de arquivo ainda.
4. Grave em CSV com o schema da seção 4.
5. Laço, `systemd`, `Restart=always`.
6. Disjuntor e alertas do Telegram (seção 5). **Antes** de deixar rodando sozinho.
7. Cache de catálogo, quando o tráfego começar a doer.
8. Só então pense em estratégia. Instrumento primeiro.

---

## 9. O que não fazer

- Não coloque lógica de aposta no coletor. Separe os processos.
- Não comece pela estratégia. Seis meses de dado bem medido vale mais que seis meses de backtest em
  preço de fechamento de base pública.
- Não confie em placar vindo do próprio coletor de odds. O nosso erra 27% — para liquidar resultado,
  use fonte dedicada.
- Não valide um método numa fonte de dados e opere em outra.
- Não deixe o processo tentando login em laço. Repetindo porque é o erro que custa a conta.

---

*Destilado de um coletor em produção em outubro/2026. Confirme a documentação atual da Betfair antes
de implementar, e trate a seção 0 como parte do entregável, não como ressalva.*
