# GUIA DE CONTEXTO: Workshop de Desenvolvimento Assistido por IA - Sistema Estuda Fácil

> **Instrução para o Gemini:** Este documento contém a transcrição completa, literal e sem cortes de um workshop de desenvolvimento de software assistido por inteligência artificial (1h53min de duração). O instrutor ensina a estruturar e desenvolver um SaaS completo em Django chamado **"Estuda Fácil"** utilizando agentes de IA (Cursor, Open Code e Codex) e a filosofia de "Superpowers" (habilidades verificáveis).
>
> Use este material como base de conhecimento (grounding) para me ajudar a:
> 1. Estruturar a arquitetura do projeto Django conforme as regras do instrutor.
> 2. Implementar os prompts e os arquivos de configuração do workflow de agentes (`agents.md`, `PRD.md`, `tasks.md`, `worklog.md`).
> 3. Configurar os clientes de IA (Open Code, Codex e Cursor) para trabalharem de forma síncrona.
> 4. Desenvolver as funcionalidades do sistema "Estuda Fácil".

---

## PARTE 1: Introdução, Contexto do Workshop e o "Vibe Coding"
**[0:00 - 3:58]**

Agora coloquei de novo. Então, pronto, agora sim. Bom dia a todos, né? Prazer estar com vocês nesta manhã de sábado, né? Tanto nesse sábado quanto no outro, a gente vai dar continuidade ao nosso workshop de desenvolvimento assistido por inteligência artificial. Eh, é um prazer, né, estar aqui com vocês.

Eh, o objetivo desse segundo workshop é a gente preencher, né, os vazios, né, os problemas que a gente teve ao longo desse período, né, desde quando surgiu esse boom de inteligência artificial, né, bem como essas várias ferramentas. A gente ficou meio que perdido, né, com que ferramentas utilizar, né, como eu até falei ontem no grupo do Telegram, no grupo do WhatsApp.

Eh, quando eu montei a ideia do primeiro workshop, eu utilizava duas ferramentas que eram fantásticas, né? Uma era o antigravity, que era gratuito na época, tinha excelentes limites pro Claude, tinha o Gemini, sempre a versão mais recente. E aí eu resolvi lançar o workshop apresentando ele e o Trey, que era uma outra ferramenta que eu utilizava também na época.

Só que durante o período, né, da abertura das inscrições até o workshop propriamente dito, o antigravity ele deixou de ser gratuito e passou a ter limites irrisórios, né, horríveis os limites. E o Trey também mudou a forma de precificação, né, ao invés de ser requisições, ele passou a ser por tokens, né? E aí, né, eu tentei naquele workshop apresentar as duas ferramentas e acabei utilizando o GitHub Copilot, que era a principal ferramenta naquele momento.

Então a gente foi e montou o curso, né, estruturamos o curso, tivemos acho que quatro ou foi cinco aulas de 12 ou era acho que era 12 projetos, né? A gente fez três projetos e o GitHub Copilot também sofreu alteração, né? Na época o GitHub Copilot, o plano de $10, ele trazia todos os modelos, inclusive o Claude, o Sonnet, o Claude Opus. E a gente tinha bons limites de requisição também. Eles mudaram a proposta para trabalhar com a tokenização e eles bloquearam o acesso a novos usuários, né? Nem tanto a questão dos tokens, mas o bloqueio a novos usuários fez com que aquele projeto também deixasse de ser viável pra gente.

Então eu comecei a trabalhar com outras alternativas, né? Inclusive na própria comunidade eu apresentei o Open Code, né? O Open Code ele é uma ferramenta open source que traz diversos modelos de inteligência artificial e são planos de baixo custo, podemos dizer assim. E eles ainda têm, né, o modelo de requisições, né? Cada requisição você sabe exatamente quantos tokens tá utilizando e você pode utilizar o plano gratuito, né, que tem alguns modelos que são muito bons. Você pode trabalhar com um bom limite durante as 5 horas de utilização.

E a gente também tem o plano GO, que é o plano melhor custo-benefício, né? Você paga $5 no primeiro mês e...

**[3:58 - 4:47]**

...$10 a partir do segundo mês e você tem acesso a modelos extremamente bons, extremamente conhecidos também, né? E você tem o plano Zen, onde você coloca os modelos principais, né? A mesma lógica, você bota lá $20 e vai utilizando os $20 nos outros modelos. Então essa é a ferramenta, né, que vai ser o carro-chefe desses nossos dois dias.

Mas também vou trabalhar com o Codex, né, e o Cursor, mas especificamente hoje, né, eu vou apresentar somente o Open Code e o Codex, porque essa semana o GPT também mudou a forma de precificação.

**[4:47 - 5:42]**

Não de precificação, não, a forma de a gente que tinha... quem tem o plano pago, né, eh, estava fora da janela de 5 horas, né, e agora eles colocaram, né, essa janela de 5 horas e tiraram a possibilidade da gente utilizar direto, né, que era o que a gente estava fazendo, né? Então a gente, por exemplo, "ah, acabei minha cota semanal", aí eu ia lá e dizia: "Pronto, eu já quero utilizar a cota da próxima semana". E assim a gente estava conseguindo trabalhar, e eles tiraram isso.

Mas, né, quando for segunda-feira, o meu limite volta e aí a gente volta a trabalhar, trabalha com Codex na semana que vem. Mas eu vou mostrar para vocês também o Codex, né, para vocês conhecerem, tá certo? Conhecer, assim, vocês já conhecem, né? Mas eh pra gente trabalhar aqui também, tá certo?

## PARTE 2: Estrutura do Workshop, "Vibe Coders" e Filosofia de Desenvolvimento com IA
**[5:42 - 6:52]**

Então, eh qual é a minha ideia para a nossa aula, né, para o nosso workshop? Eh, a gente trabalhar, né? Deixa eu só fechar aqui, botar aqui para baixo, é a gente trabalhar, né, com a... a gente vai estruturar o nosso projeto, né? Coloquei aqui, né, o plano do jogo: do contexto ao plano executável.

Então, qual vai ser a nossa motivação, né? Eu não diria assim motivação especificamente, né? Qual seria o nosso fio condutor, né? O que é que a gente vai fazer, né? Por que a gente vai desenvolver utilizando agente de IA hoje em dia, né? Tá muito na moda, né? Desenvolver com, né? Todo mundo, não é? Você que, pelo menos a gente que está muito ligado à área de tecnologia, a gente vê muito, né? Eh, principalmente no Instagram, né?

**[6:52 - 7:58]**

As pessoas dizendo assim: "Ah, desenvolva seu SAS, SAS para cá, SAS para lá, né? Venha, né? Não precisa nem saber programar, né? Você consegue desenvolver a sua aplicação e vender e ganhar dinheiro."

Isso é verdade até certo ponto, né? Porque assim, o que é que é desenvolver um software, né? Desenvolver um sistema é a gente transformar uma intenção, né, uma ideia, né, em uma coisa prática, né, em uma coisa pronta, em um artefato, né?

Então, eh, quando a gente vai desenvolver um software com agentes de IA, né, a gente tem uma ideia. Essa ideia ela vai virar um documento de requisitos. Esse documento vai virar um plano de tarefas, esse plano vai virar um código e o código vira uma implantação.

Na prática, né, o que que esses vibe coders fazem?

**[7:58 - 8:52]**

Eles têm uma ferramenta que por trás dela tem vários agentes que você simplesmente joga sua ideia e ele vai desenvolver para você, né? Só que qual é o grande problema disso? O usuário ele praticamente ele não tem controle do que está acontecendo ali por trás, né? Se a gente for pensar, né, fazendo da maneira certa, né, cada coisa que a gente vai implementar, a gente precisa de uma ferramenta diferente, né?

Então, por exemplo, para criar o sistema Django, eu preciso ser especializado em Django, né? Para fazer o front-end, né, o HTML ou sei lá, utilizando o NextJS, o CSS, por exemplo, eu preciso de conhecimento de JavaScript, de HTML. Ah, para desenvolver a parte da modelagem, né, eu preciso saber Python, ciência de dados.

## PARTE 3: O Sistema "Estuda Fácil" e o Uso de Agentes de IA
**[8:52 - 10:01]**

Então, eh, eu preciso, né, para ser um desenvolvedor completo, ter todos esses conhecimentos, né? Mas se para algum caso eu não tenho, né, eu terceirizo para a IA. E é isso que essas ferramentas de vibe coding fazem. Só que você não tem controle sobre o que elas estão fazendo, né?

Então, a minha ideia aqui é o quê? É que a gente possa desenvolver, né, um sistema Django, né, um sistema Django para que a gente possa ser um, né, ter um sistema de estudo, né? Um sistema de estudo onde a gente possa aprender inteligência artificial, aprender ciência de dados e a partir de fontes que você vai informar, né? A gente vai colocar uma IA dentro do sistema para desenvolver isso, né?

Então, o nosso sistema vai ser um modelo que eu chamei de **Estuda Fácil**, né?

**[10:01 - 11:06]**

Para isso, né, a gente vai precisar dos agentes de inteligência artificial. E o que que é um agente de inteligência artificial? Vai ser um agente que vai unificar todas essas etapas, né?

*(Interrupção técnica do aluno)*
— Fala, Rafael. Bom dia. Dá uma ajustada no teu vídeo aí que tá cortando o slide e tua imagem.
— Certo. Certo. Vou ver aqui o que que tá acontecendo. O slide tá fixo, né?
— Tá fixo, mas tá um pouco cortado.
— Ah, sim. É por causa do meu quadradinho, né? Ah, não, tá certo. Deu certo. Aqui já achei o problema já. É na minha imagem aqui.
— Ah, beleza. Beleza.
— Mas eu vou diminuir eu aqui para eu ficar aqui bem pequenininho no cantinho. Pronto. Pronto. Tá aqui, Dio.

Então, eh, beleza, né? Então, o nosso curso ele vai ter basicamente isso.

**[11:06 - 12:06]**

A gente vai ter dois tipos de análise, né? A gente vai começar hoje a desenvolver o **Estuda Fácil**, né? Que qual é a minha ideia? A minha ideia é que a gente crie um sistema Django, que a gente possa utilizar ele como se fosse uma ferramenta de estudo. Ah, por exemplo, "ah, eu quero aprender Arduino", então eu vou, forneço materiais para ele e ele vai criar sistemas de estudo.

Ele vai montar o material em texto, ele vai montar questões, vai montar slide, vai fazer... ou então vai preparar esse material para me jogar no notebook, no Gemini Notebook, para montar materiais de estudo, né?

E a minha ideia é colocar um modelo de inteligência artificial dentro dessa ferramenta para que ela possa ver o nosso desenvolvimento à medida que a gente responde essas perguntas.

**[12:06 - 13:08]**

Então, a minha ideia é a gente criar esse sistema, né, nessas duas aulas. E aí, ao final da próxima da da aula que vem, né, é a gente ver como é que a gente pode utilizar os agentes para trabalhar com ciência de dados, né? Vou pegar uma base de dados de futebol e vamos fazer uma análise utilizando data science e machine learning, e vocês verem, né, como que agentes especializados eles conseguem fornecer pra gente, né, uma ajuda muito grande que vai otimizar o nosso processo de desenvolvimento ou de aprendizado, tá certo?

Coisa que quando o pessoal fala, né, de vibe coding, né, o pessoal trabalha com essas ferramentas sem se preocupar, né, com o que acontece por trás, né? A gente não tem essas informações, esse cuidado, tá certo?

## PARTE 4: Vantagens de um Agente de IA para Programação
**[13:08 - 14:15]**

Então, vamos lá, deixa eu só ver aqui. Perfeito, né? Então, por que a gente gerar um agente de IA, né, um agente de inteligência artificial de programação? No nosso caso, ele unifica algumas etapas, né?

1. Ele lê o repositório, né?
2. Ele conversa em linguagem natural.
3. Ele executa ações, né? Por exemplo, ele pode criar um arquivo, ele pode rodar um comando.
4. Ele pode delegar para papéis especializados, né?

Você ganha não apenas com velocidade, né, mas com continuidade, né? Então, por exemplo, o mesmo comando orienta o agente lá do planejamento, né, que a gente chama de briefing ou roadmap, até o deploy.

Então, a ideia desse workshop é o quê?

**[14:15 - 14:52]**

É como fazer, né, os nossos clients diferentes, que no caso eu vou utilizar o Open Code, vou utilizar o Codex, vou utilizar o Cursor para que eles trabalhem no mesmo projeto, né, com os mesmos papéis, né, com as mesmas regras, com a mesma memória, sem perder o controle do escopo, né, sem perder o fio da meada, né, como a gente diz.

Por qual é a grande limitação disso, né? Por que que eu quero fazer isso? A minha ideia é, por exemplo, eu estou aqui desenvolvendo o sistema, né? Esse sistema que a gente vai desenvolver é um sistema grande, né? Eu estou pensando alto, eu ainda não desenvolvi, né? A gente vai fazer aqui ao vivo, né, com vocês, e eu quero fazer com Open Code inicialmente.

**[14:52 - 15:52]**

Aí digamos que em determinado momento o Open Code acaba, [risadas] acaba o limite, né, de 5 horas. Aí o que é que eu faço? Eu fecho o Open Code e abro o Codex. Aí vou trabalhar no Codex.

Vou trabalhar no Codex. Acabou o limite de 5 horas do Codex, eu fecho o Codex, abro o Cursor e continuo o meu trabalho, né, professor? Então eu preciso ter a assinatura das três. Eu tenho, né, porque eu trabalho exclusivamente com isso, né? Então o meu gasto com inteligência artificial é o quê? Eu pago...

## PARTE 5: Custos e Investimentos em IA e os Quatro Pilares do Projeto
**[15:52 - 20:10]**

...R$ 100 do Cursor por mês, R$ 100 do Codex por mês e R$ 50 do Open Code por mês. Então o meu investimento, eu já sei, né, tá lá no meu cartão de crédito, todo mês R$ 250 de inteligência artificial. É suficiente? É muito mais do que suficiente. Às vezes passa, né?

Por exemplo, estou trabalhando em três projetos ao mesmo tempo. O meu Codex acabou o limite semanal, né? E hoje ainda é dia 5, né? Então ainda tenho que esperar até o dia 7 para recuperar esse limite. Acontece, né? Então atualmente eu estou trabalhando só com Cursor e Open Code. Aí acabou ontem, eu acho, o meu Open Code, então eu já tenho que renovar, né, meu Open agora. Então, eh, é um gasto que eu tenho, mas que vai me dar muita velocidade no meu desenvolvimento, né? Então, ao meu ver, vale muito a pena esse investimento, tá certo?

Eh, então, né, o nosso projeto, né, ele vai ter quatro pilares, né? Quais são os pilares?

1. **Os Papéis (Roles):** Primeiro são os papéis, né? A gente chama de papel canônico que está dentro da pasta `agents/`, né? Eu vou mostrar para vocês já essas pastas. Eh, a gente tem o quê? Uma única fonte de verdade por papel replicada para cada um dos sistemas, né? O que que é isso? São as orientações, né, que cada um dos meus agentes vão desenvolver, né? Então, por exemplo, né, eu tenho o agente de *brainstorm*, né? O que que é isso, professor? O que que é brainstorm? É o agente que simplesmente vai pegar a minha ideia e vai tentar escrever em cima dela, montar um planejamento estruturado daquele sistema, né? Então, eu tenho o *brainstorm*, eu tenho o *writing plans*, né, que é o sistema. Deixa eu mostrar logo para vocês aqui, né, que fica mais fácil. Então vamos aqui no VS Code.

Então aqui ó, dentro desse meu agente, né, a gente tem a... ó, dentro do agente a gente tem... Então vamos lá. Ficou bem estranho isso aí. Deixa eu aumentar aqui, ó. Eu tenho o `brainstorm`, eu tenho `system primer`, eu tenho `execution plans`, eu tenho os `lang chains`, eu tenho o `workflow` do SAS, né, do Django, eu tenho `systematic debugging`, eu tenho o `superpowers`, eu tenho o `verification before completion` e tenho o `writing plans`. Cada um desses agentes, né, cada um desses papéis canônicos, eles têm um papel específico dentro do projeto, né?

Eh, esse manual mestre, né, ele está dentro do `agents.md`, né? Então vamos aqui em `agents.md`. Vamos fechar aqui. Deixar... agora ficou muito grande. Deixa eu fechar tudo aí. `agents.md`. Cadê? Por que que não fecha tudo, né? Está aqui, ó. `agents.md`, né? Ele é a parte, né... Deixa eu colocar aqui como visualização, fica mais bonitinho, né? Ele é o manual mestre compartilhado pelo Codex, Open Code, Cursor para o desenvolvimento SAS Django. Ele vai receber a descrição em linguagem natural, vai gerar o PRD, vai planejar as sprints, vai executar o código, vai gerar doc file para deploy. E se você já tem um...

**[20:10 - 24:24]**

...sistema, ele pode também inserir novas funcionalidades. Então os três, né, tanto Codex, quanto Open Code, quanto Cursor, na hora que eles entram na pasta, eles leem esse arquivo para eles saberem, né, qual é o próximo passo que eles vão ter que seguir, né? E é esse arquivo, né, que ele vai dizer, ó, primeiro faça isso, depois faça aquilo, depois faça aquilo, depois faça aquilo, tá certo? O planejamento desse sistema, ele é todo feito para não pular etapas.

Então, por exemplo, a primeira coisa que ele quer, que ele vai pedir, né, é a ideia. Depois você vai... ele vai escrever, você vai escrever, né, escrever, escrever, escrever, o que é que você quer desenvolver. Aí ele vai montar um planejamento com você e aí você vai discutindo. Então o primeiro passo, ele vai pedir a ideia, depois ele vai pedir a descrição, depois ele vai gerar um planejamento e vai executar com você, né? Vai trabalhar com você.

E aí para depois ele gerar, né, o PRD, né, a lista de tarefas e o open spec, né, que são as especificações do projeto, né?

E aí vem o terceiro, né, pilar, que é o quê? As **skills**, né? As skills elas fazem parte de um conjunto maior, né? Eu retirei esse conjunto e trouxe para cá, que é a biblioteca **Superpowers**, né? Que são skills verificáveis, né, que são skills públicas, né? Eu não inventei essas skills, eu apenas compilei as melhores pro nosso sistema.

E aí o que que ela vai fazer? Ela vai planejar antes de codificar, ela vai buscar evidências antes de declarar pronto o projeto, ela vai corrigir o código, vai testar, retestar. Então, se a gente for lá, né, nas skills do nosso sistema, né, vamos lá nas skills... Cadê? Aqui está o agente Open Code. Então, vamos pegar aqui dentro do Codex, né? Ó, os agentes, ó, tudo bem bonitinho aqui, ó. Agentes também. E dentro do Open Code estão aqui os agentes.

Então esses agentes aqui, né, que a gente chama de agentes, na realidade são as nossas skills, né? Aí aqui a gente tem o nosso open specs, né, e está aqui as skills principais, né? Cadê? Coloquei nante, macho. Não, script prompt não. Aqui são os prompts prontos. Docs agentes... daqui, ó.

Então, a gente tem o `agent_engineer`, né, que é o especialista em LangChain. Ah, deixa eu clicar com o preview. É o especialista em LangChain, RAG, traces, avaliações, né? Então eu coloquei aqui para ele usar o DeepSeek V4 flash do Open Code, né, para que ele possa inserir dentro do nosso sistema.

Esse outro aqui é o agente especialista em desenvolvimento assíncrono, né? Ele é especialista em Celery, RabbitMQ para integrar dentro do Django. Para que que serve isso aqui? Serve pra gente, por exemplo, digamos que eu tenho uma API, né? Aí, sei lá, 10 pessoas solicitam a API, fazem uma requisição. Para que essas pessoas não esperem a requisição de uma terminar para ter acesso à sua requisição, a gente trabalha em paralelo, né? Então, esses são os agentes especialistas, né?

## PARTE 6: Os Agentes Especialistas de Domínio e o Princípio do Open Spec
**[24:24 - 28:11]**

O Django Back End, né, é o especialista em Django, né, especialista em PostgreSQL, REST.

A gente tem o Django Front End, né, que é o especialista em templates, em Tailwind CSS, em Bootstrap, né, JavaScript.

A gente tem o DevOps, né, que é o especialista em GitHub, no caso GitHub Actions, Docker, Swarm, Traefik, Easy Panel, Cloudflare, tudo vai depender de onde você vai querer fazer o seu deploy, né?

A gente tem o QA Engineer, né, que é o especialista no Django, né, na stack Django, né?

E a gente tem o Security Review, né, que é o especialista em revisão do Django, né, questão de segurança, infraestrutura, agente de IA, né?

Então, esses agentes, né, são as nossas skills, né? E o que que é uma skill? É simplesmente um código, né, um código que é extremamente simples, mas que ele vai, por definição, desenvolver aquilo que a gente está pedindo, né? Então são as disciplinas verificáveis. E aí ele chama o Superpowers, né, para utilizar essas skills através do organograma do Superpowers.

E aí a gente tem os arquivos que são visíveis, né, que é o quê? Ele vai gerar o PRD, né? Ele vai gerar o PRD lá pra gente, ele vai gerar as *tasks*, né, que são as sprints, né, a sequência de passos que ele vai ter que desenvolver para montar aquele sistema, né?

As Open Spec, né, as especificações do projeto, o plano dele. Isso aqui é extremamente importante. Por quê? Trabalhar com IA tem vários problemas, né? Dentre eles a IA alucinar, né, como a gente fala. A gente resolve isso com RAG, mas o maior problema é a questão do contexto, né? Questão do contexto, ela compacta contexto, ela se perde no contexto.

Então o Open Spec veio para ajudar e resolver esse problema, né? Então, o plano de desenvolvimento e tudo que você conversou com a IA, ela fica dentro do repositório no Open Spec, né? Não na memória do agente. Então ele não vai esquecer. Por quê? Porque a todo instante ele vai ficar revisitando esse plano dentro do repositório, tá certo?

Então, eh, basicamente, né, o que que a gente vai fazer? A gente vai fazer um sistema Django, que vai ser um site pra gente aprender inteligência artificial e ciência de dados. Eh, você vai informar um tema: "Ah, eu quero aprender redes neurais". Aí vai lá no YouTube, procura vídeos, joga dentro da plataforma, né? A IA vai analisar esses vídeos, vai gerar material, vai gerar resumo, questões, né?

E aí a gente vai pegar esse sistema, né, vai colocar aí dentro para que a gente possa verificar o nosso rendimento e ela ser a nossa tutora pra gente aprender o que a gente quiser. Eu estou colocando aqui para aprender IA e ciência de dados, mas vocês podem pegar esse mesmo projeto e colocar para outra coisa, tá certo?

Então, eh, vou fornecer esse workspace para vocês, né, a gente vai desenvolver, vou explicar cada um dos agentes, vou explicar o que é o Open, o que é o Superpowers, mas quais seriam os pré-requisitos desse workshop, né, o ideal, né? Ideal ideal mesmo seria o quê? Que...

**[28:11 - 32:51]**

...você soubesse Python, né? Que você tenha conhecimento de Django. Por quê? Porque a gente vai usar o Python como back-end, né? Vamos utilizar o Django para ser a base do nosso sistema, vocês precisam saber de GitHub, né? Preciso que vocês saibam mexer no GitHub. Eh, e preciso, né, que vocês saibam ou conheçam o Open Code, o Cursor e o Codex, né? Vocês não precisam saber nada de agentes, nada de skill, nada de PRD, tudo isso, né, é o que a gente vai ver aqui dentro da aula, né?

Então vamos lá, né? Vamos, vamos começar. Se vocês tiverem dúvida, só levantar a mão, né? Eh, pode mandar mensagem no chat que aí eu vou olhando aqui sempre que possível.

Então, vamos lá. O que que são agentes, né? Agente, né? Um agente ele é o quê? Ele é um papel, né? Um código, um MD, né, especializado. É um papel especializado, né? Ele tem a sua função específica. No nosso sistema, ele é definido por um markdown, né, e ele está lá dentro de `agents/`, né?

Que um cliente, seja o Open Code, seja o Codex ou seja o Cursor, ele assume esse papel para executar cada uma das tarefas que foram delegadas, né?

Três elementos compõem essa definição, né:

1. **Especialidade:** Cada agente ele cobre um domínio específico, né? Como eu mostrei para vocês, um é só back-end, o outro é só front-end, o outro questão do deploy, o outro segurança, o outro tarefas assíncronas. E aí esse agente, né, ele tem lá bem declarado qual é o domínio que ele cobre e o que ele não faz, né? Então esse é o primeiro elemento.
2. **Contexto:** Esse contexto ele é obrigatório. Então, antes de atuar, todo agente vai ler o arquivo `agents.md`, `prd.md`, `tasks.md` e `worklog.md`, né? Então, eh, se o arquivo não existir, né, ele vai ignorar, mas se esses arquivos existirem, todo agente lê isso aí. O `agents` para ele saber o contexto geral do projeto; o `PRD` para saber qual é o SAS que ele vai desenvolver, quais foram as etapas que já foram desenvolvidas; o `tasks` para saber qual é a próxima sprint, qual é a próxima tarefa que ele vai fazer; e o `worklog` é o arquivo responsável pela mudança de agente. Então o Open Code, né, ele vai escrever lá no `worklog`, o Codex pega esse `worklog` e já sabe: "Ah, o Open Code trabalhou até aqui, então eu tenho que fazer esse próximo passo", tá certo? Então esse é o segundo elemento.
3. **Escopo Delegado com Evidências:** O agente ele vai trabalhar no escopo que lhe foi passado, né? Ao concluir, ele vai dizer: "Ah, eu mudei tal arquivo, tal arquivo, tal arquivo, eu executei tal comando, né? E os riscos que eu encontrei foram esses aqui", né? Ele simplesmente não vai dizer assim: "Pronto, acabei o que você pediu". Não, ele vai dizer: "Ó, eu fiz, desenvolvi essa tela, porém você jogou uma credencial sensível no chat. Sugiro que você recicle, né, a sua senha lá, o seu... como é... o seu token lá do Open Code ou então...

## PARTE 7: Camadas do Workflow e a "Regra de Ouro" dos Adaptadores
**[32:51 - 37:27]**

...sua senha do Postgres. Então ele vai dizer tudo que ele fez, quais foram os arquivos que ele mudou e quais foram os riscos que ele encontrou, tá certo? Então esses são os nossos agentes, tá bom?

O nosso workspace, né, o nosso workflow, ele vai ter quatro camadas, né? Quais são as camadas?

1. **As Skills:** Que são 10 disciplinas do fluxo `iniciar`. E aí eu coloquei essa palavra-chave, né, `iniciar` para ser o padrão de todos os meus agentes, né? Eu não tenho apenas o agente de Django, né, eu tenho agente para coisas do meu trabalho, né? Então eu simplesmente digo, digito `iniciar` e o agente vai lá e vai ler todo o processo, domínio e vai ver as coisas que ele pode fazer.
2. **Os Papéis (Roles):** Então o `agents.md` é a definição canônica dos sete agentes, né? E é uma fonte única, ele não copia pro produto gerado.
3. **Os Adaptadores:** Que é o `.codex`, o `.opencode`, o `.cursor`, né, que vocês podem ver aqui, ó. Para cada um: `.cursor`, `.opencode`, `.codex`. Está aqui, ó: `.codex`, aí `.cursor`, né, tem os seus agents, aí tem o `.opencode/agents.md`. Claro, cada um está adaptado para a sua realidade, né? Isso aí cada um dos agentes eu tive o cuidado de fazer com que eles sejam específicos para cada papel.
4. **O Produto:** Para isso, a gente tem os prompts e os agentes, né, que é o que é e como construir.

Deixa eu voltar aqui de novo pro VS Code. E aqui eu tenho alguns prompts que eu já deixei pré-montados, né? Então: prompt inicial bruto, prompt de refinamento, prompt para extração do design system que a gente vai ver, prompt para execução de sprint, prompt para execução de várias sprints, prompt para implementar o deploy, né? Então aqui são modelos de prompts que o próprio sistema vai utilizar para gerar, né, os artefatos que a gente vai precisar quando a gente estiver desenvolvendo, né, essa tabela, essa arquitetura, né, do nosso workspace.

Ele tem uma regra básica, né, chamada **Regra de Ouro**, que é o quê? Esses adaptadores eles não duplicam conteúdo, né? O adaptador do Codex, né, o Django back-end, ele tem três campos, né: *Name*, *Description*, *Develop*. É a instrução apenas: "Leia e siga o `agents.md`". Aí o arquivo lá do Open Code tem a mesma coisa: "Leia o `agents.md` mais a skill", né? O adaptador do Open Code: "Leia o `agents.md`" e assim vai.

Então, eh, a gente não tem vários agentes para cada um, não. A gente tem um agente que é adaptável para cada um desses clientes que a gente tem, tá certo?

Uma dúvida... ah, não, aqui eu acho que uma dúvida que muita gente tem, né, antes da gente passar para os clientes, é qual a diferença entre **agente**, **skill** e **prompt**, né? São coisas distintas. Ah, ainda tem o comando, né? Ó: agentes criam o prompt, comando... né? Ó, aqui eu tenho agente, aqui eu tenho skill, aqui eu tenho prompt...

**[37:27 - 41:51]**

...e ainda tem o comando `iniciar`.

Vamos lá. Eh, muita gente me pergunta isso, né? Então, é por isso que é importante a gente trabalhar.

*   **O Agente** é quem executa aquele papel, né? Então, ele é especialista e tem as suas restrições, né? Por exemplo, "revise sem editar o código da página inicial", né? Então ele vai fazer isso, ele é o agente, né? Ele vai executar aquele comando. Esse é o `agents.md`.
*   A gente tem a **Skill**, né, que é o `skill.md`, que ele é o *como executar*, né? Então, eh, o agente ele chama a skill, né? Então ele sabe como fazer, né? Então é basicamente... ele impõe, por exemplo, a de debugging, né, a skill de debugging, ela vai fazer uma investigação da causa daquele problema, né? Então o agente chamou a skill de debugging, a skill vai lá e faz esse fluxo de análise, tá bom? Explica para ele como é que ele vai fazer.
*   O **Prompt** é o *o que construir*, né? É o artefato do produto, é o briefing, é o PRD, é o design, é o deploy. O prompt ele é a autoridade máxima do nosso workspace, né?
*   E aí a gente tem os **Comandos**, né, que é a ação dentro que eu vou colocar dentro do cliente, né? E aí essas ações elas são salvas dentro do Open Spec, né?

Eh, é basicamente essa a diferença, tá bom?

## PARTE 8: Deep Dive no Cliente de IA: Open Code (CLI, Instalação e Planos)
Passando pro próximo slide, a gente tem os clientes, né? A gente tem os clientes de IA.

O primeiro cliente é o **Open Code**, né? O Open Code atualmente é o meu principal fluxo de inteligência artificial, né? O que eu mais uso disparado, né? Ele é um CLI open source, né? Ele, dentro do nosso sistema, os agentes estão dentro de `opencode/agents.md`, né? Ele utiliza o formato MD para trabalhar, e os comandos que são executados dentro dele estão dentro de *Open Code Commands*, né? A memória dele é não compartilhada.

Então, vamos lá pro navegador pra gente ver o Open Code, tá certo? Então, vamos abrir aqui o Open Code. Para você acessar o Open Code é só você digitar `opencode.ai`. Eh, você pode fazer o download para Windows, para Mac, para Linux, né? A gente já tem a versão do Open Code Desktop, que é muito boa, mas eu ainda prefiro utilizar o Open Code dentro do VS Code, né?

Para isso, né, eu tenho que baixar o Node e executar esse comando aqui, ó: `npm i -g opencode`, né? Então eu abro lá o meu PowerShell dentro do Windows e digito esse código aqui. Se por algum acaso eu estou dentro do Linux, né, eu só digito `curl -fsSL ...` e aqui ele vai instalar. Se eu estiver dentro do Mac, eu acho que é esse aqui: `brew install ...`. Vou colocar para vocês duas aulas lá na plataforma, né, dessa parte básica de instalação de todas as ferramentas, tá bom? Eu acho que eu só não gravei do Cursor, mas o Cursor é a mesma coisa de instalar o VS...

**[41:51 - 46:26]**

...Code, né? Então é só next, install.

Então vamos entrar. Aqui dentro a gente tem o plano GO, né? Que é esse que eu indico para vocês, né? Assinar $10 por mês, né?

*(Interrupção técnica do aluno)*
— Fala, Nálio.
— Professor, tá mostrando a tela lá do workshop.
— Vala, é mesmo, ó! Pois deixa eu voltar aqui de novo. Desculpa aqui. Tá aí que agora eu não entendi. Aí pronto, agora sim. Foi mal.

Então vamos lá de novo. O que eu falei continua válido, né? Mas tá aqui o site do Open Code. Então tá aqui, ó, Open Code. E aí está aqui os códigos para você instalar. Para você instalar no Linux é esse aqui, né? `curl install`. No Windows, é esse aqui, `npm`. Mas você precisa ter o Node instalado. E para instalar no Mac é esse aqui, ó, `brew`.

O modelo que eu utilizo é o GO, né? O plano GO, né? Está aqui, ó, $10 por mês. Eu vou entrar aqui e está aqui os modelos, né? Ó, vamos aqui pro Go. Esse aqui é o meu uso, ó. Faltam 18 dias e 8 horas e eu tenho ainda 54%, né? O uso semanal vai reiniciar em um dia e não tenho nada utilizado aqui, né? Então são limites bem generosos, né?

Coloquei aqui no "saiba mais" pra gente ver os modelos que estão disponíveis, pessoal. Então, esses aqui são os modelos: o Grok, o GLM 5.3. Pessoal, o GLM é fantástico. É fantástico. Hoje em dia, para desenvolvimento é o que eu acho melhor, né? Porém, ele é caro, né? Vou já mostrar para vocês que ele é caro. Então eu fico trabalhando com GLM, com o Qwen e com o DeepSeek, né? São os três modelos que eu mais utilizo.

E quando eu quero coisas em grande demanda, né? Por exemplo, "ah, eu tenho um sistema que entra jogo por jogo e faz um resumo de inteligência artificial", aí eu uso o Mimo, né? O Mimo tem muitas, muitas requisições.

Então, bem aqui estão as requisições, ó:
*   O Grok tem 169 requisições a cada 5 horas.
*   O GPT, ó, tem 2050.
*   O GLM, olha, o GLM aumentou, está 1580. Pronto, ó, GLM Flash. Vamos utilizar ele.
*   Aí tem o GLM 5.2, que era o que eu utilizava e desenvolvi quase tudo recentemente. 800 requisições. Por isso que eu disse que ele é caro, né? Porque 800 requisições em 5 horas você acaba gastando tudo.
*   O Mimo, né, quando você quer coisas em produção: "ah, deixei um, criei um sistema lá que ele fica rodando a IA direto". Aí você bota o Mimo, que é o que a gente vai fazer aqui.
*   O DeepSeek Flash, antes ele era mais ou menos essa mesma quantidade aqui do Mimo, né? Aí reduziram agora drasticamente.
*   Aí tem o Homem Alfa. Esse aqui é novo, o i3, o i4 também é novo.

Então, todos esses modelos são os modelos que estão disponíveis nesse plano de $10, né? Aqui são os limites que você tem, né, 12 de uso para cada uma das ferramentas. E você pode acompanhar o seu uso aqui, né? Você vê, ó, quanto que você gastou a cada dia, né, em cada um dos modelos. Aqui eu estava brincando com Qwen.

Aí para isso você cria, né, a chave da API. Então vou criar aqui uma chave chamada "workshop", né? Vou criar e aí, ó, eu vou copiar pra gente trabalhar lá no nosso Open Code.

**[46:26 - 51:15]**

Então, ó, vou copiar e aí eu vou lá para o nosso digníssimo VS Code. A parte da instalação vocês vão receber numa aula à parte, tá certo pessoal, quem ainda não sabe instalar o Open Code.

Aí eu vou vir aqui no terminal, novo terminal, e eu digito `opencode`. Então, pronto, ó. Abri aqui o Open Code.

Aí o Open Code, pessoal, ele funciona assim, né? Ele funciona via CLI, né? Então ele é direto no terminal, ele tem acesso a todas as pastas do projeto, né? Mas toda a sua interação vai ser aqui, né? Vai ser aqui.

Antes da gente começar, né, quais são as coisas que a gente precisa saber? Vocês estão vendo esse `build`? É a forma como ele está disponível para trabalhar. Se você der um tab, né, tab ele muda para `plan` — ó, `build`, `plan`. Eu estou clicando o tab no meu teclado.
*   No modo **Build**, ele vai estar trabalhando como agente, ou seja, ele vai acessar os arquivos desse projeto. Ele só pode acessar os arquivos desse projeto aqui e ele vai poder editar, vai poder criar código, vai poder fazer o que você pedir.
*   No modo **Plan (Planejamento)**, ele somente vai planejar, ele consegue ler todos os arquivos, mas ele não edita nada, né? Então é somente planejar e ele vai mostrar tudo aqui dentro, né, dentro dessa telinha.

Outra coisa que a gente precisa: tudo que a gente vai mexer, a gente tem que dar um barra alguma coisa, né?
*   `\connect`: ó, `\connect` está aqui, ó. O que que esse `connect` vai fazer? Ele vai conectar a algum provedor. Então, vou clicar aqui, ó, `connect`. Aí vai aparecer quais são os provedores. Eu posso colocar o Open Code Zen (ele recomenda porque é o que ele mais ganha dinheiro, né? Vou já mostrar para vocês o que é o Open Code Zen). O Open Code Go é esse que é o que eu uso. Você pode colocar sua API da OpenAI, do GitHub Copilot, da Anthropic, do Google, né? Então você vai lá no Gemini e pega a sua API e coloca aqui, e você tem acesso a todos os modelos da Google. Aí tem o Abacai, que é muito bom. Ó, tem o Alibaba da China. Então, todos esses aqui são modelos que você pode utilizar dentro do Open Code, né? Olha, tem até o Claudinho, tá aí, que esse aqui é novo.
*   Pronto, né? Vamos aqui colocar o Open Code Go. Aí vou colocar aqui de novo que não pegou não. Eu colo aqui, dou um enter. Eu depois eu excluo, né, essa API key. Aqui eu escolho os modelos, ó. Qual o modelo que eu vou querer? Eu vou querer o GLM 5.3. Cadê GLM 5.3 flash. Olha aí a sacanagem, mas tudo bem. Default, low, height. Eu boto sempre default. Então tá aqui, ó: GLM 5.3 flash, só que ele está dizendo que é duas vezes o uso, né? Se a gente voltar lá pra tela do Open Code do Google Chrome...

Vamos ver aqui. Uso Open Code Go, ó. Vamos ver aqui quanto é que é o uso. Ele diz que é 1580, né? Se você pegar 1580 div por 2, na realidade você tem 790 requisições, né? Que é menos do que o GLM 5.2. Então vamos usar o DeepSeek mesmo, né? O DeepSeek V4 Pro, que...

## PARTE 9: Deep Dive no Cliente de IA: Codex e seu Funcionamento
**[51:15 - 55:36]**

...é 1050, tá bom?

O que que é o **Open Code Zen**, né, que estava lá naquela tela? O Open Code Zen é esse aqui, pessoal. Ele pega e seleciona os melhores modelos de IA. Então, ele tem o GPT-6 Astra, o Sol, Terra e Luna, ele tem o GPT, tudinho, né? O Cloud Fable 5.1 (nem sabia que tinha 5.1), o Opus. Então todos esses modelos aqui você vai poder utilizar, só que você utiliza pagando via API, né? Então sai caro, sai caro, né?

Então vamos lá voltar pro Open Code. Que mais, né, que a gente precisa saber antes da gente começar?

Primeira coisa, fazer a conexão com o modelo, né? Então, estou aqui, ó, `\models`. Aí eu vou agora procurar o DeepSeek. DeepSeek V4 flash... não, V4 Pro. Aí aqui, ó, qual a variante? Default, he mat? Eu boto sempre default que aí ele ajusta pra gente. Estou no modo de planejamento, modo de build.

Eh, que mais a gente tem então?
*   `\connect` para conectar o servidor, o sistema, o open go.
*   `\models` pra gente trocar de modelo.
*   `\sessions`: um outro comando que eu uso. O que que é esse `\sessions`? Por exemplo, eu estou aqui trabalhando, aí precisei fechar o computador, precisei sair. Aí quando eu fecho, aí abro de novo, ele abre novamente nessa tela. Ele não abre na tela que estava salva. Aí você dá um `\sessions` e aí ele vai aparecer todas as sessões que você já trabalhou com Open Code, tá certo? Então eu atualmente só uso basicamente esses três comandos. Beleza? Então esse é o Open Code, né?

Vamos voltar pro slide. Agora o próximo é o **Codex**, né? O Codex, o que que é o Codex? Ele é o sistema, né, o agente de programação da OpenAI, né? O formato dele é o TOML, e o comando ele vai rodar os scripts que estão dentro de agentes, tá certo? Então vamos ver o nosso digníssimo Codex.

O Codex tem duas formas da gente utilizar, né? Eu vou mostrar as duas para vocês. A gente pode utilizar ele com o Codex App, que é esse aqui. "Professor, onde é que eu baixo esse negócio aí?" Então vamos lá baixar esse negócio. Vocês vão entrar aqui dentro do Google, vão digitar "Codex". E aqui, pronto, está aqui, ó. É o mesmo agente de programação poderoso, só que agora dentro do ChatGPT. Então você vem aqui e baixa pro Windows. Só isso, né? Aí você faz login com a sua conta do ChatGPT e você tem acesso a esta interface.

Esta interface, como vocês podem ver, ela é bem parecida com o Antigravity, né? Então aqui, ó, a mensagem que eu falei para vocês: "Você passou do seu limite, o seu limite vai ser resetado dia 7". Só que agora apareceu isso aqui (não tinha ontem isso): "Você tem uma nova redefinição do limite". Olha aí! Vamos ver. Ver redefinições. Olha aí, rapaz! Vamos ver se eu posso fazer... Ó, eu tenho uma redefinição. Pronto, vou redefinir. Confirmar. Voltei a ter limite. Coisa boa!

**[55:36 - 59:32]**

Só que eu tenho mais uma que se vence no cinco do... ah, pronto. Ó, é uma redefinição por mês. Pronto. Voltei a ter limite, pessoal, no Codex.

Então, basicamente o que que a gente precisa, né? A gente precisa abrir um projeto. Então vou abrir aqui uma pasta. Cadê meu GitHub? GitHub SAS. Pronto, abri minha pasta. "Selecione as configurações". Ok. Pronto. Então, meu projeto está aqui, ó. Então, esse é o meu projeto. Aí eu vou interagir com ele por aqui. Aqui eu posso escolher os modelos, né?

Vamos ver quais são os modelos. A gente ainda não tem acesso ao Astra, né? O Open Code já tem, mas a gente ainda não tem, ó. Mas está aqui. Pronto. Então a gente já pode utilizar o Codex também.

Só que eu não gosto de usar o Codex por aqui, né? Eu gosto de usar o Codex, pessoal, pelo VS Code. Então vou voltar agora pro VS Code. Vou dar um Ctrl+C, ó. Ctrl+C ele mata o Open Code, tá certo? Ele dá um kill.

E aí, ó, em extensões, né, aqui dentro do VS Code, em extensões, você vai procurar "Codex" e vai instalar essa extensão aqui. No momento que você instala essa extensão, o Codex aparece bem aqui, ó. Aí você clica e aí, ó, vai aparecer junto com o chat do Copilot. Ó, esse chat aqui, ó, é o chat do Copilot, né? Se você não quiser ele, você pode desmarcar, né, porque tem que pagar. E aí vem o Codex, né?

Então, pronto, ó, o Codex já chegou aqui no VS Code. "Apresentamos o GPT-6 Astra, nova geração de inteligência, o estado da arte do código, computing science". Então eu vou colocar: "Experimente já o GPT-6 Astra". Pronto, está aqui, ó. Aqui são os meus chats antigos, ó, todas as chats que todas as definições que eu pedi. E aqui eu posso simplesmente, ó, digitar `iniciar`, né, que aí ele vai reconhecer o workflow e vai funcionar, tá bom?

Aqui a gente tem os modelos, ó: modelo Astra, etc. Ah, então acho que é por... ah, pronto. Ó, agora eu me liguei de uma coisa aqui: "para você escolher, solicitar aprovação, aprovar por mim o acesso completo". Agora eu me liguei de uma coisa, pessoal. Deixa eu voltar aqui pro Codex. Aqui ainda não apareceu o GPT Astra, né? Ó, vocês viram que não apareceu, né? Porque, ó, ele está para atualizar. Esse 00 aqui é porque ele pediu para atualizar, mas eu não sei porque que não atualizou ainda, né? Eu acho que eu vou fechar para poder ele atualizar, tá bom? Então acho que é isso.

## PARTE 10: Deep Dive no Cliente de IA: Cursor (IDE e Modelos)
**[59:32 - 1:03:26]**

E para finalizar, a gente tem o **Cursor**, né? O Cursor ele também é uma IDE, né? O que que é o Cursor? Ele é um fork do VS Code, né? E aí ele tem também os mesmos modelos, né, do Open Code, né? Então, *Cursor Agents*. Os agentes dele são MD e também o *Cursor Commands*. Então, ele... a interface é parecida com Codex, mas o uso é parecido com Open Code, tá certo?

Então, todas as pastas utilizam os mesmos agentes chamados pelos respectivos comandos. Então vamos aqui agora abrir o Cursor, né? Então deixa eu fechar aqui. Abrir o Cursor, né? O navegador pra gente procurar o Cursor: `cursor.com`.

Pronto. Então, para você baixar o Cursor, é só você clicar aqui em baixar. Os preços dele são aquele mesmo clássico, né, de $20 mensais, né, tem bastante limite. Quando você faz login, você tem acesso a ele diretamente no navegador, o que é uma coisa boa, né? Porque, "ah, se eu estou no celular, eu consigo acessar ele pelo navegador" e também tem o aplicativo, né?

Então, pronto, ó, aqui, né, ó, *Meet Grok Bot*, né? Então, tem um bot aqui e aqui são os modelos, ó. Ele tem o Grok, que é fantástico. Pense num modelo bom para programar. É um dos melhores modelos que eu já utilizei, né? Acho que entre ele e o GLM, eu não sei qual é o melhor, mas eu gosto de todos os dois. Aí tem o Opus, que eu gosto de utilizar para planejamento. Aí tem os outros, né: o GPT, o Fable, até o Gemini 3.8.

Aí como é que funciona? O Grok você tem quase ilimitado. É token que não acaba mais. E os outros você tem, parece que é 20 para cada modelo. Acaba num instante os outros.

E deixa eu abrir agora a interface do Cursor, né? A interface do Cursor é essa aqui, ó. Eu coloquei ele branco para diferenciar do VS Code, né? Mas vou aqui, ó, *Open Project*. Aí eu vou lá no meu GitHub e abri o projeto. Mas eu acho que não ficou bom, não. Ficou feio ele branco.

Então tá aqui, ó, né? Deixa eu dar um zoom aqui, ó. Aqui está todos os nossos agentes, né? Está aqui o `.cursor` e aqui a gente pode escolher os modelos, né? Ó, o modelo que a gente vai querer utilizar. Eu deixei habilitado só esses aqui. Aí você pode escolher qual é a... extra, right, right, medium. E se você quer rápido ou fast.

Aqui você tem o modo *Agent*, modo *Plan*, modo *Build*. E se você quer só conversar com ele, você usa o modo *Ask*, tá bom?

Então, muita coisa, muita coisa, muita coisa. Então, esses são esses são os nossos três clientes, né? E aí vem a parte que é importante: é que a gente... eu consegui montar dentro desse workflow — deixa eu ver se eu estou cobrindo alguma coisa. Não, não estou — dentro desse workflow, a questão da compatibilidade da memória operacional, né?

Então, independente de qual ambiente eu esteja, seja Open Code, Codex ou Cursor, a gente sempre vai ter os mesmos projetos. Então, `agents`, `PRD`, `tasks`, cada um pode conseguir, pode conseguir acessar e conseguir continuar, tá certo? Então quando ele termina, né, uma determinada tarefa, ele vai lá no `worklog` e adiciona, né? E aí ele tem essa obrigatoriedade de nunca sobrescrever o histórico, né? O histórico fica sempre salvo, tá bom?

Então vamos lá, né? Basicamente como é que funciona esse meu workflow, né, que vai ser... vou apresentar para vocês, né? E vocês, se vocês quiserem utilizar, obviamente podem utilizar sem problema, tá bom?

## PARTE 11: O Fluxo Operacional de Contexto, Tecnologias (Stack) e Banco de Dados (Postgres)
**[1:03:26 - 1:07:43]**

Então, assim que a gente abre, ele vai ler o `agents.md`, né? Ele vai ler o `agents.md`. O `agents.md` é o arquivo mestre do workspace, né? É o arquivo que o Open Code, o Codex e o Cursor leem automaticamente ao abrir a pasta.

Dentro dele a gente tem um processo: a palavra de start, né, que eu coloquei `iniciar`. Ele vai rotear os modos, as fases de execuções e o que a gente chama de *hard gates* (que são as regras inegociáveis que o agente ele não pode violar, né?). Ele também tem dentro do `agents.md` o conteúdo do produto, né, o que que ele vai construir em cada artefato que vai ficar lá dentro dos prompts, né? Ele vai adicionar/alterar os prompts para isso.

Qual é a ordem de autoridade, né? "Ah, por exemplo, duas fontes conflitam, né?".
1. Primeiro vem o **Prompt**.
2. Depois vem o **Agente**.
3. Só depois vem a **Skill**, né?

Então essa é a ordem de prioridade dentro do nosso sistema. Se um prompt pede uma seção do PRD que contraria o agente, o prompt vence. Se a skill sugere um fluxo que contraria o agente, o agente vence, né? Então a skill ela é válida apenas no núcleo, né? Então é basicamente essa a ordem do sistema.

Eh, quais são as regras de configuração, né? O `agents` ele criou uma stack, né? Ele fixou uma stack para que a gente não precise tomar decisões repetidas, né, e para que ele não fique sempre perguntando, né? Então, quais são essas regras?

*   **O Back-End:** Python e Django sempre. Por quê? Porque eu gosto de Python e eu gosto de Django. Então, por isso que eu criei e coloquei isso dentro do meu workflow.
*   **Front-End:** Os templates de Django, né? O Django templates, mas com Tailwind CSS ou Bootstrap, né? Vai depender do que for pedido e do design system também, né? E se for necessário, HTMX.
*   **Banco de Dados:** O que que a gente vai utilizar sempre? PostgreSQL, né? PostgreSQL. Eu uso tanto em desenvolvimento quanto em produção. Por quê? Porque eu não quero perder tempo, né? Então eu uso o **Easy Panel** para fazer o meu deploy. Então antes de começar um projeto, eu vou lá no Easy Panel e crio um Postgres. Coloco dentro do `.env`. E pronto, use esse Postgres aí e acabou, né? Na hora que eu tiver em desenvolvimento, eu quero usar a mesma credencial que eu usei em produção. O contrário, né: quando eu tiver em produção, eu quero usar as mesmas credenciais quando eu estou em desenvolvimento.
*   **SQLite é proibido** dentro desse workflow, né? E aí vem um detalhe: se você não fornece a `DATABASE_URL`, né, o link do Postgres, ele não vai conseguir continuar, né? Então, se você não tem o Easy Panel, você pode criar um Postgres. Faz muita diferença, né, a gente usar o Postgres em comparação com o SQLite quando a gente quer, como é a palavra... ampliar, né... como é... tem um nomezinho, esqueci agora...

**[1:07:43 - 1:11:45]**

...né, quando o seu projeto cresce muito, né, é importante você ter o Postgres.

*   **A questão das Filas:** Questão das filas, a gente vai usar o Celery e o RabbitMQ.
*   **A Inteligência Artificial dentro do sistema (Opcional):** Às vezes a gente precisa de IA dentro do sistema, né? Por exemplo, eu desenvolvi um sistema para uma empresa que ela tinha dados de clientes, né, uma empresa de motos, e ela precisava que o próprio sistema analisasse todos os dados: dados do cliente, dados de pagamento, dados de entrada e saída das motos. Então a gente precisa colocar IA, né? É opcional. E aí o agente vai perguntar: "Você quer colocar IA dentro do seu sistema?". Se sim, aí ele sugere o LangChain e, para como API da IA, ele vai perguntar se você quer usar o Open Code com Mimo V2.5. Por quê? Porque o Mimo V2.5 é aquele modelo lá do Open Code que tem 30.000 requisições a cada 5 horas, 150.000 requisições por mês. Então, para você rodar dentro do seu sistema, né, é importante um que não consuma muito seus créditos, né? Então por isso que eu coloquei a IA como opcional, né, mas se você quer utilizar...
*   **A Infraestrutura:** GitHub para salvar o projeto. Tem outro melhor? Não sei. Eu gosto do GitHub, aprendi com GitHub e sou feliz com GitHub. Docker, Docker, né? Docker a gente precisa, é mais fácil a gente dockerizar as nossas aplicações. Uso a VPS da Hostinger porque é barato, mais barato, e utilizo o Easy Panel.

Para quem não conhece o Easy Panel, deixa eu mostrar aqui o Easy Panel. O Easy Panel é este troço aqui, ó. Ele é um... deixa eu abrir aqui, tá aqui o projeto da comunidade, né, que foi de Python. Então, o Easy Panel é um ambiente, né, um sistema, um painel que quase todas as fornecedoras de VPS trazem pra gente, né? Tem vários painéis desse, tem até outros que são mais recomendados do que o Easy Panel, mas eu gosto do Easy Panel pela simplicidade que ele traz, né?

Então, por exemplo, ó, aqui, né, a gente tem o IP da VPS, aqui em configurações, você tem o token do GitHub, que você precisa colocar aqui dentro para você vincular o seu GitHub. Eh, aqui você tem, ó, o uso da CPU. Eita, meu irmão! Ó, 65% da CPU, 64% de memória, 23% de disco. Então, aqui são os vários projetos que você consegue colocar dentro. Aqui as ações que foram feitas, você consegue monitorar, ó, tudo bem bonitinho.

Então, é muito fácil, né? Então, por exemplo, vou criar aqui um novo projeto, ó, novo projeto. Eu clico aqui no mais e aí eu escolho: "Ah, eu quero um aplicativo, eu quero um Postgres", né?

**[1:11:45 - 1:15:52]**

Vamos, ó: um aplicativo. Aí você coloca aqui o nome do serviço, né? Vou colocar aqui "workshop_django". Eh, tem que ser com underline. Eu e minha mania de colocar letras maiúsculas. Workshop Django. Aí, clico em criar, ó. Aí ele vai simplesmente... você escolhe aqui, ó: escolhe qual repositório, e aí, pronto, né? Como você já vinculou o seu GitHub, aí você coloca o repositório, coloca o main (o ramo é main, né?). Aí o caminho de build é esse. Só escolher o repositório, né?

Só que para isso vamos criar aqui um repositório. Vamos aqui no GitHub e vamos criar um repositório pro nosso sistema. Então eu vou aqui em "repositórios", "novo repositório". Aí vou colocar "workshop-django". Aqui eu posso colocar a letra maiúscula: Workshop-Django. Vou deixar ele privado. Vou colocar um README, gitignore do Python e "create repository". Pronto, né? Está aqui.

Então, temos aqui o nosso coisinha. Aí eu posso subir os arquivos ou eu posso utilizar o GitHub para isso, né? Mas vamos fazer isso já. Aí, ó, quando for aqui no Easy Panel, eu já puxo, ó. Deixa eu só atualizar aqui, ó. GitHub. Aí eu vou aqui, ó. Aí, "workshop-django". Ramo main, né, que é o ramo principal, e caminho de build.

Aí, o que que ele pede? Além disso, ele pede um Dockerfile, né? Aí esse Dockerfile, o nosso agente vai criar esse Dockerfile, né? Então essa é a parte do agente: ele vai criar o Dockerfile.

*   O nosso **código** vai estar sempre em **inglês**, né, óbvio.
*   E a **integração** (as telas) vai estar em **português**, né, no meu caso.
*   Cada **app** vai estar separado por um **domínio**.
*   E obrigatoriamente **não quero MCP** (Model Context Protocol). Por quê? Porque eu quero ter controle de tudo que eu faço, né? Desenvolvimento assistido por IA não é "desenvolvido por IA", né?

E aí vem a parte mais interessante, que é o quê? Eu **não vou inventar um visual**. Eu vou colocar um **HTML de referência**, né? Como é que a gente faz isso? A gente escolhe um modelo de site e coloca dentro do sistema, coloca o HTML e pede para ele tirar o *design system*. O *design system* é o quê? É a estrutura do site, as cores, o padrão daquele site. Se você já tiver o arquivo do site, melhor ainda, né? Mas se não tiver, uma maneira é você salvar o site, salvar a página da web completa e aí vai ter o arquivo HTML daquele site, tá certo?

## PARTE 12: Detalhamento dos 7 Agentes Especialistas e Suas Regras
**[1:15:52 - 1:20:09]**

Então, vamos já começar, né? A gente vai terminar aqui de apresentar a parte teórica, a gente faz o intervalo e depois a gente volta para começar a implementar os agentes, tá bom?

Então vamos lá. Os nossos sete agentes, só já mostrei assim rapidamente para vocês, mas:

1.  **Django Back End:** O que que ele faz? Ele é o especialista em Django, né? Modelagem de domínio, ports. O que que ele vai fazer? Ele vai colocar cada app, vai preservar o app por domínio, vai criar aquelas regras: o código em inglês, a parte das telas em português, e tudo que ele for fazendo ele vai criando e atualizando lá dentro de `models/`. Ele utiliza CBV (Class-Based Views). Ele não altera front-end, ele monta a infraestrutura e ele fala quais foram os arquivos que ele trabalhou.
2.  **Django Front End:** Ele é especialista em Django templates, Tailwind, Bootstrap, JavaScript, HTMX. A base dele é o HTML Server como base, né? Ele só vai utilizar JavaScript se for necessário. Ele é o responsável pela responsividade, por você ter acessibilidade no teclado. Ele não usa React (que é chato pra caramba). E ele valida a interface quando tem o ambiente: no caso, dentro do Codex ele consegue acessar o navegador para verificar o que ele fez. O Cursor também faz isso, só o Open Code que não faz.
3.  **Async Worker (Assinc Worker):** O que que ele faz? Ele é o especialista em Celery, em RabbitMQ no Django. As tarefas têm que ser idempotentes, têm que ter timeouts, têm que ter retries limitados, ele tem os identificadores pras filas, ele vê tarefas mortas e, enfim, ele cuida dessa parte.
4.  **AI Engineer (AI Agentining):** O agente de IA. Ele tem conhecimento em LangChain, LangGraph. Eu o configurei com o provedor Open Code GO, mas ele valida as saídas estruturadas, ele cuida também da questão de prompt injection. Também ele toma cuidado com custo, timeout, sem dados pessoais, enfim, ele tem essas responsabilidades.
5.  **Platform DevOps (DevOps):** Ele é especialista no GitHub, Docker, Traefik, Easy Panel, Cloudflare, o que você quiser fazer. O padrão é o Docker e o Easy Panel, mas o que você quiser fazer, ele faz também. Ele toma muito cuidado com a questão das chaves, né, que um dos maiores problemas de você fazer vibe coding é alguém chegar, clicar em "inspecionar" o seu código e estar lá a sua chave da API no código, as senhas dos usuários dentro do código, né? Então, esse agente ele também toma esse cuidado. Os segredos têm que estar sempre fora da camada do manifesto para que a gente esteja seguro com os nossos dados.

**[1:20:09 - 1:24:08]**

Ele não faz o deploy se você não pedir, né, sem autorização. E eu prefiro que seja assim, né? Eu mesmo gosto de fazer o deploy. Nem é tão manual assim, já que eu uso o Easy Panel, mas é muito fácil, muito tranquilo fazer daquele jeito lá.

6.  **QA Engineer (QA Engineering):** Ele é o especialista no comportamento do Django, ele prioriza o risco sobre a cobertura, ele faz testes automatizados, mas só lá no fim, porque ele precisa ter a estrutura toda pronta para isso. Ele testa, reproduz a falha antes de propor a correção, né? Então ele não simplesmente diz "está errado", não. Ele vai lá, testa, descobre e aí ele vai e propõe a correção. Ele, como eu falei, ele não declara só o sucesso ("pronto, acabado"), e ele revisa por padrão sem editar o código.
7.  **Security Review:** Que é o nosso sétimo e último agente, ele é o nosso revisor de segurança, ele não faz edição, somente leitura. Ele procura os modelos de ameaça: auth, injeção, vazamento de segredos, Celery, Docker, fronteiras do Linux. Então ele procura tudo, né? E aí cada achado ele mostra a evidência, o impacto e como deve corrigir, e obviamente ele nunca vai expor os nossos segredos.

E a regra comum de todos é o quê? Ler os agentes (`agents.md`), ler o `PRD.md`, ler o `tasks.md` e o `worklog.md`, e só o agente principal registra dentro do worklog, certo?

## PARTE 13: O Núcleo do Sistema e a Filosofia "Superpowers"
A gente tem o núcleo, né? O nosso núcleo é a base do sistema, e ele se divide em dois grupos: seis são skills de processo que estão lá dentro do **Superpowers** e quatro skills de domínio (que é workflow e chain, né?).

O que que é o Superpowers? O Superpowers ficou muito famoso com um vídeo, eu acho que foi do Mateus da IA, né? Eu acho que foi esse cara que colocou assim: "Transformei meu Codex em um agente de programação sênior, um programador sênior", uma coisa assim. E aí ele simplesmente instalou o Superpowers. Foi só isso que ele fez.

O Superpowers é o nome da filosofia adotada pelo workspace, herdada do projeto *Work Superpowers*. O que que o Superpowers... qual é a filosofia do Superpowers?

> **Toda ação criativa ou correção precisa de uma skill correta invocada antes de dar a resposta.**

É basicamente isso. Eu preciso, por exemplo... eu faço uma pergunta sobre alguma coisa do sistema, não é o Open Code que vai responder. Ele vai procurar qual é a skill que tem a melhor habilidade para responder aquela pergunta, né?

Vamos lá, vamos ver esses Superpowers, né? Eu vou abrir aqui o navegador novamente. Navegador e eu vou digitar "superpowers".

**[1:24:08 - 1:28:21]**

Pronto. Superpowers é isso aqui, né? Aí você entra aqui no GitHub e é isso aqui, só isso. É um repositório do GitHub que tem agentes para o Codex, para o Claude, para o Cursor, para o Devin, para o GitHub, para o Hermes, para o Kim, para o Open Code.

*(Pausa rápida para interação com os alunos)*
— E travou agora o GitHub. Vocês estão me ouvindo? Pessoal, vocês ainda estão aí? Só para saber, porque faz tempo que ninguém fala nada. Aproveitar beber uma aguinha. Show, show.

Então, voltando, o que que ele traz aqui? Ele traz essas skills, são muitas.
*   `brainstorm` para você colocar agentes em paralelo.
*   `executing_plans`.
*   Tem muitas aqui que eu não sei, ó: `saving_requesting`, `subagent_driving`, `development`, `debugging`, `systematic_testing`, `test_driving_development`, `git_use_superpowers`, `verification_before_completion`, `writing_plans`, `writing_skills`.

Então são skills que foram criadas pela comunidade para que a gente possa... e a skill é só isso, né? É só um arquivo `.md` que aí ele traz bem direitinho o que ela tem que fazer para cada um: no Codex, no Gemini, no Copilot.

E aí, o que foi que eu fiz? Eu peguei esse pacote com todas essas skills e tirei somente o que era extremamente necessário para o projeto, basicamente isso.

Se você for no Codex... vou abrir aqui agora o Codex. Se você for aqui no Codex e vier em "plugins" e digitar "superpowers"... Isso também tem no Claude, viu pessoal? Eu não gosto do Claude não, mas também tem a mesma coisa. Você vai lá, "plugins", "superpowers". Aí você tem aqui as 14 habilidades, mas são 14 skills.

Então, dessas 14 skills, eu peguei as seis principais e coloquei dentro do nosso projeto. Se você clicar aqui em "instalar o plugin", o seu Codex vai ter todas essas 14 habilidades. E cada vez que eles vão atualizando isso aqui, o seu Codex vai ser atualizado automaticamente, tá certo?

Então isso é o Superpowers. Então são essas as skills aí que eu selecionei.

Basicamente é isso. O `using_superpowers`, para que que ele serve? Ele estabelece como descobrir e invocar as demais skills, né? Eh, basicamente ele é o porteiro da filosofia. Ele vai definir quem da lista está disponível para responder e é o melhor para responder aquela questão.

O `brainstorm`, ele é o que vai fazer o trabalho criativo, né? Ele faz a sondagem obrigatória, depois ele faz um design curto no chat, e você tem que fazer a aprovação. Ele tem a parte de arquitetura: ele faz perguntas, traça duas ou três abordagens...

## PARTE 14: Detalhamento das 6 Habilidades de Processo (Superpowers)
**[1:28:21 - 1:32:26]**

...escolhe o desenho e aí faz as especificações lá no arquivo de `specs/`. E aí tem o *hard gate* (que é: "nada de código sem aprovação"). Então ele simplesmente monta a estrutura do planejamento conversando com você.

Depois, tem o **Writing Plans** (`writing_plans.md`), que ele transforma a spec aprovada em um plano de implementação com passos bem detalhados, tarefas, arquivos, interface, e aí ele salva no planejamento.

Depois a gente tem o **Executing Plans** (`executing_plans.md`), que ele carrega, revisa e executa tarefa por tarefa com verificação, né? Dependendo de como você pedir, ele pode, por exemplo, "ah, eu quero que você faça a tarefa 1, 2 e 3". Aí ele faz, mas "eu quero que você faça somente a tarefa 1". Aí ele faz a tarefa 1, pede a aprovação, aí pergunta se pode ir para a tarefa 2. Então depende muito de como você interage com ele.

**Verification Before Completion** (`verification_before_completion.md`): Evidência antes de afirmação sempre! Antes de declarar qualquer coisa concluída, ele vai mapear, vai exigir provas, testes, build, correção de bug, enfim, bem interessante.

E o **Systematic Debugging** (`systematic_debugging.md`): Ele não faz nenhuma correção sem investigar qual é a causa raiz. Então ele tem a causa raiz, ele vê os erros, ele reproduz, revisa o que ele fez recentemente, ele faz análise do padrão, ele cria uma hipótese e testa, ele corrige, testa se falha de novo. Se três ou mais correções falharem, ele questiona a arquitetura. Então ele realmente vai ser aquele cara chato, né? É o seu QA sênior, é o seu superior que vai ficar procurando erro dentro do seu projeto.

### As Habilidades de Domínio (Domínio Skills)
A gente tem o **Django SAS Workflow**, que foi criada por mim para ser o pipeline completo. Então eu escrevo em linguagem natural, gero o PRD, gero as sprints, tem os modos do Django (tem dois modos: um modo de desenvolver um SAS do zero e o outro é adicionar uma nova funcionalidade em um SAS existente). Cada módulo tem seis fases. Cada modo tem os padrões dentro dessa skill: o padrão de código, ele tem uma referência pro template inicial, tem uma referência pro Docker (o Docker Compose, Dockerfile), o `settings` também é padrão, o gerar documentação (que é importante, a gente precisa que o nosso sistema tenha a documentação completa). Então tudo isso é o workflow do Django, né? Ele é a nossa skill principal porque ele diz como é que o negócio vai funcionar.

O **Ecosystem Prime** ele é o ponto de partida para a inteligência artificial. Você tem os deep agents, LangChain, LangGraph, as variáveis de ambiente de LLM ficam dentro da documentação do LLM, né? E aí ele pega isso e joga para as skills *LangChain Fundamentals* e *LangChain Dependencies*.

Então, se você vai usar somente um SAS sem IA, ele só vai usar essa skill (Django SAS Workflow). Se você vai colocar IA dentro do seu SAS, ele vai utilizar isso aqui tudo, né?

Ok. Então, por que só são 10? 10 skills principais, porque as outras skills do Superpowers eu não achei interessante, né? Então peguei seis, coloquei mais essas outras, mas poderia ter mais. Mas essas aqui já são suficientes pro meu workflow, e para mim já funciona bem, atualmente funciona bem.

**[1:32:26 - 1:36:15]**

OK. Então o Superpowers tem isso aqui: toda ação criativa ou correção começa pela skill, invocação antes de qualquer resposta. E aqui a gente tem essa questão da prioridade: skill de processo primeiro, implementação depois. A hierarquia, como eu já tinha falado: instrução do usuário, skill e o padrão.

Então está aí as skills do processo, só resumindo.

### O Fluxo (Workflow)
O fluxo a gente tem dois:
1.  **Um projeto do zero.**
2.  **Adicionar uma nova feature.**

Então ele faz **oito perguntas**, uma por vez. Ele faz, você responde; ele faz, você responde.

E aí as fases iniciais: a "casca do projeto" (que é quando ele entende o que você quer fazer, ele gera o PRD, ele gera as tasks, as tarefas). Aí ele vai pedir o design system e aí vai fazer as sprints, até chegar na documentação.

Eu tenho um arquivo `setup` que cria essa casca, que é a fase zero. O que que é esse setup? No caso do Windows é `setup.ps1`, no Linux é `setup.sh`. É um arquivo que eu criei dentro do projeto. Deixa eu ver se eu mostro aqui para vocês. É um Bootstrap básico pro nosso sistema. O que que ele vai fazer? Bem simples: ele vai instalar dependências, vai criar o projeto Django, vai configurar o Postgres. Não tem o Git, não tem o GitHub (porque no Windows é mais chato, eu teria que utilizar o WSL, que é o Linux dentro do Windows).

Então o que que ele vai fazer? Ele basicamente vai criar o ambiente virtual, vai criar o Django project, ele vai criar os requirements só depois que eu souber o que que a gente vai precisar... a casca, a parte inicial. Para quê? Para eu não gastar token de IA com essas coisas que eu mesmo posso fazer.

E aí ele vai pedir a database URL, se você já tiver um domínio vai pedir, e a chave do Open Code. Então essa é a fase zero, essa é a casca do meu projeto.

O módulo dois...

## PARTE 15: O Processo de Escopo, as 8 Perguntas do Agente e a Engenharia de Prompts
**[1:36:15 - 1:40:08]**

...já tem o projeto pronto, né? Então ele vai analisar o projeto, ele vai ler o PRD, a documentação, e aí ele vai pegar o PRD que já existe e vai atualizar, tá certo? Aí vai criar uma nova lista de tarefas e vai desenvolver, tá certo?

### As Oito Perguntas (Fase Inicial de Escopo)
Temos aqui:
1.  Qual o **nome do projeto**?
2.  Descreva o **SaaS em duas ou três frases** (O que é? Para quem? Qual o problema?).
3.  Quais as **funcionalidades principais**?
4.  Você tem um **HTML de referência**? (Se não tiver, ele para. Ele não inventa visual. Por quê? Porque eu estou cansado de ver sites e saber: "isso aí foi o Claude que fez, esse aí foi o Codex que fez, esse aí foi o Open Code que fez". Dá para saber, né? Então, tem sites que vendem templates de HTML bem baratinho, você compra pacotes com vários HTMLs e aí você tem uma biblioteca pronta. "Ah, eu gostei desse aqui, vou usar ele no meu sistema". Pronto, aí você usa. Mas deixar a IA inventar o template, eu não gosto).
5.  Você já tem a **arquitetura de apps** mapeada? (Sim, porque o nosso setup já faz isso).
6.  O sistema **vai ter IA integrada**? (Se sim, LangChain; se não, não).
7.  Qual a **URL do banco de dados** (Database URL)?
8.  Quer **criar o repositório GitHub** agora? (Eu boto não, porque eu gosto de fazer manualmente).

Então são as oito perguntas do modo 1.

Tem as fases que eu já tinha falado, o Bootstrap (projeto, `setup.sh` que é a fase zero).

### Os Prompts do Sistema
E a parte que talvez seja a mais difícil, que é a gente descrever o que a gente quer. Então lá naquele arquivo de prompt tem um modelo que é o que o Python Code utiliza, dando as referências pro prompt. Achei muito legal essa ideia do prompt bruto, depois o prompt refinado, e aí eu coloquei dentro do meu workflow. O meu agente ele já faz isso, mas se você não quiser pedir pro agente fazer o PRD direto, você pode utilizar esses prompts também.

E aí o prompt ele traz o quê? Um briefing bruto (qual é a situação, qual é o problema, qual é a solução, qual é o impacto, stack, apps e as regras). Ele vai fazer tudo isso pra gente. Então a gente vai criar esse prompt bruto com tudo que a gente quer.

Depois a gente faz o refinamento, ou seja, eu escrevo em linguagem natural o meu prompt, aí eu boto pro GPT (por exemplo): "GPT, transforma, refina esse prompt aí". Aí eu vou ter um refinamento, e aí eu utilizo **linguagem mandatória** (*deve*, *nunca* — o que ele deve fazer e o que ele nunca deve fazer, tá certo?).

**[1:40:08 - 1:44:25]**

Então, as boas práticas do nosso prompt vão ser: quem vai usar esse nosso sistema, para quê, quais são as restrições, a stack é fixa, o que eu estou proibindo, quais são as seções que o seu sistema vai ter, e aí ele vai fazer perguntas, uma por vez, tá bom? Ele vai gerar o PRD a partir desses prompts.

### O que é o PRD (Product Requirements Document)?
E aí vem a pergunta: o que que é esse negócio de PRD que eu tanto falo desde o começo? O PRD ele é o *Product Requirements Document*, ele é um documento que traduz o briefing que foi aprovado com a IA na especificação completa do produto: o que vai ser construído, para quem, com quais requisitos, que arquitetura, que visual, qual é a ordem das tarefas.

Dentro do nosso workflow, ele vai ser gerado na fase 1, seguindo o template que a gente vai utilizar. O que que tem que ter dentro desse nosso PRD?
*   **Visão Geral:** Informações sobre o produto, propósito, público-alvo, objetivos.
*   **Requisitos Funcionais:** O que vai funcionar no sistema (e aí eu coloco diagramas Mermaid, que são diagramas de fluxo em texto que você salva no Git, pode ficar lá no README).
*   **Requisitos Não Funcionais:** Desempenho, segurança, usabilidade.
*   **Arquitetura Técnica:** Qual é a stack técnica utilizada.
*   **Design System:** A gente fornece o HTML de referência e o próprio sistema vai extrair a estrutura daquela página para poder colocar dentro do Tailwind CSS dele.
*   **Critérios de Aceite** e métricas de sucesso.
*   **Riscos e Mitigações**.
*   **Lista de Tarefas (Tasks):** Essa lista de tarefas vai ter caixinhas de seleção, e a cada tarefa que ele for fazendo ele vai marcando com um xis, e assim você consegue acompanhar o progresso.

Depois que o PRD é gerado, você precisa aprovar antes de ir para a fase seguinte. Ele sempre vai gerar o PRD no mesmo template. E é isso.

### O que são as Tasks (Lista de Tarefas)?
A *tasks.md* é uma parte do PRD, são as tarefas que eu vou copiar e vou colocar em outro arquivo. Ele é o plano de execução do PRD, então eu tenho as tarefas numeradas que vão ter a tarefa e a subtarefa. Por exemplo:
*   *Tarefa 7:* Inserir a IA no dashboard.
    *   *Subtarefa 7.1:* Configurar o LangChain.
    *   *Subtarefa 7.2:* Configurar a API do Open Code.
    *   *Subtarefa 7.3:* Ler os dados.

E assim ele vai tendo essas subtarefas. E aí quando ele vai fazendo a sprint, você pode ficar com esse arquivo de tasks ou com o próprio PRD aberto, e você vai vendo ele marcando à medida que vai progredindo.

## PARTE 16: Checkpoints, Open Spec (Desenvolvimento Dirigido por Especificações) e Encerramento
**[1:44:25 - 1:48:25]**

Vai ter o **Checkpoint**, né? Eu coloco o checkpoint logo após cada fase e cada sprint, e aí você verifica se tudo que ele fez está de acordo com o que você pediu, e aí você aprova ou não.

No começo a gente não vai ter muita coisa para ver não, porque as sprints iniciais são a base do sistema. Então, por exemplo: rodou a tarefa 1, aí "ah, eu quero ver o sistema funcionando". Não tem sistema ainda! Ele criou os apps, criou o settings, criou o core. Então vai demorar umas quatro ou cinco sprints para você conseguir ver alguma coisa realmente funcionando. E aí, cada sprint vai ter o seu checkpoint com perguntas explícitas ao usuário, e o agente não vai avançar sem a resposta, tá certo?

O Codex cuida dessa divisão de agentes muito bem, o Open Code também. Mas no Codex você consegue ver cada agente fazendo a sua tarefa, no Open Code eles trabalham em paralelo e a gente só vê o resultado, tá bom?

### O que é o Open Spec (Especificações de Desenvolvimento)?
Uma coisa que eu também falei bastante é o **Open Spec**, que é a nossa terceira camada (a primeira são os agentes, a segunda é o Superpowers e a terceira é o Open Spec). O que que é o Open Spec? É o *Spec-Driven Development*.

O Open Spec é um sistema de desenvolvimento dirigido por especificações. Os requisitos vivem como specs que são versionadas por capacidade e que ficam na pasta `specs/`. Toda mudança que você colocar vai ser uma proposta, vai ter uma tarefa e você vai ter que validar essa especificação.

E para que que isso serve? Isso serve para que a gente possa ter controle do nosso sistema e para que a **memória de curtíssimo prazo da IA seja aumentada**. Então, basicamente, a gente tem cinco Open Specs de processo:
1.  **Open Spec Explore (`ops-explore`):** Ela investiga o código, compara, usa diagramas, captura insights quando pedido.
2.  **Open Spec Propose (`ops-propose`):** Ela cria uma mudança nova e gera todo o artefato de planejamento. Ela planeja, não implementa.
3.  **Open Spec Apply (`ops-apply`):** Ela implementa as tarefas de mudança, executando as tarefas e marcando com o xis.

**[1:48:25 - 1:52:38]**

4.  **Open Spec Sync (`ops-sync`):** Ela mescla mudanças com as specs principais.
5.  **Open Spec Archive (`ops-archive`):** Ela arquiva uma mudança concluída.

Então esses specs servem basicamente para ser as especificações e o versionamento do projeto. Então, por exemplo, você tem o seu PRD original, e o Open Spec vai registrar todas as mudanças que a gente vai ter no nosso sistema, tá certo?

Pronto, né? Aí aqui a gente já vai pro exemplo do dia, que é o que a gente vai começar a trabalhar, tá certo?

*(Interação final e dúvidas com os alunos antes do intervalo)*
— Dúvidas, perguntas até o presente momento? Podemos fazer o nosso intervalo e voltar daqui a 15 minutinhos pra gente pôr a mão na massa. Tiverem alguma pergunta? Senão a gente dá uma pausa agora e volta já já.
— Sim, Thalis tem sim. Pois pode falar... tu quer escrever ou falar?
— Comparando tudo que você falou até agora com o meu projeto que eu estou fazendo, eu estou bem... mas tem muita ponta solta ainda.
— Boa. E a ideia aqui é a gente ampliar ainda mais, porque isso aqui é um mundo de possibilidades, né? Não é só chegar pro agente e pedir: "ah, faça isso pro Codex, faça aquilo". Quanto mais você controlar o projeto, melhor ele vai sair. Porque assim, você toma todos esses cuidados, e depois, enquanto ele vai trabalhando, você assiste a um jogo, joga uma partidinha de FIFA enquanto a IA trabalha para você.
— É, mas eu já retirei muitas pontas soltas, mas revisando cada dia mais para melhorar. Cada dia aparecem coisas novas.
— Então, pronto.
— O Thalis colocou aqui: "Seu workflow é muito parecido com o do Pod Codar. Eu assisti todo e é muito interessante. Já tenho usado, mas na hora de solicitar IA para gerar, tive um problema de limite."
— Pronto. O Pod Codar eu acompanho desde quando começou, então todos os cursos que ele faz, tudo que ele produz, eu estou lá dentro, gosto muito. É uma referência para mim. Aí quando ele lançou esse último workshop, esse último material do AI Master, eu fiz igual o Rafael colocou aí: vi as pontas soltas que tinha no meu e fui ajustando. E aí a forma como ele faz o PRD, eu coloquei aqui dentro, dentro daqueles prompts. Deixa eu até mostrar para vocês. Vamos aqui em VS Code... eu coloquei a forma como ele faz aqui dentro e vou mostrar como ele faz aqui em prompts, que são os modelos de prompts. Só que vocês vão ver que o prompt gerado dessa forma e o prompt que o agente do Open Code do Superpowers vai fazer ficam muito parecidos e não fica tão grande. Então eu já tive esse problema de limite.

**[1:52:38 - 1:53:22]**

Quando a gente pede para gerar lá no ChatGPT, fica muito grande, ele não consegue. Por isso que eu inseri esse fluxo aqui dentro para ele gerar aqui dentro. E aí vocês vão ver como fica muito bom, fica muito legal. Depois do intervalo vocês vão ver. Beleza? Mas o Pod Codar é a minha referência número um dentro desse mundo de desenvolvimento.

Então, pronto. São 11h05 no horário do Brasil, então 11h20, daqui a 15 minutos a gente retorna, tá certo? Então, até daqui a pouco.

*(Fim da primeira parte do Workshop)*