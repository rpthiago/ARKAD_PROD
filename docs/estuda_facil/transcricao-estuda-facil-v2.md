# DOCUMENTO MESTRE: Workshop de Desenvolvimento Assistido por IA — Sistema Estuda Fácil (Dia 1 e Dia 2 Completos)

> **Instrução de Contexto Mestre para o Gemini:**
> Este documento contém a descrição completa, detalhada, sem cortes e estruturada de **todo o Workshop de Desenvolvimento Assistido por Inteligência Artificial (Dia 1 e Dia 2)** para a criação do sistema **Estuda Fácil (Estudia)**.
> 
> Ele reúne todas as diretrizes de arquitetura, especificações técnicas, hardgates, soluções de erros, configuração de ambiente e comandos de deploy em VPS.
> 
> **Utilize todo este material como base de conhecimento (grounding) para:**
> 1. Atuar como Co-Desenvolvedor / Arquiteto especialista no projeto Django "Estuda Fácil".
> 2. Gerar ou ajustar os arquivos da metodologia Open Spec (`PRD.md`, `tasks.md`, `agents.md`, `worklog.md`).
> 3. Escrever e revisar código Django, Celery, LangChain/LangGraph, Tailwind CSS e Docker sem violar nenhum Hardgate.
> 4. Guiar a execução das Sprints de desenvolvimento e auxiliar na resolução de bugs de produção e deploy no Easy Panel.

---

# PARTE 1 — DIA 1: DO CONTEXTO AO PLANO EXECUTÁVEL (PRD, TASKS E ARQUITETURA)

## 1.1. Visão Geral e Filosofia do Workshop
* **Superação do "Vibe Coding":** O workshop contrapõe a programação desordenada à abordagem de **Desenvolvimento Dirigido por Especificação (Open Spec)**, onde cada decisão arquitetural e funcional é registrada no próprio repositório em arquivos Markdown.
* **Ferramentas e Custos:**
  * **Open Code:** CLI Open Source para orquestração de múltiplos LLMs. Utilização do plano Open Code GO ($10/mês) e modelos como DeepSeek, GLM 5.3 e Mimo v2.5.
  * **Cursor IDE:** Editor inteligente com suporte a modelos como Grok e Claude Sonnet.
  * **OpenAI Codex / ChatGPT:** Utilizado para geração de back-end, autenticação e regras de negócio.
  * **Investimento Total de IA:** Cerca de R$ 250/mês para manter autonomia completa de desenvolvimento.

## 1.2. A Stack Tecnológica do Estuda Fácil (Estudia)
* **Back-end:** Python (3.11/3.12) e Django Web Framework.
* **Banco de Dados:** PostgreSQL obrigatório desde o primeiro dia de desenvolvimento (SQLite é expressamente proibido).
* **Front-end:** Templates Django, Tailwind CSS, Bootstrap e HTMX (sem SPA/React para manter simplicidade e velocidade).
* **Tarefas Assíncronas:** Celery + RabbitMQ para tarefas demoradas (como processamento de arquivos PDF e chamadas de IA).
* **Camada de IA:** LangChain e LangGraph integrados com Open Code GO (Modelo Mimo v2.5) para geração adaptativa de trilhas, notas de aula, quizzes e flashcards.
* **Infraestrutura:** Docker, Gunicorn, VPS (Hostinger KVM4/KVM8) e Easy Panel.

## 1.3. Os 4 Pilares da Metodologia Open Spec
1. **Papéis Canônicos (`agents.md`):** Definição estrita das responsabilidades de cada agente de IA.
2. **Adaptadores Multiclientes (`.opencode`, `.codex`, `.cursor`):** Compatibilidade para alternar entre IDEs e clientes de IA sem perder memória de contexto.
3. **Biblioteca de Superpowers (Skills):** Protocolos e verificações rigorosas executados antes da implementação de qualquer código.
4. **Memória Compartilhada (`worklog.md`):** Registro contínuo de progresso, decisões e handoffs de sessão.

## 1.4. Os 7 Agentes Especialistas
* **Django Back End:** Modelagem de dados, regras de negócio e views.
* **Django Front End:** Interfaces responsivas em Tailwind e HTMX.
* **Async Worker:** Filas e tarefas assíncronas no Celery.
* **AI Engineer:** Integração com LangChain, Mimo v2.5 e tratamento de prompts.
* **Platform DevOps:** Dockerfiles, Docker Compose, VPS e Easy Panel.
* **QA Engineer:** Testes automatizados, reprodução prévia de bugs e checagens de regressão.
* **Security Review:** Verificação de vulnerabilidades, saneamento de segredos e prevenção de vazamento de chaves de API.

---

# PARTE 2 — DIA 2: EXECUÇÃO, TESTE TRIPLO EM TEMPO REAL E DEPLOY EM PRODUÇÃO

## 2.1. Os 8 Hardgates Inegociáveis de Execução
1. **Casca Primeiro:** A IA deve criar a estrutura funcional básica do Django antes de implementar qualquer tela ou modelo de domínio.
2. **Sem Código Extra:** Nenhuma funcionalidade é criada se não estiver prevista no `PRD.md` e no `tasks.md`.
3. **PostgreSQL Obrigatório:** O banco PostgreSQL deve ser configurado e conectado via `DATABASE_URL` antes de qualquer migration.
4. **Design System Extraído:** O visual de todas as páginas deve copiar rigorosamente os templates salvos na pasta `material/template/`.
5. **Idioma Padrão:** Código, nomes de variáveis, comentários e arquivos de configuração exclusivamente em inglês; interface do usuário (UI) exclusivamente em português.
6. **Rastreabilidade de Dados:** Todo model do Django deve herdar os campos `created_at` e `updated_at`.
7. **Sem Testes Automatizados Iniciais:** As sprints iniciais focam apenas na fundação da aplicação (MVP).
8. **Sem MCP Desnecessário:** A IA não recria agentes nem instala pacotes não autorizados.

## 2.2. Acompanhamento Prático das Sprints do Projeto
* **Sprint 1 (Bootstrap & Auth):** Criação da estrutura Django, modelo de usuário customizado (`CustomUser`), cadastro e autenticação exclusivamente por e-mail.
* **Sprint 2 (Design System & UI Base):** Extração dos componentes de estilo em Tailwind CSS a partir dos modelos de referência HTML.
* **Sprint 3 (Landing Page & Dashboard):** Criação da página pública do sistema e painel do estudante com contadores de espaços e materiais.
* **Sprint 4 (CRUD de Espaços e Upload de Materiais):** Criação de áreas de estudo privadas (ex: Biologia Celular) e upload de arquivos PDF/Texto.
* **Sprint 5 (Trilha de Aprendizagem com IA):** Leitura de PDFs via LangChain e geração automática de resumo, notas de aula e plano de estudo pelo modelo Mimo v2.5.
* **Sprint 6 (Exercícios Práticos - Quizzes e Flashcards):** Geração de simulados adaptativos com explicação de respostas e visual de cartas interativas em flashcards.
* **Sprint 14 (Recuperação de Senha & E-mail):** Inserção de funcionalidade de recuperação de senha via SMTP (Gmail) e validação de e-mail do usuário.

## 2.3. O Teste Triplo ao Vivo (Cursor vs. Codex vs. Open Code)
Durante a demonstração prática, o instrutor colocou 3 IAs para rodar simultaneamente na mesma base de código:
* **Cursor (com modelo Grok):** Apresentou a maior velocidade na implementação de telas, ajustes de Tailwind CSS e lógica de quizzes e flashcards (rodando na porta `8000`).
* **OpenAI Codex / ChatGPT:** Trabalhou de forma sólida na orquestração de back-end, autenticação e validação de envios de e-mail (rodando na porta `8001`).
* **Open Code (com DeepSeek / Mimo):** Exigiu mais intervenções manuais em layout e modelos, mas entregou flexibilidade de custos (rodando na porta `8002`).
* **Handoff em Ação:** Mostrou na prática que quando os limites de requisição de uma ferramenta acabam, basta abrir outra IDE, que ela lê o `worklog.md` e continua exatamente de onde a anterior parou.

## 2.4. Diagnóstico e Resolução de Erros de Produção em Tempo Real
1. **Erro de Layout "Tela de Celular" no Open Code:**
   * *Problema:* A página inicial ficou reduzida ao canto esquerdo da tela.
   * *Causa:* O modelo DeepSeek omitiu a renderização de classes pai do grid do Tailwind e deixou elementos da sidebar vazios na home.
   * *Solução:* Leitura da estrutura DOM, ajuste na herança de templates e forçamento da re-compilação do CSS.
2. **Erro 401 (Invalid API Key / Unauthorized) na IA:**
   * *Problema:* Falha ao gerar trilhas de aprendizagem no LangChain.
   * *Causa:* Chave de API antiga mantida no processo em memória do Django sem re-inicialização do servidor.
   * *Solução:* Atualização da variável no `.env`, reinicialização do servidor e especificação do modelo `mimo-v2.5`.
3. **Erro de Timeout no Gunicorn (Internal Server Error / HTTP 500):**
   * *Problema:* Requisições pesadas de IA derrubavam a aplicação no meio da geração de quizzes.
   * *Causa:* O tempo padrão de resposta do Gunicorn é de 30 segundos, e a geração levava perto de 45 a 60 segundos.
   * *Solução:* Alteração do parâmetro `--timeout 180` no comando do Gunicorn dentro do `Dockerfile`.
4. **Erro Bad Request (HTTP 400) / CSRF 403 no Easy Panel:**
   * *Problema:* O Django recusava requisições de login e exibia tela de erro ao rodar na VPS.
   * *Causa:* O domínio da VPS não estava listado em `ALLOWED_HOSTS` e os orígens confiáveis em `CSRF_TRUSTED_ORIGINS`.
   * *Solução:* Ajuste das variáveis no painel do Easy Panel incluindo o protocolo `https://` para o CSRF e a URL limpa sem barra no final para o `ALLOWED_HOSTS`.
5. **Conflito de Migrations no PostgreSQL:**
   * *Problema:* O Django recusava rodar `migrate` dizendo que o histórico já existia.
   * *Causa:* Reutilização da mesma base de dados PostgreSQL para duas instâncias diferentes do sistema.
   * *Solução:* Criação de bancos de dados isolados no Easy Panel para cada ambiente/cliente (`studia_cursor`, `studia_codex`, `studia_opencode`).

## 2.5. Guia Completo de Deploy na VPS com Easy Panel
* **Passo 1: Repositório GitHub:** Subir o código-fonte para um repositório privado no GitHub, garantindo que o `.env` esteja no `.gitignore`.
* **Passo 2: Configuração do Dockerfile:**
  ```dockerfile
  FROM python:3.12-slim
  WORKDIR /app
  COPY requirements.txt .
  RUN pip install --no-cache-dir -r requirements.txt
  COPY . .
  RUN python manage.py collectstatic --noinput
  EXPOSE 8000
  CMD ["gunicorn", "core.wsgi:application", "--bind", "0.0.0.0:8000", "--timeout", "180"]
  ```
* **Passo 3: Aplicação no Easy Panel:**
  * Criar um novo **App** do tipo **Service** apontando para o repositório do GitHub.
  * Definir o arquivo de build como `Dockerfile`.
  * Ajustar a porta de escuta do container para `8000`.
* **Passo 4: Variáveis de Ambiente (Environment Variables):**
  * Configurar na aba **Environment**:
    * `DATABASE_URL=postgres://usuario:senha@host:5432/nome_banco`
    * `SECRET_KEY=sua_chave_secreta_longa_e_aleatoria`
    * `DEBUG=False`
    * `ALLOWED_HOSTS=seu-dominio.com,seu-app.easypanel.host`
    * `CSRF_TRUSTED_ORIGINS=https://seu-dominio.com,https://seu-app.easypanel.host`
    * `OPENCODE_API_KEY=sua_chave_opencode`
* **Passo 5: Deploy & Certificado SSL:**
  * Clicar em **Deploy / Implantar**.
  * O Easy Panel irá compilar a imagem Docker, executar as etapas de coleta estática, conectar ao PostgreSQL e emitir o certificado SSL automaticamente via Let's Encrypt.

---

# RESUMO CRONOLÓGICO DA TRANSCRIÇÃO DOS VÍDEOS (DIA 1 + DIA 2)

## Bloco 1: Abertura e Mudanças no Mercado de IAs (0:00 - 15:00)
O instrutor faz a abertura do workshop de sábado, explicando que o cenário de ferramentas mudou drasticamente desde o workshop anterior. Ferramentas como AntiGravity e Trey alteraram suas políticas e preços, assim como o GitHub Copilot e o ChatGPT mudaram seus limites de uso de 5 horas. Por isso, a metodologia atual adota o **Open Code**, o **Cursor** e o **Codex**, garantindo independência e custos controlados.

## Bloco 2: Filosofia Open Spec e a Estrutura de Agentes (15:00 - 45:00)
Explicação detalhada sobre o funcionamento dos 7 Agentes Especialistas e como os arquivos de especificação (`PRD.md`, `tasks.md`, `agents.md` e `worklog.md`) funcionam como o cérebro persistente do projeto no Git. Apresentação da stack Django + PostgreSQL + Tailwind + Celery.

## Bloco 3: Execução Prática e Hardgates no Dia 2 (45:00 - 1h30min)
Início da transmissão prática do Dia 2. O instrutor estabelece os 8 Hardgates inegociáveis. Demonstração de criação de casca Django, extração de componentes do `material/template/` para o Tailwind CSS, e início do teste simultâneo no Cursor, Codex e Open Code.

## Bloco 4: Teste Triplo e Resolução de Erros de Sistema (1h30min - 2h30min)
Acompanhamento em tempo real do progresso das Sprints 1 a 6. Resolução ao vivo dos erros de layout no Open Code, estouro de timeout no Gunicorn ao chamar a IA LangChain, rotação de chaves de API e ajustes visuais nos Quizzes e Flashcards (transformando-os em cartas interativas).

## Bloco 5: Deploy em Produção na VPS com Easy Panel (2h30min - 3h28min)
Criação do banco de dados PostgreSQL na Hostinger VPS via Easy Panel. Configuração do `Dockerfile`, upload das variáveis de ambiente com `CSRF_TRUSTED_ORIGINS`, execução de migrations automáticas e disponibilização da aplicação "Estuda Fácil" com HTTPS ativo para acesso mundial.
