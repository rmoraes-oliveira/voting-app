# Voting App

Sistema de votação em tempo real: uma API recebe votos, publica cada voto como evento no Kafka, um consumer processa esses eventos e atualiza os contadores no Redis. Inclui observabilidade completa (métricas, logs, dashboards) e um frontend simples para votar.

## Sumário

- [Arquitetura](#arquitetura)
- [Serviços](#serviços)
- [Como subir o ambiente localmente](#como-subir-o-ambiente-localmente)
- [Documentação das APIs](#documentação-das-apis)
- [Proteção contra bots](#proteção-contra-bots)
- [Observabilidade](#observabilidade)
- [Teste de carga](#teste-de-carga)
- [CI/CD](#cicd)

## Arquitetura

```
                    ┌─────────────┐
                    │  Frontend   │  :8080
                    └──────┬──────┘
                           │
                           ▼
┌──────────────┐    ┌─────────────┐    ┌───────────┐    ┌──────────┐
│   Admin API  │───▶│    Redis    │◀───│  Consumer │◀───│  Kafka   │
│ /admin/poll  │    │ (estado do  │    │ (Karafka) │    │ (tópico  │
└──────────────┘    │  poll +     │    └───────────┘    │  votes)  │
                     │  contadores)│                     └────▲─────┘
┌──────────────┐    └──────▲──────┘                          │
│  Voting API  │───────────┘                                 │
│  POST /votes │──────────────────────────────────────────────┘
└──────────────┘   (publica evento de voto)
      :9292
```

**Fluxo de um voto:**
1. Cliente faz `POST /votes` com `candidate_id`.
2. A API valida (poll ativo, candidato existe) consultando o Redis (1 round-trip via pipeline).
3. A API publica o evento no tópico Kafka `votes` (fire-and-forget, sem esperar confirmação de delivery) e responde `202 Accepted` imediatamente.
4. O `consumer` (Karafka) lê o tópico `votes` de forma assíncrona e incrementa os contadores no Redis (`votes:total:<candidate_id>` e `votes:hourly:<candidate_id>:<hora>`).
5. Se o Redis perder dados (crash, volume apagado), o `VoteReconciler` reconstrói os contadores lendo o histórico completo do tópico Kafka no boot da API — o Kafka é a fonte da verdade dos votos.

**Por que Kafka entre a API e o Redis?** Desacopla o caminho de escrita (aceitar o voto) do caminho de processamento (contabilizar o voto). A API responde rápido sem depender da disponibilidade do consumer, e o Kafka garante que nenhum voto se perde mesmo se o consumer cair temporariamente.

## Serviços

| Serviço | Papel | Porta |
|---|---|---|
| `api` | Sinatra + Falcon — recebe votos (`VotingAPI`) e administra polls (`AdminAPI`) | `9292` |
| `consumer` | Karafka — consome o tópico `votes` e atualiza contadores no Redis | — |
| `kafka` | Broker Kafka (modo KRaft, sem Zookeeper) | `9092` |
| `kafka-ui` | Painel web para inspecionar tópicos/mensagens/consumer groups | `8090` |
| `redis` | Estado do poll atual + contadores de votos (com AOF habilitado) | `6379` |
| `frontend` | Página HTML+JS para votar (lista candidatos, resolve o proof-of-work, envia o voto) | `8080` |
| `prometheus` | Coleta métricas da API (`/metrics`) | `9090` |
| `grafana` | Dashboards de métricas e logs | `3000` |
| `loki` | Armazena logs agregados | `3100` |
| `promtail` | Coleta logs dos containers e do arquivo de log da API, envia para o Loki | — |
| `hey` | Ferramenta de teste de carga (só roda sob demanda) | — |

### Por que cada serviço/tecnologia foi escolhido

- **Sinatra + Falcon**: Sinatra é minimalista o suficiente para uma API com poucas rotas, sem o overhead de um framework full-stack. Falcon foi escolhido no lugar de um servidor tradicional (Puma/WEBrick) por ser assíncrono nativo (fibers), o que combina bem com I/O concorrente para Redis e Kafka sem bloquear threads.
- **Kafka**: desacopla o recebimento do voto (caminho crítico, precisa responder rápido) do processamento (contabilização, que pode ser assíncrona). Também funciona como log de eventos imutável — é a fonte da verdade que permite reconstruir os contadores do Redis caso os dados se percam (ver `VoteReconciler`). Rodar em modo KRaft (sem Zookeeper) simplifica a topologia para este escopo.
- **Karafka**: framework de consumer mais usado no ecossistema Ruby para Kafka, dá roteamento de tópicos/consumer por classe (`VotesConsumer`) sem precisar gerenciar o loop de poll manualmente.
- **Redis**: acesso de baixíssima latência para o estado que muda a cada voto (contadores, status do poll). AOF habilitado (`appendonly yes`, `everysec`) para não depender só da memória — sobrevive a reinícios do container sem perder votos já contabilizados.
- **kafka-ui**: única forma prática de inspecionar mensagens e o estado dos consumer groups sem depender de `kafka-console-consumer.sh` via `docker exec` a cada consulta.
- **Prometheus + Grafana**: par padrão de mercado para métricas — Prometheus faz scrape/armazena séries temporais, Grafana visualiza. Permite medir SLI (latência, disponibilidade) e comparar contra o SLO definido, algo que só olhar o Redis (snapshot atual) não permite.
- **Loki + Promtail**: mesmo raciocínio do Prometheus, mas para logs (“Prometheus para logs”). Promtail foi necessário em vez de só usar o `docker logs` porque a API roda em múltiplos processos forkados (Falcon `--count 8`) e o stdout dos processos filhos não chega ao PID 1 do container — a API grava em arquivo compartilhado e o Promtail lê esse arquivo diretamente.
- **hey**: gerador de carga leve (binário único, sem dependências) usado para validar o SLO de latência/disponibilidade sob diferentes níveis de concorrência. Roda via `profiles: ["tools"]` para não subir com o restante da stack.
- **Frontend em HTML+JS puro**: para uma única tela (votar + ver candidatos), um SPA com framework (React/Vue) adicionaria build step e dependências sem benefício real — a página usa `fetch` e a Web Crypto API nativa do navegador para resolver o proof-of-work, sem nenhuma biblioteca externa.
- **Proof-of-work em vez de CAPTCHA**: resolve o mesmo problema (distinguir voto de pessoa vs. de máquina em massa) sem depender de um serviço de terceiros (Google/hCaptcha) nem exigir chave de API — funciona 100% local, inclusive em CI. Ver [Proteção contra bots](#proteção-contra-bots).

## Como subir o ambiente localmente

### Pré-requisitos
- Docker e Docker Compose

### Passos

1. Configure as variáveis de ambiente da API (opcional — já tem defaults funcionais):
   ```bash
   cp api/.env.example api/.env
   ```

2. Suba a stack completa:
   ```bash
   docker compose up -d --build
   ```

3. Aguarde todos os serviços ficarem saudáveis:
   ```bash
   docker compose ps
   ```

4. Crie um poll (veja [Documentação das APIs](#documentação-das-apis) para o payload completo):
   ```bash
   curl -X POST http://localhost:9292/admin/poll/start \
     -H "X-Api-Key: dev-secret-key" \
     -H "Content-Type: application/json" \
     -d '{"candidates":[{"id":"a","name":"Alice"},{"id":"b","name":"Bob"}]}'
   ```

5. Acesse http://localhost:8080 para votar pela interface, ou use os endpoints diretamente:
   - Frontend: http://localhost:8080
   - API: http://localhost:9292
   - Kafka UI: http://localhost:8090
   - Prometheus: http://localhost:9090
   - Grafana: http://localhost:3000 (`admin` / `admin`)

### Encerrar

```bash
docker compose down        # mantém os volumes (dados do Redis, Grafana)
docker compose down -v     # remove tudo, incluindo dados persistidos
```

### Desenvolvimento (hot reload sem rebuild)

Os serviços `api` e `consumer` montam `./api` como volume dentro do container. Isso significa que alterações no código Ruby não exigem `docker compose build` — basta reiniciar o(s) processo(s) para carregar o novo código:

```bash
docker compose restart api consumer
```

O `frontend` monta `./frontend/index.html` como volume — como é servido estaticamente pelo nginx, basta salvar o arquivo e dar refresh no navegador (sem restart, sem rebuild).

O rebuild da imagem (`docker compose up -d --build`) só é necessário ao alterar `Gemfile`/`Gemfile.lock` ou os `Dockerfile`s.

## Documentação das APIs

### Voting API (`http://localhost:9292`)

#### `GET /health`
Healthcheck da aplicação.

```bash
curl http://localhost:9292/health
```

#### `GET /polls`
Lista todos os polls ativos no momento (pode haver mais de um simultaneamente). Usado pelo frontend para exibir a tela de escolha de votação.

```bash
curl http://localhost:9292/polls
```

Exemplo de resposta:
```json
{
  "status": "success",
  "count": 1,
  "polls": [
    {
      "poll_id": "abc123",
      "status_value": "active",
      "started_at": "1753200000",
      "candidates": { "a": "Alice", "b": "Bob" }
    }
  ]
}
```

#### `GET /poll/:poll_id`
Retorna os dados de um poll específico: status, candidatos e horários de início/fim. Usado pelo frontend para montar a lista de opções de voto depois que o usuário escolhe uma votação em `GET /polls`. Retorna `404` se o `poll_id` não existir (nem ativo, nem com resultado salvo).

```bash
curl http://localhost:9292/poll/<poll_id>
```

#### `GET /votes/challenge`
Gera um desafio de proof-of-work necessário para votar (ver [Proteção contra bots](#proteção-contra-bots)).

```bash
curl http://localhost:9292/votes/challenge
```

#### `POST /votes`
Registra um voto. Requer um poll ativo, um `candidate_id` válido e a resolução do proof-of-work.

```bash
curl -X POST http://localhost:9292/votes \
  -H "Content-Type: application/json" \
  -d '{"candidate_id":"a","challenge_token":"<challenge>","nonce":"<nonce>"}'
```

Respostas:
- `202 Accepted` — voto aceito e publicado no Kafka
- `400` — `candidate_id`/`challenge_token`/`nonce` ausente, proof-of-work inválido/expirado, ou candidato inexistente
- `403` — poll não está ativo
- `503` — falha ao publicar no Kafka

#### `GET /votes/summary/:poll_id`
Retorna o total geral de votos, o total por candidato e o total de votos por hora do poll informado. Aceita qualquer `poll_id` ativo ou já encerrado (com snapshot salvo); retorna `404` se o `poll_id` for desconhecido.

```bash
curl http://localhost:9292/votes/summary/<poll_id>
```

Exemplo de resposta:
```json
{
  "status": "success",
  "poll_id": "abc123",
  "total_votes": 150,
  "summary": {
    "a": { "name": "Alice", "total_votes": 100 },
    "b": { "name": "Bob", "total_votes": 50 }
  },
  "hourly_votes": {
    "a": { "name": "Alice", "hourly_votes": { "2026-07-07T21": 100 } },
    "b": { "name": "Bob", "hourly_votes": { "2026-07-07T21": 50 } }
  }
}
```

#### `GET /metrics`
Métricas em formato Prometheus (ver [Observabilidade](#observabilidade)).

```bash
curl http://localhost:9292/metrics
```

### Admin API (`http://localhost:9292/admin`)

Todas as rotas exigem o header `X-Api-Key` com o valor de `ADMIN_API_KEY` (default: `dev-secret-key`).

#### `POST /admin/poll/start`
Cria e ativa um novo poll. Vários polls podem estar ativos ao mesmo tempo, desde que tenham `poll_id` diferentes — falha apenas se o `poll_id` informado já estiver ativo.

```bash
curl -X POST http://localhost:9292/admin/poll/start \
  -H "X-Api-Key: dev-secret-key" \
  -H "Content-Type: application/json" \
  -d '{
    "candidates": [
      {"id": "a", "name": "Alice"},
      {"id": "b", "name": "Bob"}
    ]
  }'
```

Campos:
- `candidates` (obrigatório): lista de objetos `{id, name}`
- `poll_id` (opcional): se omitido, um UUID é gerado

#### `POST /admin/poll/stop/:poll_id`
Encerra o poll informado e grava um snapshot imutável do resultado (candidatos, votos totais e por hora). Falha com `423` se ainda houver lag de mensagens não processadas no Kafka (evita encerrar antes de contabilizar votos em trânsito).

```bash
curl -X POST http://localhost:9292/admin/poll/stop/<poll_id> \
  -H "X-Api-Key: dev-secret-key"
```

#### `GET /admin/poll/results/:poll_id`
Retorna o snapshot salvo de um poll já encerrado.

```bash
curl http://localhost:9292/admin/poll/results/<poll_id> \
  -H "X-Api-Key: dev-secret-key"
```

Retorna `404` se não houver snapshot para o `poll_id` informado.

#### `GET /admin/poll/results`
Lista os snapshots de todos os polls já encerrados, do mais recente para o mais antigo.

```bash
curl http://localhost:9292/admin/poll/results \
  -H "X-Api-Key: dev-secret-key"
```

## Proteção contra bots

**Problema**: sem nenhuma barreira, um script pode disparar milhares de `POST /votes` por segundo pela mesma máquina, distorcendo o resultado. Bloquear por IP não resolve — penalizaria pessoas legítimas atrás do mesmo NAT/Wi-Fi público — e um CAPTCHA de terceiros (reCAPTCHA/hCaptcha) exigiria uma conta/chave externa e não funciona bem em ambiente local sem domínio.

**Solução**: proof-of-work (mesma ideia usada para mineração de blocos), resolvido no navegador antes de cada voto:

1. O cliente busca um desafio em `GET /votes/challenge` — um token aleatório, guardado no Redis com TTL de 5 minutos.
2. O cliente calcula, localmente, um `nonce` tal que `sha256(challenge + nonce)` comece com `4` zeros hexadecimais (~65 mil tentativas em média, ~100–300ms num navegador comum via Web Crypto API).
3. O `POST /votes` envia `challenge_token` e `nonce`. O backend recalcula o hash, confere o prefixo, e invalida o challenge atomicamente (`DEL` no Redis) — garante que cada desafio resolvido vale exatamente 1 voto, sem replay.

Isso não impede um bot de votar, mas encarece cada voto em CPU real — quem quiser votar 10 mil vezes precisa realmente gastar 10 mil desafios de proof-of-work, o que é ordens de magnitude mais caro do que apenas disparar requisições HTTP.

**Bypass para testes de carga**: como o `hey` não sabe resolver o proof-of-work, existe a variável `SKIP_PROOF_OF_WORK` (default `false`) que desativa essa exigência — ver [Teste de carga](#teste-de-carga). Nunca deixe essa variável ativa fora de um teste local.

## Observabilidade

### Métricas (Prometheus + Grafana)

A API expõe métricas em `/metrics`:
- `votes_total{candidate_id}` — total de votos por candidato
- `http_requests_total{method, path, status}` — contagem de requisições
- `http_request_duration_seconds{method, path}` — histograma de latência

Dashboards provisionados automaticamente no Grafana (pasta **Voting App**):
1. **SLO/SLI - Voting API** — disponibilidade e latência p95 de `/votes` frente às metas definidas (disponibilidade ≥ 99%, p95 < 250ms)
2. **Métricas da Aplicação - Votação** — votos por candidato, taxa de votos, volume de requisições
3. **Logs - Voting API** — contagem de logs por nível, stream de erros/warnings, logs em tempo real

### Logs (Loki + Promtail)

A API grava logs estruturados (JSON, níveis DEBUG/INFO/WARN/ERROR) em `/app/log/app.log`. O Promtail lê esse arquivo (necessário porque a API roda em múltiplos processos forkados) e também coleta os logs padrão (stdout) de todos os outros containers via Docker, enviando tudo para o Loki.

Nível de log configurável via `LOG_LEVEL` (default `INFO`).

## Teste de carga

`POST /votes` exige a resolução de um proof-of-work (ver [Proteção contra bots](#proteção-contra-bots)), que o `hey` não sabe calcular. Para testes de carga, ative o bypass `SKIP_PROOF_OF_WORK=true` — **nunca deixe essa variável ativa fora de testes locais**, pois desativa a proteção anti-bot por completo.

```bash
# sobe a API com o proof-of-work desativado
SKIP_PROOF_OF_WORK=true docker compose up -d --build api

POLL_ID=<poll_id>
CANDIDATE=$(docker exec redis redis-cli smembers "poll:current:candidates:$POLL_ID" | head -1)
docker compose run --rm hey -n 10000 -c 100 -m POST \
  -H "Content-Type: application/json" \
  -d "{\"candidate_id\":\"$CANDIDATE\",\"poll_id\":\"$POLL_ID\"}" \
  http://api:9292/votes

# depois do teste, volta ao normal (proof-of-work exigido)
docker compose up -d --build api
```

- `-n`: total de requisições
- `-c`: concorrência (conexões simultâneas)

Acompanhe o impacto em tempo real no dashboard **SLO/SLI - Voting API** do Grafana.

### Resultados obtidos

Testes executados contra `POST /votes`, com a stack completa rodando localmente (todos os serviços, incluindo observabilidade, no mesmo host: laptop Intel i7-1165G7, 4 núcleos físicos / 8 threads). Cada nível de concorrência foi executado 2–4 vezes; a tabela mostra a faixa observada.

| Concorrência (`-c`) | Requisições/s | p95 | Respostas recebidas |
|---|---|---|---|
| 100 | ~2500–3100 | ~80–95ms | 10000/10000 |
| 400 | ~2400–2840 | ~360–430ms | 10000/10000 |
| 1000 | ~2560–3080 | ~910ms–1.27s | 10000/10000 |

**Meta de 1000 req/s**: superada com folga em todos os níveis testados, incluindo em c=1000 (~2560–3080 req/s, 2.5–3x acima da meta).

**SLO de disponibilidade (≥ 99%)**: cumprido em todos os níveis — 100% das requisições respondidas (nenhum 5xx, nenhuma conexão sem resposta) em c=100, c=400 e c=1000.

**SLO de latência (p95 < 250ms)**: cumprido de forma consistente até c=100. A partir de c=400 a meta não é mais atingida.

## CI/CD

O pipeline (`.github/workflows/ci.yml`) roda a cada push/PR na branch `main`, em dois jobs sequenciais:

**1. `test`** — testes automatizados da API, sem precisar subir a stack Docker:
   - `bundle exec rubocop` (lint)
   - `bundle exec rspec` (62 exemplos: request specs, consumer, reconciler, cálculo de lag — usa `MockRedis`, não depende de infraestrutura externa)

**2. `build-and-test`** (roda só se `test` passar) — valida a stack de ponta a ponta:
   1. **Build** das imagens (`api`, `consumer`, `frontend`)
   2. **Sobe a stack completa** (`docker compose up -d --wait`) — falha se algum serviço não passar no healthcheck
   3. **Smoke test**: cria um poll e registra um voto via `curl` (com `SKIP_PROOF_OF_WORK=true`, ver [Proteção contra bots](#proteção-contra-bots)), validando o fluxo funcional real, não só que os containers sobem
   4. Em caso de falha, despeja os logs de todos os serviços antes de encerrar

### Rodando localmente

Use o [`act`](https://github.com/nektos/act) para simular o runner do GitHub sem precisar dar push:

```bash
curl -sL https://github.com/nektos/act/releases/latest/download/act_Linux_x86_64.tar.gz | tar -xz act

docker compose down -v   # evita conflito com uma stack já rodando
./act push -j build-and-test --bind -P ubuntu-latest=catthehacker/ubuntu:act-latest
```
