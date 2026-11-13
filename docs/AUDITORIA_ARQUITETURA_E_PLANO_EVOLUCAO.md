# Auditoria da Arquitetura Atual e Plano de Evolução — IGE2A CSMS Next Generation

> Documento da **Etapa 1 (Auditoria)** e **Etapa 2 (Plano)** do projeto.
> Nenhuma alteração estrutural foi feita nesta etapa. Tudo abaixo é baseado em
> leitura direta do código existente (evidências citadas) e no `project_memory` do workspace.

---

## 0. Método e escopo

**Analisado (somente leitura):**

- `IGE2A_CSMS/` — CSMS em produção (Fastify + TypeScript + Supabase).
- `IGE2A_ADMIN/` — painel administrativo "IGE2A PULSE" (Next.js 14).
- `IGE2A_APP_MOBILE/` — aplicativo (Expo/React Native).
- `IGE2A_OCPP_LAB/` — laboratório OCPP 1.6J isolado (referência arquitetural).
- `NEW_IGE2A_CSMS/` — **diretório-alvo, atualmente vazio**.

**Não foi feito:** nenhuma escrita no `IGE2A_CSMS`, nenhuma migration executada, nenhum
deploy, nenhuma conexão com produção.

**Convenção de status usada neste documento:**

- `IMPLEMENTADO` = código existe.
- `VALIDADO` = comportamento verificado com evidência real (teste/hardware).
- `PUBLICADO` = em ambiente.

---

## 1. Inventário do ecossistema

| Repositório | Papel | Stack | Estado |
|---|---|---|---|
| `IGE2A_CSMS` | CSMS produtivo (orquestrador OCPP + negócio) | Node ≥18, TypeScript, Fastify 5, `ws`, Supabase (PostgREST), Stripe, Redis | Em produção (PM2 `csms`, VM GCP `ige2a-charger-v2`) |
| `IGE2A_ADMIN` | Painel PULSE (monitoração/finanças/instalações) | Next.js 14+, BFF proxy server-side | Em produção (Vercel) |
| `IGE2A_APP_MOBILE` | App do usuário final | Expo / React Native | Em produção |
| `IGE2A_OCPP_LAB` | Laboratório OCPP 1.6J mínimo | Node ≥20, `ws`, sem banco | Etapa A concluída (validado com simulador) |
| `NEW_IGE2A_CSMS` | **Nova versão alvo (vazia)** | — | A iniciar |

Infra de produção (confirmada no `project_memory`): GCP `us-east1-c`, VM `ige2a-charger-v2`
(IP público `34.148.28.79`), Caddy como reverse proxy/TLS, PM2 1 instância, Supabase PostgreSQL
(schema `orchestrator`). Endpoint público OCPP: `wss://api.ige2a.app.br/ocpp/CentralSystemService/{id}`.

---

## 2. Arquitetura atual do `IGE2A_CSMS`

### 2.1 Stack e estrutura

- Runtime Node.js + TypeScript, `type: commonjs`.
- HTTP: **Fastify 5**; WebSocket: **`ws`** (`WebSocketServer { noServer: true }`).
- Persistência: **Supabase** via `@supabase/supabase-js` (PostgREST), schema `orchestrator`.
- Integrações: **Stripe** (pagamentos/carteira), **Redis** (roteamento distribuído de carregadores).
- Layout declarado como Clean Architecture, mas com **monólitos severos**:
  - [restAdapter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/restAdapter.ts) — **11.593 linhas**
  - [supabaseRepo.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/infra/supabaseRepo.ts) — **6.348 linhas**
  - [ocppService.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/application/services/ocppService.ts) — **2.669 linhas**

### 2.2 Fluxo WebSocket OCPP (transporte)

Entrada única em [server.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/server.ts#L274-L291):
o handler de `upgrade` do servidor roteia `/v1/ws*` para o app mobile e todo o resto para o
[ocppWsAdapter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/ocppWsAdapter.ts).

Decisões de handshake já implementadas (ponto forte):
- Rotas: `/ocpp/CentralSystemService/:id` e `/ocpp/CentralSystemService?charge_box_id=`.
- Eco **exato** do subprotocolo oferecido (RFC 6455 §4.1); fallback leniente via `OCPP_LENIENT_PROTOCOL_FALLBACK=1`; variante `socpp1.6` (Teison/TLS) aceita.
- Bind em loopback (`HOST=127.0.0.1`), tráfego público só via Caddy (defeito F3 tratado).
- `x-forwarded-for` só é confiado quando o peer é loopback ([netTrust.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/infra/netTrust.ts)).

### 2.3 Gerenciamento de conexões

Dois registros em memória, em `global`:

1. `wsRegistry: Map<chargeBoxId, WebSocket>` (em [ocppWsAdapter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/ocppWsAdapter.ts#L14)).
2. `connectionManager` ([connectionManager.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/application/connection/connectionManager.ts)) — **observador** com 3 dimensões de estado (transporte/registro/operacional), `connectionId` por socket e `reconnectCount`.

Já tratado corretamente (evidência no código):
- **Supersede**: socket novo substitui o antigo; o novo é registrado antes; o `close` do antigo é ignorado via `isCurrentSocket()` e comparção de `connectionId` ([ocppWsAdapter.ts#L220-L234](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/ocppWsAdapter.ts#L220-L234) e [L587-L609](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/ocppWsAdapter.ts#L587-L609)).
- Fechamento de socket obsoleto **não** marca offline o socket novo.
- Watchdog por inatividade (`OCPP_WATCHDOG_TIMEOUT_MS`) e keepalive ping/pong (`OCPP_WS_PING_INTERVAL_MS`).
- Reconciliação de `chargers.ws_online` (reparo do defeito "ws_online fantasma").

Limitação relevante: todo o estado de conexão é **em memória** (por instância). `wsRegistry`
não é persistido; após restart, o banco é reconciliado, mas não há **histórico de eventos de conexão**
(conexão/desconexão/reconexão) consultável.

### 2.4 Persistência de comunicação (lacuna central do projeto)

O que existe hoje:
- `emitOcppEvent` ([ocppEvents.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/infra/ocppEvents.ts)) emite **JSON estruturado para stdout** (coletado por PM2).
  **Decisão explícita de projeto: NÃO persiste em banco** para não agravar o pool do Postgres nem o disco.
- Tabela `ocpp_events` guarda apenas **eventos semânticos** (`event_type`, `payload_json`, `charge_box_id`, `transaction_id`, `timestamp`) — ver [update_ocpp_events_schema.sql](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/scripts/update_ocpp_events_schema.sql) e insersão em [supabaseRepo.ts#L1505](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/infra/supabaseRepo.ts#L1505).

O que **não existe** (mas é exigido pelo projeto):
- Registro de **100% das mensagens OCPP de fio** (CALL/CALLRESULT/CALLERROR, ambas as direções).
- Campos `direction`, `message_type_id`, `unique_id`, `latency_ms`, `received_at/processed_at/responded_at`, `status`, `error_code`.
- Ordem cronológica garantida e reconstrução do fluxo completo por `unique_id`.

O adaptador **já observa** essas mensagens (instrumentação de `ws.send` e do handler de `message`,
com sanitização de campos sensíveis e correlação por `msgId`), mas apenas **emite evento de texto** —
não há destino persistente consultável nem UI.

### 2.5 Heartbeat (lacuna)

- `BootNotification.conf` retorna **`interval: 60` fixo** ([ocppService.ts#L830](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/application/services/ocppService.ts#L830)). Não é negociado por estação nem configurável.
- `Heartbeat` atualiza `chargers.last_heartbeat_at` e a memória do `connectionManager` ([ocppService.ts#L880-L891](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/application/services/ocppService.ts#L880-L891)).
- Não há: `next_expected_at`, contagem de atrasados/perdidos, latência média/máxima, classificação NORMAL/ATRASADO/CRÍTICO/OFFLINE, nem distinção formal entre "sem Heartbeat" e "sem mensagem" e "socket fechado".
- Conectividade hoje é derivada de `ws_online` OU janela de 5 min de `last_heartbeat_at` ([supabaseRepo.ts#L3536-L3537](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/infra/supabaseRepo.ts#L3536-L3537)).

### 2.6 Observabilidade atual

- Métricas Prometheus em [metrics.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/infra/metrics.ts) expostas em `/metrics` (protegido por API key fora do loopback): `ocpp_ws_connections`, `ocpp_connection_events_total`, `ocpp_reconnects_total`, `ocpp_ping_timeouts_total`, `ocpp_boot_events_total`, `ocpp_heartbeat_total`, `ocpp_message_invalid_total`, `ocpp_registry_size`, `ocpp_pending_commands`, `ocpp_connections_detail`, rate limit, pool Postgres, ownership.
- `/debug/ocpp-info` expõe `wsRegistry` + snapshot do `connectionManager` (administrativo).
- Logs: JSON por linha em stdout + `console`. **Sem UI de terminal ao vivo, sem dashboard consolidado, sem diagnóstico automático.**

### 2.7 Interface atual

- Painel `IGE2A_ADMIN` (PULSE) possui páginas de monitoração
  ([monitoring/realtime](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_ADMIN/src/app/(admin)/monitoring/realtime/page.tsx),
  [monitoring/faults](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_ADMIN/src/app/(admin)/monitoring/faults/page.tsx),
  `analytics/charge-points`), mas **não** existe terminal de logs OCPP ao vivo, nem os indicadores de heartbeat/diagnóstico exigidos.

### 2.8 Referência arquitetural já disponível

O `IGE2A_OCPP_LAB` já implementa, de forma limpa e modular, exatamente a separação que o
projeto pede no núcleo OCPP: `websocket/` (handshake, connectionManager, websocketServer),
`ocpp/` (protocol, messageRouter, boot/heartbeat/status, errors), `diagnostics/` (logger
sanitizado, catálogo de eventos de conexão), `config/environment`. É o **melhor molde** para o
núcleo do novo CSMS (ver [README do LAB](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_OCPP_LAB/README.md)).

---

## 3. Pontos fortes a preservar

- Handshake OCPP 1.6J robusto e já validado em campo (Teison/TLS `socpp1.6`, fallback leniente, rotas com e sem ID).
- Modelo de 3 dimensões de estado do `connectionManager` + `connectionId` por socket + supersede correto.
- Watchdog, keepalive e reconciliação de `ws_online`.
- Instrumentação já existente das mensagens OCPP com **sanitização de dados sensíveis** (tokens, idTag, serial) — reaproveitável como base do motor de logs.
- Métricas Prometheus e rate limiting (shadow/enforce) já prontos.
- Regras de negócio, pagamentos, carteira, ownership e integrações do app **em produção** — não devem ser recriadas nem quebradas.

---

## 4. Lacunas frente ao requisito (mapeamento à especificação)

| § Spec | Requisito | Estado atual | Lacuna |
|---|---|---|---|
| 3 | Log de 100% das mensagens OCPP (fio, 2 direções) | Instrumentação parcial só em stdout | **Persistência + modelo de dados + rastreabilidade** |
| 4 | Monitoramento de Heartbeat por intervalo negociado | `interval:60` fixo, `last_heartbeat_at` | **Negociação por estação, watchdog, classificação, histórico** |
| 5 | Gerenciador de conexões + histórico de reconexão | Supersede OK; estado só em memória | **Eventos de conexão persistidos** |
| 6 | Tabelas `ocpp_message_logs`, `charger_heartbeat_monitor`, `charger_connection_events` | Não existem | **Criar/evoluir schema + índices + retenção** |
| 7 | Central de Monitoramento OCPP (terminal ao vivo + filtros + export) | Não existe | **UI completa + streaming** |
| 8 | Dashboard de observabilidade (KPIs + gráficos) | Parcial (métricas brutas) | **Agregações e gráficos** |
| 9 | Diagnóstico automático de anomalias | Não existe | **Módulo de regras + evidências** |
| 10 | Testes automatizados + validação com Teison físico | Testes parciais de negócio | **Suíte OCPP dedicada + validação real** |
| 11 | Separação transporte/protocolo/processamento/persistência/observabilidade | Monólitos | **Refatoração do núcleo** |
| 12/13 | Critérios de aceite e entregáveis | — | **Executar o plano** |

---

## 5. Riscos identificados

1. **Monólitos** (`restAdapter` 11.6k linhas) elevam o custo de qualquer mudança no núcleo OCPP.
2. **Persistência de log de alta frequência no Supabase PostgREST** pode degradar o pool (P01) — é exatamente a razão pela qual a instrumentação atual não vai ao banco. Precisa de writer assíncrono em lote + retenção.
3. **Estado só em memória**: reinício do processo perde o histórico; a auditoria "por que ficou offline" depende de logs externos.
4. **`interval` fixo** ignora o negociado e mascara carregadores lentos.
5. **Sem histórico de conexão**, cenários de reconexão/duplicidade não são auditáveis a posteriori.
6. Preservar app/pagamentos/ownership em produção exige mudanças incrementais e reversíveis.

---

## 6. Arquitetura proposta (nova)

Princípio: **núcleo OCPP novo e isolado** (molde do `OCPP_LAB`), reaproveitando as
**integrações de negócio já validadas** do `IGE2A_CSMS` por uma fronteira explícita
(ports/adapters), evitando que o motor de observabilidade dependa do pool Postgres crítico.

```
NEW_IGE2A_CSMS/
  src/
    transport/        # HTTP + upgrade WS + subprotocolo + Caddy-safe (loopback, netTrust)
    ocpp/
      protocol/       # CALL/CALLRESULT/CALLERROR, parse defensivo
      router/         # roteamento das ações 1.6J
      handlers/       # boot, heartbeat, status, authorize, start/stop, meterValues, remote*
    connection/       # ConnectionManager (supersede, sessionId, ownership da conexão ativa)
    logging/          # OcppMessageLogEngine (captura 100% das mensagens, 2 direções)
    observability/    # metricas Prometheus + heartbeat monitor + dashboard aggregations
    diagnostics/      # regras de anomalia + evidencias
    persistence/      # writers assincronos em lote (padrao outbox), retencao, indices
    integration/      # ports -> Supabase (negocio), Stripe, Redis, webhooks existentes
    config/           # config imutavel via env
  tests/              # unit/integration/e2e + simulador 1.6J
  deploy/             # Caddyfile + PM2
```

Regra de ouro (spec §6): **falha de armazenamento de log nunca derruba o processamento OCPP**.
Writer assíncrono com fila limitada, descarte auditado e métrica de perda de persistência.

---

## 7. Modelo de dados proposto (alinhar ao padrão real do banco)

> Nomes/relacionamentos finais serão adaptados ao schema `orchestrator` existente antes de criar.

- `ocpp_message_logs`: `id`, `charger_id`/`charge_point_id`, `connection_id`, `direction`,
  `action`, `message_type_id`, `unique_id`, `payload` (jsonb), `received_at`, `processed_at`,
  `responded_at`, `latency_ms`, `status`, `error_code`, `error_message`, `created_at`.
  Índices: `(charge_point_id, received_at desc)`, `(unique_id)`, `(action)`, `(status)`, `(direction)`.
- `charger_heartbeat_monitor`: `charge_point_id` (PK), `negotiated_interval_seconds`,
  `last_heartbeat_at`, `next_expected_at`, `last_ocpp_message_at`, `heartbeat_count`,
  `delayed_count`, `average_latency_ms`, `connection_status`, `updated_at`.
- `charger_connection_events`: `id`, `charge_point_id`, `connection_id`, `event_type`,
  `event_timestamp`, `disconnect_reason`, `metadata` (jsonb), índice por `(charge_point_id, event_timestamp desc)`.
- **Retenção** configurável (equivalente ao `RETENTION_*` já existente) e paginação.

---

## 8. Componentes novos

1. **OcppMessageLogEngine** — captura antes do processamento e após a resposta; preserva ordem; registra mensagens inválidas como erro de protocolo; reaproveita a sanitização existente.
2. **HeartbeatMonitor** — negociação de `interval` por estação, `next_expected_at`, classificação NORMAL/ATRASADO/CRÍTICO/OFFLINE, contadores, latência média/máxima; **nunca** marcar offline só por HB ausente.
3. **ConnectionManager v2** — sessionId, propriedade da conexão ativa, eventos de conexão persistidos, anti-falso-desconexão.
4. **DiagnosticsEngine** — regras (HB ausente, HB sem resposta, latência alta, reconexões, OCPP inválido, sessão duplicada, sem StatusNotification, falha de processamento, HB fora do intervalo), com gravidade, evidências e recomendação — **separando evidência de hipótese**.
5. **Central de Monitoramento OCPP** (UI) — status da conexão, monitor de HB, terminal ao vivo, filtros, busca, export JSON/CSV, dashboard com KPIs e gráficos.

---

## 9. Plano de evolução incremental (com validação por fase)

| Fase | Entrega | Validação |
|---|---|---|
| **F0** | Esqueleto do `NEW_IGE2A_CSMS` + núcleo de transporte/protocolo (molde OCPP_LAB) | Testes de handshake/boot/heartbeat/status (node:test) |
| **F1** | OcppMessageLogEngine (em memória) capturando 100% das mensagens | Teste: toda CALL/CALLRESULT/CALLERROR gera registro com `unique_id` e `latency_ms` |
| **F2** | Persistência assíncrona (outbox) + schema + índices + retenção | Teste de queda do writer sem afetar OCPP; logs sobrevivem a restart |
| **F3** | HeartbeatMonitor (negociação de intervalo + classificação) | Cenários de HB atrasado/ausente vs socket fechado |
| **F4** | ConnectionManager v2 + histórico de eventos de conexão | Teste de conexões simultâneas, socket obsoleto, reconexão |
| **F5** | API de consulta + streaming (SSE/WS) para a Central | Teste de filtros/busca/paginação/export |
| **F6** | Central de Monitoramento OCPP (UI) | E2E na UI |
| **F7** | Dashboard de observabilidade + Diagnóstico automático | Testes de regras + evidências |
| **F8** | Integração com negócio existente (pagamentos/carteira/ownership/app) | Contratos preservados |
| **F9** | Suíte E2E + validação com TeisonMe @7KW físico | Evidência de comunicação bidirecional real |
| **F10** | Relatório comparativo (arquitetura anterior × nova) + docs de operação | — |

Cada fase: `typecheck` + testes + evidência. Nada é declarado concluído sem execução real.

---

## 10. Estratégia de testes

- **Unit**: parsing OCPP-J, correlação por `unique_id`, classificador de HB, regras de diagnóstico.
- **Integração**: handshake + Boot/Heartbeat/Status, supersede, reconexão, mensagem inválida, persistência após restart.
- **E2E**: fluxo completo (Boot→Authorize→Start→MeterValues→Stop) com **simulador OCPP 1.6J**.
- **Validação real**: carregador físico TeisonMe @7KW em `wss://api.ige2a.app.br/ocpp/CentralSystemService/{id}` (somente com aprovação e janela definida).

---

## 11. Decisões pendentes (requerem aprovação antes de implementar)

1. **Estratégia do novo CSMS**: núcleo novo standalone reutilizando o negócio existente × evoluir o `IGE2A_CSMS` in-place × extrair só o núcleo OCPP como serviço dedicado.
2. **Persistência dos logs**: mesmo Supabase (tabelas dedicadas + writer em lote) × banco/schema separado × arquivo/objeto + índice no Postgres.
3. **Interface**: estender o `IGE2A_ADMIN` (PULSE) × UI dedicada no `NEW_IGE2A_CSMS`.
4. **Transporte em tempo real**: SSE (reaproveitar `/v1/stream`) × WebSocket dedicado × ambos.

---

### Evidências-base (arquivos consultados)

- [server.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/server.ts)
- [ocppWsAdapter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/ocppWsAdapter.ts)
- [connectionManager.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/application/connection/connectionManager.ts)
- [ocppService.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/application/services/ocppService.ts)
- [ocppEvents.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/infra/ocppEvents.ts)
- [metrics.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/infra/metrics.ts)
- [update_ocpp_events_schema.sql](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/scripts/update_ocpp_events_schema.sql)
- [IGE2A_OCPP_LAB README](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_OCPP_LAB/README.md)
