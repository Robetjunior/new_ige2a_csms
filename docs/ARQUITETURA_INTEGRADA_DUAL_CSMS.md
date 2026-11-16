# Arquitetura Integrada Dual CSMS — Viabilidade e Alvo

> Documento de arquitetura. Nao contem implementacao de producao.
> Toda afirmacao sobre comportamento e sustentada por leitura direta de codigo
> (evidencia citada com arquivo e linha). O que nao foi observado esta marcado
> como HIPOTESE ou DECISAO PENDENTE.

---

## 0. Camada de leitura executiva

**Veredito: VIAVEL**, com ressalva arquitetural relevante. Operar os dois CSMS em
paralelo e tecnicamente possivel porque o modelo de responsabilidades e naturalmente
separavel: o NOVO ja domina o canal OCPP 1.6J com o carregador fisico
([ocppWebSocketServer.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/ocppWebSocketServer.ts)) e o ANTIGO ja concentra o negocio
(transacao, preco, cobranca pos-paga via Stripe, reconciliacao)
([POSTPAID_BACKEND_RUNTIME_RUNBOOK.md](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/POSTPAID_BACKEND_RUNTIME_RUNBOOK.md)).
**Risco principal:** a identidade de transacao. O NOVO gera `transactionId` localmente
([chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts#L208-L217)) enquanto o ANTIGO usa a
propria sessao como `transaction_id` ([ocppService.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/application/services/ocppService.ts#L997-L1019)). Sem uma tabela de correlacao
explicita, `RemoteStopTransaction` e cobranca quebram. **Decisao recomendada:**
NOVO como "edge OCPP/telemetria" e ANTIGO como "negocio/pagamento", ligados por uma
camada de integracao com chave de correlacao propria e com o NOVO emitindo telemetria
para o ANTIGO.

### 0.1 Decisoes aprovadas pelo usuario (baseline)

As decisoes abaixo foram confirmadas e passam a reger a implementacao futura:

1. **O CSMS NOVO PODE ser alterado, de forma aditiva, sem tocar a comunicacao OCPP.**
   Permanecem intactos o transporte, o protocolo e os handlers
   ([src/transport](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/ocppWebSocketServer.ts),
   [src/ocpp](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/handlers.ts)).
   Modulos novos (rotas de integracao, `inbox`, `outbox`) sao permitidos.
2. **Modulo de integracao aditivo no NOVO.** Endpoints dedicados `POST /integration/v1/*`
   convivem com a rota generica existente e **nao** substituem nem alteram o caminho OCPP.
3. **NOVO e autoridade do `ocppTransactionId`; ANTIGO adota** (ADR-02).
4. **Telemetria por PUSH** (webhook HMAC + `outbox` no NOVO; endpoint de ingresso no ANTIGO),
   aprovado (ADR-03).
5. **Os dois CSMS rodam na MESMA VM.** Evidencia: na VM `ige2a-charger-v2`, o ANTIGO
   (`IGE2A_CSMS`) escuta em `:3000` e o NOVO (`csms-next`) em `:3100`
   (`RELATORIO_HANDSHAKE_OCPP16J_PARA_AUDITORIA_2026-11-17.md`, secao 1.1).

Consequencia direta: a idempotencia do start passa a ser coberta por uma tabela `inbox`
**dentro do NOVO** (secao 5.3), o que torna o retry de `RemoteStartTransaction` seguro.
Como os dois processos compartilham a VM, o `outbox` (NOVO) e o ingresso (ANTIGO) trafegam
pela rede interna, reduzindo latencia e risco de rede.

---

## 1. Escopo, metodo e evidencias

### 1.1 Metodo

Leitura direta (somente leitura) dos dois repositorios, sem executar migrations, sem
conectar a producao e sem alterar codigo. Contratos internos foram deduzidos do codigo,
nao de suposicao. Onde o comportamento nao pode ser provado pelo codigo lido, o item foi
marcado como HIPOTESE.

### 1.2 Arquivos do NOVO lidos (evidencia)

- [src/transport/ocppWebSocketServer.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/ocppWebSocketServer.ts) (handshake, subprotocolo, supersede, `sendCall`)
- [src/transport/chargerSession.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/chargerSession.ts) (ciclo do socket, `sendCall` com timeout, keepalive/watchdog)
- [src/transport/connectionManager.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/connectionManager.ts) (3 dimensoes de estado, `seedIdentity`)
- [src/ocpp/handlers.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/handlers.ts) (handlers recebidos, `startTransaction`, `meterValues`)
- [src/ocpp/protocol.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/protocol.ts), [src/ocpp/router.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/router.ts), [src/ocpp/errors.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/errors.ts)
- [src/chargers/chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts) (estado, `activeTransactions`, `meterReadings`)
- [src/http/routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts) (rotas REST, `/api/stream` SSE, rota generica de comandos)
- [src/http/monitorWebSocket.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/monitorWebSocket.ts) (`/ws/monitor`)
- [src/persistence/persistenceService.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/persistence/persistenceService.ts), [src/persistence/supabaseBatchWriter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/persistence/supabaseBatchWriter.ts)
- [src/app.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/app.ts), [src/components.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/components.ts), [src/config/env.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/config/env.ts)
- [src/domain/types.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/domain/types.ts), [src/availability/availabilityProbe.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/availability/availabilityProbe.ts)
- [db/migrations/001_ocpp_observability.sql](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/db/migrations/001_ocpp_observability.sql), [003](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/db/migrations/003_charger_operational_state.sql)
- [docs/AUDITORIA_ARQUITETURA_E_PLANO_EVOLUCAO.md](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/docs/AUDITORIA_ARQUITETURA_E_PLANO_EVOLUCAO.md), [docs/RELATORIO_REQUISITOS_MINIMOS_CSMS.md](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/docs/RELATORIO_REQUISITOS_MINIMOS_CSMS.md)

### 1.3 Arquivos do ANTIGO lidos (evidencia)

- [src/adapters/restAdapter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/restAdapter.ts) (trechos de remote start/stop e roteamento)
- [src/infra/distributedChargerRouting.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/infra/distributedChargerRouting.ts) (Redis request/response por `requestId`)
- [src/application/services/ocppService.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/application/services/ocppService.ts) (StartTransaction, StopTransaction, cobranca pos-paga)
- [POSTPAID_FLOW_PROMPT.md](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/POSTPAID_FLOW_PROMPT.md), [POSTPAID_BACKEND_RUNTIME_RUNBOOK.md](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/POSTPAID_BACKEND_RUNTIME_RUNBOOK.md), [MOBILE_POSTPAID_BACKEND_READINESS.md](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/MOBILE_POSTPAID_BACKEND_READINESS.md)
- Inventario de `src/` do ANTIGO (adapters, application/services, infra, usecases, supabase/migrations)

### 1.4 O que NAO foi lido ou verificado

- Corpo integral de [restAdapter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/restAdapter.ts) (mais de 11.000 linhas): apenas os trechos de remote start/stop.
- Schema real das tabelas de sessao/pagamento do ANTIGO (o repositorio nao traz o DDL completo das tabelas de negocio em um unico arquivo lido).
- Nenhum teste foi executado; nada foi validado em runtime. Nada foi publicado.

### 1.5 Limites de confianca

- "O NOVO nao possui cliente HTTP de saida": confirmado por busca no codigo
  (`fetch(`/`axios`/`http.request` sem ocorrencias em `src/`).
- "O NOVO nao possui tabela de sessao de recarga nem tarifa": confirmado pelas
  migrations lidas (apenas observabilidade/frota) e pela ausencia de handlers de negocio.

---

## 2. Modelo de operacao dos dois CSMS

### 2.1 Fronteira de responsabilidade (modelo recomendado)

| Dimensao | CSMS NOVO | CSMS ANTIGO |
|---|---|---|
| Quem fala OCPP 1.6J com o carregador | **Sim** (unico dono do socket) | Nao (legado; handshake case-insensitive ja corrigido e em producao, commit `56eade2`) |
| Handshake/subprotocolo/keepalive/watchdog | **Sim** | Nao no modelo alvo |
| Autoridade sobre `ocppTransactionId` (valor de fio) | **Sim** (contador local por carregador) | Nao |
| Sessao de negocio (`sessionId`) e usuario | Nao | **Sim** |
| Preco, tarifa, wallet, cobranca pos-paga, Stripe | Nao | **Sim** |
| Reconciliacao com webhook Stripe | Nao | **Sim** |
| Autenticacao do app mobile / aceite legal | Nao | **Sim** |
| Historico de mensagens de fio OCPP | **Sim** (`ocpp_message_logs`) | Parcial (`ocpp_events` semantico) |
| Telemetria em tempo real do carregador | **Sim** (fonte primaria) | Consumidor |

### 2.2 Justificativa

O NOVO ja tem a comunicacao OCPP validada e robusta (handshake com variante
`socpp1.6` e fallback leniente, supersede correto, observabilidade de handshake)
([ocppWebSocketServer.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/ocppWebSocketServer.ts#L70-L84)).
O ANTIGO ja tem todo o negocio em producao e nao deve ser reescrito
([AUDITORIA_ARQUITETURA_E_PLANO_EVOLUCAO.md](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/docs/AUDITORIA_ARQUITETURA_E_PLANO_EVOLUCAO.md)). Duplicar a comunicacao OCPP no ANTIGO
seria recriar exatamente o componente que ja falha e que o NOVO ja resolve. Portanto a
divisao "NOVO = edge OCPP/telemetria, ANTIGO = negocio/pagamento" e a que reaproveita o
que ja funciona em cada lado, com a menor superficie de mudanca.

### 2.3 O que muda em relacao ao desenho atual

Hoje o ANTIGO e simultaneamente edge OCPP e negocio. No modelo alvo, o ANTIGO deixa de
falar OCPP e passa a consumir/emitir contratos de integracao. O NOVO passa a ter um
modulo de integracao (novo), sem tocar o nucleo OCPP (transporte/handlers/protocolo).

---

## 3. Diagrama textual da arquitetura alvo

```
                         +-------------------------------+
        (Android App)    |        IGE2A_ADMIN (PULSE)     |
             |           +-------------------------------+
             |  REST/HTTPS                     |
             v                                 v
   +---------------------+           +-----------------------------+
   |   CSMS ANTIGO       |           |  Central de Monitoramento    |
   |  (negocio/pagamento)|           |  (UI estatica + SSE no NOVO) |
   | - sessao/usuario    |           +-----------------------------+
   | - preco/tarifa      |                     ^ SSE /api/stream
   | - Stripe pos-pago   |                     | (GET, read-only)
   | - reconciliacao     |                     |
   +----------+----------+                     |
              |                                |
   (A) Start/Stop: HTTP (integracao)           |
   (C) Telemetria: webhook/MQ NOVO->ANTIGO     |
              |                                |
              v                                |
   +-----------------------------------------+-+
   |              CSMS NOVO (edge OCPP)       |
   |  transport / ocpp / chargers / logging    |
   |  + [NOVO] modulo integracao (proposto)    |
   +---------------------+-------------------+
                         |  OCPP 1.6J (WebSocket, ws://)
                         v
                 +----------------+
                 |  Carregador    |
                 |  (NeoCharge /  |
                 |   MyTeison)    |
                 +----------------+
```

### 3.1 Fluxo de START (pos-pago)

```
App -> ANTIGO:  POST /v1/transactions/start-postpaid
ANTIGO: valida usuario, aceite, carregador disponivel, cartao
ANTIGO -> NOVO:  POST /integration/v1/sessions/start
                 { idempotencyKey, integrationSessionId, chargePointId, connectorId, idTag }
NOVO -> Carregador: RemoteStartTransaction {connectorId, idTag}   (wsServer.sendCall)
Carregador -> NOVO: CALLRESULT {status: Accepted|Rejected}
Carregador -> NOVO: StartTransaction {connectorId, idTag, meterStart, timestamp}
NOVO: atribui ocppTransactionId (contador local), grava correlacao
NOVO -> ANTIGO:  evento "session.started" { integrationSessionId, ocppTransactionId, meterStart }
ANTIGO: abre/atualiza sessao de negocio; responde ao App com transactionId de negocio
```

### 3.2 Fluxo de MeterValues (monitoramento)

```
Carregador -> NOVO: MeterValues {connectorId, meterValue[...]}
NOVO: normaliza (measurand, value, unit, phase, location, context, timestamp)
NOVO -> ANTIGO:  evento "meter.sampled" { integrationSessionId, ocppTransactionId, samples[] }
NOVO -> UI:      SSE "ocpp-message" + "charger-state"
ANTIGO: calcula potencia/tempo/SoC/valor e atualiza a sessao; UI ADMIN mostra valores
```

### 3.3 Fluxo de STOP e cobranca

```
[Opcao A] App -> ANTIGO: POST /v1/commands/remote-stop
          ANTIGO -> NOVO: POST /integration/v1/sessions/{id}/stop { ocppTransactionId }
          NOVO -> Carregador: RemoteStopTransaction {transactionId: ocppTransactionId}
[Opcao B] Usuario encerra no carregador: StopTransaction chega ao NOVO
NOVO -> ANTIGO:  evento "session.stopped" { integrationSessionId, ocppTransactionId, meterStop, timestamp, reason }
ANTIGO: fecha sessao com valor oficial, dispara cobranca pos-paga, registra tentativa
ANTIGO: reconcilia via webhook Stripe
```

### 3.4 Fluxo de monitoramento (UI)

```
Navegador -> NOVO:  GET /api/stream (SSE)  /  /ws/monitor  /  GET /api/chargers/:id
Navegador -> NOVO:  servido por @fastify/static (public/)
```

---

## 4. Camada de Integracao CSMS <-> CSMS

> **Nota de revisao (baseline, secao 0.1).** O NOVO pode ser alterado de forma aditiva,
> sem tocar o nucleo OCPP. Portanto:
> - **Start/Stop (ANTIGO -> NOVO):** passa a usar o endpoint dedicado
>   `POST /integration/v1/sessions/start` e `/stop` (secao 4.1), com `inbox` de idempotencia.
>   A rota generica `POST /api/chargers/:chargePointId/commands`
>   ([routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L323-L341)) permanece como esta e continua sendo o caminho de comandos OCPP.
> - **Telemetria (NOVO -> ANTIGO):** modo ainda em aberto (pull via SSE x push com outbox);
>   ver secao 13.2.

Observacao de escopo: o NOVO hoje expoe uma rota generica de comando
`POST /api/chargers/:chargePointId/commands` que executa `wsServer.sendCall`
([routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L323-L341)). Ela serve para acionar `RemoteStartTransaction`,
mas **nao resolve correlacao, idempotencia nem o `ocppTransactionId` resultante**. Por
isso o alvo proposto adiciona um modulo de integracao dedicado.

### 4.1 Contrato de Start (ANTIGO -> NOVO)

**Direcao:** ANTIGO e cliente; NOVO e servidor.
**Metodo/rota (proposta):** `POST /integration/v1/sessions/start`
**Auth:** header `x-integration-key` (chave dedicada, distinta de `CSMS_API_KEY`) + `x-request-id`.

Request (proposta):

```json
{
  "idempotencyKey": "9f1c0b2e-6b0a-4b2f-9d2e-5a1c7f0e1234",
  "integrationSessionId": "sess_ANTIGO_uuid",
  "chargePointId": "IGE2A-001",
  "connectorId": 1,
  "idTag": "POSTPAID-USER-123",
  "timeoutMs": 30000
}
```

Response 202 (aceito, aguardando ack do carregador):

```json
{
  "accepted": true,
  "integrationSessionId": "sess_ANTIGO_uuid",
  "phase": "PENDING_ACK",
  "correlationId": "int_..."
}
```

Response 200 (ja concluido, reenvio idempotente):

```json
{
  "accepted": true,
  "integrationSessionId": "sess_ANTIGO_uuid",
  "phase": "STARTED",
  "ocppTransactionId": 42
}
```

Mapeamento de erro:

| HTTP | `error` | Significado (condicionado por) |
|---|---|---|
| 400 | `bad_request` | corpo invalido (campo ausente). Base: validacao de corpo em [routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L326-L329) |
| 401 | `unauthorized` | chave de integracao invalida/ausente |
| 404 | `charger_unknown` | `chargePointId` nao pertence a frota conhecida |
| 409 | `charger_not_connected` | socket ausente. Base: `isConnected` em [ocppWebSocketServer.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/ocppWebSocketServer.ts#L292-L294) |
| 409 | `duplicate_in_progress` | mesma `idempotencyKey` ainda em andamento |
| 409 | `conflict_active_session` | ja existe transacao para o conector |
| 502 | `command_failed` | `sendCall` rejeitado pelo socket. Base: erro atual em [routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L338-L340) |
| 504 | `charger_timeout` | timeout do ack. Base: timeout de `sendCall` em [chargerSession.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/chargerSession.ts#L309-L327) |

**Idempotencia:** `idempotencyKey` (obrigatoria). Reenvio com a mesma chave retorna o
mesmo resultado sem reenviar `RemoteStartTransaction`.
**Correlacao:** `integrationSessionId` e o `correlationId` retornado amarram todos os
eventos subsequentes. **Timeout:** `timeoutMs` repassado ao `sendCall` do NOVO.
**Retry:** o NOVO nao faz retry automatico do `RemoteStartTransaction`; o ANTIGO decide
(o ANTIGO ja tem esse padrao: `remoteStartMaxAttempts = 3`
[restAdapter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/restAdapter.ts#L8782-L8784)).

### 4.2 Contrato de telemetria e encerramento (NOVO -> ANTIGO)

Dois modos possiveis.

**Modo 1 (push, recomendado para tempo real):** NOVO envia eventos ao ANTIGO.
`POST {ANTIGO}/v1/integration/ocpp/events`, header `x-signature` (HMAC-SHA256 do corpo).

```json
{
  "eventId": "evt_uuid",
  "eventType": "session.started | meter.sampled | connector.status | session.stopped",
  "integrationSessionId": "sess_ANTIGO_uuid",
  "ocppTransactionId": 42,
  "chargePointId": "IGE2A-001",
  "connectorId": 1,
  "occurredAt": "2026-11-17T14:03:21.000Z",
  "data": {}
}
```

`meter.sampled` carrega `data.samples[]` com o formato ja normalizado pelo NOVO
(measurand, value, unit, phase, location, context, timestamp), conforme
[handlers.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/handlers.ts#L109-L146).

`session.stopped` carrega `data.meterStop`, `data.timestamp`, `data.reason`.

**Modo 2 (pull, zero mudanca no NOVO):** o ANTIGO consome a fonte existente do NOVO:
SSE `GET /api/stream` (eventos `ocpp-message`, `charger-state`, `snapshot`)
([routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L344-L407)) e `GET /api/chargers/:id`. Nenhuma alteracao no NOVO; em troca, a
correlacao com a sessao de negocio precisa ser reconstruida pelo ANTIGO a partir de
`chargePointId` + `connectorId` + janela temporal (frágil, ver secao 4.3).

**Retorno de encerramento:** `session.stopped` e o gatilho do fechamento e da cobranca
no ANTIGO, que hoje ja e disparado no `StopTransaction`
([ocppService.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/application/services/ocppService.ts#L554-L591)). O contrato apenas troca a origem do gatilho, mantendo a
logica de cobranca existente.

### 4.3 Mapeamento de identidade

Conflito central, com evidencia:

- O NOVO gera `transactionId` local, incrementando por carregador, em
  [chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts#L208-L217), e o devolve no `StartTransaction.conf`
  ([handlers.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/handlers.ts#L84-L91)).
- O ANTIGO usa a propria sessao como `transaction_id` e correlaciona o
  `RemoteStopTransaction` por esse valor
  ([restAdapter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/restAdapter.ts#L8395-L8437)).

Proposta de resolucao por tres identificadores distintos:

| Identificador | Dono | Papel |
|---|---|---|
| `chargeBoxId` / `chargePointId` | compartilhado | chave do carregador. Deve ser o mesmo valor nas duas bases |
| `integrationSessionId` | ANTIGO | correlacao de negocio (UUID). Gerado no start |
| `ocppTransactionId` | NOVO | valor de fio OCPP (inteiro por carregador). Usado no `RemoteStopTransaction` |

Regra: o `ocppTransactionId` e sempre o do NOVO (autoridade de fio). O ANTIGO guarda o
`ocppTransactionId` recebido em `session.started` e o usa no stop. O `transactionId` de
negocio do ANTIGO (usado pelo app e pelas rotas `/v1/...`) permanece interno ao ANTIGO.
`chargeBoxId` deve ser normalizado (mesmo casing/espacos) nas duas bases para evitar
divergencia de frota.

Alternativa (nao recomendada): o ANTIGO pre-aloca o `ocppTransactionId` e o NOVO o honra
no `StartTransaction.conf`. Isso exige alterar o nucleo OCPP do NOVO e amplia o risco.

Chave composta de correlacao (conector nivel): `(chargePointId, connectorId)` identifica
o conector; `activeTransactions` no NOVO e mapeado por `connectorId`
([chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts#L36-L37)).

### 4.4 Autenticacao, idempotencia e anti-replay

- **Canal ANTIGO -> NOVO:** API key dedicada (`x-integration-key`), distinta de
  `CSMS_API_KEY` (que hoje protege `/api/*`
  [routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L158-L167)). Recomenda-se restringir por IP de origem
  (o ANTIGO rodando na VM) ou mTLS interno.
- **Canal NOVO -> ANTIGO:** HMAC-SHA256 do corpo com segredo compartilhado
  (`x-signature`) + `x-request-id`. Rejeitar corpo cujo HMAC nao confira.
- **Idempotencia:** `idempotencyKey` no start; `eventId` unico em cada evento de
  telemetria; o ANTIGO deve deduplicar por `eventId`.
- **Anti-replay:** validar `occurredAt` dentro de janela aceitavel (ex.: +-5 min) e
  rejeitar `eventId` ja processado.

### 4.5 Inbox no NOVO: como funciona na pratica

Objetivo: tornar **seguro** o retry de `RemoteStartTransaction`. Hoje o ANTIGO tenta 3
vezes ([restAdapter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/restAdapter.ts#L8782-L8784));
se o ACK se perde sem inbox, o comando pode ser disparado duas vezes ao carregador.

**Ciclo de vida (rota `POST /integration/v1/sessions/start`):**

1. O ANTIGO envia `idempotencyKey` estavel da sessao de negocio, junto de
   `chargePointId`, `connectorId` e `idTag`.
2. O NOVO faz `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING` em
   `integration_start_inbox` (secao 5.3). A **PK textual** decide o "dono".
3. **Insert ganho (chave inedita):** e o dono. Executa o comando pelo metodo **ja
   existente** `wsServer.sendCall(chargePointId, "RemoteStartTransaction", {connectorId, idTag}, timeoutMs)`
   ([chargerSession.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/chargerSession.ts#L309-L327)). Grava o desfecho em `response` (jsonb).
4. **Chave ja existe:**
   - `response` preenchida: devolve a **mesma resposta** anterior, **sem** reenviar comando.
   - `response` nula (ainda em andamento): responde `409 duplicate_in_progress`.
5. `request_hash` detecta a mesma chave com corpo diferente, que vira erro (nao silencio).
6. O `StartTransaction` seguinte do carregador e capturado **sem alterar handler**:
   o modulo de integracao assina as superficies publicas existentes
   `chargers.subscribe(...)` ([chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts#L68)) e `messageLog.subscribe(...)`
   ([messageLogEngine.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/logging/messageLogEngine.ts#L145)), exatamente como o SSE faz hoje
   ([routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L371-L379)).
   Com isso obtem o `ocppTransactionId` e atualiza `integration_charge_session`.

**Por que isso nao toca o OCPP:** o inbox e um modulo de borda que apenas **consome** duas
superficies publicas ja existentes (`wsServer.sendCall` e os `subscribe`). Nenhum arquivo de
`src/transport/*` ou `src/ocpp/*` e modificado.

**Garantia de concorrencia:** duas chamadas simultaneas com a mesma chave nunca executam o
comando duas vezes, pois a unicidade e resolvida pelo proprio banco, nao por logica de
aplicacao.

---

## 5. Modelo de dados e historico de operacoes

### 5.1 Fonte da verdade

| Dado | Fonte da verdade | Observacao |
|---|---|---|
| Mensagens de fio OCPP | NOVO (`ocpp_message_logs`) | ja implementado |
| Estado do carregador/conector | NOVO (`charger_fleet`) | ja implementado |
| `ocppTransactionId` | NOVO | valor de fio |
| Sessao de negocio, usuario, preco, pagamento | ANTIGO | ja implementado |
| Correlacao sessao <-> transacao | ANTIGO (tabela de correlacao) | novo |

### 5.2 Persistencia no NOVO (o que ja existe)

- `orchestrator.ocpp_message_logs`, `charger_heartbeat_monitor`,
  `charger_connection_events` ([001_ocpp_observability.sql](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/db/migrations/001_ocpp_observability.sql)).
- `orchestrator.charger_fleet` com estado operacional
  ([003_charger_operational_state.sql](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/db/migrations/003_charger_operational_state.sql)).
- Persistencia e nao-critica e opcional; falha de escrita nunca interrompe o OCPP
  ([persistenceService.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/persistence/persistenceService.ts#L11-L15)).

Lacuna confirmada: **nao existe** tabela de sessao de recarga nem serie temporal de
telemetria. O NOVO guarda apenas a ultima leitura por
`connectorId|measurand|phase|location`, com limite de 256 entradas
([chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts#L186-L205)). Isso nao permite reconstruir a curva de potencia.

### 5.3 Migrations conceituais (NAO executar nesta etapa)

> **Nota de revisao (baseline, secao 0.1).** Com o NOVO alteravel de forma aditiva, as
> tabelas abaixo sao retomadas como migrations a aplicar no schema `orchestrator` do NOVO.
> A `integration_start_inbox` e a que torna o retry de `RemoteStartTransaction` seguro
> (detalhe de funcionamento na secao 4.5).

No schema `orchestrator` (mesmo schema, tabelas novas, sem alterar as existentes):

```sql
-- Sessao de recarga (projecao de integracao; o NOVO nao cobra, so correlaciona)
create table if not exists orchestrator.charge_session (
  integration_session_id text primary key,     -- gerado pelo ANTIGO
  idempotency_key        text unique,          -- dedupe do start
  charge_point_id        text not null,
  connector_id           integer not null,
  ocpp_transaction_id    integer,              -- preenchido no StartTransaction
  state                  text not null,        -- PENDING_ACK|STARTED|STOPPING|STOPPED|FAILED
  id_tag                 text,
  meter_start_wh         numeric,
  meter_stop_wh          numeric,
  started_at             timestamptz,
  stopped_at             timestamptz,
  last_meter_at          timestamptz,
  created_at             timestamptz not null default now(),
  updated_at             timestamptz not null default now()
);
create index if not exists charge_session_cp_idx
  on orchestrator.charge_session (charge_point_id, started_at desc);

-- Serie temporal de amostras (append-only)
create table if not exists orchestrator.charge_meter_sample (
  id                     uuid primary key,
  integration_session_id text,
  charge_point_id        text not null,
  connector_id           integer not null,
  measurand              text not null,
  value                  numeric,
  unit                   text,
  phase                  text,
  location               text,
  context                text,
  sampled_at             timestamptz not null,
  created_at             timestamptz not null default now()
);
create index if not exists charge_meter_sample_session_idx
  on orchestrator.charge_meter_sample (integration_session_id, sampled_at desc);

-- Inbox de idempotencia dos starts recebidos do ANTIGO
create table if not exists orchestrator.integration_start_inbox (
  idempotency_key text primary key,
  charge_point_id text not null,
  connector_id    integer not null,
  request_hash    text,
  response        jsonb,
  created_at      timestamptz not null default now()
);

-- Outbox opcional (se o NOVO empurrar eventos ao ANTIGO)
create table if not exists orchestrator.integration_outbox (
  id                     uuid primary key,
  event_type             text not null,
  integration_session_id text,
  payload                jsonb not null,
  status                 text not null default 'pending', -- pending|sent|failed
  attempts               integer not null default 0,
  next_attempt_at        timestamptz,
  created_at             timestamptz not null default now()
);
```

No ANTIGO: adicionar colunas de correlacao na entidade de sessao/pagamento existente
(`integration_session_id`, `ocpp_transaction_id`, `ocpp_edge`), sem remover nem renomear
colunas atuais, para preservar compatibilidade com o app mobile.

---

## 6. Monitoramento em tempo real

### 6.1 Como derivar as metricas (OCPP 1.6)

| Metrica | Origem OCPP 1.6 | Calculo |
|---|---|---|
| Tempo de sessao | `StartTransaction.timestamp` e agora / `StopTransaction.timestamp` | `fim - inicio` |
| Energia consumida | `MeterValues` measurand `Energy.Active.Import.Register` (Wh) | `ultima - meterStart` (o NOVO guarda o valor bruto por leitura; ver 5.2) |
| Potencia instantanea | `MeterValues` measurand `Power.Active.Import` (W) **ou** delta de energia | `dE / dt` entre duas amostras; por fase em `phase` L1/L2/L3 |
| SoC | `MeterValues` measurand `SoC` (Percent) | valor direto quando o firmware envia; **HIPOTESE** se o firmware nao envia `SoC` |
| Valor monetario | nao existe no OCPP | calculado pelo ANTIGO (tarifa x energia x tempo), jamais pelo NOVO |

### 6.2 O que o NOVO consegue medir hoje

- Recebe e normaliza `MeterValues` com todos os campos relevantes
  ([handlers.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/handlers.ts#L101-L148)).
- Expoe o estado atual por carregador via `GET /api/chargers/:id` e `charger-state` no SSE
  ([routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L234-L253)).
- Mantem `activeTransactions` por conector
  ([chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts#L36-L37)).

### 6.3 O que falta

- **Serie temporal:** o NOVO guarda so a ultima amostra por measurand/fase/local
  (limite 256). Sem serie temporal nao ha curva de potencia nem SoC historico. Fonte
  alternativa zero-mudanca: consumir o SSE `/api/stream` e persistir no ANTIGO
  (tabela `charge_meter_sample`), ou persistir no NOVO (writer de serie, secao 5.3).
- **Interpretacao de dominio:** o NOVO nao classifica measurand (apenas guarda o valor
  bruto); potencia/energia/SoC precisam de interpretacao no consumidor.
- **Valor monetario:** inexistente no NOVO por desenho; pertence ao ANTIGO.

### 6.4 Exposicao na UI existente

- Manter a UI atual do NOVO (vanilla JS + SSE, servida por `@fastify/static`
  [app.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/app.ts#L278-L282)).
- Indicadores da sessao (tempo, energia, potencia, SoC) podem ser calculados a partir de
  `charger-state`/`ocpp-message` ja emitidos, sem novo backend, desde que o consumidor
  mantenha a serie (secao 6.3).
- Valores monetarios devem ser exibidos a partir do ANTIGO (contrato de leitura), pois o
  NOVO nao os possui.

---

## 7. Consistencia de dados entre plataformas

### 7.1 Estrategia

1. **Idempotencia por chave:** `idempotencyKey` no start e `eventId` na telemetria.
2. **Outbox/inbox:** o NOVO grava eventos em `integration_outbox` antes de envia-los
   (garante reenvio apos falha). O ANTIGO deduplica por `eventId` (`inbox`).
3. **Reconciliacao periodica:** job no ANTIGO compara sessoes abertas com o estado do
   NOVO (`GET /api/chargers/:id`), usando `integrationSessionId` e `ocppTransactionId`.
4. **Event sourcing leve:** a verdade da sessao e a sequencia de eventos
   `session.started -> meter.sampled* -> session.stopped`; o estado e a projecao.

### 7.2 Classes de divergencia e acao

| Divergencia | Deteccao | Acao |
|---|---|---|
| Sessao aberta no ANTIGO, sem transacao no NOVO | reconciliacao | marcar `ORFA` no ANTIGO; nao cobrar; investigar com `ocpp_message_logs` |
| Transacao no NOVO, sem sessao no ANTIGO | reconciliacao | sessao fantasma: encerrar no NOVO via `RemoteStopTransaction`; nao cobrar |
| `meterStop` divergente entre as partes | reconciliacao | usar `MeterValues` persistidos no NOVO como verdade de energia |
| Evento de telemetria duplicado | `eventId` repetido | descartar no ANTIGO |
| `ocppTransactionId` divergente para a mesma sessao | reconciliacao | prevalece o valor do NOVO; corrigir mapeamento |
| `chargeBoxId` com casing/espacos diferentes | comparacao normalizada | normalizar em ambos; padronizar a chave de frota |

Nada nesta secao foi executado; e proposta.

---

## 8. Tratamento de falhas e continuidade

### 8.1 Cenarios e comportamento

| Cenario | Efeito | Comportamento definido |
|---|---|---|
| **ANTIGO cai** | start/stop de negocio param; NOVO segue lendo OCPP e gravando | NOVO continua operando o OCPP e persistindo `ocpp_message_logs`/sessoes. Eventos ficam em `integration_outbox` e sao reenviados no retorno do ANTIGO. Sessoes ja iniciadas continuam ate o carregador encerrar; a cobranca e feita apos o ANTIGO voltar, pela reconciliacao |
| **NOVO cai** | canal OCPP cai; carregador reconecta quando o NOVO voltar | ANTIGO marca sessoes como "edge indisponivel"; nao cobra ate reconciliar. Ao voltar, o NOVO reidrata a frota pelo `charger_fleet` (nao reidrata `activeTransactions`, por desenho [chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts#L101-L109)); o ANTIGO reafirma sessoes ativas e o `ocppTransactionId` (a tabela de correlacao do ANTIGO e a memoria da sessao) |
| **Canal ANTIGO<->NOVO cai** | start/telemetria nao fluem | outbox/inbox com retry e backoff exponencial; circuit breaker abre apos N falhas e sonda periodicamente; idempotencia evita duplicacao no retorno |
| **Rede do carregador cai** | socket fecha | ja tratado pelo NOVO: watchdog, keepalive ping/pong, supersede, reconexao ([chargerSession.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/chargerSession.ts#L129-L172)); o ANTIGO ve a mudanca via telemetria |
| **Carregador aceita e nao inicia** | ack `Accepted` sem `StartTransaction` | o NOVO nao bloqueia a conexao; o start expira por timeout e o ANTIGO classifica `timeout_start_transaction` (padrao ja usado no ANTIGO [restAdapter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/restAdapter.ts#L8377-L8390)) |

### 8.2 Padroes adotados

- **Fila/outbox persistente:** para eventos NOVO -> ANTIGO e para comandos quando
  aplicavel.
- **Retry com backoff:** no reenvio do outbox e no retry de start (ja existente no ANTIGO).
- **Circuit breaker:** no cliente de integracao, para nao martelar um lado indisponivel.
- **Idempotencia:** chaves em ambos os sentidos.
- **Reconciliacao:** job periodico descrito na secao 7.

Ponto de atencao de confiabilidade: **retry de `RemoteStartTransaction` so e seguro com
idempotencia no NOVO**, pois o carregador pode ter iniciado a carga mesmo sem o ack ter
chegado. O `chargerState.startTransaction` ja retorna a transacao existente para o mesmo
conector ([chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts#L208-L217)), o que ajuda, mas nao substitui a idempotencia no endpoint de start.

---

## 9. Comunicacao bidirecional

| Fluxo | Tipo | Justificativa |
|---|---|---|
| ANTIGO -> NOVO (start/stop) | **sincrono** (HTTP request/response) | o usuario aguarda confirmacao; o ANTIGO ja opera sincrono no start |
| NOVO -> ANTIGO (telemetria) | **assincrono** (push com outbox, ou pull via SSE) | alto volume; nao pode travar o OCPP |
| NOVO -> UI | **streaming** (SSE `/api/stream`, WS `/ws/monitor`) | ja implementado |
| ANTIGO -> App | **sincrono** (REST) | ja implementado |

Garantia de bidirecionalidade: (a) ANTIGO inicia e encerra via HTTP (direcao abaixo ->
acima), (b) NOVO reporta estado/telemetria via webhook/SSE (direcao acima -> abaixo),
(c) ambos com correlacao por `integrationSessionId`. Validacao proposta: injetar um start
pelo ANTIGO e comprovar o ciclo completo `start -> ack -> StartTransaction -> MeterValues
-> session.stopped` correlacionado nos dois lados, com evidencia em
`ocpp_message_logs` (NOVO) e no log de integracao (ANTIGO). Isso e criterio de aceite, nao
um resultado ja obtido.

---

## 10. Decisoes arquiteturais (ADRs resumidos)

**ADR-01: Divisao NOVO=edge OCPP, ANTIGO=negocio/pagamento.**
Contexto: dois CSMS, um com OCPP validado e outro com negocio em producao.
Alternativas: (a) unificar tudo no ANTIGO, (b) unificar tudo no NOVO, (c) dividir.
Escolha: (c). Consequencia: menor retrabalho; exige camada de integracao e correlacao.

**ADR-02: NOVO e autoridade do `ocppTransactionId`; ANTIGO e autoridade do `sessionId`.**
Contexto: conflito de identidade (secao 4.3). Alternativas: NOVO atribui (atual) x
ANTIGO pre-aloca. Escolha: NOVO atribui. Consequencia: ANTIGO adota o id de fio no stop;
evita alterar o nucleo OCPP do NOVO.

**ADR-03: Telemetria NOVO->ANTIGO via push (webhook HMAC + outbox). [APROVADO]**
Contexto: tempo real x zero mudanca. Alternativas: push (webhook HMAC com outbox) x pull
(SSE existente). Com o NOVO alteravel de forma aditiva, o push foi **aprovado** por
entregar tempo real com correlacao explicita. Consequencia: exige cliente HTTP e `outbox`
no NOVO, e um endpoint de ingresso (`POST /v1/integration/ocpp/events`) no ANTIGO. Como os
dois processos rodam na MESMA VM (secao 0.1), o transporte e interno.

**ADR-04: Persistencia de serie temporal em tabela dedicada.**
Contexto: NOVO guarda so a ultima amostra. Alternativas: serie no NOVO x serie no ANTIGO.
Escolha: serie dedicada (secao 5.3), consumida pelo ANTIGO. Consequencia: habilita curva
de potencia/SoC; adiciona escrita de alta frequencia (usar writer em lote, padrao ja
existente).

**ADR-05: Modulo de integracao aditivo no NOVO, sem tocar o nucleo OCPP. [APROVADO]**
Contexto: o pedido exige camada de integracao; o NOVO pode ser alterado desde que a
comunicacao OCPP permaneca intacta. Alternativas: alterar o nucleo OCPP (descartada) x
modulo aditivo no NOVO (escolhida) x tudo no ANTIGO (superado). Escolha **aprovada**:
modulo aditivo no NOVO (rotas `/integration/v1/*` + `inbox`) e, do lado do ANTIGO, o
cliente de integracao. Consequencia: `src/transport/*` e `src/ocpp/*` permanecem sem
alteracao; habilita idempotencia real no start.

**ADR-06: Idempotencia obrigatoria em ambos os sentidos.**
Contexto: retry seguro. Alternativas: sem idempotencia (risco de dupla cobranca/duplo
start). Escolha: chaves `idempotencyKey`/`eventId`. Consequencia: robustez a duplicacao.

**ADR-07: Reaproveitar mecanica de roteamento request/response do ANTIGO como referencia.**
Contexto: o ANTIGO ja tem `executeOnOwner` com `requestId`, timeout e ownership em Redis
([distributedChargerRouting.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/infra/distributedChargerRouting.ts)). Escolha: espelhar esse padrao
na integracao NOVO<->ANTIGO (request/response correlacionado, timeout, erro tipado).

**ADR-08: UI de monitoramento permanece no NOVO (vanilla JS + SSE).**
Contexto: restricao de UI sem build complexo. Escolha: manter. Consequencia: valores
monetarios vem do ANTIGO por contrato, nao sao calculados no NOVO.

---

## 11. Riscos

| # | Risco | Classe | Mitigacao |
|---|---|---|---|
| R1 | Divergencia de `ocppTransactionId` quebra `RemoteStopTransaction` | NECESSARIO | tabela de correlacao (secao 5.3); reconciliacao (secao 7) |
| R2 | Dupla cobranca por start/stop duplicado | NECESSARIO | idempotencia por chave + inbox |
| R3 | Perda de sessoes ativas no restart do NOVO (`activeTransactions` nao reidratado) | NECESSARIO | ANTIGO mantem a memoria da sessao; reafirmar sessoes ativas na reconciliacao |
| R4 | Serie temporal ausente impede curva de potencia/SoC | NECESSARIO | tabela `charge_meter_sample` |
| R5 | Modificacao do NOVO contraria restricao historica | NECESSARIO (decidir) | decisao pendente (secao 13); modulo aditivo e opt-in |
| R6 | Alta frequencia de `MeterValues` degrada a persistencia | RECOMENDADO | writer em lote com fila limitada (padrao ja existente) |
| R7 | Firmware nao envia `SoC` | RECOMENDADO | tratar ausencia como "nao disponivel"; nao estimar sem base |
| R8 | Rede/casing do `chargeBoxId` divergente entre bases | RECOMENDADO | normalizacao + validacao de frota |
| R9 | ~~Defeito de handshake do ANTIGO~~ **RESOLVIDO** | FECHADO | comparacao case-insensitive corrigida (commit `56eade2`) e validada em producao (0 rejeicoes); ver `RELATORIO_HANDSHAKE_OCPP16J_PARA_AUDITORIA_2026-11-17.md` |
| R10 | Reconstrucao de correlacao via pull SSE sem chave explicita | OPCIONAL | migrar para push com `integrationSessionId` |
| R11 | Observabilidade da integracao inexistente | RECOMENDADO | metricas (starts, falhas, latencia, outbox pendente) e alertas |

---

## 12. Plano de evolucao incremental

Convencao: IMPLEMENTADO = codigo existe; VALIDADO = comportamento comprovado com
evidencia; PUBLICADO = em ambiente.

| Fase | Entrega | O que ja existe | Falta | Validacao da fase |
|---|---|---|---|---|
| **F0** | Contratos congelados (start/stop/telemetria, ids, erros) | base: rota generica de comandos [routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L323-L341) | documento de contrato assinado | revisao: contratos sem ambiguidade; `typecheck` verde |
| **F1** | Correlacao e mapeamento de identidade | `activeTransactions` [chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts#L208-L217) | tabelas de correlacao/inbox (secao 5.3) | testes: start reenvia idempotente; mapeamento `sessionId<->ocppTransactionId` correto |
| **F2** | Modulo de integracao no NOVO (start/stop) | `sendCall` [chargerSession.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/chargerSession.ts#L309-L327) | rotas `/integration/v1/*`, auth dedicada | teste de integracao: 409/504/502; sem regressao nas suites OCPP existentes |
| **F3** | Telemetria NOVO->ANTIGO (outbox/pull) | SSE [routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L344-L407) | outbox + cliente HTTP ou consumidor SSE no ANTIGO | ciclo `start->meter->stop` correlacionado nos dois lados |
| **F4** | Serie temporal e metricas de monitoramento | normalizacao de `MeterValues` [handlers.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/handlers.ts#L101-L148) | tabela `charge_meter_sample` + calculo de potencia/tempo/SoC | teste: potencia = dE/dt; tempo e SoC coerentes com as amostras |
| **F5** | Encerramento e cobranca pos-paga via telemetria | cobranca no `StopTransaction` [ocppService.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/application/services/ocppService.ts#L554-L591) | gatilho por `session.stopped` | `summary` com `billingModel=postpaid`, `status=succeeded` (criterio do runbook) |
| **F6** | Falhas, outbox, circuit breaker, reconciliacao | padroes de reconexao no NOVO | jobs e clientes resilientes | testes de queda: ANTIGO cai, NOVO cai, canal cai |
| **F7** | Observabilidade e interface de sessao | UI atual do NOVO | KPIs de integracao e painel de sessao | smoke E2E com hardware real |

Cada fase exige `npm run typecheck` e a suite de testes verde antes de avancar. Nenhuma
fase e declarada VALIDADA sem execucao real; nenhuma e PUBLICADA sem deploy.

---

## 13. Decisoes aprovadas e perguntas remanescentes

### 13.1 Decisoes aprovadas (congeladas)

| # | Pergunta anterior | Decisao aprovada |
|---|---|---|
| 1 | Alterar o NOVO? | **Sim, de forma aditiva**, preservando intacta a comunicacao OCPP ([src/transport](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/ocppWebSocketServer.ts), [src/ocpp](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/handlers.ts)). |
| 2 | Modo de telemetria | **PUSH aprovado** (webhook HMAC + `outbox` no NOVO; ingresso no ANTIGO). |
| 6 | Topologia | Os dois CSMS na **mesma VM** (`ige2a-charger-v2`): ANTIGO `:3000`, NOVO `:3100`. |
| 3 | Autoridade do `ocppTransactionId` | **NOVO atribui; ANTIGO adota** (ADR-02). |
| 4 | Inbox de idempotencia | **Reside no NOVO** (`integration_start_inbox`, secao 4.5). |
| 5 | Escopo imediato | Arquitetura aprovada. Correcoes G4/G3/G1 aplicadas no ANTIGO (commit `04885ff`, push `56eade2..04885ff`), validando `typecheck`, `build` e a suite (32/32). Sequencia autorizada a seguir: **F1 -> F2 -> F3** (secao 12). |

### 13.2 Perguntas remanescentes (nao bloqueiam a aprovacao, decidir antes de codar)

1. **(RESOLVIDA) Topologia de rede.** Confirmado: os dois CSMS rodam na mesma VM
   (`ige2a-charger-v2`), o ANTIGO em `:3000` e o NOVO em `:3100`. A chamada interna
   dispensa exposicao publica; ainda assim, o modulo novo tera credencial propria
   (a rota `/api/*` existente e protegida por `CSMS_API_KEY`,
   [routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts#L158-L167)).
2. **Serie temporal.** Com o modo aditivo no NOVO habilitado, a serie pode residir no NOVO
   (`charge_meter_sample`, secao 5.3) ou no ANTIGO. Confirmar esquema e retencao.
3. **Firmware envia `SoC`?** Precisa de evidencia de campo; sem isso, SoC fica "nao
   disponivel" e nao deve ser estimado.
4. **Valor monetario.** Confirmar que a UI do NOVO exibe o valor vindo do ANTIGO por leitura
   (o NOVO nao tem tarifa).
5. **Retry de `RemoteStartTransaction`.** O ANTIGO ja tenta 3 vezes
   ([restAdapter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/restAdapter.ts#L8782-L8784)).
   Com o `inbox` no NOVO (secao 4.5), o reenvio fica idempotente sem depender da logica do
   ANTIGO; confirmar que o ANTIGO usara `idempotencyKey` estavel.
6. **(RESOLVIDA) Modo de telemetria (pull x push).** **PUSH aprovado** (13.1, item 2;
   ADR-03): `outbox` + webhook HMAC no NOVO e endpoint de ingresso no ANTIGO.

---

## 14. Anexo — Referencias de codigo

### CSMS NOVO (`/Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS`)

- [src/transport/ocppWebSocketServer.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/ocppWebSocketServer.ts)
- [src/transport/chargerSession.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/chargerSession.ts)
- [src/transport/connectionManager.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/transport/connectionManager.ts)
- [src/ocpp/handlers.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/handlers.ts)
- [src/ocpp/protocol.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/protocol.ts)
- [src/ocpp/router.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/router.ts)
- [src/ocpp/errors.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/ocpp/errors.ts)
- [src/chargers/chargerState.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/chargers/chargerState.ts)
- [src/http/routes.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/routes.ts)
- [src/http/monitorWebSocket.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/http/monitorWebSocket.ts)
- [src/persistence/persistenceService.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/persistence/persistenceService.ts)
- [src/persistence/supabaseBatchWriter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/persistence/supabaseBatchWriter.ts)
- [src/availability/availabilityProbe.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/availability/availabilityProbe.ts)
- [src/app.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/app.ts)
- [src/components.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/components.ts)
- [src/config/env.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/config/env.ts)
- [src/domain/types.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/src/domain/types.ts)
- [db/migrations/001_ocpp_observability.sql](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/db/migrations/001_ocpp_observability.sql)
- [db/migrations/003_charger_operational_state.sql](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/db/migrations/003_charger_operational_state.sql)
- [docs/AUDITORIA_ARQUITETURA_E_PLANO_EVOLUCAO.md](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/docs/AUDITORIA_ARQUITETURA_E_PLANO_EVOLUCAO.md)
- [docs/RELATORIO_REQUISITOS_MINIMOS_CSMS.md](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/NEW_IGE2A_CSMS/docs/RELATORIO_REQUISITOS_MINIMOS_CSMS.md)

### CSMS ANTIGO (`/Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS`)

- [src/adapters/restAdapter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/restAdapter.ts) (remote start/stop e roteamento)
- [src/infra/distributedChargerRouting.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/infra/distributedChargerRouting.ts)
- [src/application/services/ocppService.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/application/services/ocppService.ts)
- [src/adapters/ocppWsAdapter.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/adapters/ocppWsAdapter.ts)
- [src/application/connection/connectionManager.ts](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/src/application/connection/connectionManager.ts)
- [POSTPAID_FLOW_PROMPT.md](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/POSTPAID_FLOW_PROMPT.md)
- [POSTPAID_BACKEND_RUNTIME_RUNBOOK.md](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/POSTPAID_BACKEND_RUNTIME_RUNBOOK.md)
- [MOBILE_POSTPAID_BACKEND_READINESS.md](file:///Users/joseroberto/Documents/trae_projects/Work_HOME/IGE2A/repositorios/IGE2A_CSMS/MOBILE_POSTPAID_BACKEND_READINESS.md)
