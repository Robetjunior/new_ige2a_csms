-- =============================================================================
-- IGE2A CSMS Next Generation - F1: correlacao e mapeamento de identidade
--
-- Integracao com o CSMS ANTIGO (docs/ARQUITETURA_INTEGRADA_DUAL_CSMS.md, secoes
-- 4.5 e 5.3). Cria duas tabelas novas no MESMO schema usado pelo CSMS antigo
-- (`orchestrator`), sem alterar nenhuma tabela existente. Todas as instrucoes
-- sao idempotentes (`if not exists`) e podem ser reaplicadas com seguranca.
--
-- Escopo desta migration (F1):
--   1) integration_start_inbox    - inbox de idempotencia do start (o "dono");
--   2) integration_charge_session - correlacao integration_session_id <-> ocpp_transaction_id.
-- As tabelas integration_outbox e charge_meter_sample pertencem a F3/F4 e NAO
-- fazem parte desta etapa.
-- =============================================================================

create schema if not exists orchestrator;

-- -----------------------------------------------------------------------------
-- 1) integration_start_inbox - inbox de idempotencia dos starts recebidos do ANTIGO
--
-- Semantica (secao 4.5):
--   - `response` NULO e `state = 'PENDING'` significa "start em andamento": um
--     retry com a mesma chave recebe 409 (duplicate_in_progress) na F2, sem
--     reenviar o comando ao carregador.
--   - `response` PREENCHIDO significa "ja concluido": um retry devolve a MESMA
--     resposta, sem reenviar o comando.
--   - A chave primaria TEXTUAL (`idempotency_key`) e o que torna a operacao
--     concorrente-segura no banco (INSERT ... ON CONFLICT DO NOTHING), e nao na
--     aplicacao: duas chamadas simultaneas com a mesma chave nunca executam o
--     comando duas vezes.
--   - `request_hash` detecta a mesma chave reenviada com corpo diferente, que
--     vira erro explicito (nunca silencio).
-- -----------------------------------------------------------------------------
create table if not exists orchestrator.integration_start_inbox (
  idempotency_key text primary key,
  charge_point_id text        not null,
  connector_id    integer     not null,
  request_hash    text,
  response        jsonb,
  state           text        not null default 'PENDING'
                              check (state in ('PENDING', 'COMPLETED', 'FAILED')),
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);

comment on table orchestrator.integration_start_inbox is
  'Inbox de idempotencia do POST /integration/v1/sessions/start. A PK textual decide o dono do start.';
comment on column orchestrator.integration_start_inbox.response is
  'Resposta do start. Nulo + state=PENDING => start em andamento (retry => 409); preenchido => ja concluido (retry devolve a MESMA resposta sem reenviar comando).';
comment on column orchestrator.integration_start_inbox.request_hash is
  'Hash do corpo do start. Mesma chave com hash diferente e rejeitada (nao silenciada).';

-- -----------------------------------------------------------------------------
-- 2) integration_charge_session - projecao de correlacao sessao <-> transacao OCPP
--
-- O NOVO nao cobra: apenas correlaciona o `integration_session_id` (gerado pelo
-- ANTIGO) com o `ocpp_transaction_id` (atribuido pelo NOVO no StartTransaction).
-- -----------------------------------------------------------------------------
create table if not exists orchestrator.integration_charge_session (
  integration_session_id text primary key,       -- gerado pelo ANTIGO
  idempotency_key        text unique,            -- dedupe do start
  charge_point_id        text        not null,
  connector_id           integer     not null,
  ocpp_transaction_id    integer,                -- preenchido no StartTransaction
  state                  text        not null
                                     check (state in ('PENDING_ACK', 'STARTED', 'STOPPING', 'STOPPED', 'FAILED')),
  id_tag                 text,
  meter_start_wh         numeric,
  meter_stop_wh          numeric,
  started_at             timestamptz,
  stopped_at             timestamptz,
  last_meter_at          timestamptz,
  created_at             timestamptz not null default now(),
  updated_at             timestamptz not null default now()
);

comment on table orchestrator.integration_charge_session is
  'Correlacao integration_session_id (ANTIGO) <-> ocpp_transaction_id (NOVO). Projecao de integracao; o NOVO nao cobra.';

create index if not exists integration_charge_session_cp_started_idx
  on orchestrator.integration_charge_session (charge_point_id, started_at desc);
create index if not exists integration_charge_session_cp_conn_idx
  on orchestrator.integration_charge_session (charge_point_id, connector_id);
create index if not exists integration_charge_session_ocpp_tx_idx
  on orchestrator.integration_charge_session (ocpp_transaction_id);
