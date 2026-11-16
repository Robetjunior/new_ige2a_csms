-- =============================================================================
-- IGE2A CSMS Next Generation - F4: serie temporal de MeterValues
--
-- Integracao com o CSMS ANTIGO (docs/ARQUITETURA_INTEGRADA_DUAL_CSMS.md, secoes
-- 5.3, 6.1 e 12/F4). Cria uma tabela nova no MESMO schema usado pelo CSMS
-- antigo (`orchestrator`), sem alterar nenhuma tabela existente. Todas as
-- instrucoes sao idempotentes (`if not exists`) e podem ser reaplicadas.
--
-- Semantica:
--   - append-only: cada leitura de `MeterValues` gera uma linha por
--     `sampledValue` normalizado; nao ha upsert de estado;
--   - `integration_session_id` e nulo quando a leitura nao pertence a uma
--     sessao de negocio correlacionada (ex.: swipe local no proprio carregador);
--     a serie continua util para monitoramento e reconciliacao de energia;
--   - `value` e numerico: valores nao numericos sao gravados como NULL (o valor
--     bruto permanece no envelope de telemetria `meter.sampled`);
--   - esta tabela e a VERDADE DE ENERGIA do NOVO (secao 7.2: usar `MeterValues`
--     persistidos como referencia quando `meterStop` divergir entre as partes).
-- =============================================================================

create schema if not exists orchestrator;

create table if not exists orchestrator.charge_meter_sample (
  id                     uuid        primary key,
  integration_session_id text,
  charge_point_id        text        not null,
  connector_id           integer     not null,
  measurand              text        not null,
  value                  numeric,
  unit                   text,
  phase                  text,
  location               text,
  context                text,
  sampled_at             timestamptz not null,
  created_at             timestamptz not null default now()
);

comment on table orchestrator.charge_meter_sample is
  'Serie temporal append-only dos MeterValues recebidos pelo NOVO (F4). Verdade de energia para reconciliacao.';
comment on column orchestrator.charge_meter_sample.integration_session_id is
  'Sessao de negocio correlacionada; nulo quando a leitura nao pertence a uma sessao.';
comment on column orchestrator.charge_meter_sample.value is
  'Valor numerico da amostra; NULL quando o valor original nao e numerico.';
comment on column orchestrator.charge_meter_sample.sampled_at is
  'Timestamp da amostra (`meterValue[].timestamp`; fallback: instante de recebimento).';

-- Consumo por sessao (curva de potencia/energia de uma recarga).
create index if not exists charge_meter_sample_session_idx
  on orchestrator.charge_meter_sample (integration_session_id, sampled_at desc);

-- Consumo por carregador/conector (monitoramento e reconciliacao sem sessao).
create index if not exists charge_meter_sample_connector_idx
  on orchestrator.charge_meter_sample (charge_point_id, connector_id, sampled_at desc);
