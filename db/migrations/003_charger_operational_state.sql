-- Migration 003: estado operacional persistido na frota.
--
-- Requisito: sobreviver a reinicializacoes do CSMS preservando, alem da
-- identidade, o ESTADO OPERACIONAL observado (ultimo boot, ultimo
-- StatusNotification e o status por conector). Assim, apos o restart, o CSMS
-- reconhece o carregador como conhecido -- inclusive com o ultimo estado de
-- cada conector -- sem nenhuma reconfiguracao no carregador.
--
-- Idempotente: pode ser reaplicada com seguranca.

ALTER TABLE orchestrator.charger_fleet
  ADD COLUMN IF NOT EXISTS boot_at timestamp with time zone,
  ADD COLUMN IF NOT EXISTS last_status_at timestamp with time zone,
  ADD COLUMN IF NOT EXISTS connectors jsonb NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN orchestrator.charger_fleet.boot_at IS
  'Instante do ultimo BootNotification observado (ultima atividade).';
COMMENT ON COLUMN orchestrator.charger_fleet.last_status_at IS
  'Instante do ultimo StatusNotification observado.';
COMMENT ON COLUMN orchestrator.charger_fleet.connectors IS
  'Ultimo StatusNotification por conector: [{connectorId,status,errorCode,info,updatedAt}]. Nao implica transacao ativa.';
