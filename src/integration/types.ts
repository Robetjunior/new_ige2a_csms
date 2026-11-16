/**
 * Tipos publicos do modulo de integracao (F1 - correlacao e mapeamento de
 * identidade). Consumidos pela logica de repositorio/correlacao; as rotas HTTP
 * (F2) farao uso destes contratos depois.
 */

/** Ciclo de vida da sessao de recarga correlacionada. */
export type IntegrationSessionState = 'PENDING_ACK' | 'STARTED' | 'STOPPING' | 'STOPPED' | 'FAILED';

/** Entrada da reivindicacao de idempotencia do start. */
export interface InboxClaimInput {
  idempotencyKey: string;
  chargePointId: string;
  connectorId: number;
  requestHash?: string | null;
}

/**
 * Resultado da reivindicacao de idempotencia:
 *   - `owner`: chave inedita; este chamador e o dono e deve executar o comando;
 *   - `duplicate`: chave ja concluida; devolver a MESMA resposta, sem reenviar;
 *   - `in_progress`: chave existente ainda sem resposta (retry => 409 na F2);
 *   - `hash_mismatch`: mesma chave com corpo (request_hash) diferente;
 *   - `error`: falha de infraestrutura; nunca lanca.
 */
export type InboxClaimResult =
  | { outcome: 'owner' }
  | { outcome: 'duplicate'; response: unknown }
  | { outcome: 'in_progress' }
  | { outcome: 'hash_mismatch'; expectedHash: string | null; receivedHash: string | null }
  | { outcome: 'error'; message: string };

/** Linha de `integration_charge_session` no formato de dominio. */
export interface IntegrationSessionRow {
  integration_session_id: string;
  idempotency_key: string | null;
  charge_point_id: string;
  connector_id: number;
  ocpp_transaction_id: number | null;
  state: IntegrationSessionState;
  id_tag: string | null;
  started_at: string | null;
  stopped_at: string | null;
}

// -----------------------------------------------------------------------------
// F3 - Telemetria NOVO -> ANTIGO (PUSH com outbox)
// -----------------------------------------------------------------------------

/**
 * Tipos de evento do contrato de telemetria (secao 4.2 do documento de
 * arquitetura). `connector.status` nao e correlacionado a uma sessao: carrega
 * `integrationSessionId` nulo quando o conector nao esta em recarga.
 */
export type IntegrationEventType = 'session.started' | 'meter.sampled' | 'connector.status' | 'session.stopped';

/** Amostra de `MeterValues` no formato ja normalizado pelo NOVO. */
export interface IntegrationMeterSample {
  measurand: string;
  value: string;
  unit: string | null;
  phase: string | null;
  location: string | null;
  context: string | null;
  timestamp: string;
}

/**
 * Envelope do evento de telemetria. E o CORPO exato enviado ao ANTIGO em
 * `POST /v1/integration/ocpp/events` e tambem o `payload` gravado no outbox.
 * `eventId` e a chave de idempotencia (o ANTIGO deduplica por ele).
 */
export interface IntegrationEvent {
  eventId: string;
  eventType: IntegrationEventType;
  integrationSessionId: string | null;
  ocppTransactionId: number | null;
  chargePointId: string;
  connectorId: number | null;
  occurredAt: string;
  data: Record<string, unknown>;
}
