/**
 * Repositorio do modulo de integracao (F1). Encaixa o CSMS NOVO nas tabelas do
 * schema `orchestrator`:
 *   - integration_start_inbox    (inbox de idempotencia do start);
 *   - integration_charge_session (correlacao sessao <-> transacao OCPP).
 *
 * Regras de robustez (espelham a spec §6 e a secao 4.5 do documento de
 * arquitetura):
 *   - a unicidade/dono e resolvida pelo BANCO (PK textual + ON CONFLICT
 *     DO NOTHING), nunca pela aplicacao;
 *   - nenhum metodo lanca: falha de infraestrutura vira resultado observavel
 *     (`{ outcome: 'error' }` ou log + retorno nulo), pois o caminho critico e
 *     o OCPP.
 */
import type { AnySupabaseClient } from '../persistence/supabaseBatchWriter';
import type {
  InboxClaimInput,
  InboxClaimResult,
  IntegrationSessionRow,
  IntegrationSessionState,
} from './types';

const TABLE_INBOX = 'integration_start_inbox';
const TABLE_SESSION = 'integration_charge_session';

export interface IntegrationInboxRepositoryOptions {
  schema?: string;
  log?: (msg: string, meta?: unknown) => void;
}

/** Entrada do upsert de `integration_charge_session` (nomes de dominio). */
export interface IntegrationSessionUpsert {
  integrationSessionId: string;
  idempotencyKey?: string | null;
  chargePointId: string;
  connectorId: number;
  ocppTransactionId?: number | null;
  state: IntegrationSessionState;
  idTag?: string | null;
  meterStartWh?: number | null;
  meterStopWh?: number | null;
  startedAt?: string | null;
  stoppedAt?: string | null;
  lastMeterAt?: string | null;
}

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.trim().length > 0 ? value.trim() : null;
}

function asNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === '') return null;
  const parsed = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function errorMessage(error: unknown): string {
  if (error && typeof error === 'object' && 'message' in error) {
    const message = (error as { message?: unknown }).message;
    if (typeof message === 'string' && message.length > 0) return message;
  }
  return 'erro desconhecido';
}

export class IntegrationInboxRepository {
  private readonly schema: string;
  private readonly log: (msg: string, meta?: unknown) => void;

  constructor(
    private readonly client: AnySupabaseClient,
    options: IntegrationInboxRepositoryOptions = {},
  ) {
    this.schema = options.schema ?? 'orchestrator';
    this.log = options.log ?? (() => undefined);
  }

  /**
   * Reivindica a chave de idempotencia do start. A PK textual decide o dono:
   * duas chamadas concorrentes com a mesma chave nunca retornam `owner` duas
   * vezes (a unicidade e resolvida pelo banco, nao pela aplicacao).
   */
  async claim(input: InboxClaimInput): Promise<InboxClaimResult> {
    const { idempotencyKey, chargePointId, connectorId } = input;
    const receivedHash = input.requestHash ?? null;
    try {
      // Passo 1: INSERT ... ON CONFLICT (idempotency_key) DO NOTHING. O
      // `ignoreDuplicates` + `.select()` retorna apenas as linhas efetivamente
      // inseridas: 1 linha => chave inedita (este chamador e o dono).
      const insert = await this.client
        .schema(this.schema)
        .from(TABLE_INBOX)
        .upsert(
          {
            idempotency_key: idempotencyKey,
            charge_point_id: chargePointId,
            connector_id: connectorId,
            request_hash: receivedHash,
          },
          { onConflict: 'idempotency_key', ignoreDuplicates: true },
        )
        .select('idempotency_key');

      if (insert.error) {
        const message = errorMessage(insert.error);
        this.log(`[integration] falha ao reivindicar inbox ${idempotencyKey}: ${message}`);
        return { outcome: 'error', message };
      }

      const inserted = Array.isArray(insert.data) ? insert.data : [];
      if (inserted.length > 0) {
        return { outcome: 'owner' };
      }

      // Passo 2: a chave ja existia. Ler o estado corrente.
      const existing = await this.client
        .schema(this.schema)
        .from(TABLE_INBOX)
        .select('request_hash, response, state')
        .eq('idempotency_key', idempotencyKey)
        .maybeSingle();

      if (existing.error) {
        const message = errorMessage(existing.error);
        this.log(`[integration] falha ao ler inbox ${idempotencyKey}: ${message}`);
        return { outcome: 'error', message };
      }

      const row = existing.data as { request_hash?: unknown; response?: unknown; state?: unknown } | null;
      if (!row) {
        // Corrida rara: a linha sumiu entre o upsert e o select.
        return { outcome: 'error', message: 'linha do inbox ausente apos conflito' };
      }

      const expectedHash = asString(row.request_hash);
      if (expectedHash !== null && receivedHash !== null && expectedHash !== receivedHash) {
        return { outcome: 'hash_mismatch', expectedHash, receivedHash };
      }

      if (row.state === 'COMPLETED' || row.response != null) {
        return { outcome: 'duplicate', response: row.response };
      }

      return { outcome: 'in_progress' };
    } catch (err) {
      const message = errorMessage(err);
      this.log(`[integration] erro inesperado ao reivindicar inbox ${idempotencyKey}`, err);
      return { outcome: 'error', message };
    }
  }

  /** Conclui a chave do inbox com a resposta a ser devolvida em retries. */
  async complete(
    idempotencyKey: string,
    response: unknown,
    state: 'COMPLETED' | 'FAILED' = 'COMPLETED',
  ): Promise<void> {
    try {
      const { error } = await this.client
        .schema(this.schema)
        .from(TABLE_INBOX)
        .update({ response, state, updated_at: new Date().toISOString() })
        .eq('idempotency_key', idempotencyKey);
      if (error) {
        this.log(`[integration] falha ao concluir inbox ${idempotencyKey}: ${errorMessage(error)}`);
      }
    } catch (err) {
      this.log(`[integration] erro inesperado ao concluir inbox ${idempotencyKey}`, err);
    }
  }

  /** Upsert da sessao correlacionada (idempotente por integration_session_id). */
  async upsertSession(row: IntegrationSessionUpsert): Promise<void> {
    try {
      const payload: Record<string, unknown> = {
        integration_session_id: row.integrationSessionId,
        idempotency_key: row.idempotencyKey ?? null,
        charge_point_id: row.chargePointId,
        connector_id: row.connectorId,
        ocpp_transaction_id: row.ocppTransactionId ?? null,
        state: row.state,
        id_tag: row.idTag ?? null,
        meter_start_wh: row.meterStartWh ?? null,
        meter_stop_wh: row.meterStopWh ?? null,
        started_at: row.startedAt ?? null,
        stopped_at: row.stoppedAt ?? null,
        last_meter_at: row.lastMeterAt ?? null,
        updated_at: new Date().toISOString(),
      };
      const { error } = await this.client
        .schema(this.schema)
        .from(TABLE_SESSION)
        .upsert(payload, { onConflict: 'integration_session_id' });
      if (error) {
        this.log(`[integration] falha ao gravar sessao ${row.integrationSessionId}: ${errorMessage(error)}`);
      }
    } catch (err) {
      this.log(`[integration] erro inesperado ao gravar sessao ${row.integrationSessionId}`, err);
    }
  }

  /**
   * Vincula o `ocpp_transaction_id` (atribuido pelo NOVO no StartTransaction) a
   * sessao correlacionada ainda sem transacao daquele (charge_point_id,
   * connector_id). Como o filtro exige `ocpp_transaction_id is null`, a linha
   * so pode ser a PENDENTE_ACK; por isso `started_at` e setado neste primeiro
   * vinculo (equivalente a "se nulo").
   */
  async linkOcppTransaction(
    chargePointId: string,
    connectorId: number,
    ocppTransactionId: number,
  ): Promise<void> {
    try {
      const now = new Date().toISOString();
      const { error } = await this.client
        .schema(this.schema)
        .from(TABLE_SESSION)
        .update({ ocpp_transaction_id: ocppTransactionId, state: 'STARTED', started_at: now, updated_at: now })
        .eq('charge_point_id', chargePointId)
        .eq('connector_id', connectorId)
        .is('ocpp_transaction_id', null);
      if (error) {
        this.log(
          `[integration] falha ao vincular transacao ${chargePointId}/${connectorId}: ${errorMessage(error)}`,
        );
      }
    } catch (err) {
      this.log(`[integration] erro inesperado ao vincular transacao ${chargePointId}/${connectorId}`, err);
    }
  }

  /** Marca como STOPPED a sessao aberta daquele (charge_point_id, connector_id). */
  async markSessionStopped(chargePointId: string, connectorId: number): Promise<void> {
    try {
      const now = new Date().toISOString();
      const { error } = await this.client
        .schema(this.schema)
        .from(TABLE_SESSION)
        .update({ state: 'STOPPED', stopped_at: now, updated_at: now })
        .eq('charge_point_id', chargePointId)
        .eq('connector_id', connectorId)
        .in('state', ['STARTED', 'STOPPING']);
      if (error) {
        this.log(
          `[integration] falha ao encerrar sessao ${chargePointId}/${connectorId}: ${errorMessage(error)}`,
        );
      }
    } catch (err) {
      this.log(`[integration] erro inesperado ao encerrar sessao ${chargePointId}/${connectorId}`, err);
    }
  }

  /** Le uma sessao correlacionada. Nunca lanca; retorna null se indisponivel. */
  async getSession(id: string): Promise<IntegrationSessionRow | null> {
    try {
      const { data, error } = await this.client
        .schema(this.schema)
        .from(TABLE_SESSION)
        .select('*')
        .eq('integration_session_id', id)
        .maybeSingle();
      if (error) {
        this.log(`[integration] falha ao ler sessao ${id}: ${errorMessage(error)}`);
        return null;
      }
      if (!data) return null;
      return toSessionRow(data as Record<string, unknown>);
    } catch (err) {
      this.log(`[integration] erro inesperado ao ler sessao ${id}`, err);
      return null;
    }
  }

  /**
   * Resolve o `integration_session_id` de negocio aberto para um conector
   * (F3). Considera apenas estados nao terminais; usado pelo produtor de
   * telemetria para correlacionar os eventos. Nunca lanca; null se indisponivel.
   */
  async getOpenSessionByConnector(chargePointId: string, connectorId: number): Promise<string | null> {
    try {
      const { data, error } = await this.client
        .schema(this.schema)
        .from(TABLE_SESSION)
        .select('integration_session_id')
        .eq('charge_point_id', chargePointId)
        .eq('connector_id', connectorId)
        .in('state', ['PENDING_ACK', 'STARTED', 'STOPPING'])
        .order('created_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (error) {
        this.log(
          `[integration] falha ao resolver sessao de ${chargePointId}/${connectorId}: ${errorMessage(error)}`,
        );
        return null;
      }
      return asString((data as { integration_session_id?: unknown } | null)?.integration_session_id);
    } catch (err) {
      this.log(`[integration] erro inesperado ao resolver sessao de ${chargePointId}/${connectorId}`, err);
      return null;
    }
  }

  /**
   * Lista sessoes em `PENDING_ACK` criadas antes de `olderThanIso`. Consumida
   * pelo watcher de timeout de start: um start aceito pelo carregador que nunca
   * virou `StartTransaction` fica preso em `PENDING_ACK` e precisa ser
   * cancelado. Nunca lanca; `[]` se indisponivel.
   */
  async listStalePendingAckSessions(olderThanIso: string, limit = 100): Promise<IntegrationSessionRow[]> {
    try {
      const { data, error } = await this.client
        .schema(this.schema)
        .from(TABLE_SESSION)
        .select('*')
        .eq('state', 'PENDING_ACK')
        .lt('created_at', olderThanIso)
        .order('created_at', { ascending: true })
        .limit(Math.max(1, limit));
      if (error) {
        this.log(
          `[integration] falha ao listar sessoes PENDING_ACK antigas: ${errorMessage(error)}`,
        );
        return [];
      }
      return (data ?? [])
        .map((row) => toSessionRow(row as Record<string, unknown>))
        .filter((row): row is IntegrationSessionRow => row !== null);
    } catch (err) {
      this.log('[integration] erro inesperado ao listar sessoes PENDING_ACK antigas', err);
      return [];
    }
  }
}

function toSessionRow(row: Record<string, unknown>): IntegrationSessionRow | null {
  const integrationSessionId = asString(row.integration_session_id);
  if (!integrationSessionId) return null;
  const state = asString(row.state) as IntegrationSessionState | null;
  return {
    integration_session_id: integrationSessionId,
    idempotency_key: asString(row.idempotency_key),
    charge_point_id: asString(row.charge_point_id) ?? '',
    connector_id: asNumber(row.connector_id) ?? 0,
    ocpp_transaction_id: asNumber(row.ocpp_transaction_id),
    state: state ?? 'FAILED',
    id_tag: asString(row.id_tag),
    started_at: asString(row.started_at),
    stopped_at: asString(row.stopped_at),
  };
}
