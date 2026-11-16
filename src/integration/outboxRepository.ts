/**
 * Repositorio do outbox de telemetria (F3). Encaixa o NOVO na tabela
 * `orchestrator.integration_outbox` (secao 5.3 do documento de arquitetura).
 *
 * Garantias (espelham o padrao de robustez do restante do modulo):
 *   - `enqueue` grava o evento ANTES do envio (at-least-once); a unicidade de
 *     `event_id` e resolvida pelo BANCO (ON CONFLICT DO NOTHING);
 *   - nenhum metodo lanca: falha de infraestrutura vira log + retorno neutro,
 *     pois o caminho critico e o OCPP.
 */
import { randomUUID } from 'node:crypto';
import type { AnySupabaseClient } from '../persistence/supabaseBatchWriter';
import type { IntegrationEvent } from './types';

const TABLE_OUTBOX = 'integration_outbox';

export interface IntegrationOutboxRepositoryOptions {
  schema?: string;
  log?: (msg: string, meta?: unknown) => void;
}

/** Evento reivindicado para envio (recorte necessario ao dispatcher). */
export interface OutboxClaimedEvent {
  id: string;
  eventId: string;
  eventType: string;
  integrationSessionId: string | null;
  payload: unknown;
  attempts: number;
}

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.trim().length > 0 ? value.trim() : null;
}

function errorMessage(error: unknown): string {
  if (error && typeof error === 'object' && 'message' in error) {
    const message = (error as { message?: unknown }).message;
    if (typeof message === 'string' && message.length > 0) return message;
  }
  return 'erro desconhecido';
}

export class IntegrationOutboxRepository {
  private readonly schema: string;
  private readonly log: (msg: string, meta?: unknown) => void;

  constructor(
    private readonly client: AnySupabaseClient,
    options: IntegrationOutboxRepositoryOptions = {},
  ) {
    this.schema = options.schema ?? 'orchestrator';
    this.log = options.log ?? (() => undefined);
  }

  /**
   * Grava um evento de telemetria. Idempotente por `event_id`: enfileirar o
   * mesmo evento duas vezes nao cria duplicata. Nunca lanca.
   */
  async enqueue(event: IntegrationEvent): Promise<void> {
    try {
      const now = new Date().toISOString();
      const { error } = await this.client
        .schema(this.schema)
        .from(TABLE_OUTBOX)
        .upsert(
          {
            id: randomUUID(),
            event_id: event.eventId,
            event_type: event.eventType,
            integration_session_id: event.integrationSessionId,
            payload: event,
            status: 'pending',
            attempts: 0,
            next_attempt_at: now,
            updated_at: now,
          },
          { onConflict: 'event_id', ignoreDuplicates: true },
        );
      if (error) {
        this.log(`[integration] falha ao enfileirar evento ${event.eventId}: ${errorMessage(error)}`);
      }
    } catch (err) {
      this.log(`[integration] erro inesperado ao enfileirar evento ${event.eventId}`, err);
    }
  }

  /**
   * Reivindica ate `limit` eventos pendentes e vencidos, do mais antigo para o
   * mais recente. Nunca lanca; devolve lista vazia em caso de erro.
   */
  async claimBatch(limit: number): Promise<OutboxClaimedEvent[]> {
    try {
      const now = new Date().toISOString();
      const { data, error } = await this.client
        .schema(this.schema)
        .from(TABLE_OUTBOX)
        .select('id, event_id, event_type, integration_session_id, payload, attempts')
        .eq('status', 'pending')
        .lte('next_attempt_at', now)
        .order('created_at', { ascending: true })
        .limit(limit);
      if (error) {
        this.log(`[integration] falha ao reivindicar lote do outbox: ${errorMessage(error)}`);
        return [];
      }
      if (!Array.isArray(data)) return [];
      return data.map((row) => toClaimedEvent(row as Record<string, unknown>));
    } catch (err) {
      this.log('[integration] erro inesperado ao reivindicar lote do outbox', err);
      return [];
    }
  }

  /** Marca o evento como enviado com sucesso. Nunca lanca. */
  async markSent(id: string): Promise<void> {
    try {
      const { error } = await this.client
        .schema(this.schema)
        .from(TABLE_OUTBOX)
        .update({ status: 'sent', last_error: null, updated_at: new Date().toISOString() })
        .eq('id', id);
      if (error) {
        this.log(`[integration] falha ao marcar evento ${id} como enviado: ${errorMessage(error)}`);
      }
    } catch (err) {
      this.log(`[integration] erro inesperado ao marcar evento ${id} como enviado`, err);
    }
  }

  /**
   * Registra uma falha de envio: incrementa `attempts` e agenda o proximo
   * instante. Quando `terminal` e verdadeiro (teto de tentativas atingido), o
   * evento sai da fila (`status = failed`); caso contrario volta a `pending`.
   * Nunca lanca.
   */
  async markFailed(
    id: string,
    attempts: number,
    nextAttemptAtIso: string,
    error: string,
    terminal: boolean,
  ): Promise<void> {
    try {
      const { error: dbError } = await this.client
        .schema(this.schema)
        .from(TABLE_OUTBOX)
        .update({
          status: terminal ? 'failed' : 'pending',
          attempts,
          next_attempt_at: nextAttemptAtIso,
          last_error: error,
          updated_at: new Date().toISOString(),
        })
        .eq('id', id);
      if (dbError) {
        this.log(`[integration] falha ao agendar reenvio do evento ${id}: ${errorMessage(dbError)}`);
      }
    } catch (err) {
      this.log(`[integration] erro inesperado ao agendar reenvio do evento ${id}`, err);
    }
  }
}

function toClaimedEvent(row: Record<string, unknown>): OutboxClaimedEvent {
  const attemptsRaw = row.attempts;
  const attempts = typeof attemptsRaw === 'number' && Number.isFinite(attemptsRaw) ? attemptsRaw : 0;
  return {
    id: asString(row.id) ?? '',
    eventId: asString(row.event_id) ?? '',
    eventType: asString(row.event_type) ?? '',
    integrationSessionId: asString(row.integration_session_id),
    payload: row.payload,
    attempts,
  };
}
