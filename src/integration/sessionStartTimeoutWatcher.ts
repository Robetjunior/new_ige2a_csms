/**
 * Watcher de timeout de inicio de sessao (endurecimento do fluxo
 * `start_pending` / `PENDING_ACK`).
 *
 * Problema que resolve: o carregador pode ACEITAR o `RemoteStartTransaction`
 * (estado passa a `PENDING_ACK`) e nunca enviar o `StartTransaction` — por
 * exemplo quando o veiculo nao consome corrente ou por limitacao de firmware.
 * A sessao fica presa em `PENDING_ACK` indefinidamente, sem `transactionId`,
 * e nao ha caminho de cobranca nem de encerramento.
 *
 * Regras:
 *  - apenas OBSERVA: nao envia OCPP. Passado `timeoutMs`, encerra a sessao
 *    local (`STOPPED`) e emite `session.stopped` com `reason: start_timeout`,
 *    para o ANTIGO finalizar a sessao de negocio (sem cobranca, pois nenhuma
 *    energia foi entregue);
 *  - nunca lanca: erro de infraestrutura vira log; o caminho critico e o OCPP;
 *  - sweeps nao se sobrepoem (`sweeping`); `timeoutMs <= 0` desabilita;
 *  - `unref()` no timer para nao segurar o processo.
 */
import { randomUUID } from 'node:crypto';
import type { IntegrationEvent, IntegrationSessionRow } from './types';
import type { IntegrationSessionUpsert } from './inboxRepository';

export interface SessionStartTimeoutRepository {
  listStalePendingAckSessions(olderThanIso: string, limit?: number): Promise<IntegrationSessionRow[]>;
  upsertSession(row: IntegrationSessionUpsert): Promise<void>;
}

export interface SessionStartTimeoutOutbox {
  enqueue(event: IntegrationEvent): Promise<void>;
}

export interface SessionStartTimeoutWatcherOptions {
  /** Tempo (ms) apos o qual uma sessao `PENDING_ACK` e considerada expirada. `<= 0` desabilita. */
  timeoutMs: number;
  /** Periodo (ms) do sweep. */
  intervalMs: number;
  /** Maximo de sessoes canceladas por sweep. */
  batchLimit?: number;
  /** Seam de teste (default: `Date.now`). */
  now?: () => number;
}

export interface SessionStartTimeoutWatcherDeps {
  repository: SessionStartTimeoutRepository;
  outbox: SessionStartTimeoutOutbox | null;
  log?: (msg: string, meta?: unknown) => void;
}

export class SessionStartTimeoutWatcher {
  private readonly opts: Required<SessionStartTimeoutWatcherOptions>;
  private readonly deps: SessionStartTimeoutWatcherDeps;
  private timer: ReturnType<typeof setInterval> | null = null;
  private sweeping = false;

  constructor(deps: SessionStartTimeoutWatcherDeps, options: SessionStartTimeoutWatcherOptions) {
    this.deps = deps;
    this.opts = {
      timeoutMs: options.timeoutMs,
      intervalMs: Math.max(1000, options.intervalMs),
      batchLimit: Math.max(1, options.batchLimit ?? 100),
      now: options.now ?? (() => Date.now()),
    };
  }

  /** `true` quando o watcher esta ativo (timeout configurado). */
  get enabled(): boolean {
    return this.opts.timeoutMs > 0;
  }

  start(): void {
    if (!this.enabled || this.timer) return;
    this.timer = setInterval(() => {
      void this.sweep();
    }, this.opts.intervalMs);
    this.timer.unref?.();
    // Sweep inicial: limpa sessoes presas antes do restart.
    void this.sweep();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  /**
   * Executa UM ciclo de verificacao e cancela as sessoes expiradas.
   * Publico para teste; nunca lanca. Retorna quantas foram canceladas.
   */
  async sweep(): Promise<number> {
    if (!this.enabled || this.sweeping) return 0;
    this.sweeping = true;
    try {
      const cutoff = new Date(this.opts.now() - this.opts.timeoutMs).toISOString();
      const stale = await this.deps.repository.listStalePendingAckSessions(cutoff, this.opts.batchLimit);
      let cancelled = 0;
      for (const session of stale) {
        try {
          await this.cancel(session);
          cancelled += 1;
        } catch (err) {
          this.deps.log?.(
            `[integration] falha ao cancelar start expirado ${session.integration_session_id}`,
            err,
          );
        }
      }
      return cancelled;
    } catch (err) {
      this.deps.log?.('[integration] falha no sweep de timeout de start', err);
      return 0;
    } finally {
      this.sweeping = false;
    }
  }

  private async cancel(session: IntegrationSessionRow): Promise<void> {
    const stoppedAt = new Date(this.opts.now()).toISOString();
    await this.deps.repository.upsertSession({
      integrationSessionId: session.integration_session_id,
      idempotencyKey: session.idempotency_key,
      chargePointId: session.charge_point_id,
      connectorId: session.connector_id,
      ocppTransactionId: null,
      state: 'STOPPED',
      idTag: session.id_tag,
      startedAt: session.started_at,
      stoppedAt,
    });
    if (this.deps.outbox) {
      const event: IntegrationEvent = {
        eventId: randomUUID(),
        eventType: 'session.stopped',
        integrationSessionId: session.integration_session_id,
        ocppTransactionId: null,
        chargePointId: session.charge_point_id,
        connectorId: session.connector_id,
        occurredAt: stoppedAt,
        data: { meterStop: null, reason: 'start_timeout' },
      };
      await this.deps.outbox.enqueue(event);
    }
  }
}
