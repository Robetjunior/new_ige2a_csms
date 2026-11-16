/**
 * Dispatcher do outbox (F3): retira eventos pendentes e os envia ao ANTIGO,
 * marcando sucesso ou reagendando com backoff exponencial.
 *
 * Garantias:
 *   - reenvio at-least-once: um evento so sai da fila quando o envio confirma
 *     (`markSent`) ou quando o teto de tentativas e atingido (`failed`);
 *   - backoff com teto: `min(backoffMax, base * 2^(attempts-1))`, o que tambem
 *     atua como throttling quando o ANTIGO esta indisponivel;
 *   - o loop nunca concorre consigo mesmo (`running`) e nunca derruba o processo
 *     (timer `unref`, erros capturados).
 */
import type { IntegrationEvent } from './types';
import type { PushResult } from './pushClient';

/** Recorte do repositorio de outbox consumido pelo dispatcher. */
export interface OutboxStore {
  claimBatch(limit: number): Promise<
    Array<{
      id: string;
      eventId: string;
      eventType: string;
      integrationSessionId: string | null;
      payload: unknown;
      attempts: number;
    }>
  >;
  markSent(id: string): Promise<void>;
  markFailed(id: string, attempts: number, nextAttemptAtIso: string, error: string, terminal: boolean): Promise<void>;
}

/** Recorte do cliente de push consumido pelo dispatcher. */
export interface EventPusher {
  push(event: IntegrationEvent): Promise<PushResult>;
}

/**
 * Gate global de envio (F6). Quando presente, o dispatcher consulta antes de
 * tentar e reporta o desfecho de cada envio. Ver `CircuitBreaker`.
 */
export interface CircuitGate {
  /** Checa (sem consumir sonda) se vale reivindicar o lote neste ciclo. */
  canAttempt(): boolean;
  /** Checa e consome uma sonda do `half_open` antes de enviar um item. */
  allowAttempt(): boolean;
  onSuccess(): void;
  onFailure(): void;
}

export interface OutboxDispatcherOptions {
  store: OutboxStore;
  client: EventPusher;
  intervalMs: number;
  batchSize: number;
  maxAttempts: number;
  backoffBaseMs: number;
  backoffMaxMs: number;
  /** Circuit breaker opcional (F6): evita avalanche quando o ANTIGO cai. */
  breaker?: CircuitGate;
  log?: (msg: string, meta?: unknown) => void;
  /** Relogio injetavel (backoff deterministico em teste). */
  now?: () => number;
}

export class OutboxDispatcher {
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private stopped = false;

  constructor(private readonly options: OutboxDispatcherOptions) {}

  /** Inicia o loop periodico. Idempotente. */
  start(): void {
    if (this.timer || this.stopped) return;
    this.timer = setInterval(() => {
      void this.drainOnce();
    }, this.options.intervalMs);
    if (typeof this.timer.unref === 'function') this.timer.unref();
  }

  /** Para o loop. Idempotente. */
  stop(): void {
    this.stopped = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Executa um ciclo: reivindica um lote e tenta enviar cada evento. Nunca
   * lanca. Nao executa em paralelo consigo mesmo.
   */
  async drainOnce(): Promise<void> {
    if (this.running) return;
    this.running = true;
    const { store, client, breaker, log } = this.options;
    try {
      // F6: com o circuito aberto, nem reivindicamos o lote — os eventos seguem
      // `pending` no outbox e sao reenviados no proximo ciclo apos o cooldown.
      // A checagem aqui NAO consome a sonda do `half_open` (isso fica por item,
      // em `allowAttempt`), senao um lote vazio gastaria a unica sonda.
      if (breaker && !breaker.canAttempt()) {
        log?.('[integration] circuito aberto; reenvio de telemetria adiado');
        return;
      }
      const batch = await store.claimBatch(this.options.batchSize);
      for (const item of batch) {
        // O circuito pode abrir no meio do lote (falha consecutiva): interrompe
        // o restante e devolve a fila para o proximo ciclo.
        if (breaker && !breaker.allowAttempt()) {
          log?.('[integration] circuito aberto durante o lote; restante adiado');
          return;
        }
        const event = item.payload as IntegrationEvent;
        const result = await client.push(event);
        if (result.ok) {
          breaker?.onSuccess();
          await store.markSent(item.id);
          continue;
        }
        breaker?.onFailure();
        const attempts = item.attempts + 1;
        const terminal = attempts >= this.options.maxAttempts;
        const nextAt = this.nextAttemptIso(attempts);
        await store.markFailed(item.id, attempts, nextAt, result.error ?? 'push_failed', terminal);
        log?.(
          `[integration] reenvio do evento ${item.eventId} (${item.eventType}) agendado`,
          { attempts, terminal, error: result.error ?? 'push_failed' },
        );
      }
    } catch (err) {
      log?.('[integration] erro inesperado no ciclo do outbox', err);
    } finally {
      this.running = false;
    }
  }

  /** `min(backoffMax, base * 2^(attempts-1))` a partir de agora. */
  private nextAttemptIso(attempts: number): string {
    const now = this.options.now ? this.options.now() : Date.now();
    const exponent = Math.max(0, attempts - 1);
    const raw = this.options.backoffBaseMs * 2 ** exponent;
    const delay = Math.min(this.options.backoffMaxMs, raw);
    return new Date(now + delay).toISOString();
  }
}
