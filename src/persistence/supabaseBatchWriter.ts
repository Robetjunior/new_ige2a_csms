/**
 * Writer em lote para o Supabase (PostgreSQL/PostgREST).
 *
 * Garantias exigidas pela spec §6:
 *  - a falha de persistencia NUNCA derruba o OCPP: todos os erros sao capturados
 *    e apenas registrados; `enqueue` jamais lanca;
 *  - a memoria e limitada: acima de `maxQueue` o registro mais antigo e
 *    descartado (contabilizado em `stats().dropped`) em vez de crescer;
 *  - gravacao idempotente por `onConflict` (upsert), permitindo reemissao.
 */
import type { SupabaseClient } from '@supabase/supabase-js';

/** Client com schema/tabela resolvidos em runtime (tabelas nao tipadas). */
export type AnySupabaseClient = SupabaseClient<any, any, any, any, any>;

export interface SupabaseBatchWriterOptions<Row = Record<string, unknown>> {
  readonly schema: string;
  readonly table: string;
  /** Coluna(s) que definem a idempotencia do upsert (ex.: 'id'). */
  readonly onConflict: string;
  readonly batchSize: number;
  readonly flushIntervalMs: number;
  readonly maxQueue: number;
  /**
   * Chave de coalescencia opcional (ex.: `charge_point_id`). Quando informada,
   * um novo registro com a mesma chave SUBSTITUI o que ja esta na fila em vez
   * de ser anexado. Isso evita duas linhas com a mesma chave de conflito no
   * mesmo lote (o `ON CONFLICT DO UPDATE` do PostgreSQL rejeita esse caso) e
   * limita a amplificacao de escritas quando o estado do mesmo registro muda
   * varias vezes entre dois flushes.
   */
  readonly dedupeKey?: (row: Row) => string | null;
  /** Logger nao-bloqueante (default: silencioso). */
  readonly log?: (message: string, meta?: unknown) => void;
}

export interface BatchWriterStats {
  readonly queued: number;
  readonly written: number;
  readonly failed: number;
  readonly dropped: number;
}

export class SupabaseBatchWriter<Row extends Record<string, unknown>> {
  private readonly queue: Row[] = [];
  private timer: NodeJS.Timeout | null = null;
  private flushing = false;
  private stopped = false;
  private written = 0;
  private failed = 0;
  private dropped = 0;

  constructor(
    private readonly client: AnySupabaseClient,
    private readonly opts: SupabaseBatchWriterOptions<Row>,
  ) {
    this.timer = setInterval(() => {
      void this.flush();
    }, opts.flushIntervalMs);
    // Nunca mantem o processo vivo por causa do timer.
    if (typeof this.timer.unref === 'function') this.timer.unref();
  }

  /** Enfileira um registro. Nunca lanca; respeita o limite de memoria. */
  enqueue(row: Row): void {
    if (this.stopped) return;
    const key = this.opts.dedupeKey?.(row);
    if (key != null) {
      const index = this.queue.findIndex((queued) => this.opts.dedupeKey?.(queued) === key);
      if (index >= 0) {
        // Mantem a posicao na fila, mas com o estado mais recente.
        this.queue[index] = row;
        return;
      }
    }
    if (this.queue.length >= this.opts.maxQueue) {
      this.queue.shift();
      this.dropped += 1;
    }
    this.queue.push(row);
    if (this.queue.length >= this.opts.batchSize) void this.flush();
  }

  /** Esvazia a fila em lotes de `batchSize`. Seguro para chamadas concorrentes. */
  async flush(): Promise<void> {
    if (this.flushing || this.queue.length === 0) return;
    this.flushing = true;
    try {
      while (this.queue.length > 0) {
        const batch = this.queue.splice(0, this.opts.batchSize);
        await this.writeBatch(batch);
      }
    } finally {
      this.flushing = false;
    }
  }

  private async writeBatch(batch: Row[]): Promise<void> {
    try {
      // A tabela e dinamica (nao tipada no client), logo o cast e necessario.
      const { error } = await this.client
        .schema(this.opts.schema)
        .from(this.opts.table)
        .upsert(batch as any[], { onConflict: this.opts.onConflict });
      if (error) {
        this.failed += batch.length;
        this.opts.log?.(`[persistence] falha ao gravar em ${this.opts.table}: ${error.message}`);
        return;
      }
      this.written += batch.length;
    } catch (err) {
      this.failed += batch.length;
      this.opts.log?.(`[persistence] erro inesperado em ${this.opts.table}`, err);
    }
  }

  /** Para o timer e tenta drenar a fila (usado no shutdown). */
  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    await this.flush();
  }

  stats(): BatchWriterStats {
    return { queued: this.queue.length, written: this.written, failed: this.failed, dropped: this.dropped };
  }
}
