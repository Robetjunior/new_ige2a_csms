/**
 * Repositorio da serie temporal de `MeterValues` (F4 - secao 5.3 do documento de
 * arquitetura). Encaixa o NOVO na tabela `orchestrator.charge_meter_sample`.
 *
 * Garantias (espelham o padrao de robustez do restante do modulo):
 *   - escrita em lote via `SupabaseBatchWriter` (fila limitada, nunca lanca);
 *     a alta frequencia de `MeterValues` (risco R6) nao degrada o OCPP;
 *   - a serie e a VERDADE DE ENERGIA do NOVO (secao 7.2): `readSessionMetrics`
 *     deriva energia/potencia/tempo/SoC a partir das amostras persistidas;
 *   - leitura nunca lanca: falha de infraestrutura vira `[]`/metricas nulas.
 */
import { randomUUID } from 'node:crypto';
import { SupabaseBatchWriter, type AnySupabaseClient } from '../persistence/supabaseBatchWriter';
import type { IntegrationMeterSample } from './types';
import { computeSessionMetrics, type SessionMetrics } from './meterMetrics';

const TABLE_SAMPLE = 'charge_meter_sample';

/** Amostras normalizadas de um `MeterValues`, prontas para persistir. */
export interface MeterSampleInput {
  chargePointId: string;
  connectorId: number;
  /** Nulo quando a leitura nao pertence a uma sessao de negocio correlacionada. */
  integrationSessionId: string | null;
  samples: readonly IntegrationMeterSample[];
}

/** Superficie consumida pelo `TelemetryProducer`. Nunca lanca. */
export interface MeterSampleSink {
  record(input: MeterSampleInput): void;
}

export interface MeterSampleRepositoryOptions {
  schema?: string;
  batchSize?: number;
  flushIntervalMs?: number;
  maxQueue?: number;
  log?: (msg: string, meta?: unknown) => void;
}

/** Converte o `value` textual do OCPP no `numeric` da coluna (NULL se invalido). */
function numericValue(raw: string): number | null {
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : null;
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

/** Linha do banco -> amostra de dominio. `null` quando a linha esta incompleta. */
function toSample(row: Record<string, unknown>): IntegrationMeterSample | null {
  const measurand = asString(row.measurand);
  const timestamp = asString(row.sampled_at);
  if (!measurand || !timestamp) return null;
  return {
    measurand,
    value: row.value === null || row.value === undefined ? '' : String(row.value),
    unit: asString(row.unit),
    phase: asString(row.phase),
    location: asString(row.location),
    context: asString(row.context),
    timestamp,
  };
}

export class MeterSampleRepository implements MeterSampleSink {
  private readonly writer: SupabaseBatchWriter<Record<string, unknown>>;
  private readonly schema: string;
  private readonly log: (msg: string, meta?: unknown) => void;

  constructor(
    private readonly client: AnySupabaseClient,
    options: MeterSampleRepositoryOptions = {},
  ) {
    this.schema = options.schema ?? 'orchestrator';
    this.log = options.log ?? (() => undefined);
    this.writer = new SupabaseBatchWriter<Record<string, unknown>>(client, {
      schema: this.schema,
      table: TABLE_SAMPLE,
      onConflict: 'id',
      batchSize: options.batchSize ?? 200,
      flushIntervalMs: options.flushIntervalMs ?? 1500,
      maxQueue: options.maxQueue ?? 20000,
      // Mesmo `MeterValues` reemitido na mesma janela de flush pode repetir a
      // amostra: coalescer pela identidade logica evita duas linhas iguais no
      // lote. A granularidade temporal evita perder leituras distintas.
      dedupeKey: (row) =>
        [
          row.charge_point_id,
          row.connector_id,
          row.measurand,
          row.phase ?? '',
          row.location ?? '',
          row.sampled_at,
        ].join('|'),
      log: this.log,
    });
  }

  /** Enfileira cada amostra como uma linha append-only. Nunca lanca. */
  record(input: MeterSampleInput): void {
    for (const sample of input.samples) {
      this.writer.enqueue({
        id: randomUUID(),
        integration_session_id: input.integrationSessionId,
        charge_point_id: input.chargePointId,
        connector_id: input.connectorId,
        measurand: sample.measurand,
        value: numericValue(sample.value),
        unit: sample.unit,
        phase: sample.phase,
        location: sample.location,
        context: sample.context,
        sampled_at: sample.timestamp,
      });
    }
  }

  /** Le a serie de uma sessao em ordem cronologica. Nunca lanca; `[]` se indisponivel. */
  async readSessionSamples(integrationSessionId: string, limit = 5000): Promise<IntegrationMeterSample[]> {
    try {
      const { data, error } = await this.client
        .schema(this.schema)
        .from(TABLE_SAMPLE)
        .select('measurand, value, unit, phase, location, context, sampled_at')
        .eq('integration_session_id', integrationSessionId)
        .order('sampled_at', { ascending: true })
        .limit(limit);
      if (error) {
        this.log(`[integration] falha ao ler serie da sessao ${integrationSessionId}: ${errorMessage(error)}`);
        return [];
      }
      const rows = Array.isArray(data) ? (data as Record<string, unknown>[]) : [];
      const samples: IntegrationMeterSample[] = [];
      for (const row of rows) {
        const sample = toSample(row);
        if (sample) samples.push(sample);
      }
      return samples;
    } catch (err) {
      this.log(`[integration] erro inesperado ao ler serie da sessao ${integrationSessionId}`, err);
      return [];
    }
  }

  /** Metricas derivadas da serie de uma sessao (secao 6.1). Nunca lanca. */
  async readSessionMetrics(
    integrationSessionId: string,
    options: { meterStartWh?: number | null; startedAt?: string | null; endedAt?: string | null; limit?: number } = {},
  ): Promise<SessionMetrics> {
    const samples = await this.readSessionSamples(integrationSessionId, options.limit ?? 5000);
    return computeSessionMetrics({
      samples,
      meterStartWh: options.meterStartWh ?? null,
      startedAt: options.startedAt ?? null,
      endedAt: options.endedAt ?? null,
    });
  }

  /** Drena a fila (usado no shutdown). */
  async stop(): Promise<void> {
    await this.writer.stop();
  }
}
