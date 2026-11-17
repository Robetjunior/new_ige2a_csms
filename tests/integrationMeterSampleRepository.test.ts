import test from 'node:test';
import assert from 'node:assert/strict';
import { MeterSampleRepository } from '../src/integration/meterSampleRepository';
import type { AnySupabaseClient } from '../src/persistence/supabaseBatchWriter';

interface SampleRow {
  id: string;
  integration_session_id: string | null;
  charge_point_id: string;
  connector_id: number;
  measurand: string;
  value: number | null;
  unit: string | null;
  phase: string | null;
  location: string | null;
  context: string | null;
  sampled_at: string;
}

interface FakeState {
  rows: SampleRow[];
  failSelect?: boolean;
}

/**
 * Client Supabase FALSO que simula `orchestrator.charge_meter_sample`.
 * Reproduz a semantica usada pelo repositorio:
 *   - `upsert(batch, { onConflict })` (sem `.select`);
 *   - `select(...).eq().order().limit()`.
 * O builder e "thenable", como o PostgrestBuilder do supabase-js.
 */
function fakeClient(state: FakeState): AnySupabaseClient {
  const from = (_table: string) => {
    let upsertRows: Record<string, unknown>[] | null = null;
    let selectMode = false;
    const filters: Array<[string, unknown]> = [];
    let orderCol: string | null = null;
    let ascending = true;
    let limitN: number | null = null;

    const exec = async (): Promise<{ data: unknown; error: { message: string } | null }> => {
      if (upsertRows) {
        for (const row of upsertRows) {
          state.rows.push(row as unknown as SampleRow);
        }
        return { data: null, error: null };
      }
      if (selectMode) {
        if (state.failSelect) return { data: null, error: { message: 'falha select simulada' } };
        let rows = state.rows.slice();
        for (const [column, value] of filters) {
          rows = rows.filter((row) => (row as Record<string, unknown>)[column] === value);
        }
        if (orderCol) {
          const column = orderCol;
          rows.sort((a, b) => {
            const av = String((a as Record<string, unknown>)[column] ?? '');
            const bv = String((b as Record<string, unknown>)[column] ?? '');
            return ascending ? av.localeCompare(bv) : bv.localeCompare(av);
          });
        }
        if (limitN !== null) rows = rows.slice(0, limitN);
        return { data: rows, error: null };
      }
      return { data: null, error: null };
    };

    const builder = {
      upsert(rows: Record<string, unknown>[]) {
        upsertRows = rows;
        return builder;
      },
      select() {
        selectMode = true;
        return builder;
      },
      eq(column: string, value: unknown) {
        filters.push([column, value]);
        return builder;
      },
      order(column: string, options?: { ascending?: boolean }) {
        orderCol = column;
        ascending = options?.ascending ?? true;
        return builder;
      },
      limit(n: number) {
        limitN = n;
        return builder;
      },
      then(resolve: (value: unknown) => unknown, reject: (reason: unknown) => unknown) {
        return exec().then(resolve, reject);
      },
    };
    return builder;
  };
  return { schema: () => ({ from }) } as unknown as AnySupabaseClient;
}

const T0 = '2026-01-01T00:00:00.000Z';
const T2 = '2026-01-01T00:00:02.000Z';

function row(overrides: Partial<SampleRow>): SampleRow {
  return {
    id: overrides.id ?? 'r1',
    integration_session_id: overrides.integration_session_id ?? 'sess-1',
    charge_point_id: overrides.charge_point_id ?? 'CP-1',
    connector_id: overrides.connector_id ?? 1,
    measurand: overrides.measurand ?? 'Energy.Active.Import.Register',
    value: overrides.value ?? 1000,
    unit: overrides.unit ?? 'Wh',
    phase: overrides.phase ?? null,
    location: overrides.location ?? null,
    context: overrides.context ?? null,
    sampled_at: overrides.sampled_at ?? T0,
  };
}

test('record persiste cada amostra com value numerico e sessao correlacionada', async () => {
  const state: FakeState = { rows: [] };
  const repo = new MeterSampleRepository(fakeClient(state), { flushIntervalMs: 50 });

  repo.record({
    chargePointId: 'CP-1',
    connectorId: 1,
    integrationSessionId: 'sess-1',
    samples: [
      {
        measurand: 'Energy.Active.Import.Register',
        value: '1000',
        unit: 'Wh',
        phase: null,
        location: null,
        context: 'Sample.Periodic',
        timestamp: T0,
      },
      { measurand: 'SoC', value: 'abc', unit: 'Percent', phase: null, location: null, context: null, timestamp: T2 },
    ],
  });
  await repo.stop();

  assert.equal(state.rows.length, 2);
  assert.equal(state.rows[0]?.value, 1000);
  assert.equal(state.rows[0]?.integration_session_id, 'sess-1');
  assert.equal(state.rows[0]?.charge_point_id, 'CP-1');
  assert.equal(state.rows[0]?.connector_id, 1);
  // valor nao numerico vira NULL (o bruto permanece no envelope de telemetria).
  assert.equal(state.rows[1]?.value, null);
});

test('readSessionSamples e readSessionMetrics leem a serie e derivam as metricas', async () => {
  const state: FakeState = {
    rows: [
      row({ id: 'a', value: 1000, sampled_at: T0 }),
      row({ id: 'b', value: 1001, sampled_at: T2 }),
      row({ id: 'c', measurand: 'SoC', value: 55, unit: 'Percent', sampled_at: T2 }),
    ],
  };
  const repo = new MeterSampleRepository(fakeClient(state));

  const samples = await repo.readSessionSamples('sess-1');
  assert.equal(samples.length, 3);
  assert.equal(samples[0]?.value, '1000', 'o valor numerico volta como string de dominio');
  assert.equal(samples[0]?.measurand, 'Energy.Active.Import.Register');

  const metrics = await repo.readSessionMetrics('sess-1', { meterStartWh: 950 });
  assert.equal(metrics.energyWh, 51);
  assert.equal(metrics.powerW, 1800);
  assert.equal(metrics.socPercent, 55);

  await repo.stop();
});

test('falha de leitura devolve [] e metricas nulas (nunca lanca)', async () => {
  const state: FakeState = { rows: [row({ id: 'a' })], failSelect: true };
  const repo = new MeterSampleRepository(fakeClient(state));

  assert.deepEqual(await repo.readSessionSamples('sess-1'), []);
  const metrics = await repo.readSessionMetrics('sess-1');
  assert.equal(metrics.energyWh, null);
  assert.equal(metrics.powerW, null);
  assert.equal(metrics.socPercent, null);
  assert.equal(metrics.durationSeconds, null);

  await repo.stop();
});
