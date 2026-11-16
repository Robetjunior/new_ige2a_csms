import test from 'node:test';
import assert from 'node:assert/strict';
import { IntegrationOutboxRepository } from '../src/integration/outboxRepository';
import type { AnySupabaseClient } from '../src/persistence/supabaseBatchWriter';
import type { IntegrationEvent } from '../src/integration/types';

interface OutboxRow {
  id: string;
  event_id: string;
  event_type: string;
  integration_session_id: string | null;
  payload: unknown;
  status: string;
  attempts: number;
  next_attempt_at: string;
  last_error: string | null;
  created_at: string;
  updated_at: string;
}

interface FakeState {
  rows: OutboxRow[];
  failUpsert?: boolean;
  failSelect?: boolean;
}

/**
 * Client Supabase FALSO que simula `orchestrator.integration_outbox` em memoria.
 *
 * Reproduz a semantica usada pelo repositorio:
 *   - `upsert(row, { onConflict, ignoreDuplicates })` (nao retorna `.select`);
 *   - `select(...).eq().lte().order().limit()`;
 *   - `update(...).eq('id', ...)`.
 * O builder e "thenable" (possui `then`), como o PostgrestBuilder do supabase-js.
 */
function fakeClient(state: FakeState): AnySupabaseClient {
  const from = (_table: string) => {
    let upsertRow: Record<string, unknown> | null = null;
    let upsertIgnore = false;
    let selectMode = false;
    let patch: Record<string, unknown> | null = null;
    const filters: Array<[string, unknown]> = [];
    let lteFilter: [string, string] | null = null;
    let limitN: number | null = null;

    const exec = async (): Promise<{ data: unknown; error: { message: string } | null }> => {
      if (upsertRow) {
        if (state.failUpsert) return { data: null, error: { message: 'falha upsert simulada' } };
        const eventId = String(upsertRow.event_id);
        const existing = state.rows.find((r) => r.event_id === eventId);
        if (!existing) {
          const now = new Date().toISOString();
          state.rows.push({
            id: String(upsertRow.id),
            event_id: eventId,
            event_type: String(upsertRow.event_type),
            integration_session_id: (upsertRow.integration_session_id as string | null) ?? null,
            payload: upsertRow.payload,
            status: 'pending',
            attempts: 0,
            next_attempt_at: String(upsertRow.next_attempt_at),
            last_error: null,
            created_at: now,
            updated_at: now,
          });
        } else if (!upsertIgnore) {
          Object.assign(existing, upsertRow);
        }
        return { data: null, error: null };
      }

      if (patch) {
        const idFilter = filters.find(([column]) => column === 'id');
        const row = idFilter ? state.rows.find((r) => r.id === idFilter[1]) : undefined;
        if (row) Object.assign(row, patch);
        return { data: null, error: null };
      }

      if (selectMode) {
        if (state.failSelect) return { data: null, error: { message: 'falha select simulada' } };
        let rows = state.rows.slice();
        for (const [column, value] of filters) {
          rows = rows.filter((r) => (r as unknown as Record<string, unknown>)[column] === value);
        }
        if (lteFilter) {
          const [column, value] = lteFilter;
          rows = rows.filter((r) => String((r as unknown as Record<string, unknown>)[column]) <= value);
        }
        rows.sort((a, b) => a.created_at.localeCompare(b.created_at));
        if (limitN !== null) rows = rows.slice(0, limitN);
        return { data: rows, error: null };
      }

      return { data: null, error: null };
    };

    const builder = {
      upsert(rows: unknown, opts?: { ignoreDuplicates?: boolean }) {
        upsertRow = Array.isArray(rows) ? (rows[0] as Record<string, unknown>) : (rows as Record<string, unknown>);
        upsertIgnore = opts?.ignoreDuplicates === true;
        return builder;
      },
      select(_cols: string) {
        selectMode = true;
        return builder;
      },
      update(values: Record<string, unknown>) {
        patch = values;
        return builder;
      },
      eq(column: string, value: unknown) {
        filters.push([column, value]);
        return builder;
      },
      lte(column: string, value: string) {
        lteFilter = [column, value];
        return builder;
      },
      order(_column: string, _opts?: unknown) {
        return builder;
      },
      limit(count: number) {
        limitN = count;
        return builder;
      },
      then(resolve: (value: unknown) => unknown, reject?: (reason: unknown) => unknown) {
        return exec().then(resolve, reject);
      },
    };
    return builder;
  };

  return { schema: () => ({ from }) } as unknown as AnySupabaseClient;
}

function makeEvent(eventId: string): IntegrationEvent {
  return {
    eventId,
    eventType: 'meter.sampled',
    integrationSessionId: 'sess-1',
    ocppTransactionId: 7,
    chargePointId: 'CP-1',
    connectorId: 1,
    occurredAt: '2026-01-01T00:00:00.000Z',
    data: { samples: [] },
  };
}

function row(
  id: string,
  eventId: string,
  nextAttemptAt: string,
  createdAt: string,
  status: string,
  attempts: number,
): OutboxRow {
  return {
    id,
    event_id: eventId,
    event_type: 'meter.sampled',
    integration_session_id: null,
    payload: makeEvent(eventId),
    status,
    attempts,
    next_attempt_at: nextAttemptAt,
    last_error: null,
    created_at: createdAt,
    updated_at: createdAt,
  };
}

function makeRepo(state: FakeState): IntegrationOutboxRepository {
  return new IntegrationOutboxRepository(fakeClient(state), { schema: 'orchestrator', log: () => undefined });
}

test('enqueue grava o evento e e idempotente por event_id', async () => {
  const state: FakeState = { rows: [] };
  const repo = makeRepo(state);
  const event = makeEvent('e1');

  await repo.enqueue(event);
  await repo.enqueue(event); // mesmo event_id: nao duplica

  assert.equal(state.rows.length, 1);
  assert.equal(state.rows[0].event_id, 'e1');
  assert.equal(state.rows[0].status, 'pending');
  assert.equal(state.rows[0].attempts, 0);
  assert.deepEqual(state.rows[0].payload, event);
});

test('claimBatch devolve apenas pendentes vencidos, respeitando limite e ordem', async () => {
  const state: FakeState = {
    rows: [
      row('o1', 'e1', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z', 'pending', 0),
      row('o2', 'e2', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:01.000Z', 'pending', 0),
      row('o3', 'e3', '2999-01-01T00:00:00.000Z', '2026-01-01T00:00:02.000Z', 'pending', 0), // futuro: excluido
      row('o4', 'e4', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:03.000Z', 'sent', 0), // ja enviado: excluido
    ],
  };
  const repo = makeRepo(state);

  const batch = await repo.claimBatch(10);

  assert.deepEqual(batch.map((item) => item.eventId), ['e1', 'e2']);
  assert.equal(batch[0].id, 'o1');
  assert.equal(batch[0].attempts, 0);

  const limited = await repo.claimBatch(1);
  assert.equal(limited.length, 1);
  assert.equal(limited[0].eventId, 'e1');
});

test('markSent muda o status para sent', async () => {
  const state: FakeState = { rows: [row('o1', 'e1', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z', 'pending', 0)] };
  const repo = makeRepo(state);

  await repo.markSent('o1');

  assert.equal(state.rows[0].status, 'sent');
});

test('markFailed reagenda (pendente) e encerra no teto (failed)', async () => {
  const state: FakeState = {
    rows: [
      row('o1', 'e1', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z', 'pending', 0),
      row('o2', 'e2', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:01.000Z', 'pending', 0),
    ],
  };
  const repo = makeRepo(state);

  await repo.markFailed('o2', 3, '2026-01-01T00:00:10.000Z', 'http_500', false);
  const notTerminal = state.rows.find((r) => r.id === 'o2') as OutboxRow;
  assert.equal(notTerminal.status, 'pending');
  assert.equal(notTerminal.attempts, 3);
  assert.equal(notTerminal.last_error, 'http_500');
  assert.equal(notTerminal.next_attempt_at, '2026-01-01T00:00:10.000Z');

  await repo.markFailed('o1', 8, '2026-01-01T00:00:20.000Z', 'http_502', true);
  assert.equal(state.rows.find((r) => r.id === 'o1')?.status, 'failed');
});

test('nenhum metodo lanca: falha de infra vira retorno neutro', async () => {
  const state: FakeState = { rows: [], failUpsert: true, failSelect: true };
  const repo = makeRepo(state);

  await assert.doesNotReject(() => repo.enqueue(makeEvent('e1')));
  await assert.doesNotReject(() => repo.markSent('inexistente'));
  await assert.doesNotReject(() => repo.markFailed('inexistente', 1, '2026-01-01T00:00:00.000Z', 'erro', false));

  const batch = await repo.claimBatch(5);
  assert.deepEqual(batch, []);
});
