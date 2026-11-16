import test from 'node:test';
import assert from 'node:assert/strict';
import { IntegrationInboxRepository } from '../src/integration/inboxRepository';
import type { AnySupabaseClient } from '../src/persistence/supabaseBatchWriter';

interface InboxRow {
  idempotency_key: string;
  charge_point_id: string;
  connector_id: number;
  request_hash: string | null;
  response: unknown;
  state: string;
  created_at: string;
  updated_at: string;
}

interface FakeState {
  rows: Map<string, InboxRow>;
  failUpsert?: boolean;
  failSelect?: boolean;
}

/**
 * Client Supabase FALSO que simula `integration_start_inbox` em memoria.
 *
 * Reproduz a semantica real usada pelo repositorio:
 *   - `upsert(rows, { ignoreDuplicates: true }).select('...')` retorna apenas as
 *     linhas EFETIVAMENTE inseridas (a unicidade e resolvida na "tabela");
 *   - `update(...).eq(...)` altera a linha;
 *   - `select(...).eq(...).maybeSingle()` le uma linha.
 * O builder e "thenable" (possui `then`), como o PostgrestBuilder do supabase-js.
 */
function fakeInboxClient(state: FakeState): AnySupabaseClient {
  const from = (_table: string) => {
    let insertRows: Record<string, unknown>[] | null = null;
    let selectCols: string | null = null;
    let filterKey: string | null = null;
    let patch: Record<string, unknown> | null = null;

    // execucao sincrona (sem await): garante a atomicidade por chave exigida
    // pelo teste de concorrencia.
    const exec = async (): Promise<{ data: unknown; error: { message: string } | null }> => {
      if (insertRows) {
        if (state.failUpsert) return { data: null, error: { message: 'falha upsert simulada' } };
        const inserted: Record<string, unknown>[] = [];
        for (const row of insertRows) {
          const key = String(row.idempotency_key);
          if (!state.rows.has(key)) {
            const now = new Date().toISOString();
            state.rows.set(key, {
              idempotency_key: key,
              charge_point_id: String(row.charge_point_id),
              connector_id: Number(row.connector_id),
              request_hash: (row.request_hash as string | null) ?? null,
              response: null,
              state: 'PENDING',
              created_at: now,
              updated_at: now,
            });
            inserted.push({ idempotency_key: key });
          }
        }
        return { data: inserted, error: null };
      }

      if (patch) {
        const existing = filterKey !== null ? state.rows.get(filterKey) : undefined;
        if (existing) Object.assign(existing, patch);
        return { data: null, error: null };
      }

      if (selectCols) {
        if (state.failSelect) return { data: null, error: { message: 'falha select simulada' } };
        const row = filterKey !== null ? state.rows.get(filterKey) : undefined;
        return { data: row ?? null, error: null };
      }

      return { data: null, error: null };
    };

    const builder = {
      upsert(rows: unknown) {
        insertRows = Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [rows as Record<string, unknown>];
        return builder;
      },
      select(cols: string) {
        selectCols = cols;
        return builder;
      },
      update(values: Record<string, unknown>) {
        patch = values;
        return builder;
      },
      eq(column: string, value: unknown) {
        if (column === 'idempotency_key') filterKey = String(value);
        return builder;
      },
      maybeSingle() {
        return exec();
      },
      then(resolve: (value: unknown) => unknown, reject?: (reason: unknown) => unknown) {
        return exec().then(resolve, reject);
      },
    };
    return builder;
  };

  return { schema: () => ({ from }) } as unknown as AnySupabaseClient;
}

function makeRepo(state: FakeState): IntegrationInboxRepository {
  return new IntegrationInboxRepository(fakeInboxClient(state), { schema: 'orchestrator', log: () => undefined });
}

const claimInput = (overrides: Partial<{ idempotencyKey: string; requestHash: string | null }> = {}) => ({
  idempotencyKey: overrides.idempotencyKey ?? 'key-1',
  chargePointId: 'CP-1',
  connectorId: 1,
  requestHash: overrides.requestHash,
});

test('primeiro claim de uma chave nova retorna owner', async () => {
  const state: FakeState = { rows: new Map() };
  const repo = makeRepo(state);

  const result = await repo.claim(claimInput({ requestHash: 'h1' }));
  assert.equal(result.outcome, 'owner');
  assert.equal(state.rows.size, 1);
});

test('segundo claim da MESMA chave antes de complete retorna in_progress', async () => {
  const repo = makeRepo({ rows: new Map() });

  assert.equal((await repo.claim(claimInput({ requestHash: 'h1' }))).outcome, 'owner');
  const second = await repo.claim(claimInput({ requestHash: 'h1' }));
  assert.equal(second.outcome, 'in_progress');
});

test('apos complete o retry retorna duplicate com a MESMA resposta', async () => {
  const repo = makeRepo({ rows: new Map() });
  const response = { commandId: 'cmd-1', accepted: true };

  assert.equal((await repo.claim(claimInput({ requestHash: 'h1' }))).outcome, 'owner');
  await repo.complete('key-1', response);

  const retry = await repo.claim(claimInput({ requestHash: 'h1' }));
  assert.equal(retry.outcome, 'duplicate');
  assert.deepEqual((retry as { response: unknown }).response, response);
});

test('mesma chave com request_hash diferente retorna hash_mismatch', async () => {
  const repo = makeRepo({ rows: new Map() });

  assert.equal((await repo.claim(claimInput({ requestHash: 'h1' }))).outcome, 'owner');
  const mismatch = await repo.claim(claimInput({ requestHash: 'h2' }));
  assert.equal(mismatch.outcome, 'hash_mismatch');
  const typed = mismatch as { expectedHash: string | null; receivedHash: string | null };
  assert.equal(typed.expectedHash, 'h1');
  assert.equal(typed.receivedHash, 'h2');
});

test('duas chamadas concorrentes para a mesma chave: exatamente uma e owner', async () => {
  const repo = makeRepo({ rows: new Map() });

  const [a, b] = await Promise.all([
    repo.claim(claimInput({ requestHash: 'h1' })),
    repo.claim(claimInput({ requestHash: 'h1' })),
  ]);

  const outcomes = [a.outcome, b.outcome];
  assert.equal(outcomes.filter((o) => o === 'owner').length, 1, 'exatamente um dono');
  assert.equal(outcomes.filter((o) => o === 'in_progress' || o === 'duplicate').length, 1);
});

test('erro do client retorna { outcome: error } e nunca lanca', async () => {
  const repo = makeRepo({ rows: new Map(), failUpsert: true });

  let result: { outcome: string; message?: string } | null = null;
  await assert.doesNotReject(async () => {
    result = await repo.claim(claimInput());
  });
  assert.equal(result!.outcome, 'error');
  assert.equal(typeof result!.message, 'string');
});

test('falha de select no caminho duplicado tambem retorna error sem lancar', async () => {
  const state: FakeState = { rows: new Map() };
  const repo = makeRepo(state);

  // Primeiro claim insere a chave normalmente.
  assert.equal((await repo.claim(claimInput({ requestHash: 'h1' }))).outcome, 'owner');

  // Depois força falha de leitura: o repositorio deve capturar e retornar error.
  state.failSelect = true;
  const result = await repo.claim(claimInput({ requestHash: 'h1' }));
  assert.equal(result.outcome, 'error');
});
