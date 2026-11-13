import test from 'node:test';
import assert from 'node:assert/strict';
import { SupabaseBatchWriter, type AnySupabaseClient } from '../src/persistence/supabaseBatchWriter';

/** Client Supabase falso: registra os lotes gravados, sem rede. */
function fakeClient(opts: { fail?: boolean; onUpsert?: (rows: unknown[]) => void } = {}): AnySupabaseClient {
  const builder = {
    async upsert(rows: unknown[]) {
      if (opts.fail) return { error: { message: 'falha simulada' } };
      opts.onUpsert?.(rows);
      return { error: null };
    },
  };
  const from = () => builder;
  const schema = () => ({ from });
  return { schema } as unknown as AnySupabaseClient;
}

function makeWriter(
  client: AnySupabaseClient,
  overrides: Partial<{ batchSize: number; maxQueue: number; dedupeKey: (row: Record<string, unknown>) => string | null }> = {},
) {
  return new SupabaseBatchWriter<Record<string, unknown>>(client, {
    schema: 'orchestrator',
    table: 'ocpp_message_logs',
    onConflict: 'id',
    batchSize: overrides.batchSize ?? 10,
    // Intervalo alto: o flush e acionado explicitamente nos testes.
    flushIntervalMs: 1_000_000,
    maxQueue: overrides.maxQueue ?? 100,
    dedupeKey: overrides.dedupeKey,
    log: () => undefined,
  });
}

test('grava a fila em lote e contabiliza registros escritos', async () => {
  const batches: unknown[][] = [];
  const writer = makeWriter(
    fakeClient({
      onUpsert: (rows) => batches.push(rows),
    }),
  );

  writer.enqueue({ id: 'a' });
  writer.enqueue({ id: 'b' });
  writer.enqueue({ id: 'c' });
  assert.equal(writer.stats().queued, 3);

  await writer.flush();
  assert.equal(batches.length, 1);
  assert.equal(batches[0]?.length, 3);
  assert.equal(writer.stats().written, 3);
  assert.equal(writer.stats().queued, 0);
  await writer.stop();
});

test('respeita batchSize dividindo a fila em varios lotes', async () => {
  const batches: unknown[][] = [];
  const writer = makeWriter(
    fakeClient({
      onUpsert: (rows) => batches.push(rows),
    }),
    { batchSize: 2 },
  );

  writer.enqueue({ id: '1' });
  writer.enqueue({ id: '2' });
  writer.enqueue({ id: '3' });

  // O enqueue dispara flush automatico em paralelo; aguarda o dreno completo.
  const deadline = Date.now() + 2000;
  while (writer.stats().written < 3 && Date.now() < deadline) {
    await writer.flush();
    await new Promise((resolve) => setTimeout(resolve, 5));
  }

  const total = batches.reduce((acc, b) => acc + b.length, 0);
  assert.equal(total, 3);
  assert.equal(writer.stats().written, 3);
  // Nenhum lote pode exceder o batchSize configurado.
  assert.ok(batches.every((b) => b.length <= 2));
  await writer.stop();
});

test('falha de gravacao nunca lanca e e contabilizada', async () => {
  const writer = makeWriter(fakeClient({ fail: true }));
  writer.enqueue({ id: 'x' });
  writer.enqueue({ id: 'y' });

  await assert.doesNotReject(() => writer.flush());
  assert.equal(writer.stats().failed, 2);
  assert.equal(writer.stats().written, 0);
  await writer.stop();
});

test('limita a memoria descartando o registro mais antigo', () => {
  const writer = makeWriter(fakeClient(), { maxQueue: 3 });
  for (let i = 0; i < 5; i += 1) writer.enqueue({ id: `n${i}` });

  const stats = writer.stats();
  assert.equal(stats.queued, 3);
  assert.equal(stats.dropped, 2);
  void writer.stop();
});

test('stop drena a fila pendente e ignora novos enqueues', async () => {
  const batches: unknown[][] = [];
  const writer = makeWriter(
    fakeClient({
      onUpsert: (rows) => batches.push(rows),
    }),
  );

  writer.enqueue({ id: 'p' });
  writer.enqueue({ id: 'q' });
  await writer.stop();

  assert.equal(writer.stats().written, 2);
  writer.enqueue({ id: 'r' });
  assert.equal(writer.stats().queued, 0);
});

test('coalesce por chave mantem uma unica linha por registro no lote', async () => {
  const batches: Record<string, unknown>[][] = [];
  const writer = makeWriter(
    fakeClient({
      onUpsert: (rows) => batches.push(rows as Record<string, unknown>[]),
    }),
    { dedupeKey: (row) => String(row.charge_point_id ?? '') },
  );

  // O mesmo carregador muda tres vezes antes de qualquer flush (attempt,
  // accepted, status): apenas o estado mais recente deve ir para o lote.
  writer.enqueue({ charge_point_id: 'CP-1', reconnect_count: 0 });
  writer.enqueue({ charge_point_id: 'CP-2', reconnect_count: 0 });
  writer.enqueue({ charge_point_id: 'CP-1', reconnect_count: 2 });
  assert.equal(writer.stats().queued, 2, 'uma linha por chargePointId');

  await writer.flush();
  assert.equal(batches.length, 1);
  assert.equal(batches[0]?.length, 2);
  const cp1 = batches[0]?.find((row) => row.charge_point_id === 'CP-1');
  assert.equal(cp1?.reconnect_count, 2, 'o estado mais recente prevalece');
  await writer.stop();
});
