/**
 * Testes do watcher de timeout de start. Nao dependem de rede nem de Supabase:
 * usam repositorio e outbox FALSOS, com `now` injetado.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { SessionStartTimeoutWatcher } from '../src/integration/sessionStartTimeoutWatcher';
import type { IntegrationSessionRow } from '../src/integration/types';
import type { IntegrationSessionUpsert } from '../src/integration/inboxRepository';

const NOW = Date.parse('2026-11-17T12:00:00.000Z');
const TIMEOUT_MS = 120_000;

function makeSession(overrides: Partial<IntegrationSessionRow> = {}): IntegrationSessionRow {
  return {
    integration_session_id: 'S1',
    idempotency_key: 'K1',
    charge_point_id: 'CP-1',
    connector_id: 1,
    ocpp_transaction_id: null,
    state: 'PENDING_ACK',
    id_tag: 'TAG-1',
    started_at: null,
    stopped_at: null,
    ...overrides,
  };
}

function makeRepo(sessions: IntegrationSessionRow[]) {
  const listCalls: Array<{ olderThanIso: string; limit?: number }> = [];
  const upserts: IntegrationSessionUpsert[] = [];
  const failList = { value: false };
  return {
    listCalls,
    upserts,
    failList,
    async listStalePendingAckSessions(olderThanIso: string, limit?: number) {
      listCalls.push({ olderThanIso, limit });
      if (failList.value) throw new Error('supabase indisponivel');
      return sessions;
    },
    async upsertSession(row: IntegrationSessionUpsert) {
      upserts.push(row);
    },
  };
}

function makeOutbox() {
  const enqueued: unknown[] = [];
  return {
    enqueued,
    async enqueue(event: unknown) {
      enqueued.push(event);
    },
  };
}

function buildWatcher(
  repo: ReturnType<typeof makeRepo>,
  outbox: ReturnType<typeof makeOutbox> | null,
  timeoutMs = TIMEOUT_MS,
) {
  return new SessionStartTimeoutWatcher(
    { repository: repo, outbox, log: () => undefined },
    { timeoutMs, intervalMs: 30_000, now: () => NOW },
  );
}

test('sweep cancela sessoes PENDING_ACK expiradas: STOPPED + session.stopped (start_timeout)', async () => {
  const repo = makeRepo([makeSession()]);
  const outbox = makeOutbox();
  const watcher = buildWatcher(repo, outbox);

  const cancelled = await watcher.sweep();

  assert.equal(cancelled, 1);
  // Cutoff = now - timeoutMs.
  assert.equal(repo.listCalls.length, 1);
  assert.equal(repo.listCalls[0]?.olderThanIso, new Date(NOW - TIMEOUT_MS).toISOString());
  // Sessao encerrada localmente.
  assert.equal(repo.upserts.length, 1);
  assert.equal(repo.upserts[0]?.state, 'STOPPED');
  assert.equal(repo.upserts[0]?.ocppTransactionId, null);
  assert.equal(repo.upserts[0]?.stoppedAt, new Date(NOW).toISOString());
  // Evento emitido para o ANTIGO finalizar a sessao de negocio.
  assert.equal(outbox.enqueued.length, 1);
  const event = outbox.enqueued[0] as { eventType: string; data: { reason: string } };
  assert.equal(event.eventType, 'session.stopped');
  assert.equal(event.data.reason, 'start_timeout');
});

test('sweep sem sessoes expiradas nao cancela nada', async () => {
  const repo = makeRepo([]);
  const outbox = makeOutbox();
  const watcher = buildWatcher(repo, outbox);

  const cancelled = await watcher.sweep();

  assert.equal(cancelled, 0);
  assert.equal(repo.upserts.length, 0);
  assert.equal(outbox.enqueued.length, 0);
});

test('timeoutMs <= 0 desabilita: sweep nao consulta o repositorio', async () => {
  const repo = makeRepo([makeSession()]);
  const outbox = makeOutbox();
  const watcher = buildWatcher(repo, outbox, 0);

  assert.equal(watcher.enabled, false);
  const cancelled = await watcher.sweep();

  assert.equal(cancelled, 0);
  assert.equal(repo.listCalls.length, 0);
  assert.equal(repo.upserts.length, 0);
});

test('falha do repositorio nunca lanca e nao cancela', async () => {
  const repo = makeRepo([makeSession()]);
  repo.failList.value = true;
  const outbox = makeOutbox();
  const watcher = buildWatcher(repo, outbox);

  const cancelled = await watcher.sweep();

  assert.equal(cancelled, 0);
  assert.equal(repo.upserts.length, 0);
  assert.equal(outbox.enqueued.length, 0);
});

test('sem outbox a sessao ainda e encerrada localmente', async () => {
  const repo = makeRepo([makeSession()]);
  const watcher = buildWatcher(repo, null);

  const cancelled = await watcher.sweep();

  assert.equal(cancelled, 1);
  assert.equal(repo.upserts.length, 1);
  assert.equal(repo.upserts[0]?.state, 'STOPPED');
});

test('sweeps concorrentes nao se sobrepoem', async () => {
  const repo = makeRepo([makeSession()]);
  const outbox = makeOutbox();
  const watcher = buildWatcher(repo, outbox);

  await Promise.all([watcher.sweep(), watcher.sweep()]);

  assert.equal(repo.listCalls.length, 1);
  assert.equal(repo.upserts.length, 1);
});
