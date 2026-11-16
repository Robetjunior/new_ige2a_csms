import test from 'node:test';
import assert from 'node:assert/strict';
import {
  OutboxDispatcher,
  type EventPusher,
  type OutboxDispatcherOptions,
  type OutboxStore,
} from '../src/integration/outboxDispatcher';
import { CircuitBreaker } from '../src/integration/circuitBreaker';
import type { IntegrationEvent } from '../src/integration/types';

interface ClaimedItem {
  id: string;
  eventId: string;
  eventType: string;
  integrationSessionId: string | null;
  payload: unknown;
  attempts: number;
}

const NOW = 1_000_000;

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

function claimed(eventId: string, attempts: number, id = `o-${eventId}`): ClaimedItem {
  return { id, eventId, eventType: 'meter.sampled', integrationSessionId: 'sess-1', payload: makeEvent(eventId), attempts };
}

function fakeStore(batch: ClaimedItem[]) {
  const sent: string[] = [];
  const failed: Array<{ id: string; attempts: number; next: string; error: string; terminal: boolean }> = [];
  const store: OutboxStore = {
    async claimBatch() {
      return batch;
    },
    async markSent(id) {
      sent.push(id);
    },
    async markFailed(id, attempts, next, error, terminal) {
      failed.push({ id, attempts, next, error, terminal });
    },
  };
  return { store, sent, failed };
}

function clientOk(): EventPusher {
  return {
    async push() {
      return { ok: true, status: 200 };
    },
  };
}

function clientFail(error = 'http_500'): EventPusher {
  return {
    async push() {
      return { ok: false, status: 500, error };
    },
  };
}

function makeDispatcher(store: OutboxStore, client: EventPusher, overrides: Partial<OutboxDispatcherOptions> = {}): OutboxDispatcher {
  return new OutboxDispatcher({
    store,
    client,
    intervalMs: 1000,
    batchSize: 10,
    maxAttempts: 8,
    backoffBaseMs: 2000,
    backoffMaxMs: 300_000,
    now: () => NOW,
    ...overrides,
  });
}

test('sucesso marca o evento como enviado', async () => {
  const { store, sent, failed } = fakeStore([claimed('e1', 0)]);
  const dispatcher = makeDispatcher(store, clientOk());

  await dispatcher.drainOnce();

  assert.deepEqual(sent, ['o-e1']);
  assert.equal(failed.length, 0);
});

test('falha agenda reenvio nao-terminal com backoff exponencial', async () => {
  const { store, sent, failed } = fakeStore([claimed('e1', 0)]); // attempts 0 -> 1
  const dispatcher = makeDispatcher(store, clientFail());

  await dispatcher.drainOnce();

  assert.equal(sent.length, 0);
  assert.equal(failed.length, 1);
  assert.equal(failed[0].attempts, 1);
  assert.equal(failed[0].terminal, false);
  assert.equal(failed[0].error, 'http_500');
  // delay = min(max, base * 2^(attempts-1)) = 2000 * 2^0 = 2000
  assert.equal(failed[0].next, new Date(NOW + 2000).toISOString());
});

test('ao atingir o teto de tentativas o evento e terminal', async () => {
  const { store, failed } = fakeStore([claimed('e1', 1)]);
  const dispatcher = makeDispatcher(store, clientFail(), { maxAttempts: 2 });

  await dispatcher.drainOnce();

  assert.equal(failed.length, 1);
  assert.equal(failed[0].attempts, 2);
  assert.equal(failed[0].terminal, true);
});

test('o backoff respeita o teto maximo', async () => {
  const { store, failed } = fakeStore([claimed('e1', 4)]); // 2000 * 2^4 = 32000 > 5000
  const dispatcher = makeDispatcher(store, clientFail(), { backoffBaseMs: 2000, backoffMaxMs: 5000, maxAttempts: 10 });

  await dispatcher.drainOnce();

  assert.equal(failed[0].attempts, 5);
  assert.equal(failed[0].next, new Date(NOW + 5000).toISOString());
});

test('drainOnce nunca lanca quando o repositorio falha', async () => {
  const store: OutboxStore = {
    async claimBatch() {
      throw new Error('banco indisponivel');
    },
    async markSent() {
      /* noop */
    },
    async markFailed() {
      /* noop */
    },
  };
  const dispatcher = makeDispatcher(store, clientOk());

  await assert.doesNotReject(() => dispatcher.drainOnce());
});

test('drainOnce nao concorre consigo mesmo', async () => {
  let resolveClaim: (value: ClaimedItem[]) => void = () => undefined;
  let calls = 0;
  const store: OutboxStore = {
    async claimBatch() {
      calls += 1;
      return new Promise<ClaimedItem[]>((resolve) => {
        resolveClaim = resolve;
      });
    },
    async markSent() {
      /* noop */
    },
    async markFailed() {
      /* noop */
    },
  };
  const dispatcher = makeDispatcher(store, clientOk());

  const first = dispatcher.drainOnce();
  const second = dispatcher.drainOnce(); // deve retornar de imediato (guard `running`)

  await second;
  assert.equal(calls, 1, 'claimBatch deve ser chamado uma unica vez');

  resolveClaim([]);
  await first;
});

test('start e stop sao idempotentes', () => {
  const { store } = fakeStore([]);
  const dispatcher = makeDispatcher(store, clientOk());

  assert.doesNotThrow(() => {
    dispatcher.start();
    dispatcher.start();
    dispatcher.stop();
    dispatcher.stop();
  });
});

// --- F6: circuit breaker -----------------------------------------------------

test('circuito aberto adia o ciclo sem reivindicar o lote', async () => {
  let claims = 0;
  const store: OutboxStore = {
    async claimBatch() {
      claims += 1;
      return [];
    },
    async markSent() {
      /* noop */
    },
    async markFailed() {
      /* noop */
    },
  };
  const breaker = new CircuitBreaker({ failureThreshold: 1, openMs: 60_000 });
  breaker.onFailure(); // abre o circuito (ANTIGO indisponivel)
  const dispatcher = makeDispatcher(store, clientFail(), { breaker });

  await dispatcher.drainOnce();

  assert.equal(claims, 0, 'nada e reivindicado enquanto o circuito esta aberto');
});

test('falha consecutiva abre o circuito e interrompe o restante do lote', async () => {
  const { store, sent, failed } = fakeStore([
    claimed('e1', 0, 'o1'),
    claimed('e2', 0, 'o2'),
    claimed('e3', 0, 'o3'),
  ]);
  const breaker = new CircuitBreaker({ failureThreshold: 1, openMs: 60_000 });
  const dispatcher = makeDispatcher(store, clientFail(), { breaker });

  await dispatcher.drainOnce();

  assert.equal(sent.length, 0);
  assert.equal(failed.length, 1, 'apenas o primeiro item do lote foi tentado');
  assert.equal(breaker.getState(), 'open');
});

test('apos o cooldown o circuito sonda e volta a enviar', async () => {
  let now = 0;
  const breaker = new CircuitBreaker({ failureThreshold: 1, openMs: 1_000, now: () => now });

  const failing = fakeStore([claimed('e1', 0, 'o1')]);
  await makeDispatcher(failing.store, clientFail(), { breaker }).drainOnce();
  assert.equal(breaker.getState(), 'open');

  now += 1_000; // cooldown vencido

  const succeeding = fakeStore([claimed('e2', 0, 'o2')]);
  await makeDispatcher(succeeding.store, clientOk(), { breaker }).drainOnce();

  assert.deepEqual(succeeding.sent, ['o2']);
  assert.equal(breaker.getState(), 'closed');
});
