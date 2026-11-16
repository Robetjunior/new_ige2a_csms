import test from 'node:test';
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { IntegrationPushClient, signBody, type PushFetchLike } from '../src/integration/pushClient';
import type { IntegrationEvent } from '../src/integration/types';

function makeEvent(overrides: Partial<IntegrationEvent> = {}): IntegrationEvent {
  return {
    eventId: 'evt-1',
    eventType: 'session.started',
    integrationSessionId: 'sess-1',
    ocppTransactionId: 42,
    chargePointId: 'CP-1',
    connectorId: 1,
    occurredAt: '2026-01-01T00:00:00.000Z',
    data: { meterStart: 0 },
    ...overrides,
  };
}

function makeClient(fetchImpl: PushFetchLike, targetUrl = 'http://127.0.0.1:3000/', secret = 'segredo'): IntegrationPushClient {
  return new IntegrationPushClient({ targetUrl, hmacSecret: secret, timeoutMs: 1000, fetchImpl });
}

test('signBody produz o HMAC-SHA256 hex do corpo exato', () => {
  const body = '{"a":1,"b":"x"}';
  const expected = createHmac('sha256', 'segredo').update(body, 'utf8').digest('hex');
  assert.equal(signBody('segredo', body), expected);
  assert.notEqual(signBody('segredo', body), signBody('outro', body));
});

test('push envia POST assinado com headers e corpo exatos', async () => {
  let captured: { url: string; init: { method: string; headers: Record<string, string>; body: string } } | null = null;
  const fetchImpl: PushFetchLike = async (url, init) => {
    captured = { url, init };
    return { ok: true, status: 202 };
  };
  const client = makeClient(fetchImpl);
  const event = makeEvent();

  const result = await client.push(event);

  assert.deepEqual(result, { ok: true, status: 202 });
  assert.ok(captured, 'fetch deve ter sido chamado');
  const sent = captured as unknown as { url: string; init: { method: string; headers: Record<string, string>; body: string } };
  // `targetUrl` com barra final nao duplica a barra (normalizacao do cliente).
  assert.equal(sent.url, 'http://127.0.0.1:3000/v1/integration/ocpp/events');
  assert.equal(sent.init.method, 'POST');
  assert.equal(sent.init.headers['content-type'], 'application/json');
  assert.equal(sent.init.headers['x-request-id'], 'evt-1');
  assert.equal(sent.init.headers['x-signature'], signBody('segredo', sent.init.body));
  assert.deepEqual(JSON.parse(sent.init.body), event);
});

test('push sinaliza falha HTTP sem lancar', async () => {
  const client = makeClient(async () => ({ ok: false, status: 500 }));

  const result = await client.push(makeEvent());

  assert.equal(result.ok, false);
  assert.equal(result.status, 500);
  assert.equal(result.error, 'http_500');
});

test('push converte erro de rede em resultado observavel', async () => {
  const client = makeClient(async () => {
    throw new Error('connect ECONNREFUSED 127.0.0.1:3000');
  });

  const result = await client.push(makeEvent());

  assert.equal(result.ok, false);
  assert.match(result.error ?? '', /ECONNREFUSED/);
});

test('push aborta no timeout e devolve erro observavel', async () => {
  const fetchImpl: PushFetchLike = (_url, init) =>
    new Promise((_resolve, reject) => {
      init.signal?.addEventListener('abort', () => reject(new Error('The operation was aborted')));
    });
  const client = new IntegrationPushClient({
    targetUrl: 'http://127.0.0.1:3000',
    hmacSecret: 'segredo',
    timeoutMs: 10,
    fetchImpl,
  });

  const result = await client.push(makeEvent());

  assert.equal(result.ok, false);
  assert.match(result.error ?? '', /aborted/i);
});
