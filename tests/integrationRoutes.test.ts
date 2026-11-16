/**
 * Testes das rotas de integracao (F2). Nao dependem de rede nem de Supabase:
 * usam uma instancia Fastify limpa com repositorio e `wsServer` FALSOS.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { registerIntegrationRoutes } from '../src/integration/routes';
import type { IntegrationInboxRepository, IntegrationSessionUpsert } from '../src/integration/inboxRepository';
import type { InboxClaimInput, InboxClaimResult, IntegrationSessionRow } from '../src/integration/types';
import { makeTestConfig } from './helpers';

const INTEGRATION_KEY = 'integration-secret';

interface FakeRepo {
  claimCalls: InboxClaimInput[];
  completeCalls: Array<{ idempotencyKey: string; response: unknown; state?: 'COMPLETED' | 'FAILED' }>;
  upsertCalls: IntegrationSessionUpsert[];
  claimResult: InboxClaimResult;
  session: IntegrationSessionRow | null;
  claim(input: InboxClaimInput): Promise<InboxClaimResult>;
  complete(idempotencyKey: string, response: unknown, state?: 'COMPLETED' | 'FAILED'): Promise<void>;
  upsertSession(row: IntegrationSessionUpsert): Promise<void>;
  getSession(id: string): Promise<IntegrationSessionRow | null>;
}

function makeRepo(overrides: { claimResult?: InboxClaimResult; session?: IntegrationSessionRow | null } = {}): FakeRepo {
  const claimCalls: InboxClaimInput[] = [];
  const completeCalls: FakeRepo['completeCalls'] = [];
  const upsertCalls: IntegrationSessionUpsert[] = [];
  return {
    claimCalls,
    completeCalls,
    upsertCalls,
    claimResult: overrides.claimResult ?? { outcome: 'owner' },
    session: overrides.session ?? null,
    async claim(input) {
      claimCalls.push(input);
      return this.claimResult;
    },
    async complete(idempotencyKey, response, state) {
      completeCalls.push({ idempotencyKey, response, state });
    },
    async upsertSession(row) {
      upsertCalls.push(row);
    },
    async getSession() {
      return this.session;
    },
  };
}

interface FakeWs {
  connected: boolean;
  fail: boolean;
  calls: Array<{ chargePointId: string; action: string; payload: unknown; timeoutMs?: number }>;
  isConnected(chargePointId: string): boolean;
  sendCall(chargePointId: string, action: string, payload: unknown, timeoutMs?: number): Promise<unknown>;
}

function makeWs(overrides: { connected?: boolean; fail?: boolean } = {}): FakeWs {
  const calls: FakeWs['calls'] = [];
  return {
    connected: overrides.connected ?? true,
    fail: overrides.fail ?? false,
    calls,
    isConnected() {
      return this.connected;
    },
    async sendCall(chargePointId, action, payload, timeoutMs) {
      calls.push({ chargePointId, action, payload, timeoutMs });
      if (this.fail) throw new Error('comando recusado pelo carregador');
      return { status: 'Accepted' };
    },
  };
}

async function buildApp(options: {
  integrationKey?: string;
  repo?: FakeRepo | null;
  ws?: FakeWs;
}): Promise<{ app: ReturnType<typeof Fastify>; repo: FakeRepo | null; ws: FakeWs }> {
  const app = Fastify();
  const repo = options.repo === undefined ? makeRepo() : options.repo;
  const ws = options.ws ?? makeWs();
  await registerIntegrationRoutes(app, {
    config: makeTestConfig({ integrationKey: options.integrationKey ?? INTEGRATION_KEY }),
    repository: repo as unknown as IntegrationInboxRepository | null,
    wsServer: ws,
  });
  return { app, repo, ws };
}

const startPayload = {
  integrationSessionId: 'S1',
  idempotencyKey: 'K1',
  chargePointId: 'CP-1',
  connectorId: 1,
  idTag: 'TAG-1',
};

const authHeaders = { 'x-integration-key': INTEGRATION_KEY };

test('start sem chave retorna 401', async () => {
  const { app } = await buildApp({ ws: makeWs() });
  const res = await app.inject({
    method: 'POST',
    url: '/integration/v1/sessions/start',
    payload: startPayload,
  });
  assert.equal(res.statusCode, 401);
  assert.deepEqual(res.json(), { error: 'unauthorized' });
  await app.close();
});

test('start com chave errada retorna 401', async () => {
  const { app } = await buildApp({ ws: makeWs() });
  const res = await app.inject({
    method: 'POST',
    url: '/integration/v1/sessions/start',
    headers: { 'x-integration-key': 'chave-errada' },
    payload: startPayload,
  });
  assert.equal(res.statusCode, 401);
  assert.deepEqual(res.json(), { error: 'unauthorized' });
  await app.close();
});

test('start owner: 202, envia RemoteStartTransaction e conclui COMPLETED', async () => {
  const repo = makeRepo({ claimResult: { outcome: 'owner' } });
  const ws = makeWs({ connected: true });
  const { app } = await buildApp({ repo, ws });

  const res = await app.inject({
    method: 'POST',
    url: '/integration/v1/sessions/start',
    headers: authHeaders,
    payload: startPayload,
  });

  assert.equal(res.statusCode, 202);
  assert.deepEqual(res.json(), { ok: true, integrationSessionId: 'S1', status: 'accepted' });
  assert.equal(ws.calls.length, 1);
  assert.equal(ws.calls[0]?.chargePointId, 'CP-1');
  assert.equal(ws.calls[0]?.action, 'RemoteStartTransaction');
  assert.deepEqual(ws.calls[0]?.payload, { connectorId: 1, idTag: 'TAG-1' });
  assert.equal(ws.calls[0]?.timeoutMs, 30_000);
  // Sessao criada como PENDING_ACK antes do comando.
  assert.equal(repo.upsertCalls.length, 1);
  assert.equal(repo.upsertCalls[0]?.state, 'PENDING_ACK');
  // Inbox concluido com a MESMA resposta devolvida ao cliente.
  assert.equal(repo.completeCalls.length, 1);
  assert.equal(repo.completeCalls[0]?.idempotencyKey, 'K1');
  assert.equal(repo.completeCalls[0]?.state, 'COMPLETED');
  assert.deepEqual(repo.completeCalls[0]?.response, { ok: true, integrationSessionId: 'S1', status: 'accepted' });
  await app.close();
});

test('start retry (duplicate): 200 com a MESMA resposta e sem reenviar comando', async () => {
  const previous = { ok: true, integrationSessionId: 'S1', status: 'accepted' };
  const repo = makeRepo({ claimResult: { outcome: 'duplicate', response: previous } });
  const ws = makeWs({ connected: true });
  const { app } = await buildApp({ repo, ws });

  const res = await app.inject({
    method: 'POST',
    url: '/integration/v1/sessions/start',
    headers: authHeaders,
    payload: startPayload,
  });

  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.json(), previous);
  assert.equal(ws.calls.length, 0);
  assert.equal(repo.completeCalls.length, 0);
  await app.close();
});

test('start in_progress retorna 409 duplicate_in_progress', async () => {
  const repo = makeRepo({ claimResult: { outcome: 'in_progress' } });
  const ws = makeWs();
  const { app } = await buildApp({ repo, ws });

  const res = await app.inject({
    method: 'POST',
    url: '/integration/v1/sessions/start',
    headers: authHeaders,
    payload: startPayload,
  });

  assert.equal(res.statusCode, 409);
  assert.deepEqual(res.json(), { error: 'duplicate_in_progress' });
  assert.equal(ws.calls.length, 0);
  await app.close();
});

test('start hash_mismatch retorna 409', async () => {
  const repo = makeRepo({
    claimResult: { outcome: 'hash_mismatch', expectedHash: 'a', receivedHash: 'b' },
  });
  const ws = makeWs();
  const { app } = await buildApp({ repo, ws });

  const res = await app.inject({
    method: 'POST',
    url: '/integration/v1/sessions/start',
    headers: authHeaders,
    payload: startPayload,
  });

  assert.equal(res.statusCode, 409);
  assert.deepEqual(res.json(), { error: 'hash_mismatch' });
  assert.equal(ws.calls.length, 0);
  await app.close();
});

test('start com carregador desconectado retorna 409 not_connected sem enviar comando', async () => {
  const repo = makeRepo({ claimResult: { outcome: 'owner' } });
  const ws = makeWs({ connected: false });
  const { app } = await buildApp({ repo, ws });

  const res = await app.inject({
    method: 'POST',
    url: '/integration/v1/sessions/start',
    headers: authHeaders,
    payload: startPayload,
  });

  assert.equal(res.statusCode, 409);
  assert.deepEqual(res.json(), { error: 'not_connected', chargePointId: 'CP-1' });
  assert.equal(ws.calls.length, 0);
  assert.equal(repo.upsertCalls.length, 0);
  assert.equal(repo.completeCalls.length, 1);
  assert.equal(repo.completeCalls[0]?.state, 'FAILED');
  assert.deepEqual(repo.completeCalls[0]?.response, { ok: false, reason: 'not_connected' });
  await app.close();
});

test('start com falha do sendCall retorna 502 command_failed e conclui FAILED', async () => {
  const repo = makeRepo({ claimResult: { outcome: 'owner' } });
  const ws = makeWs({ connected: true, fail: true });
  const { app } = await buildApp({ repo, ws });

  const res = await app.inject({
    method: 'POST',
    url: '/integration/v1/sessions/start',
    headers: authHeaders,
    payload: startPayload,
  });

  assert.equal(res.statusCode, 502);
  // F6: o corpo HTTP nao expoe o detalhe de infraestrutura.
  const body = res.json() as { error: string; message?: string };
  assert.equal(body.error, 'command_failed');
  assert.equal(body.message, undefined);
  assert.equal(ws.calls.length, 1);
  assert.equal(repo.completeCalls.length, 1);
  assert.equal(repo.completeCalls[0]?.state, 'FAILED');
  assert.deepEqual(repo.completeCalls[0]?.response, {
    ok: false,
    integrationSessionId: 'S1',
    reason: 'command_failed',
  });
  await app.close();
});

test('start com corpo invalido retorna 400 invalid_body', async () => {
  const { app } = await buildApp({ ws: makeWs() });
  const res = await app.inject({
    method: 'POST',
    url: '/integration/v1/sessions/start',
    headers: authHeaders,
    payload: { integrationSessionId: 'S1' },
  });
  assert.equal(res.statusCode, 400);
  assert.equal((res.json() as { error: string }).error, 'invalid_body');
  await app.close();
});

test('start sem integrationKey configurada retorna 503 integration_disabled', async () => {
  const { app } = await buildApp({ integrationKey: '', ws: makeWs() });
  const res = await app.inject({
    method: 'POST',
    url: '/integration/v1/sessions/start',
    headers: authHeaders,
    payload: startPayload,
  });
  assert.equal(res.statusCode, 503);
  assert.deepEqual(res.json(), { error: 'integration_disabled' });
  await app.close();
});

test('stop com sessao inexistente retorna 404 not_found', async () => {
  const repo = makeRepo({ session: null });
  const ws = makeWs();
  const { app } = await buildApp({ repo, ws });

  const res = await app.inject({
    method: 'POST',
    url: '/integration/v1/sessions/stop',
    headers: authHeaders,
    payload: { integrationSessionId: 'S1' },
  });

  assert.equal(res.statusCode, 404);
  assert.deepEqual(res.json(), { error: 'not_found', integrationSessionId: 'S1' });
  assert.equal(ws.calls.length, 0);
  await app.close();
});

test('stop com sessao sem transacao OCPP retorna 409 not_started', async () => {
  const repo = makeRepo({
    session: {
      integration_session_id: 'S1',
      idempotency_key: 'K1',
      charge_point_id: 'CP-1',
      connector_id: 1,
      ocpp_transaction_id: null,
      state: 'PENDING_ACK',
      id_tag: 'TAG-1',
      started_at: null,
      stopped_at: null,
    },
  });
  const ws = makeWs();
  const { app } = await buildApp({ repo, ws });

  const res = await app.inject({
    method: 'POST',
    url: '/integration/v1/sessions/stop',
    headers: authHeaders,
    payload: { integrationSessionId: 'S1' },
  });

  assert.equal(res.statusCode, 409);
  assert.deepEqual(res.json(), { error: 'not_started' });
  assert.equal(ws.calls.length, 0);
  await app.close();
});

test('stop com transacao presente: 202 e envia RemoteStopTransaction com transactionId', async () => {
  const repo = makeRepo({
    session: {
      integration_session_id: 'S1',
      idempotency_key: 'K1',
      charge_point_id: 'CP-1',
      connector_id: 1,
      ocpp_transaction_id: 555,
      state: 'STARTED',
      id_tag: 'TAG-1',
      started_at: '2026-11-17T00:00:00.000Z',
      stopped_at: null,
    },
  });
  const ws = makeWs();
  const { app } = await buildApp({ repo, ws });

  const res = await app.inject({
    method: 'POST',
    url: '/integration/v1/sessions/stop',
    headers: authHeaders,
    payload: { integrationSessionId: 'S1' },
  });

  assert.equal(res.statusCode, 202);
  assert.deepEqual(res.json(), { ok: true, integrationSessionId: 'S1', status: 'stopping' });
  assert.equal(repo.upsertCalls.length, 1);
  assert.equal(repo.upsertCalls[0]?.state, 'STOPPING');
  assert.equal(repo.upsertCalls[0]?.ocppTransactionId, 555);
  assert.equal(ws.calls.length, 1);
  assert.equal(ws.calls[0]?.chargePointId, 'CP-1');
  assert.equal(ws.calls[0]?.action, 'RemoteStopTransaction');
  assert.deepEqual(ws.calls[0]?.payload, { transactionId: 555 });
  await app.close();
});
