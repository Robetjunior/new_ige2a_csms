import test from 'node:test';
import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import { WebSocket } from 'ws';
import { createApp, type CsmsApp } from '../src/app';
import { OcppSimulator } from '../scripts/ocpp-simulator';
import { makeTestConfig, waitFor, delay } from './helpers';

async function startApp(app: CsmsApp): Promise<{ baseUrl: string; httpUrl: string }> {
  await app.start();
  const { port } = app.fastify.server.address() as AddressInfo;
  return { baseUrl: `ws://127.0.0.1:${port}`, httpUrl: `http://127.0.0.1:${port}` };
}

function openClient(url: string, protocol?: string | null): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const ws = protocol ? new WebSocket(url, protocol) : new WebSocket(url);
    ws.once('open', () => resolve(ws));
    ws.once('error', reject);
  });
}

test('integracao: boot, heartbeat e status sao registrados, correlacionados e exportaveis', async (t) => {
  const app = await createApp(makeTestConfig());
  const { baseUrl, httpUrl } = await startApp(app);
  const sim = new OcppSimulator({
    baseUrl,
    chargePointId: 'TEISON-01',
    heartbeatIntervalMs: 120,
    respectServerInterval: false,
  });
  t.after(async () => {
    sim.stop();
    await app.stop();
  });

  await sim.connect();
  await waitFor(
    () => app.components.connections.get('TEISON-01')?.registration === 'ACCEPTED',
    4000,
    20,
    'BootNotification aceito',
  );
  await waitFor(
    () => app.components.heartbeat.getStatus('TEISON-01').heartbeatCount >= 2,
    4000,
    20,
    'Heartbeats recebidos',
  );

  sim.sendStatusNotification(1, 'Charging');
  await waitFor(
    () => app.components.chargers.get('TEISON-01')?.connectors.get(1)?.status === 'Charging',
    4000,
    20,
    'StatusNotification aplicado',
  );

  const messages = app.components.messageLog.recent({ chargePointId: 'TEISON-01', limit: 500 });
  const actions = messages.map((m) => m.action);
  assert.ok(actions.includes('BootNotification'), 'deve registrar BootNotification');
  assert.ok(actions.includes('Heartbeat'), 'deve registrar Heartbeat');
  assert.ok(actions.includes('StatusNotification'), 'deve registrar StatusNotification');

  const boot = messages.find((m) => m.action === 'BootNotification' && m.direction === 'received');
  assert.ok(boot, 'BootNotification de entrada registrado');
  assert.equal(boot?.status, 'succeeded');
  assert.equal(typeof boot?.latencyMs, 'number');
  assert.ok((boot?.latencyMs ?? -1) >= 0);

  // Intervalo negociado = valor informado pelo CSMS no BootNotification.
  assert.equal(app.components.heartbeat.getNegotiatedInterval('TEISON-01'), 60);
  assert.equal(app.components.heartbeat.getStatus('TEISON-01').classification, 'NORMAL');

  // Nenhuma mensagem foi descartada: todas as CALLs de entrada tem resposta correlacionada.
  const drain = messages.filter((m) => m.direction === 'received' && m.messageType === 'CALL');
  for (const call of drain) {
    assert.notEqual(call.status, 'received', `CALL ${call.action} ficou sem resposta correlacionada`);
  }

  // API HTTP: overview e export JSON.
  const overview = (await (await fetch(`${httpUrl}/api/overview`)).json()) as { chargers: unknown[] };
  assert.equal(overview.chargers.length, 1);
  const exported = await fetch(`${httpUrl}/api/messages/export?format=json&chargePointId=TEISON-01`);
  assert.equal(exported.status, 200);
  const body = (await exported.json()) as { count: number };
  assert.ok(body.count > 0);

  const diagnosis = (await (await fetch(`${httpUrl}/api/chargers/TEISON-01/diagnosis`)).json()) as {
    severity: string;
    findings: Array<{ code: string }>;
  };
  assert.ok(['OK', 'ATENCAO', 'CRITICO'].includes(diagnosis.severity));
  assert.ok(diagnosis.findings.some((f) => f.code === 'TRANSPORT_CONNECTED'));
});

test('simulador reconecta apos queda inesperada e retoma status/heartbeat', async (t) => {
  const app = await createApp(makeTestConfig());
  const { baseUrl } = await startApp(app);
  const sim = new OcppSimulator({
    baseUrl,
    chargePointId: 'CP-RECONNECT',
    heartbeatIntervalMs: 80,
    respectServerInterval: false,
    autoReconnect: true,
    reconnectDelayMs: 40,
    maxReconnectDelayMs: 120,
  });
  t.after(async () => {
    sim.stop();
    await app.stop();
  });

  await sim.connect();
  await waitFor(
    () =>
      app.components.connections.get('CP-RECONNECT')?.registration === 'ACCEPTED' &&
      app.components.chargers.get('CP-RECONNECT')?.connectors.get(1)?.status === 'Available',
    3000,
    20,
    'BootNotification e StatusNotification iniciais',
  );
  await waitFor(() => app.components.heartbeat.getStatus('CP-RECONNECT').heartbeatCount >= 1, 3000, 20, 'heartbeat inicial');

  const firstConnectionId = app.components.connections.get('CP-RECONNECT')?.connectionId;
  app.components.wsServer.getSession('CP-RECONNECT')?.close(1012, 'temporary test disconnect');

  await waitFor(
    () =>
      app.components.connections.get('CP-RECONNECT')?.registration === 'ACCEPTED' &&
      app.components.connections.get('CP-RECONNECT')?.connectionId !== firstConnectionId,
    4000,
    20,
    'reconexao e novo BootNotification aceito',
  );
  await waitFor(
    () =>
      app.components.messageLog
        .recent({ chargePointId: 'CP-RECONNECT', action: 'StatusNotification' })
        .filter((m) => m.direction === 'received').length >= 2,
    3000,
    20,
    'StatusNotification reenviado apos reconexao',
  );
  await waitFor(
    () => app.components.heartbeat.getStatus('CP-RECONNECT').heartbeatCount >= 2,
    3000,
    20,
    'heartbeat retomado apos reconexao',
  );

  assert.equal(sim.reconnectCount, 1);
  assert.equal(app.components.connections.get('CP-RECONNECT')?.transport, 'CONNECTED');
  assert.equal(app.components.chargers.get('CP-RECONNECT')?.connectors.get(1)?.status, 'Available');
  assert.equal(app.components.messageLog.recent({ chargePointId: 'CP-RECONNECT', onlyErrors: true }).length, 0);
  assert.equal(app.components.messageLog.stats().pendingCorrelations, 0);
});

test('handshake: ecoa exatamente o subprotocolo oferecido (ocpp1.6 e variante socpp1.6)', async (t) => {
  const app = await createApp(makeTestConfig());
  const { baseUrl } = await startApp(app);
  t.after(async () => app.stop());

  const plain = await openClient(`${baseUrl}/ocpp/CentralSystemService/CP-A`, 'ocpp1.6');
  assert.equal(plain.protocol, 'ocpp1.6');
  plain.close();

  const teison = await openClient(`${baseUrl}/ocpp/CentralSystemService/CP-B`, 'socpp1.6');
  assert.equal(teison.protocol, 'socpp1.6');
  teison.close();
});

test('handshake: fallback leniente aceita conexao sem subprotocolo (sem eco)', async (t) => {
  const app = await createApp(makeTestConfig());
  const { baseUrl } = await startApp(app);
  t.after(async () => app.stop());

  const ws = await openClient(`${baseUrl}/ocpp/CentralSystemService?charge_box_id=CP-Q`);
  assert.equal(ws.protocol, '');
  await waitFor(() => app.components.connections.get('CP-Q') !== undefined, 2000, 20, 'conexao registrada por query');
  ws.close();
});

test('handshake: rejeita quando o fallback leniente esta desabilitado', async (t) => {
  const base = makeTestConfig();
  const app = await createApp(
    Object.freeze({ ...base, ocpp: Object.freeze({ ...base.ocpp, lenientProtocolFallback: false }) }),
  );
  const { baseUrl } = await startApp(app);
  t.after(async () => app.stop());

  await assert.rejects(
    () => openClient(`${baseUrl}/ocpp/CentralSystemService/CP-R`, 'ocpp9.9'),
    /unexpected server response|400/i,
  );
});

test('supersede: nova conexao do mesmo carregador nao e derrubada pela antiga', async (t) => {
  const app = await createApp(makeTestConfig());
  const { baseUrl } = await startApp(app);
  const makeSim = () =>
    new OcppSimulator({
      baseUrl,
      chargePointId: 'CP-DUP',
      heartbeatIntervalMs: 500,
      respectServerInterval: false,
      autoReconnect: false,
    });
  const sim1 = makeSim();
  const sim2 = makeSim();
  t.after(async () => {
    sim1.stop();
    sim2.stop();
    await app.stop();
  });

  await sim1.connect();
  await waitFor(() => app.components.wsServer.isConnected('CP-DUP'), 3000, 20, 'primeira conexao');

  const firstClosed = sim1.waitClose(3000);
  await sim2.connect();
  assert.equal(await firstClosed, true, 'o socket antigo deve ser encerrado');

  await delay(150);
  const record = app.components.connections.get('CP-DUP');
  assert.equal(record?.transport, 'CONNECTED', 'o socket novo permanece conectado');
  assert.equal(app.components.connections.get('CP-DUP')?.disconnectReason, null);

  const connected = app.components.wsServer.connectedChargePointIds().filter((id) => id === 'CP-DUP');
  assert.equal(connected.length, 1);
  assert.equal(app.components.heartbeat.getStatus('CP-DUP').connectionStatus, 'ONLINE');
});

test('robustez: frame invalido e acao desconhecida sao registrados sem derrubar o socket', async (t) => {
  const app = await createApp(makeTestConfig());
  const { baseUrl } = await startApp(app);
  const sim = new OcppSimulator({
    baseUrl,
    chargePointId: 'CP-ERR',
    heartbeatIntervalMs: 500,
    respectServerInterval: false,
  });
  t.after(async () => {
    sim.stop();
    await app.stop();
  });

  await sim.connect();
  await waitFor(() => app.components.wsServer.isConnected('CP-ERR'), 3000, 20, 'conexao');

  // Frame bruto invalido (nao e JSON): deve ser registrado com parseError.
  sim.sendRaw('<<lixo-nao-json>>');
  await waitFor(
    () => app.components.messageLog.recent({ chargePointId: 'CP-ERR', onlyErrors: true }).length >= 1,
    3000,
    20,
    'frame invalido registrado',
  );

  // Acao OCPP desconhecida: deve gerar CALLERROR NotImplemented correlacionado.
  sim.sendRaw(JSON.stringify([2, 'u-42', 'AcaoQueNaoExiste', {}]));
  await waitFor(
    () =>
      app.components.messageLog
        .recent({ chargePointId: 'CP-ERR' })
        .some((m) => m.direction === 'sent' && m.messageType === 'CALLERROR' && m.uniqueId === 'u-42'),
    3000,
    20,
    'CALLERROR NotImplemented',
  );

  const request = app.components.messageLog
    .recent({ chargePointId: 'CP-ERR' })
    .find((m) => m.uniqueId === 'u-42' && m.direction === 'received');
  assert.equal(request?.status, 'failed');
  assert.equal(request?.errorCode, 'NotImplemented');

  // O socket continua vivo.
  assert.equal(app.components.wsServer.isConnected('CP-ERR'), true);
});
