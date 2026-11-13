/**
 * Plano de reconexao, criterios de sucesso: reinicio do CSMS.
 *
 * O servico de persistencia e exercitado com um cliente Supabase FALSO em
 * memoria (mesma interface: schema/from/upsert/select), o que permite simular
 * o ciclo real "grava -> encerra -> reinicia -> le a frota" sem rede. O que
 * NAO e exercitado aqui: a PostgREST/Supabase real e os gatilhos SQL
 * (db/migrations/002 e 003).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import { WebSocket } from 'ws';
import { createApp, type CsmsApp } from '../src/app';
import { OcppSimulator } from '../scripts/ocpp-simulator';
import type { AppConfig } from '../src/config/env';
import type { AnySupabaseClient } from '../src/persistence/supabaseBatchWriter';
import { makeTestConfig, waitFor, delay } from './helpers';

interface FakeStore {
  client: AnySupabaseClient;
  rows(table: string): Record<string, unknown>[];
}

/** Cliente Supabase falso, em memoria, com a mesma interface usada pelo CSMS. */
function createFakeStore(): FakeStore {
  const tables = new Map<string, Map<string, Record<string, unknown>>>();
  const tableOf = (name: string): Map<string, Record<string, unknown>> => {
    let table = tables.get(name);
    if (!table) {
      table = new Map();
      tables.set(name, table);
    }
    return table;
  };
  const client = {
    schema: () => ({
      from: (name: string) => ({
        async upsert(rows: Record<string, unknown>[]) {
          const table = tableOf(name);
          for (const row of rows) table.set(String(row.charge_point_id ?? row.id ?? ''), row);
          return { error: null };
        },
        async select() {
          return { data: Array.from(tableOf(name).values()), error: null };
        },
      }),
    }),
  };
  return { client: client as unknown as AnySupabaseClient, rows: (table) => Array.from(tableOf(table).values()) };
}

/** Config com persistencia ativa (cliente falso), frota habilitada e janela curta. */
function restartConfig(): AppConfig {
  const base = makeTestConfig();
  return Object.freeze({
    ...base,
    ocpp: Object.freeze({
      ...base.ocpp,
      fleetEnabled: true,
      // O BootNotification devolve este intervalo; mantem a janela de
      // tolerancia da frota em ~1s (teste deterministico).
      defaultHeartbeatIntervalSeconds: 1,
      reconnectionGraceMultiplier: 1,
      reconnectionToleranceSeconds: 0,
      reconnectionCheckIntervalMs: 500,
    }),
    persistence: Object.freeze({
      ...base.persistence,
      supabaseUrl: 'http://127.0.0.1:54321',
      supabaseServiceKey: 'service-role-key-de-teste',
      logPersistEnabled: true,
      // Escreve assim que entra na fila: o teste observa o "banco" sem timers.
      logBatchSize: 1,
      logFlushIntervalMs: 20,
      logMaxQueue: 1000,
    }),
  });
}

async function startApp(app: CsmsApp): Promise<string> {
  await app.start();
  const { port } = app.fastify.server.address() as AddressInfo;
  return `ws://127.0.0.1:${port}`;
}

function findFleetRow(store: FakeStore, chargePointId: string): Record<string, unknown> | undefined {
  return store.rows('charger_fleet').find((row) => row.charge_point_id === chargePointId);
}

test('reinicio: reconhece o carregador conhecido, preserva reconnectCount e o estado operacional', async (t) => {
  // "Banco" compartilhado entre as duas execucoes do CSMS.
  const store = createFakeStore();
  const deps = { persistence: { createClient: () => store.client } };

  // ---------------- Sessao 1 ----------------
  const app1 = await createApp(restartConfig(), deps);
  const baseUrl1 = await startApp(app1);

  const sim = new OcppSimulator({
    baseUrl: baseUrl1,
    chargePointId: 'CP-KNOWN',
    heartbeatIntervalMs: 200,
    respectServerInterval: false,
    autoReconnect: false,
  });
  t.after(() => sim.stop());

  // Conexao inicial (reconnectCount=0) + 2 reconexoes reais do mesmo carregador.
  for (let ciclo = 0; ciclo < 3; ciclo += 1) {
    await sim.connect();
    await waitFor(() => app1.components.wsServer.isConnected('CP-KNOWN'), 4000, 20, `socket no ciclo ${ciclo}`);
    if (ciclo < 2) {
      sim.stop();
      await waitFor(() => !app1.components.wsServer.isConnected('CP-KNOWN'), 4000, 20, `queda no ciclo ${ciclo}`);
    }
  }
  await waitFor(
    () => app1.components.connections.get('CP-KNOWN')?.reconnectCount === 2,
    4000,
    20,
    'contador de reconexoes = 2',
  );

  // O "banco" passa a refletir o runtime (identidade + estado operacional).
  await waitFor(
    () => {
      const row = findFleetRow(store, 'CP-KNOWN');
      const connectors = row?.connectors;
      return Boolean(row && row.reconnect_count === 2 && Array.isArray(connectors) && connectors.length > 0);
    },
    8000,
    25,
    'linha da frota com reconnectCount=2 e conectores',
  );

  await app1.stop();

  const persisted = findFleetRow(store, 'CP-KNOWN');
  assert.ok(persisted, 'a frota foi persistida');
  assert.equal(persisted?.reconnect_count, 2, 'historico de reconexoes gravado');
  assert.equal(persisted?.vendor, 'IGE2A', 'identidade gravada');
  assert.equal((persisted?.connectors as unknown[])?.length, 1, 'estado operacional gravado');

  // ---------------- Sessao 2 (reinicio programado) ----------------
  const app2 = await createApp(restartConfig(), deps);
  t.after(async () => app2.stop());

  // A frota conhecida foi lida do "banco" ANTES de aceitar conexoes.
  const seededFleet = app2.components.fleet.get('CP-KNOWN');
  assert.equal(seededFleet?.reconnectCount, 2, 'reconnectCount sobreviveu ao reinicio');
  assert.equal(seededFleet?.connectors.length, 1, 'estado operacional sobreviveu ao reinicio');
  assert.equal(app2.components.connections.get('CP-KNOWN')?.reconnectCount, 2, 'runtime semeado com o historico');
  assert.equal(app2.components.connections.get('CP-KNOWN')?.transport, 'DISCONNECTED', 'semeado como desconectado');
  assert.equal(app2.components.chargers.get('CP-KNOWN')?.connectors.size, 1, 'estado do conector reidratado');

  const baseUrl2 = await startApp(app2);

  // Carregador conhecido que NAO retornou dentro da janela de tolerancia.
  await waitFor(
    () => app2.components.reconnectionWatcher.getStatus('CP-KNOWN').status === 'AWAITING_RECONNECTION',
    5000,
    25,
    'conhecido que nao retornou',
  );

  // Aceitacao automatica: o ID enviado no pacote inicial e reconhecido.
  const sim2 = new OcppSimulator({
    baseUrl: baseUrl2,
    chargePointId: 'CP-KNOWN',
    heartbeatIntervalMs: 200,
    respectServerInterval: false,
    autoReconnect: false,
  });
  t.after(() => sim2.stop());
  await sim2.connect();

  await waitFor(
    () => app2.components.reconnectionWatcher.getStatus('CP-KNOWN').status === 'CONNECTED',
    5000,
    25,
    'reconhecido como conectado',
  );
  assert.equal(
    app2.components.connections.get('CP-KNOWN')?.reconnectCount,
    3,
    'a reconexao apos o reinicio continuou o historico (2 -> 3)',
  );
});

test('consistencia: desconexao abrupta e multiplas reconexoes nao perdem o contador', async (t) => {
  const store = createFakeStore();
  const app = await createApp(restartConfig(), { persistence: { createClient: () => store.client } });
  const baseUrl = await startApp(app);
  t.after(async () => app.stop());

  const url = `${baseUrl}/ocpp/CentralSystemService/CP-ABRUPT`;
  const sockets: WebSocket[] = [];

  for (let ciclo = 0; ciclo < 5; ciclo += 1) {
    const ws = await new Promise<WebSocket>((resolve, reject) => {
      const socket = new WebSocket(url, 'ocpp1.6');
      socket.once('open', () => resolve(socket));
      socket.once('error', reject);
    });
    sockets.push(ws);
    await waitFor(() => app.components.wsServer.isConnected('CP-ABRUPT'), 4000, 20, `socket ${ciclo}`);
    // Desconexao abrupta (sem close frame) em todos os ciclos menos o ultimo.
    if (ciclo < 4) {
      ws.terminate();
      await waitFor(() => !app.components.wsServer.isConnected('CP-ABRUPT'), 4000, 20, `queda abrupta ${ciclo}`);
    }
  }

  assert.equal(app.components.connections.get('CP-ABRUPT')?.reconnectCount, 4, 'uma reconexao por retorno');

  await waitFor(
    () => findFleetRow(store, 'CP-ABRUPT')?.reconnect_count === 4,
    8000,
    25,
    'contador persistido apos multiplas reconexoes',
  );
  assert.equal(
    store.rows('charger_fleet').filter((row) => row.charge_point_id === 'CP-ABRUPT').length,
    1,
    'um unico registro por chargePointId',
  );

  for (const socket of sockets) socket.terminate();
  await delay(20);
});
