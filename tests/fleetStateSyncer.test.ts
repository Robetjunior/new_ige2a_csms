import test from 'node:test';
import assert from 'node:assert/strict';
import { ChargerStateService } from '../src/chargers/chargerState';
import { ConnectionManager } from '../src/transport/connectionManager';
import { FleetRegistry } from '../src/fleet/fleetRegistry';
import { FleetStateSyncer } from '../src/fleet/fleetStateSyncer';
import { HeartbeatMonitor } from '../src/heartbeat/heartbeatMonitor';

function harness() {
  const fleet = new FleetRegistry();
  const connections = new ConnectionManager();
  const chargers = new ChargerStateService();
  const heartbeat = new HeartbeatMonitor({ defaultIntervalSeconds: 60, toleranceSeconds: 15, criticalSeconds: 120 });
  const syncer = new FleetStateSyncer({ fleet, connections, chargers, heartbeat });
  syncer.start();
  return { fleet, connections, chargers, heartbeat, syncer };
}

test('as reconexoes reais chegam ao FleetRegistry (historico de reconnectCount)', () => {
  const h = harness();

  h.connections.onAttempt('CP-1', { protocol: 'ocpp1.6' });
  h.connections.onConnected('CP-1', { protocol: 'ocpp1.6' });
  h.connections.onBootAccepted('CP-1', { vendor: 'IGE2A', model: 'TEISON-7KW' });
  assert.equal(h.fleet.get('CP-1')?.reconnectCount, 0, 'primeira conexao nao e reconexao');
  assert.equal(h.fleet.get('CP-1')?.vendor, 'IGE2A');

  h.connections.onClosed('CP-1', 'queda de rede');
  assert.equal(h.fleet.get('CP-1')?.lastDisconnectReason, 'queda de rede');
  assert.ok(h.fleet.get('CP-1')?.lastConnectedAt, 'ultima conexao registrada');

  h.connections.onConnected('CP-1', { protocol: 'ocpp1.6' });
  h.connections.onClosed('CP-1', 'queda de rede');
  h.connections.onConnected('CP-1', { protocol: 'ocpp1.6' });
  assert.equal(h.fleet.get('CP-1')?.reconnectCount, 2, 'o contador real e propagado para a frota');

  h.syncer.stop();
});

test('mudanca de estado do carregador propaga o estado operacional', () => {
  const h = harness();

  h.chargers.registerBoot('CP-2', { vendor: 'IGE2A', model: 'TEISON-7KW' });
  h.chargers.setConnectorStatus('CP-2', 1, 'Charging', 'NoError', null);

  const entry = h.fleet.get('CP-2');
  assert.equal(entry?.vendor, 'IGE2A');
  assert.ok(entry?.bootAt, 'ultimo boot propagado');
  assert.equal(entry?.connectors.length, 1, 'estado do conector propagado');
  assert.equal(entry?.connectors[0]?.status, 'Charging');

  h.syncer.stop();
});

test('syncAll cobre frota, conexoes e carregadores e o stop interrompe a observacao', () => {
  const h = harness();
  h.fleet.seed({ chargePointId: 'CP-A' });
  h.connections.onAttempt('CP-B');
  h.chargers.registerBoot('CP-C', { vendor: 'IGE2A' });

  h.syncer.syncAll();
  assert.deepEqual(h.fleet.ids().sort(), ['CP-A', 'CP-B', 'CP-C']);
  assert.equal(h.fleet.get('CP-B')?.negotiatedProtocol, null, 'conexao sem protocolo negociado');

  h.heartbeat.setNegotiatedInterval('CP-C', 30);
  h.syncer.syncAll();
  assert.equal(h.fleet.get('CP-C')?.negotiatedIntervalSeconds, 30, 'intervalo negociado propagado');

  h.syncer.stop();
  h.chargers.setConnectorStatus('CP-C', 1, 'Available', 'NoError', null);
  assert.equal(h.fleet.get('CP-C')?.connectors.length, 0, 'sem listeners apos o stop');

  // A sincronizacao manual continua disponivel (usada no snapshot da persistencia).
  h.syncer.syncAll();
  assert.equal(h.fleet.get('CP-C')?.connectors.length, 1);
});
