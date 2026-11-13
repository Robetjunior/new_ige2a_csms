import test from 'node:test';
import assert from 'node:assert/strict';
import { FleetRegistry } from '../src/fleet/fleetRegistry';
import { ReconnectionWatcher, type FleetStatusEvent } from '../src/fleet/reconnectionWatcher';
import type { ConnectionManager } from '../src/transport/connectionManager';
import type { OcppWebSocketServer } from '../src/transport/ocppWebSocketServer';
import type { HeartbeatMonitor } from '../src/heartbeat/heartbeatMonitor';

interface Harness {
  fleet: FleetRegistry;
  watcher: ReconnectionWatcher;
  events: FleetStatusEvent[];
  connected: Set<string>;
  advance(ms: number): void;
  setConnected(id: string, value: boolean): void;
  connectionListenerCount(): number;
}

function createHarness(params: {
  connected?: string[];
  intervalSeconds?: number;
  graceMultiplier?: number;
  toleranceSeconds?: number;
  enabled?: boolean;
} = {}): Harness {
  const fleet = new FleetRegistry();
  const connected = new Set(params.connected ?? []);
  const intervalSeconds = params.intervalSeconds ?? 60;
  const listeners = new Set<() => void>();
  const events: FleetStatusEvent[] = [];
  let nowMs = 1_000_000;

  const connections = {
    subscribe(listener: () => void) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    get: () => undefined,
  } as unknown as ConnectionManager;

  const wsServer = {
    isConnected: (id: string) => connected.has(id),
  } as unknown as OcppWebSocketServer;

  const heartbeat = {
    getNegotiatedInterval: () => intervalSeconds,
  } as unknown as HeartbeatMonitor;

  const watcher = new ReconnectionWatcher(
    { fleet, connections, wsServer, heartbeat },
    {
      enabled: params.enabled ?? true,
      intervalMs: 1000,
      graceMultiplier: params.graceMultiplier ?? 2,
      toleranceSeconds: params.toleranceSeconds ?? 30,
      now: () => nowMs,
    },
  );
  watcher.subscribe((event) => events.push(event));

  return {
    fleet,
    watcher,
    events,
    connected,
    advance(ms: number) {
      nowMs += ms;
    },
    setConnected(id: string, value: boolean) {
      if (value) connected.add(id);
      else connected.delete(id);
      for (const listener of listeners) listener();
    },
    connectionListenerCount: () => listeners.size,
  };
}

test('ReconnectionWatcher: classifica CONNECTED, AWAITING_RECONNECTION e reconexao', () => {
  // graceWindow = 10s * 2 + 5s = 25s
  const h = createHarness({ connected: ['CP-1'], intervalSeconds: 10, graceMultiplier: 2, toleranceSeconds: 5 });
  h.fleet.seed({ chargePointId: 'CP-1' });
  h.fleet.seed({ chargePointId: 'CP-2' });
  h.watcher.start();

  assert.equal(h.watcher.getStatus('CP-1').status, 'CONNECTED');
  assert.equal(h.watcher.getStatus('CP-2').status, 'UNKNOWN', 'dentro da janela ainda e UNKNOWN');

  // CP-2 continua desconectado; avanca alem da janela de tolerancia.
  h.advance(26_000);
  h.watcher.evaluate();
  assert.equal(h.watcher.getStatus('CP-2').status, 'AWAITING_RECONNECTION');
  const awaited = h.events.find((e) => e.chargePointId === 'CP-2');
  assert.equal(awaited?.eventType, 'known_charger_awaiting_reconnection');
  assert.ok(awaited?.awaitingSince, 'evento registra desde quando aguarda');

  // CP-2 reconecta: a transicao e percebida no mesmo instante (subscribe).
  h.setConnected('CP-2', true);
  assert.equal(h.watcher.getStatus('CP-2').status, 'CONNECTED');
  assert.ok(
    h.events.some((e) => e.chargePointId === 'CP-2' && e.eventType === 'known_charger_reconnected'),
    'deve emitir known_charger_reconnected',
  );

  h.watcher.stop();
});

test('ReconnectionWatcher: eventos sao emitidos apenas em transicao', () => {
  const h = createHarness({ intervalSeconds: 5, graceMultiplier: 1, toleranceSeconds: 0 });
  h.fleet.seed({ chargePointId: 'CP-1' });
  h.watcher.start();
  h.advance(6000);
  h.watcher.evaluate();
  h.watcher.evaluate();
  h.watcher.evaluate();
  const awaitingEvents = h.events.filter((e) => e.eventType === 'known_charger_awaiting_reconnection');
  assert.equal(awaitingEvents.length, 1, 'nao repete o alerta a cada tick');
  h.watcher.stop();
});

test('ReconnectionWatcher: start idempotente e stop remove o listener', () => {
  const h = createHarness({ connected: ['CP-1'] });
  h.fleet.seed({ chargePointId: 'CP-1' });
  h.watcher.start();
  h.watcher.start();
  assert.equal(h.connectionListenerCount(), 1, 'nao duplica listener no start repetido');
  h.watcher.stop();
  assert.equal(h.connectionListenerCount(), 0, 'stop libera o listener');
  h.watcher.stop();
  assert.equal(h.connectionListenerCount(), 0);
});

test('ReconnectionWatcher: desabilitado nao publica status', () => {
  const h = createHarness({ enabled: false });
  h.fleet.seed({ chargePointId: 'CP-1' });
  h.watcher.start();
  h.advance(120_000);
  h.watcher.evaluate();
  assert.equal(h.events.length, 0);
  assert.equal(h.connectionListenerCount(), 0, 'nem assina conexoes quando desabilitado');
  h.watcher.stop();
});

test('ReconnectionWatcher: snapshot cobre toda a frota conhecida', () => {
  const h = createHarness({ connected: ['CP-1'], intervalSeconds: 30 });
  h.fleet.seedAll([{ chargePointId: 'CP-1' }, { chargePointId: 'CP-2' }]);
  h.watcher.start();
  const ids = h.watcher.snapshot().map((m) => m.chargePointId).sort();
  assert.deepEqual(ids, ['CP-1', 'CP-2']);
  assert.equal(h.watcher.getStatus('CP-2').reason, 'carregador da frota conhecida');
  h.watcher.stop();
});
