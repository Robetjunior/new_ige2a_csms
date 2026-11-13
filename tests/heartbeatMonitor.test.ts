import test from 'node:test';
import assert from 'node:assert/strict';
import { HeartbeatMonitor } from '../src/heartbeat/heartbeatMonitor';

function makeMonitor(nowRef: { value: number }) {
  return new HeartbeatMonitor({
    defaultIntervalSeconds: 60,
    toleranceSeconds: 15,
    criticalSeconds: 120,
    now: () => nowRef.value,
  });
}

test('classificacao NORMAL/ATRASADO/CRITICO com base no intervalo negociado', () => {
  const nowRef = { value: 1_000_000 };
  const monitor = makeMonitor(nowRef);
  monitor.setConnectionStatus('CP1', true);

  assert.equal(monitor.getStatus('CP1').classification, 'SEM_DADOS');

  monitor.registerHeartbeat('CP1', 120);
  assert.equal(monitor.getStatus('CP1').classification, 'NORMAL');

  nowRef.value += 80_000; // 80s > 60s + 15s (tolerancia) e <= 60s + 15s + 120s
  assert.equal(monitor.getStatus('CP1').classification, 'ATRASADO');

  nowRef.value += 200_000; // muito acima de 60s + 15s + 120s
  assert.equal(monitor.getStatus('CP1').classification, 'CRITICO');
});

test('NUNCA marca OFFLINE apenas por ausencia de heartbeat', () => {
  const nowRef = { value: 0 };
  const monitor = makeMonitor(nowRef);
  monitor.setConnectionStatus('CP1', true);
  // Recebeu apenas uma mensagem que NAO e heartbeat.
  monitor.registerMessage('CP1');
  nowRef.value += 10 * 60 * 1000; // 10 minutos sem heartbeat

  const status = monitor.getStatus('CP1');
  assert.notEqual(status.classification, 'OFFLINE');
  assert.equal(status.connectionStatus, 'ONLINE');
  assert.equal(status.lastHeartbeatAt, null);
  assert.equal(status.lastOcppMessageAt, new Date(0).toISOString());
});

test('OFFLINE depende exclusivamente do estado da conexao', () => {
  const nowRef = { value: 0 };
  const monitor = makeMonitor(nowRef);
  monitor.setConnectionStatus('CP1', true);
  monitor.registerHeartbeat('CP1', 50);
  assert.equal(monitor.getStatus('CP1').classification, 'NORMAL');

  monitor.setConnectionStatus('CP1', false);
  assert.equal(monitor.getStatus('CP1').classification, 'OFFLINE');
});

test('intervalo negociado substitui o padrao e afeta a classificacao', () => {
  const nowRef = { value: 0 };
  const monitor = makeMonitor(nowRef);
  monitor.setConnectionStatus('CP1', true);
  monitor.setNegotiatedInterval('CP1', 300);
  assert.equal(monitor.getNegotiatedInterval('CP1'), 300);

  monitor.registerHeartbeat('CP1', 10);
  nowRef.value += 120_000; // 120s: ainda dentro de 300s
  assert.equal(monitor.getStatus('CP1').classification, 'NORMAL');
  nowRef.value += 250_000; // 370s: 300s + 15s < 370s <= 300s + 15s + 120s
  assert.equal(monitor.getStatus('CP1').classification, 'ATRASADO');
});

test('metricas de latencia agregadas', () => {
  const nowRef = { value: 0 };
  const monitor = makeMonitor(nowRef);
  monitor.setConnectionStatus('CP1', true);
  monitor.registerHeartbeat('CP1', 100);
  monitor.registerHeartbeat('CP1', 300);

  const status = monitor.getStatus('CP1');
  assert.equal(status.heartbeatCount, 2);
  assert.equal(status.averageLatencyMs, 200);
  assert.equal(status.maxLatencyMs, 300);
});
