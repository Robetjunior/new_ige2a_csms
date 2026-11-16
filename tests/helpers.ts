/**
 * Helpers compartilhados pelos testes.
 */
import type { AppConfig } from '../src/config/env';

export function makeTestConfig(overrides: Partial<AppConfig> = {}): AppConfig {
  return Object.freeze({
    env: 'test',
    port: 0,
    host: '127.0.0.1',
    logLevel: 'silent',
    apiKey: '',
    integrationKey: '',
    integration: Object.freeze({
      pushEnabled: false,
      targetUrl: '',
      hmacSecret: '',
      pushIntervalMs: 5000,
      pushBatchSize: 50,
      pushTimeoutMs: 5000,
      pushMaxAttempts: 8,
      pushBackoffBaseMs: 2000,
      pushBackoffMaxMs: 300_000,
    }),
    ocpp: Object.freeze({
      lenientProtocolFallback: true,
      defaultHeartbeatIntervalSeconds: 60,
      watchdogTimeoutMs: 300_000,
      pingIntervalMs: 30_000,
      pongTimeoutMs: 15_000,
      maxMessageBytes: 262_144,
      heartbeatToleranceSeconds: 15,
      heartbeatCriticalSeconds: 120,
      statusProbeEnabled: false,
      statusProbeIntervalMs: 60_000,
      statusProbeTimeoutMs: 500,
      statusProbeConnectorId: null,
      fleetEnabled: true,
      fleetFile: '',
      reconnectionGraceMultiplier: 2,
      reconnectionToleranceSeconds: 30,
      reconnectionCheckIntervalMs: 15_000,
    }),
    persistence: Object.freeze({
      supabaseUrl: '',
      supabaseServiceKey: '',
      supabaseSchema: 'orchestrator',
      logPersistEnabled: false,
      logBatchSize: 200,
      logFlushIntervalMs: 1500,
      logMaxQueue: 20_000,
    }),
    ...overrides,
  });
}

/** Aguarda uma condicao (polling) ate o timeout; lanca se nao satisfeita. */
export async function waitFor(
  predicate: () => boolean,
  timeoutMs = 3000,
  intervalMs = 20,
  description = 'condicao',
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`Timeout aguardando ${description} (${timeoutMs}ms)`);
}

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
