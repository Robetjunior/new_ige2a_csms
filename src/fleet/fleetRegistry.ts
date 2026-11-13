/**
 * Registro da frota conhecida (plano de reconexao, passo 1).
 *
 * Fonte de verdade da IDENTIDADE dos carregadores que o CSMS ja viu, usada
 * apenas para (a) semear o estado como DISCONNECTED/OFFLINE apos um restart e
 * (b) reconhecer a reconexao em vez de tratar o carregador como novo.
 *
 * Regras:
 *  - NUNCA marca nenhum carregador como ONLINE/CONNECTED (isso e fisica de
 *    runtime e depende de um novo `upgrade`);
 *  - a identidade e acumulativa: um `seed` so preenche campos ausentes, nunca
 *    apaga um valor ja conhecido com `null`;
 *  - a chave e o `chargePointId` (mesma chave do ConnectionManager).
 */
import { readFileSync } from 'node:fs';

/** Ultimo estado observado de um conector (nao implica sessao/transacao ativa). */
export interface FleetConnectorState {
  connectorId: number;
  status: string;
  errorCode: string;
  info: string | null;
  updatedAt: string;
}

export interface FleetEntry {
  chargePointId: string;
  vendor: string | null;
  model: string | null;
  serialNumber: string | null;
  firmwareVersion: string | null;
  negotiatedProtocol: string | null;
  negotiatedIntervalSeconds: number | null;
  lastConnectedAt: string | null;
  lastBootAt: string | null;
  lastHeartbeatAt: string | null;
  lastPongAt: string | null;
  lastMessageAt: string | null;
  lastDisconnectReason: string | null;
  reconnectCount: number;
  /** Estado operacional: ultimo boot observado (ultima atividade). */
  bootAt: string | null;
  /** Estado operacional: instante do ultimo StatusNotification recebido. */
  lastStatusAt: string | null;
  /** Estado operacional: ultimo StatusNotification por conector. */
  connectors: FleetConnectorState[];
}

/** Entrada parcial aceita pelas fontes de frota (arquivo/config ou Supabase). */
export type FleetSeed = Partial<FleetEntry> & { chargePointId: string };

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.trim().length > 0 ? value.trim() : null;
}

function asNumber(value: unknown): number | null {
  const parsed = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function createEntry(seed: FleetSeed): FleetEntry {
  return {
    chargePointId: seed.chargePointId,
    vendor: seed.vendor ?? null,
    model: seed.model ?? null,
    serialNumber: seed.serialNumber ?? null,
    firmwareVersion: seed.firmwareVersion ?? null,
    negotiatedProtocol: seed.negotiatedProtocol ?? null,
    negotiatedIntervalSeconds: seed.negotiatedIntervalSeconds ?? null,
    lastConnectedAt: seed.lastConnectedAt ?? null,
    lastBootAt: seed.lastBootAt ?? null,
    lastHeartbeatAt: seed.lastHeartbeatAt ?? null,
    lastPongAt: seed.lastPongAt ?? null,
    lastMessageAt: seed.lastMessageAt ?? null,
    lastDisconnectReason: seed.lastDisconnectReason ?? null,
    reconnectCount: seed.reconnectCount ?? 0,
    bootAt: seed.bootAt ?? null,
    lastStatusAt: seed.lastStatusAt ?? null,
    connectors: (seed.connectors ?? []).map((c) => ({ ...c })),
  };
}

export class FleetRegistry {
  private readonly entries = new Map<string, FleetEntry>();

  /** Insere/atualiza a identidade conhecida sem nunca regredir para `null`. */
  seed(seed: FleetSeed): FleetEntry {
    if (!seed.chargePointId) throw new Error('FleetRegistry.seed exige chargePointId');
    const current = this.entries.get(seed.chargePointId);
    if (!current) {
      const created = createEntry(seed);
      this.entries.set(created.chargePointId, created);
      return created;
    }
    if (seed.vendor != null) current.vendor = seed.vendor;
    if (seed.model != null) current.model = seed.model;
    if (seed.serialNumber != null) current.serialNumber = seed.serialNumber;
    if (seed.firmwareVersion != null) current.firmwareVersion = seed.firmwareVersion;
    if (seed.negotiatedProtocol != null) current.negotiatedProtocol = seed.negotiatedProtocol;
    if (seed.negotiatedIntervalSeconds != null) current.negotiatedIntervalSeconds = seed.negotiatedIntervalSeconds;
    if (seed.lastConnectedAt != null) current.lastConnectedAt = seed.lastConnectedAt;
    if (seed.lastBootAt != null) current.lastBootAt = seed.lastBootAt;
    if (seed.lastHeartbeatAt != null) current.lastHeartbeatAt = seed.lastHeartbeatAt;
    if (seed.lastPongAt != null) current.lastPongAt = seed.lastPongAt;
    if (seed.lastMessageAt != null) current.lastMessageAt = seed.lastMessageAt;
    if (seed.lastDisconnectReason != null) current.lastDisconnectReason = seed.lastDisconnectReason;
    if (seed.reconnectCount != null) current.reconnectCount = seed.reconnectCount;
    if (seed.bootAt != null) current.bootAt = seed.bootAt;
    if (seed.lastStatusAt != null) current.lastStatusAt = seed.lastStatusAt;
    if (seed.connectors != null) current.connectors = seed.connectors.map((c) => ({ ...c }));
    return current;
  }

  seedAll(seeds: readonly FleetSeed[]): void {
    for (const seed of seeds) {
      if (seed?.chargePointId) this.seed(seed);
    }
  }

  get(chargePointId: string): FleetEntry | undefined {
    return this.entries.get(chargePointId);
  }

  has(chargePointId: string): boolean {
    return this.entries.has(chargePointId);
  }

  ids(): string[] {
    return Array.from(this.entries.keys());
  }

  snapshot(): FleetEntry[] {
    return Array.from(this.entries.values()).map((entry) => ({ ...entry }));
  }

  size(): number {
    return this.entries.size;
  }
}

export interface LoadFleetResult {
  entries: FleetEntry[];
  error: string | null;
}

/**
 * Le a frota esperada de um arquivo JSON. Formatos aceitos:
 *   ["CP-1", "CP-2"]
 *   [{ "chargePointId": "CP-1", "vendor": "IGE2A" }]
 *   { "chargers": [ ... ] }
 * Falha de leitura/parse nunca e fatal: devolve `error` e lista vazia.
 */
export function loadFleetFromFile(filePath: string): LoadFleetResult {
  const path = filePath.trim();
  if (!path) return { entries: [], error: null };
  let raw: string;
  try {
    raw = readFileSync(path, 'utf8');
  } catch (err) {
    return { entries: [], error: `nao foi possivel ler ${path}: ${String((err as Error)?.message ?? err)}` };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    return { entries: [], error: `JSON invalido em ${path}: ${String((err as Error)?.message ?? err)}` };
  }
  const list = Array.isArray(parsed)
    ? parsed
    : parsed !== null && typeof parsed === 'object' && Array.isArray((parsed as { chargers?: unknown }).chargers)
      ? (parsed as { chargers: unknown[] }).chargers
      : null;
  if (!list) {
    return { entries: [], error: `${path}: formato esperado e uma lista ou { "chargers": [...] }` };
  }
  const entries: FleetEntry[] = [];
  for (const item of list) {
    if (typeof item === 'string' && item.trim().length > 0) {
      entries.push(createEntry({ chargePointId: item.trim() }));
      continue;
    }
    if (item !== null && typeof item === 'object' && !Array.isArray(item)) {
      const record = item as Record<string, unknown>;
      const chargePointId = asString(record.chargePointId) ?? asString(record.charge_point_id);
      if (!chargePointId) continue;
      entries.push(
        createEntry({
          chargePointId,
          vendor: asString(record.vendor),
          model: asString(record.model),
          serialNumber: asString(record.serialNumber) ?? asString(record.serial_number),
          firmwareVersion: asString(record.firmwareVersion) ?? asString(record.firmware_version),
          negotiatedProtocol: asString(record.negotiatedProtocol) ?? asString(record.negotiated_protocol),
          negotiatedIntervalSeconds:
            asNumber(record.negotiatedIntervalSeconds) ?? asNumber(record.negotiated_interval_seconds),
          lastConnectedAt: asString(record.lastConnectedAt) ?? asString(record.last_connected_at),
          lastBootAt: asString(record.lastBootAt) ?? asString(record.last_boot_at),
          lastHeartbeatAt: asString(record.lastHeartbeatAt) ?? asString(record.last_heartbeat_at),
          lastPongAt: asString(record.lastPongAt) ?? asString(record.last_pong_at),
          lastMessageAt: asString(record.lastMessageAt) ?? asString(record.last_message_at),
          lastDisconnectReason: asString(record.lastDisconnectReason) ?? asString(record.last_disconnect_reason),
          reconnectCount: asNumber(record.reconnectCount) ?? asNumber(record.reconnect_count) ?? 0,
        }),
      );
    }
  }
  return { entries, error: null };
}
