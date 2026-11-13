/**
 * Estado em memoria de cada carregador (boot, conectores, transacoes).
 * Fonte de verdade volatil do nucleo; a persistencia e feita por adaptadores.
 */

export interface ConnectorState {
  connectorId: number;
  status: string;
  errorCode: string;
  info: string | null;
  updatedAt: string;
}

export interface MeterReading {
  connectorId: number;
  measurand: string;
  value: string;
  unit: string | null;
  phase: string | null;
  location: string | null;
  context: string | null;
  timestamp: string;
  updatedAt: string;
}

export interface ChargerInfo {
  chargePointId: string;
  vendor: string | null;
  model: string | null;
  serialNumber: string | null;
  firmwareVersion: string | null;
  bootAt: string | null;
  lastStatusAt: string | null;
  connectors: Map<number, ConnectorState>;
  meterReadings: Map<string, MeterReading>;
  /** connectorId -> transactionId da transacao aberta. */
  activeTransactions: Map<number, number>;
  lastTransactionId: number;
  updatedAt: string;
}

function createChargerInfo(chargePointId: string): ChargerInfo {
  return {
    chargePointId,
    vendor: null,
    model: null,
    serialNumber: null,
    firmwareVersion: null,
    bootAt: null,
    lastStatusAt: null,
    connectors: new Map(),
    meterReadings: new Map(),
    activeTransactions: new Map(),
    lastTransactionId: 0,
    updatedAt: new Date().toISOString(),
  };
}

export class ChargerStateService {
  private readonly chargers = new Map<string, ChargerInfo>();
  private readonly listeners = new Set<(chargePointId: string) => void>();
  private readonly maxRecords: number;

  constructor(maxRecords = 5000) {
    this.maxRecords = maxRecords;
  }

  subscribe(listener: (chargePointId: string) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private notify(chargePointId: string): void {
    for (const listener of this.listeners) {
      try {
        listener(chargePointId);
      } catch {
        /* observabilidade nunca deve interromper o processamento OCPP */
      }
    }
  }

  private ensure(chargePointId: string): ChargerInfo {
    let info = this.chargers.get(chargePointId);
    if (!info) {
      info = createChargerInfo(chargePointId);
      this.chargers.set(chargePointId, info);
      this.evictIfNeeded();
    }
    return info;
  }

  private evictIfNeeded(): void {
    while (this.chargers.size > this.maxRecords) {
      const oldest = this.chargers.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.chargers.delete(oldest);
    }
  }

  /**
   * Semeia o ESTADO OPERACIONAL conhecido de um carregador (plano de reconexao,
   * passo 2) a partir de evidencias persistidas, SEM reidratar sessao:
   *  - apenas o ultimo estado observado (vendor/model/firmware, ultimo boot,
   *    ultimo StatusNotification e status por conector);
   *  - `activeTransactions` NAO e semeado de proposito: uma transacao aberta
   *    pertence a sessao anterior e reidrata-la faria o CSMS acreditar em uma
   *    sessao inexistente apos o restart.
   */
  seedIdentity(
    chargePointId: string,
    info: {
      vendor?: string | null;
      model?: string | null;
      serialNumber?: string | null;
      firmwareVersion?: string | null;
      bootAt?: string | null;
      lastStatusAt?: string | null;
      connectors?: ReadonlyArray<{
        connectorId: number;
        status: string;
        errorCode?: string;
        info?: string | null;
        updatedAt?: string;
      }>;
    } = {},
  ): void {
    const rec = this.ensure(chargePointId);
    if (info.vendor != null) rec.vendor = info.vendor;
    if (info.model != null) rec.model = info.model;
    if (info.serialNumber != null) rec.serialNumber = info.serialNumber;
    if (info.firmwareVersion != null) rec.firmwareVersion = info.firmwareVersion;
    if (info.bootAt != null) rec.bootAt = info.bootAt;
    if (info.lastStatusAt != null) rec.lastStatusAt = info.lastStatusAt;
    if (info.connectors) {
      const fallbackAt = new Date().toISOString();
      for (const connector of info.connectors) {
        if (typeof connector.connectorId !== 'number' || !Number.isFinite(connector.connectorId)) continue;
        rec.connectors.set(connector.connectorId, {
          connectorId: connector.connectorId,
          status: connector.status,
          errorCode: connector.errorCode ?? '',
          info: connector.info ?? null,
          updatedAt: connector.updatedAt ?? fallbackAt,
        });
      }
    }
    rec.updatedAt = new Date().toISOString();
    this.notify(chargePointId);
  }

  registerBoot(
    chargePointId: string,
    info: { vendor?: unknown; model?: unknown; serialNumber?: unknown; firmwareVersion?: unknown },
  ): ChargerInfo {
    const rec = this.ensure(chargePointId);
    rec.vendor = typeof info.vendor === 'string' ? info.vendor : rec.vendor;
    rec.model = typeof info.model === 'string' ? info.model : rec.model;
    rec.serialNumber = typeof info.serialNumber === 'string' ? info.serialNumber : rec.serialNumber;
    rec.firmwareVersion = typeof info.firmwareVersion === 'string' ? info.firmwareVersion : rec.firmwareVersion;
    rec.bootAt = new Date().toISOString();
    rec.updatedAt = rec.bootAt;
    this.notify(chargePointId);
    return rec;
  }

  setConnectorStatus(
    chargePointId: string,
    connectorId: number,
    status: string,
    errorCode: string,
    info: string | null,
  ): void {
    const rec = this.ensure(chargePointId);
    const at = new Date().toISOString();
    rec.connectors.set(connectorId, { connectorId, status, errorCode, info, updatedAt: at });
    rec.lastStatusAt = at;
    rec.updatedAt = at;
    this.notify(chargePointId);
  }

  recordMeterReading(chargePointId: string, reading: Omit<MeterReading, 'updatedAt'>): void {
    this.recordMeterReadings(chargePointId, [reading]);
  }

  recordMeterReadings(chargePointId: string, readings: Array<Omit<MeterReading, 'updatedAt'>>): void {
    const rec = this.ensure(chargePointId);
    const updatedAt = new Date().toISOString();
    for (const reading of readings) {
      const key = [
        reading.connectorId,
        reading.measurand,
        reading.phase ?? '',
        reading.location ?? '',
      ].join('|');
      rec.meterReadings.set(key, { ...reading, updatedAt });
    }
    while (rec.meterReadings.size > 256) {
      const oldest = rec.meterReadings.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      rec.meterReadings.delete(oldest);
    }
    rec.updatedAt = updatedAt;
    this.notify(chargePointId);
  }

  /** Abre uma transacao para o conector e devolve o transactionId atribuido. */
  startTransaction(chargePointId: string, connectorId: number): number {
    const rec = this.ensure(chargePointId);
    const existing = rec.activeTransactions.get(connectorId);
    if (existing !== undefined) return existing;
    rec.lastTransactionId += 1;
    const transactionId = rec.lastTransactionId;
    rec.activeTransactions.set(connectorId, transactionId);
    rec.updatedAt = new Date().toISOString();
    return transactionId;
  }

  /** Encerra a transacao informada (por id) ou a do conector (fallback). */
  stopTransaction(chargePointId: string, transactionId: number | null, connectorId: number | null): void {
    const rec = this.ensure(chargePointId);
    if (transactionId !== null) {
      for (const [conn, tx] of rec.activeTransactions.entries()) {
        if (tx === transactionId) {
          rec.activeTransactions.delete(conn);
          rec.updatedAt = new Date().toISOString();
          return;
        }
      }
      return;
    }
    if (connectorId !== null) {
      rec.activeTransactions.delete(connectorId);
      rec.updatedAt = new Date().toISOString();
    }
  }

  get(chargePointId: string): ChargerInfo | undefined {
    return this.chargers.get(chargePointId);
  }

  snapshot(): ChargerInfo[] {
    return Array.from(this.chargers.values()).map((c) => ({
      ...c,
      connectors: new Map(c.connectors),
      meterReadings: new Map(c.meterReadings),
      activeTransactions: new Map(c.activeTransactions),
    }));
  }

  size(): number {
    return this.chargers.size;
  }
}
