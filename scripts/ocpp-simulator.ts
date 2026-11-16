/**
 * Simulador de carregador OCPP 1.6J.
 *
 * Uso como CLI:
 *   npm run client -- --url ws://127.0.0.1:3100 --id TEISON-TEST-01
 *
 * Reutilizavel por testes de integracao (exporta a classe OcppSimulator).
 */
import { WebSocket } from 'ws';
import { parseOcppFrame, serializeCallError, serializeCallResult } from '../src/ocpp/protocol';

export interface SimulatorOptions {
  /** URL base do CSMS, ex.: ws://127.0.0.1:3100 (sem o path). */
  baseUrl: string;
  chargePointId: string;
  /** Subprotocolo oferecido no handshake (default: 'ocpp1.6'). */
  protocol?: string | null;
  /** Intervalo de heartbeat inicial (ajustado pelo BootNotification). */
  heartbeatIntervalMs?: number;
  /** Se false, mantem o intervalo local e ignora o `interval` do CSMS (testes). */
  respectServerInterval?: boolean;
  /** Reconecta automaticamente apos queda inesperada (default: true). */
  autoReconnect?: boolean;
  /** Atraso inicial antes da reconexao; cresce exponencialmente. */
  reconnectDelayMs?: number;
  /** Limite superior do atraso exponencial entre tentativas. */
  maxReconnectDelayMs?: number;
  /** Indica se o simulador implementa TriggerMessage (default: true). */
  triggerMessageSupport?: boolean;
  /** Mensagens aceitas, mas deliberadamente nao transmitidas (teste de timeout). */
  ignoreTriggeredMessages?: string[];
  /**
   * Simula o ciclo completo de recarga ao receber `RemoteStartTransaction`:
   * `StatusNotification(Preparing->Charging)` + `StartTransaction` + fluxo
   * periodico de `MeterValues`; e `StopTransaction` em `RemoteStopTransaction`.
   * Se false, apenas responde `Accepted` ao comando (default: true).
   */
  simulateCharging?: boolean;
  /** Intervalo entre amostras de MeterValues durante a recarga (default: 5000ms). */
  meterIntervalMs?: number;
  /** Potencia ativa reportada nas amostras, em W (default: 7000). */
  meterPowerW?: number;
  vendor?: string;
  model?: string;
  firmwareVersion?: string;
}

export class OcppSimulator {
  private readonly opts: Required<Omit<SimulatorOptions, 'protocol'>> & { protocol: string | null };
  private ws: WebSocket | null = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private callSequence = 0;
  private reconnectAttempts = 0;
  private successfulReconnects = 0;
  private hasConnected = false;
  private intentionalStop = false;
  private bootUniqueId: string | null = null;
  /** Timer do fluxo periodico de MeterValues durante a recarga. */
  private meterTimer: ReturnType<typeof setInterval> | null = null;
  /** Transacao ativa atribuida pelo CSMS (apos o CALLRESULT de StartTransaction). */
  private activeTransactionId: number | null = null;
  /** Conector em uso pela transacao ativa. */
  private activeConnectorId: number | null = null;
  /** uniqueId do CALL StartTransaction aguardando CALLRESULT. */
  private startTransactionCallId: string | null = null;
  /** Energia acumulada reportada (Wh), cresce conforme meterPowerW. */
  private energyWh = 0;
  heartbeatIntervalMs: number;

  constructor(options: SimulatorOptions) {
    this.opts = {
      baseUrl: options.baseUrl,
      chargePointId: options.chargePointId,
      protocol: options.protocol === undefined ? 'ocpp1.6' : options.protocol,
      heartbeatIntervalMs: options.heartbeatIntervalMs ?? 60_000,
      respectServerInterval: options.respectServerInterval ?? true,
      autoReconnect: options.autoReconnect ?? true,
      reconnectDelayMs: options.reconnectDelayMs ?? 1000,
      maxReconnectDelayMs: options.maxReconnectDelayMs ?? 30_000,
      triggerMessageSupport: options.triggerMessageSupport ?? true,
      ignoreTriggeredMessages: options.ignoreTriggeredMessages ?? [],
      simulateCharging: options.simulateCharging ?? true,
      meterIntervalMs: options.meterIntervalMs ?? 5000,
      meterPowerW: options.meterPowerW ?? 7000,
      vendor: options.vendor ?? 'IGE2A',
      model: options.model ?? 'TEISON-7KW',
      firmwareVersion: options.firmwareVersion ?? '1.0.0-sim',
    };
    this.heartbeatIntervalMs = this.opts.heartbeatIntervalMs;
  }

  get url(): string {
    return `${this.opts.baseUrl}/ocpp/CentralSystemService/${encodeURIComponent(this.opts.chargePointId)}`;
  }

  /** Subprotocolo efetivamente negociado no handshake. */
  get negotiatedProtocol(): string {
    return this.ws?.protocol ?? '';
  }

  get isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  get reconnectCount(): number {
    return this.successfulReconnects;
  }

  connect(): Promise<void> {
    this.intentionalStop = false;
    if (this.ws?.readyState === WebSocket.OPEN) return Promise.resolve();
    if (this.ws?.readyState === WebSocket.CONNECTING) {
      return Promise.reject(new Error('Conexao do simulador ja esta em andamento'));
    }
    return this.openSocket();
  }

  private openSocket(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const ws = this.opts.protocol
        ? new WebSocket(this.url, this.opts.protocol, { handshakeTimeout: 10_000 })
        : new WebSocket(this.url, { handshakeTimeout: 10_000 });
      this.ws = ws;
      let opened = false;

      ws.on('open', () => {
        opened = true;
        if (this.hasConnected) this.successfulReconnects += 1;
        this.hasConnected = true;
        this.reconnectAttempts = 0;
        this.bootUniqueId = this.sendBoot();
        if (!this.opts.respectServerInterval) this.scheduleHeartbeat(this.opts.heartbeatIntervalMs);
        // Reconexao durante recarga ativa: reprojeta status e retoma o fluxo de MeterValues.
        if (this.activeTransactionId !== null) {
          this.sendStatusNotification(this.activeConnectorId ?? 1, 'Charging');
          this.startMeterStream();
        }
        resolve();
      });
      ws.on('message', (data) => this.onMessage(data.toString()));
      ws.on('error', (err) => {
        if (!opened) reject(err);
      });
      ws.on('close', (code, reason) => {
        if (this.ws !== ws) return;
        this.ws = null;
        this.bootUniqueId = null;
        this.clearHeartbeat();
        this.stopMeterStream();
        if (!this.intentionalStop && this.opts.autoReconnect) this.scheduleReconnect(code, reason.toString());
      });
    });
  }

  private scheduleReconnect(code: number, reason: string): void {
    if (this.reconnectTimer || this.intentionalStop) return;
    this.reconnectAttempts += 1;
    const delay = Math.min(
      this.opts.reconnectDelayMs * 2 ** Math.min(this.reconnectAttempts - 1, 10),
      this.opts.maxReconnectDelayMs,
    );
    // eslint-disable-next-line no-console
    console.warn(
      `[SIM] conexao encerrada (${code}${reason ? `:${reason}` : ''}); tentativa ${this.reconnectAttempts} em ${delay}ms`,
    );
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.openSocket().catch((err: unknown) => {
        // O evento close agenda a proxima tentativa; este log preserva a causa do handshake falho.
        // eslint-disable-next-line no-console
        console.warn(`[SIM] tentativa de reconexao falhou: ${String((err as Error)?.message ?? err)}`);
      });
    }, delay);
  }

  private sendBoot(): string {
    return this.call('BootNotification', {
      chargePointVendor: this.opts.vendor,
      chargePointModel: this.opts.model,
      firmwareVersion: this.opts.firmwareVersion,
    });
  }

  private call(action: string, payload: unknown): string {
    this.callSequence += 1;
    const uniqueId = `${this.opts.chargePointId}-${this.callSequence}`;
    this.send([2, uniqueId, action, payload]);
    return uniqueId;
  }

  private send(frame: unknown): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(frame));
  }

  private scheduleHeartbeat(intervalMs: number): void {
    this.clearHeartbeat();
    this.heartbeatIntervalMs = intervalMs;
    this.heartbeatTimer = setInterval(() => this.call('Heartbeat', {}), intervalMs);
  }

  private clearHeartbeat(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
  }

  private onMessage(raw: string): void {
    const parsed = parseOcppFrame(raw);
    if (!parsed.ok) return;
    const frame = parsed.frame;

    if (frame.messageTypeId === 3) {
      // Resposta a um CALL nosso (BootNotification/Heartbeat/StartTransaction/StopTransaction).
      const result = frame.payload as Record<string, unknown> | null;
      if (frame.uniqueId === this.bootUniqueId) {
        this.bootUniqueId = null;
        if (result && (result as { status?: string }).status === 'Accepted') {
          this.sendStatusNotification(1, 'Available');
        }
      }
      if (frame.uniqueId === this.startTransactionCallId) {
        this.startTransactionCallId = null;
        const transactionId = result?.transactionId;
        // Ignora CALLRESULT tardio se a sessao ja foi encerrada (conector liberado).
        if (typeof transactionId === 'number' && this.activeConnectorId !== null) {
          this.activeTransactionId = transactionId;
          // eslint-disable-next-line no-console
          console.log(
            `[SIM] recarga iniciada: transactionId=${transactionId} conector=${this.activeConnectorId ?? 1}`,
          );
          this.startMeterStream();
        } else if (typeof transactionId === 'number') {
          this.call('StopTransaction', {
            transactionId,
            meterStop: 0,
            timestamp: new Date().toISOString(),
            reason: 'Other',
          });
        } else {
          // eslint-disable-next-line no-console
          console.warn(`[SIM] StartTransaction nao retornou transactionId: ${JSON.stringify(result)}`);
        }
      }
      if (result && typeof result.interval === 'number' && result.interval > 0 && this.heartbeatTimer === null) {
        this.scheduleHeartbeat((result.interval as number) * 1000);
      }
      return;
    }

    if (frame.messageTypeId === 4) return;

    if (frame.messageTypeId === 2) {
      // CALL iniciado pelo CSMS: responde com CALLRESULT adequado.
      const response = this.handleCsmsCall(frame.action ?? '', frame.payload);
      this.send([3, frame.uniqueId, response]);
    }
  }

  private handleCsmsCall(action: string, payload: unknown): unknown {
    switch (action) {
      case 'RemoteStartTransaction': {
        if (!this.opts.simulateCharging) return { status: 'Accepted' };
        const request = this.asRecord(payload);
        const connectorId = typeof request.connectorId === 'number' ? request.connectorId : 1;
        const idTag =
          typeof request.idTag === 'string' && request.idTag.length > 0 ? request.idTag : this.opts.chargePointId;
        queueMicrotask(() => this.beginChargeSession(connectorId, idTag));
        return { status: 'Accepted' };
      }
      case 'RemoteStopTransaction':
        if (this.opts.simulateCharging) queueMicrotask(() => this.endChargeSession('Remote'));
        return { status: 'Accepted' };
      case 'Reset':
      case 'UnlockConnector':
      case 'ChangeAvailability':
        return { status: 'Accepted' };
      case 'ChangeConfiguration':
        return { status: 'Accepted' };
      case 'GetConfiguration':
        return { configurationKey: [], unknownKey: [] };
      case 'GetDiagnostics':
        return { fileName: `diagnostics-${this.opts.chargePointId}.log` };
      case 'TriggerMessage': {
        if (!this.opts.triggerMessageSupport) return { status: 'NotImplemented' };
        const request =
          payload !== null && typeof payload === 'object' && !Array.isArray(payload)
            ? (payload as Record<string, unknown>)
            : {};
        const requestedMessage = typeof request.requestedMessage === 'string' ? request.requestedMessage : '';
        if (!['Heartbeat', 'StatusNotification', 'MeterValues'].includes(requestedMessage)) {
          return { status: 'NotImplemented' };
        }
        if (!this.opts.ignoreTriggeredMessages.includes(requestedMessage)) {
          const connectorId = typeof request.connectorId === 'number' ? request.connectorId : 1;
          queueMicrotask(() => this.sendTriggeredMessage(requestedMessage, connectorId));
        }
        return { status: 'Accepted' };
      }
      default:
        return {};
    }
  }

  private sendTriggeredMessage(action: string, connectorId: number): void {
    switch (action) {
      case 'Heartbeat':
        this.call('Heartbeat', {});
        break;
      case 'StatusNotification':
        this.sendStatusNotification(connectorId, 'Available');
        break;
      case 'MeterValues':
        this.sendMeterValues(connectorId);
        break;
      default:
        break;
    }
  }

  /** Envia um StatusNotification (util para testes). */
  sendStatusNotification(connectorId: number, status = 'Available'): void {
    this.call('StatusNotification', { connectorId, status, errorCode: 'NoError', timestamp: new Date().toISOString() });
  }

  sendMeterValues(connectorId = 1): void {
    this.call('MeterValues', {
      connectorId,
      meterValue: [
        {
          timestamp: new Date().toISOString(),
          sampledValue: [
            { value: '7000', measurand: 'Power.Active.Import', unit: 'W' },
            { value: '28.5', measurand: 'Temperature', unit: 'Celsius' },
          ],
        },
      ],
    });
  }

  /** Inicia a recarga: Preparing -> Charging -> StartTransaction. */
  private beginChargeSession(connectorId: number, idTag: string): void {
    if (!this.isConnected || this.activeTransactionId !== null || this.startTransactionCallId !== null) return;
    this.activeConnectorId = connectorId;
    this.energyWh = 0;
    // eslint-disable-next-line no-console
    console.log(`[SIM] RemoteStart aceito; iniciando recarga no conector ${connectorId}`);
    this.sendStatusNotification(connectorId, 'Preparing');
    this.sendStatusNotification(connectorId, 'Charging');
    this.startTransactionCallId = this.call('StartTransaction', {
      connectorId,
      idTag,
      meterStart: 0,
      timestamp: new Date().toISOString(),
    });
  }

  /** Encerra a recarga: StopTransaction + StatusNotification(Available). */
  private endChargeSession(reason: string): void {
    const connectorId = this.activeConnectorId ?? 1;
    this.stopMeterStream();
    if (this.activeTransactionId !== null) {
      this.call('StopTransaction', {
        transactionId: this.activeTransactionId,
        meterStop: Math.round(this.energyWh),
        timestamp: new Date().toISOString(),
        reason,
      });
      // eslint-disable-next-line no-console
      console.log(
        `[SIM] recarga encerrada: transactionId=${this.activeTransactionId} energia=${Math.round(this.energyWh)}Wh motivo=${reason}`,
      );
    }
    this.activeTransactionId = null;
    this.activeConnectorId = null;
    this.sendStatusNotification(connectorId, 'Available');
  }

  /** Inicia o envio periodico de MeterValues enquanto houver transacao ativa. */
  private startMeterStream(): void {
    if (!this.isConnected || this.activeTransactionId === null) return;
    this.stopMeterStream();
    this.sendChargingMeterValues();
    this.meterTimer = setInterval(() => this.sendChargingMeterValues(), this.opts.meterIntervalMs);
  }

  private stopMeterStream(): void {
    if (this.meterTimer) clearInterval(this.meterTimer);
    this.meterTimer = null;
  }

  private sendChargingMeterValues(): void {
    if (!this.isConnected || this.activeTransactionId === null) return;
    this.energyWh += (this.opts.meterPowerW * this.opts.meterIntervalMs) / 3_600_000;
    this.call('MeterValues', {
      connectorId: this.activeConnectorId ?? 1,
      transactionId: this.activeTransactionId,
      meterValue: [
        {
          timestamp: new Date().toISOString(),
          sampledValue: [
            { value: String(Math.round(this.energyWh)), measurand: 'Energy.Active.Import.Register', unit: 'Wh' },
            { value: String(this.opts.meterPowerW), measurand: 'Power.Active.Import', unit: 'W' },
            { value: '28.5', measurand: 'Temperature', unit: 'Celsius' },
          ],
        },
      ],
    });
  }

  private asRecord(payload: unknown): Record<string, unknown> {
    return payload !== null && typeof payload === 'object' && !Array.isArray(payload)
      ? (payload as Record<string, unknown>)
      : {};
  }

  /** Envia um frame bruto invalido (teste de robustez/parse). */
  sendRaw(raw: string): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send(raw);
  }

  /** Espera ate a conexao fechar. */
  waitClose(timeoutMs = 5000): Promise<boolean> {
    return new Promise((resolve) => {
      if (!this.ws) {
        resolve(false);
        return;
      }
      const timer = setTimeout(() => resolve(false), timeoutMs);
      this.ws.once('close', () => {
        clearTimeout(timer);
        resolve(true);
      });
    });
  }

  stop(): void {
    this.intentionalStop = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.clearHeartbeat();
    this.stopMeterStream();
    const ws = this.ws;
    this.ws = null;
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
      ws.close(1000, 'simulator stopped');
    }
  }
}

/** Cria uma CALLERROR serializada (helper de teste). */
export function buildCallError(uniqueId: string, code: string, description: string): string {
  return serializeCallError(uniqueId, code, description);
}

/** Cria uma CALLRESULT serializada (helper de teste). */
export function buildCallResult(uniqueId: string, payload: unknown): string {
  return serializeCallResult(uniqueId, payload);
}

async function runCli(): Promise<void> {
  const args = process.argv.slice(2);
  const get = (name: string, fallback: string): string => {
    const index = args.indexOf(`--${name}`);
    return index >= 0 && args[index + 1] ? String(args[index + 1]) : fallback;
  };
  const baseUrl = get('url', 'ws://127.0.0.1:3100');
  const chargePointId = get('id', 'TEISON-SIM-01');
  const protocolArg = get('protocol', 'ocpp1.6');
  const protocol = protocolArg === 'none' ? null : protocolArg;

  const simulator = new OcppSimulator({ baseUrl, chargePointId, protocol });
  // eslint-disable-next-line no-console
  console.log(`[SIM] conectando em ${simulator.url} (subprotocolo=${protocol ?? 'nenhum'})`);
  const shutdown = (): void => {
    simulator.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  try {
    await simulator.connect();
    // eslint-disable-next-line no-console
    console.log(`[SIM] conectado; BootNotification, StatusNotification e Heartbeats ativos`);
  } catch (err) {
    // A rotina de close agenda a reconexao automatica mesmo se a primeira tentativa falhar.
    // eslint-disable-next-line no-console
    console.warn(`[SIM] conexao inicial falhou; tentando novamente: ${String((err as Error)?.message ?? err)}`);
  }
}

if (typeof require !== 'undefined' && require.main === module) {
  void runCli().catch((err) => {
    // eslint-disable-next-line no-console
    console.error('[SIM] falha:', err);
    process.exit(1);
  });
}
