/**
 * Handlers das acoes OCPP 1.6J recebidas do carregador (CHARGER -> CSMS).
 * Cada handler apenas atualiza o estado do dominio e devolve o payload de
 * CALLRESULT; o framing e a auditoria sao responsabilidade das camadas de
 * protocolo e de log.
 */
import { OcppError, OcppErrorCode } from './errors';
import type { ChargerStateService } from '../chargers/chargerState';
import type { HeartbeatMonitor } from '../heartbeat/heartbeatMonitor';
import type { AppConfig } from '../config/env';

export interface OcppCallContext {
  readonly chargePointId: string;
  readonly connectionId: string | null;
  readonly config: AppConfig;
  readonly chargers: ChargerStateService;
  readonly heartbeat: HeartbeatMonitor;
}

export type OcppHandler = (
  ctx: OcppCallContext,
  payload: Record<string, unknown>,
) => unknown | Promise<unknown>;

function requireString(payload: Record<string, unknown>, field: string): string {
  const value = payload[field];
  if (typeof value !== 'string' || value.length === 0) {
    throw new OcppError(OcppErrorCode.FormationViolation, `Campo obrigatorio ausente/invalido: ${field}`, {
      field,
    });
  }
  return value;
}

function requireNumber(payload: Record<string, unknown>, field: string): number {
  const value = payload[field];
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new OcppError(OcppErrorCode.FormationViolation, `Campo numerico obrigatorio ausente/invalido: ${field}`, {
      field,
    });
  }
  return value;
}

function requireInteger(payload: Record<string, unknown>, field: string): number {
  const value = requireNumber(payload, field);
  if (!Number.isInteger(value)) {
    throw new OcppError(OcppErrorCode.TypeConstraintViolation, `Campo deve ser inteiro: ${field}`, { field });
  }
  return value;
}

const bootNotification: OcppHandler = (ctx, payload) => {
  ctx.chargers.registerBoot(ctx.chargePointId, {
    vendor: payload.chargePointVendor,
    model: payload.chargePointModel,
    serialNumber: payload.chargePointSerialNumber,
    firmwareVersion: payload.firmwareVersion,
  });
  const interval = ctx.config.ocpp.defaultHeartbeatIntervalSeconds;
  ctx.heartbeat.setNegotiatedInterval(ctx.chargePointId, interval);
  return { status: 'Accepted', currentTime: new Date().toISOString(), interval };
};

const heartbeat: OcppHandler = () => ({ currentTime: new Date().toISOString() });

const statusNotification: OcppHandler = (ctx, payload) => {
  const connectorId = requireInteger(payload, 'connectorId');
  if (connectorId < 0) {
    throw new OcppError(OcppErrorCode.PropertyConstraintViolation, 'connectorId deve ser >= 0', { connectorId });
  }
  const status = requireString(payload, 'status');
  const errorCode = requireString(payload, 'errorCode');
  const info = typeof payload.info === 'string' ? payload.info : null;
  ctx.chargers.setConnectorStatus(ctx.chargePointId, connectorId, status, errorCode, info);
  return {};
};

const authorize: OcppHandler = (_ctx, payload) => {
  requireString(payload, 'idTag');
  return { idTagInfo: { status: 'Accepted' } };
};

const startTransaction: OcppHandler = (ctx, payload) => {
  const connectorId = requireInteger(payload, 'connectorId');
  requireString(payload, 'idTag');
  requireNumber(payload, 'meterStart');
  requireString(payload, 'timestamp');
  const transactionId = ctx.chargers.startTransaction(ctx.chargePointId, connectorId);
  return { transactionId, idTagInfo: { status: 'Accepted' } };
};

const stopTransaction: OcppHandler = (ctx, payload) => {
  const transactionId = requireInteger(payload, 'transactionId');
  requireNumber(payload, 'meterStop');
  requireString(payload, 'timestamp');
  ctx.chargers.stopTransaction(ctx.chargePointId, transactionId, null);
  return {};
};

const meterValues: OcppHandler = (ctx, payload) => {
  const connectorId = requireInteger(payload, 'connectorId');
  if (connectorId < 0) {
    throw new OcppError(OcppErrorCode.PropertyConstraintViolation, 'connectorId deve ser >= 0', { connectorId });
  }
  if (!Array.isArray(payload.meterValue)) {
    throw new OcppError(OcppErrorCode.FormationViolation, 'meterValue deve ser um array', {});
  }
  const readings: Array<{
    connectorId: number;
    measurand: string;
    value: string;
    unit: string | null;
    phase: string | null;
    location: string | null;
    context: string | null;
    timestamp: string;
  }> = [];
  for (const meterValue of payload.meterValue) {
    if (!meterValue || typeof meterValue !== 'object' || Array.isArray(meterValue)) continue;
    const value = meterValue as Record<string, unknown>;
    const timestamp =
      typeof value.timestamp === 'string' && Number.isFinite(Date.parse(value.timestamp))
        ? value.timestamp
        : new Date().toISOString();
    if (!Array.isArray(value.sampledValue)) continue;
    for (const sampledValue of value.sampledValue) {
      if (!sampledValue || typeof sampledValue !== 'object' || Array.isArray(sampledValue)) continue;
      const sample = sampledValue as Record<string, unknown>;
      if (typeof sample.value !== 'string' && typeof sample.value !== 'number') continue;
      readings.push({
        connectorId,
        measurand:
          typeof sample.measurand === 'string' && sample.measurand.length > 0
            ? sample.measurand
            : 'Energy.Active.Import.Register',
        value: String(sample.value).slice(0, 128),
        unit: typeof sample.unit === 'string' ? sample.unit.slice(0, 32) : null,
        phase: typeof sample.phase === 'string' ? sample.phase.slice(0, 32) : null,
        location: typeof sample.location === 'string' ? sample.location.slice(0, 32) : null,
        context: typeof sample.context === 'string' ? sample.context.slice(0, 32) : null,
        timestamp,
      });
    }
  }
  if (readings.length > 0) ctx.chargers.recordMeterReadings(ctx.chargePointId, readings);
  return {};
};

const dataTransfer: OcppHandler = (_ctx, payload) => {
  requireString(payload, 'vendorId');
  return { status: 'Accepted' };
};

const diagnosticsStatusNotification: OcppHandler = (_ctx, payload) => {
  requireString(payload, 'status');
  return {};
};

const firmwareStatusNotification: OcppHandler = (_ctx, payload) => {
  requireString(payload, 'status');
  return {};
};

export const ocpp16Handlers: Readonly<Record<string, OcppHandler>> = Object.freeze({
  BootNotification: bootNotification,
  Heartbeat: heartbeat,
  StatusNotification: statusNotification,
  Authorize: authorize,
  StartTransaction: startTransaction,
  StopTransaction: stopTransaction,
  MeterValues: meterValues,
  DataTransfer: dataTransfer,
  DiagnosticsStatusNotification: diagnosticsStatusNotification,
  FirmwareStatusNotification: firmwareStatusNotification,
});
