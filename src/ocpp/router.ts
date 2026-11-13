/**
 * Roteador OCPP: mapeia o nome da acao (CALL) para o handler correspondente.
 */
import { OcppError, OcppErrorCode } from './errors';
import { ocpp16Handlers, type OcppHandler } from './handlers';

/** Resolve o handler da acao; desconhecida -> NotImplemented (nao encerra o socket). */
export function resolveOcpp16Handler(action: string): OcppHandler {
  const handler = ocpp16Handlers[action];
  if (!handler) {
    throw new OcppError(OcppErrorCode.NotImplemented, `Acao OCPP nao suportada: ${action}`, { action });
  }
  return handler;
}

export function isSupportedAction(action: string): boolean {
  return Object.prototype.hasOwnProperty.call(ocpp16Handlers, action);
}
