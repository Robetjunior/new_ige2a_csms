/**
 * Codigos de erro OCPP-J (OCPP 1.6, secao de erros) e a excecao usada para
 * produzir CALLERROR correlacionado, sem encerrar o socket.
 */

export const OcppErrorCode = {
  NotImplemented: 'NotImplemented',
  NotSupported: 'NotSupported',
  InternalError: 'InternalError',
  ProtocolError: 'ProtocolError',
  SecurityError: 'SecurityError',
  FormationViolation: 'FormationViolation',
  PropertyConstraintViolation: 'PropertyConstraintViolation',
  OccurrenceConstraintViolation: 'OccurrenceConstraintViolation',
  TypeConstraintViolation: 'TypeConstraintViolation',
  GenericError: 'GenericError',
} as const;

export type OcppErrorCodeValue = (typeof OcppErrorCode)[keyof typeof OcppErrorCode];

/** Erro de protocolo/aplicacao que deve virar CALLERROR. */
export class OcppError extends Error {
  readonly code: OcppErrorCodeValue;
  readonly details: unknown;

  constructor(code: OcppErrorCodeValue, description: string, details: unknown = {}) {
    super(description);
    this.name = 'OcppError';
    this.code = code;
    this.details = details;
  }
}
