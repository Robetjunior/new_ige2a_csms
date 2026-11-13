/**
 * Diagnostico automatico por carregador (spec §9).
 *
 * Consolida as evidencias ja coletadas (conexao, heartbeat, log OCPP) e produz
 * uma conclusao verificavel, sempre distinguindo fato observado de hipotese.
 */
import type { CsmsComponents } from '../components';
import type { HeartbeatStatus, OcppMessageLog } from '../domain/types';
import type { ChargerConnectionRecord } from '../transport/connectionManager';

export type Severity = 'OK' | 'ATENCAO' | 'CRITICO';

export interface DiagnosisFinding {
  code: string;
  severity: Severity;
  message: string;
  evidence: Record<string, unknown>;
}

export interface ChargerDiagnosis {
  chargePointId: string;
  generatedAt: string;
  connection: ChargerConnectionRecord | null;
  heartbeat: HeartbeatStatus;
  /** Ultima comunicacao observada, independente do tipo. */
  lastMessageAt: string | null;
  lastHeartbeatAt: string | null;
  negotiatedIntervalSeconds: number;
  /** Contagens sobre a janela de mensagens recentes consultada. */
  recentMessageStats: { window: number; received: number; sent: number; errors: number };
  /** Ultima mensagem trocada antes do estado atual (contexto da desconexao). */
  lastMessages: OcppMessageLog[];
  protocolErrors: OcppMessageLog[];
  severity: Severity;
  findings: DiagnosisFinding[];
}

function worst(findings: DiagnosisFinding[]): Severity {
  if (findings.some((f) => f.severity === 'CRITICO')) return 'CRITICO';
  if (findings.some((f) => f.severity === 'ATENCAO')) return 'ATENCAO';
  return 'OK';
}

export function buildDiagnosis(chargePointId: string, components: CsmsComponents, now = new Date()): ChargerDiagnosis {
  const connection = components.connections.get(chargePointId) ?? null;
  const heartbeat = components.heartbeat.getStatus(chargePointId);
  const lastMessages = components.messageLog.recent({ chargePointId, limit: 50 });
  const protocolErrors = lastMessages.filter(
    (m) => m.status === 'failed' || m.messageType === 'CALLERROR' || m.parseError !== null,
  );
  const recentMessageStats = {
    window: lastMessages.length,
    received: lastMessages.filter((m) => m.direction === 'received').length,
    sent: lastMessages.filter((m) => m.direction === 'sent').length,
    errors: protocolErrors.length,
  };
  const findings: DiagnosisFinding[] = [];

  if (connection?.transport === 'CONNECTED') {
    findings.push({
      code: 'TRANSPORT_CONNECTED',
      severity: 'OK',
      message: 'Socket WebSocket ativo.',
      evidence: { connectionId: connection.connectionId, connectedAt: connection.connectedAt, remoteIp: connection.remoteIp },
    });
  } else {
    findings.push({
      code: 'TRANSPORT_DISCONNECTED',
      severity: 'CRITICO',
      message: 'Sem socket WebSocket ativo no momento.',
      evidence: {
        transport: connection?.transport ?? 'DESCONHECIDO',
        disconnectReason: connection?.disconnectReason ?? null,
        lastMessageAt: connection?.lastMessageAt ?? null,
      },
    });
  }

  if (connection && connection.registration !== 'ACCEPTED') {
    findings.push({
      code: 'BOOT_NOT_ACCEPTED',
      severity: 'ATENCAO',
      message: 'BootNotification ainda nao confirmado para a sessao atual.',
      evidence: { registration: connection.registration, lastBootAt: connection.lastBootAt },
    });
  }

  switch (heartbeat.classification) {
    case 'NORMAL':
      findings.push({
        code: 'HEARTBEAT_NORMAL',
        severity: 'OK',
        message: 'Heartbeats chegando dentro do intervalo negociado.',
        evidence: {
          negotiatedIntervalSeconds: heartbeat.negotiatedIntervalSeconds,
          secondsSinceLastHeartbeat: heartbeat.secondsSinceLastHeartbeat,
        },
      });
      break;
    case 'ATRASADO':
      findings.push({
        code: 'HEARTBEAT_DELAYED',
        severity: 'ATENCAO',
        message: 'Heartbeat atrasado em relacao ao intervalo negociado.',
        evidence: {
          negotiatedIntervalSeconds: heartbeat.negotiatedIntervalSeconds,
          secondsSinceLastHeartbeat: heartbeat.secondsSinceLastHeartbeat,
          delayedCount: heartbeat.delayedCount,
        },
      });
      break;
    case 'CRITICO':
      findings.push({
        code: 'HEARTBEAT_CRITICAL',
        severity: 'CRITICO',
        message: 'Heartbeat muito atrasado (possivel degradacao de rede/firmware).',
        evidence: {
          negotiatedIntervalSeconds: heartbeat.negotiatedIntervalSeconds,
          secondsSinceLastHeartbeat: heartbeat.secondsSinceLastHeartbeat,
          delayedCount: heartbeat.delayedCount,
        },
      });
      break;
    case 'SEM_DADOS':
      findings.push({
        code: 'HEARTBEAT_NO_DATA',
        severity: connection?.transport === 'CONNECTED' ? 'ATENCAO' : 'CRITICO',
        message: 'Nenhum Heartbeat recebido desde o inicio da observacao.',
        evidence: { lastOcppMessageAt: heartbeat.lastOcppMessageAt },
      });
      break;
    case 'OFFLINE':
      findings.push({
        code: 'HEARTBEAT_OFFLINE',
        severity: 'CRITICO',
        message: 'Carregador offline (criterio: estado do socket, NAO ausencia de heartbeat isolada).',
        evidence: {
          lastHeartbeatAt: heartbeat.lastHeartbeatAt,
          lastOcppMessageAt: heartbeat.lastOcppMessageAt,
        },
      });
      break;
  }

  if (protocolErrors.length > 0) {
    findings.push({
      code: 'PROTOCOL_ERRORS',
      severity: 'ATENCAO',
      message: 'Mensagens rejeitadas/erros de protocolo registrados.',
      evidence: { count: protocolErrors.length, samples: protocolErrors.slice(0, 5).map((m) => m.id) },
    });
  }

  if (connection && connection.reconnectCount > 3) {
    findings.push({
      code: 'FREQUENT_RECONNECTIONS',
      severity: 'ATENCAO',
      message: 'Reconexoes frequentes detectadas.',
      evidence: { reconnectCount: connection.reconnectCount },
    });
  }

  return {
    chargePointId,
    generatedAt: now.toISOString(),
    connection,
    heartbeat,
    lastMessageAt: connection?.lastMessageAt ?? heartbeat.lastOcppMessageAt,
    lastHeartbeatAt: heartbeat.lastHeartbeatAt,
    negotiatedIntervalSeconds: heartbeat.negotiatedIntervalSeconds,
    recentMessageStats,
    lastMessages: lastMessages.slice(0, 20),
    protocolErrors: protocolErrors.slice(0, 20),
    severity: worst(findings),
    findings,
  };
}
