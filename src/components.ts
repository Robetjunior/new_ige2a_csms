/**
 * Agregador dos componentes do CSMS. Permite que camadas superiores (HTTP,
 * diagnostico, persistencia) dependam de uma unica interface, sem conhecer a
 * ordem de construcao.
 */
import type { AppConfig } from './config/env';
import type { ChargerStateService } from './chargers/chargerState';
import type { HeartbeatMonitor } from './heartbeat/heartbeatMonitor';
import type { OcppMessageLogEngine } from './logging/messageLogEngine';
import type { ConnectionManager } from './transport/connectionManager';
import type { OcppWebSocketServer } from './transport/ocppWebSocketServer';
import type { AvailabilityProbeService } from './availability/availabilityProbe';
import type { FleetRegistry } from './fleet/fleetRegistry';
import type { ReconnectionWatcher } from './fleet/reconnectionWatcher';
import type { FleetStateSyncer } from './fleet/fleetStateSyncer';

export interface CsmsComponents {
  readonly config: AppConfig;
  readonly chargers: ChargerStateService;
  readonly heartbeat: HeartbeatMonitor;
  readonly messageLog: OcppMessageLogEngine;
  readonly connections: ConnectionManager;
  readonly wsServer: OcppWebSocketServer;
  readonly availabilityProbe: AvailabilityProbeService;
  /** Frota conhecida (identidade para reconhecer a reconexao). */
  readonly fleet: FleetRegistry;
  /** Deteccao de "carregador conhecido que nao voltou". */
  readonly reconnectionWatcher: ReconnectionWatcher;
  /** Mantem a identidade/estado operacional da frota atualizados em runtime. */
  readonly fleetStateSyncer: FleetStateSyncer;
}
