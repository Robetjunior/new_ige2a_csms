/**
 * Sincronizador runtime -> FleetRegistry (plano de reconexao, passo 3).
 *
 * Sem este componente, a identidade da frota so era escrita no startup
 * (`seedFleet`) e a persistencia gravava para sempre os valores semeados — em
 * particular o `reconnectCount`, que nunca refletia as reconexoes reais. O
 * resultado era a perda do historico de reconexoes a cada restart.
 *
 * Aqui a IDENTIDADE e o ESTADO OPERACIONAL observados em runtime sao copiados
 * para o FleetRegistry a cada evento de conexao (attempt/accepted/rejected/
 * closed/watchdog/pong/error) e a cada mudanca de estado do carregador
 * (BootNotification, StatusNotification, MeterValues). A persistencia apenas
 * le `FleetRegistry.snapshot()`.
 *
 * Garantias:
 *  - nao altera o protocolo: e um observador puro (listeners nunca lancam);
 *  - nao marca ninguem como ONLINE (o FleetRegistry nao tem essa nocao);
 *  - o `seed` do FleetRegistry e acumulativo: campos ausentes/null nao apagam
 *    o que ja era conhecido.
 */
import type { FleetRegistry, FleetSeed } from './fleetRegistry';
import type { ConnectionManager } from '../transport/connectionManager';
import type { ChargerStateService } from '../chargers/chargerState';

export interface FleetStateSyncerDeps {
  readonly fleet: FleetRegistry;
  readonly connections: ConnectionManager;
  readonly chargers: ChargerStateService;
  /** Fonte do intervalo negociado (HeartbeatMonitor.getNegotiatedInterval). */
  readonly heartbeat: { getNegotiatedInterval(chargePointId: string): number };
}

export class FleetStateSyncer {
  private readonly deps: FleetStateSyncerDeps;
  private readonly unsubscribers: Array<() => void> = [];

  constructor(deps: FleetStateSyncerDeps) {
    this.deps = deps;
  }

  /** Passa a observar conexoes e mudancas de estado do carregador. Idempotente. */
  start(): void {
    if (this.unsubscribers.length > 0) return;
    this.unsubscribers.push(
      this.deps.connections.subscribe((event) => {
        if (event.chargePointId) this.sync(event.chargePointId);
      }),
    );
    this.unsubscribers.push(
      this.deps.chargers.subscribe((chargePointId) => {
        if (chargePointId) this.sync(chargePointId);
      }),
    );
  }

  stop(): void {
    for (const unsubscribe of this.unsubscribers.splice(0)) {
      try {
        unsubscribe();
      } catch {
        /* observador nunca deve interromper o encerramento */
      }
    }
  }

  /** Copia o estado corrente de UM carregador para o registro da frota. */
  sync(chargePointId: string): void {
    this.deps.fleet.seed(this.buildSeed(chargePointId));
  }

  /** Copia o estado corrente de TODOS os carregadores conhecidos. */
  syncAll(): void {
    const ids = new Set<string>([
      ...this.deps.fleet.ids(),
      ...this.deps.connections.snapshot().map((c) => c.chargePointId),
      ...this.deps.chargers.snapshot().map((c) => c.chargePointId),
    ]);
    for (const id of ids) this.sync(id);
  }

  private buildSeed(chargePointId: string): FleetSeed {
    const conn = this.deps.connections.get(chargePointId);
    const charger = this.deps.chargers.get(chargePointId);
    return {
      chargePointId,
      vendor: conn?.vendor ?? charger?.vendor ?? null,
      model: conn?.model ?? charger?.model ?? null,
      serialNumber: charger?.serialNumber ?? null,
      firmwareVersion: conn?.firmwareVersion ?? charger?.firmwareVersion ?? null,
      negotiatedProtocol: conn?.negotiatedProtocol ?? null,
      negotiatedIntervalSeconds: this.deps.heartbeat.getNegotiatedInterval(chargePointId),
      lastConnectedAt: conn?.connectedAt ?? null,
      lastBootAt: conn?.lastBootAt ?? null,
      lastHeartbeatAt: conn?.lastHeartbeatAt ?? null,
      lastPongAt: conn?.lastPongAt ?? null,
      lastMessageAt: conn?.lastMessageAt ?? null,
      lastDisconnectReason: conn?.disconnectReason ?? null,
      // `reconnectCount` e um numero obrigatorio no FleetEntry: so e enviado
      // quando o runtime conhece o carregador (undefined nao sobrescreve).
      reconnectCount: conn?.reconnectCount,
      bootAt: charger?.bootAt ?? null,
      lastStatusAt: charger?.lastStatusAt ?? null,
      connectors: charger ? Array.from(charger.connectors.values()).map((c) => ({ ...c })) : undefined,
    };
  }
}
