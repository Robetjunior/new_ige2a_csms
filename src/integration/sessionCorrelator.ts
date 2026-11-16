/**
 * Correlaciona as transacoes ativas observadas pelo ChargerStateService com a
 * sessao de integracao (`integration_charge_session`).
 *
 * O modulo NAO altera o OCPP: ele apenas CONSOME superficies publicas ja
 * existentes (spec §4.5):
 *   - `chargers.subscribe(...)` + `chargers.get(...).activeTransactions`;
 *   - `messageLog.subscribe(...)`, exatamente como o SSE faz hoje (routes.ts).
 *
 * Por que os DOIS sinais: `ChargerStateService.startTransaction`/
 * `stopTransaction` NAO chamam `notify()` (so seedIdentity, registerBoot,
 * setConnectorStatus e recordMeterReadings o fazem). Logo, `chargers.subscribe`
 * sozinho nao revela a abertura/fecho da transacao. O `messageLog` emite um
 * registro para cada frame OCPP; quando chega um `StartTransaction`/
 * `StopTransaction`, reavaliamos o estado do carregador daquele ponto. O frame
 * pos-handler (CALLRESULT/re-notificacao) ja encontra o estado atualizado, e a
 * idempotencia (`lastLinked`) garante um unico link/stop por transicao.
 *
 * Observabilidade nunca pode interromper o OCPP: erros no listener sao
 * capturados e logados, e as chamadas ao repositorio sao fire-and-forget.
 */
export interface SessionCorrelatorChargers {
  subscribe(listener: (chargePointId: string) => void): () => void;
  get(chargePointId: string): { activeTransactions: Map<number, number> } | undefined;
}

/** Recorte do `OcppMessageLog` relevante para a correlacao. */
export interface SessionCorrelatorMessage {
  chargePointId: string;
  action: string | null;
}

/** Superficie equivalente a `OcppMessageLogEngine.subscribe(...)`. */
export interface SessionCorrelatorMessages {
  subscribe(listener: (entry: SessionCorrelatorMessage) => void): () => void;
}

export interface SessionCorrelatorRepository {
  linkOcppTransaction(chargePointId: string, connectorId: number, ocppTransactionId: number): Promise<void>;
  markSessionStopped(chargePointId: string, connectorId: number): Promise<void>;
}

export interface SessionCorrelatorDeps {
  chargers: SessionCorrelatorChargers;
  messages: SessionCorrelatorMessages;
  repository: SessionCorrelatorRepository;
  log?: (msg: string, meta?: unknown) => void;
}

/** Acoes OCPP que alteram `activeTransactions` e, portanto, interessam. */
const CORRELATED_ACTIONS = new Set(['StartTransaction', 'StopTransaction']);

export class SessionCorrelator {
  /** `${chargePointId}|${connectorId}` -> ultimo ocppTransactionId vinculado. */
  private readonly lastLinked = new Map<string, number>();
  private unsubscribers: Array<() => void> = [];

  constructor(private readonly deps: SessionCorrelatorDeps) {}

  /**
   * Assina o estado dos carregadores E o log de mensagens OCPP. Idempotente:
   * uma segunda chamada nao duplica assinaturas.
   */
  start(): void {
    if (this.unsubscribers.length > 0) return;
    this.unsubscribers.push(
      this.deps.chargers.subscribe((chargePointId) => this.onChargerChanged(chargePointId)),
    );
    this.unsubscribers.push(this.deps.messages.subscribe((entry) => this.onMessage(entry)));
  }

  /** Cancela as assinaturas. Idempotente. */
  stop(): void {
    for (const unsubscribe of this.unsubscribers.splice(0)) {
      try {
        unsubscribe();
      } catch {
        /* observador nunca deve interromper o encerramento */
      }
    }
  }

  /** Quantidade de transacoes atualmente correlacionadas (para teste). */
  linkedCount(): number {
    return this.lastLinked.size;
  }

  private key(chargePointId: string, connectorId: number): string {
    return `${chargePointId}|${connectorId}`;
  }

  /**
   * Reavalia o carregador quando o log registra um StartTransaction/
   * StopTransaction. Demais acoes (MeterValues, Heartbeat, ...) sao ignoradas
   * para nao reavaliar o estado a cada frame.
   */
  private onMessage(entry: SessionCorrelatorMessage): void {
    try {
      if (!entry.action || !CORRELATED_ACTIONS.has(entry.action)) return;
      this.onChargerChanged(entry.chargePointId);
    } catch (err) {
      this.deps.log?.(`[integration] erro ao correlacionar mensagem de ${entry.chargePointId}`, err);
    }
  }

  private onChargerChanged(chargePointId: string): void {
    try {
      const activeTransactions = this.deps.chargers.get(chargePointId)?.activeTransactions ?? new Map<number, number>();

      // Novos ou alterados: vincula apenas quando o valor muda em relacao ao
      // ultimo visto, para nao repetir o link a cada notificacao.
      for (const [connectorId, ocppTransactionId] of activeTransactions.entries()) {
        const key = this.key(chargePointId, connectorId);
        if (this.lastLinked.get(key) !== ocppTransactionId) {
          this.lastLinked.set(key, ocppTransactionId);
          this.fireLink(chargePointId, connectorId, ocppTransactionId);
        }
      }

      // Removidos: transacao que existia no ultimo snapshot e sumiu.
      const prefix = `${chargePointId}|`;
      for (const key of Array.from(this.lastLinked.keys())) {
        if (!key.startsWith(prefix)) continue;
        const connectorId = Number(key.slice(prefix.length));
        if (!activeTransactions.has(connectorId)) {
          this.lastLinked.delete(key);
          this.fireStop(chargePointId, connectorId);
        }
      }
    } catch (err) {
      // Nunca deixa o erro escapar para o listener do OCPP.
      this.deps.log?.(`[integration] erro ao correlacionar ${chargePointId}`, err);
    }
  }

  private fireLink(chargePointId: string, connectorId: number, ocppTransactionId: number): void {
    try {
      void Promise.resolve(this.deps.repository.linkOcppTransaction(chargePointId, connectorId, ocppTransactionId)).catch(
        (err) => this.deps.log?.(`[integration] falha ao vincular ${chargePointId}/${connectorId}`, err),
      );
    } catch (err) {
      this.deps.log?.(`[integration] falha ao vincular ${chargePointId}/${connectorId}`, err);
    }
  }

  private fireStop(chargePointId: string, connectorId: number): void {
    try {
      void Promise.resolve(this.deps.repository.markSessionStopped(chargePointId, connectorId)).catch((err) =>
        this.deps.log?.(`[integration] falha ao encerrar ${chargePointId}/${connectorId}`, err),
      );
    } catch (err) {
      this.deps.log?.(`[integration] falha ao encerrar ${chargePointId}/${connectorId}`, err);
    }
  }
}
