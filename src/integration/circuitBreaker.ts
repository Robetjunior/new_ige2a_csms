/**
 * Circuit breaker do push de telemetria (F6 - secao 8.1/8.2 do documento de
 * arquitetura: "circuit breaker abre apos N falhas e sonda periodicamente").
 *
 * Estados:
 *   - `closed`: envio normal. Apos `failureThreshold` falhas CONSECUTIVAS abre;
 *   - `open`: envios bloqueados por `openMs`. O outbox permanece intacto
 *     (os eventos continuam `pending` e sao reenviados quando o circuito fecha),
 *     evitando martelar um ANTIGO indisponivel;
 *   - `half_open`: apos o cooldown, deixa passar ate `halfOpenMax` sondas. Uma
 *     falha reabre o circuito; um sucesso volta para `closed`.
 *
 * O breaker NAO substitui o backoff do dispatcher: ele e o gate global que evita
 * a avalanche de tentativas enquanto o destino esta fora. E deterministico e sem
 * I/O (relogio injetavel), para teste de queda.
 */

export type CircuitState = 'closed' | 'open' | 'half_open';

export interface CircuitBreakerOptions {
  /** Falhas consecutivas que abrem o circuito. */
  failureThreshold: number;
  /** Tempo (ms) que o circuito permanece aberto antes de sondar. */
  openMs: number;
  /** Sondas permitidas por janela de `half_open` (default 1). */
  halfOpenMax?: number;
  /** Relogio injetavel (deterministico em teste). */
  now?: () => number;
}

export class CircuitBreaker {
  private state: CircuitState = 'closed';
  private failures = 0;
  private openedAt = 0;
  private probes = 0;
  private readonly now: () => number;

  constructor(private readonly options: CircuitBreakerOptions) {
    this.now = options.now ?? (() => Date.now());
  }

  /** Estado atual (observabilidade/teste). */
  getState(): CircuitState {
    return this.state;
  }

  /** Falhas consecutivas acumuladas no estado atual. */
  getFailures(): number {
    return this.failures;
  }

  /**
   * Pergunta (SEM consumir sonda) se vale a pena tentar agora. Faz a transicao
   * `open` -> `half_open` quando o cooldown vence, mas NAO consome a sonda: a
   * consumacao fica em `allowAttempt()`. Usado para decidir se o lote deve ser
   * reivindicado, sem gastar a sonda do `half_open` antes de haver item a enviar.
   */
  canAttempt(): boolean {
    if (this.state === 'open') {
      if (this.now() - this.openedAt < this.options.openMs) return false;
      this.state = 'half_open';
      this.probes = 0;
    }
    if (this.state === 'half_open') {
      return this.probes < (this.options.halfOpenMax ?? 1);
    }
    return true;
  }

  /**
   * Pergunta se um novo envio pode ser tentado. Consome uma sonda quando o
   * circuito esta em `half_open`. `false` = envio deve ser adiado.
   */
  allowAttempt(): boolean {
    if (this.state === 'open') {
      if (this.now() - this.openedAt < this.options.openMs) return false;
      this.state = 'half_open';
      this.probes = 0;
    }
    if (this.state === 'half_open') {
      const max = this.options.halfOpenMax ?? 1;
      if (this.probes >= max) return false;
      this.probes += 1;
      return true;
    }
    return true;
  }

  /** Envio confirmado: fecha o circuito e zera os contadores. */
  onSuccess(): void {
    this.state = 'closed';
    this.failures = 0;
    this.probes = 0;
    this.openedAt = 0;
  }

  /** Envio falho: acumula no `closed`, ou reabre quando a falha e uma sonda. */
  onFailure(): void {
    if (this.state === 'half_open') {
      this.trip();
      return;
    }
    if (this.state === 'open') return;
    this.failures += 1;
    if (this.failures >= Math.max(1, this.options.failureThreshold)) {
      this.trip();
    }
  }

  private trip(): void {
    this.state = 'open';
    this.openedAt = this.now();
    this.probes = 0;
  }
}
