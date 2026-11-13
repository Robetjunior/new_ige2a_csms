/* Central de Monitoramento OCPP - consome REST + SSE do CSMS. Sem build. */
'use strict';

const MAX_MESSAGES = 2000;
const OVERVIEW_REFRESH_MS = 10_000;
const SSE_RETRY_BASE_MS = 1_000;
const COMMON_ACTIONS = [
  'Reset',
  'RemoteStartTransaction',
  'RemoteStopTransaction',
  'UnlockConnector',
  'ChangeAvailability',
  'ChangeConfiguration',
  'GetConfiguration',
  'GetDiagnostics',
  'TriggerMessage',
];

const state = {
  messages: [], // mais recente primeiro
  byId: new Map(),
  rowEls: new Map(),
  chargers: new Map(),
  heartbeat: new Map(),
  fleetSummary: null,
  selected: null,
  source: null,
  retryMs: SSE_RETRY_BASE_MS,
};

const $ = (id) => document.getElementById(id);

/* --- API key ------------------------------------------------------------ */
function apiKey() {
  return (localStorage.getItem('csms_api_key') || '').trim();
}
function apiHeaders(extra) {
  const key = apiKey();
  return Object.assign(key ? { 'x-api-key': key } : {}, extra || {});
}
async function apiFetch(path, opts) {
  const res = await fetch(path, Object.assign({ headers: apiHeaders() }, opts || {}));
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

/* --- Helpers ------------------------------------------------------------ */
function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}
function badge(text, cls) {
  return el('span', `badge ${cls}`, text);
}
/* Horario exibido sempre no fuso do servidor (America/Sao_Paulo), e nao no fuso
   do navegador: garante que o que a UI mostra coincida com o `observedAtLocal`
   gravado pela API/persistencia (definido por TZ no .env). */
const DISPLAY_TIME_ZONE = 'America/Sao_Paulo';
const displayTimeFormat = new Intl.DateTimeFormat('en-GB', {
  timeZone: DISPLAY_TIME_ZONE,
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  fractionalSecondDigits: 3,
  hourCycle: 'h23',
});
function fmtTime(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '—';
  const parts = {};
  for (const part of displayTimeFormat.formatToParts(d)) parts[part.type] = part.value;
  return `${parts.hour}:${parts.minute}:${parts.second}.${parts.fractionalSecond}`;
}
function timeAgo(iso) {
  if (!iso) return '—';
  const ms = Date.now() - new Date(iso).getTime();
  if (Number.isNaN(ms)) return '—';
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 60) return `${s}s atrás`;
  if (s < 3600) return `${Math.floor(s / 60)}min atrás`;
  if (s < 86400) return `${Math.floor(s / 3600)}h atrás`;
  return `${Math.floor(s / 86400)}d atrás`;
}
function classificationClass(c) {
  if (c === 'NORMAL') return 'badge-ok';
  if (c === 'ATRASADO') return 'badge-warn';
  if (c === 'CRITICO' || c === 'OFFLINE') return 'badge-crit';
  return 'badge-muted';
}
function summaryOf(m) {
  const parts = [];
  if (m.direction === 'received') parts.push('⬇');
  else parts.push('⬆');
  parts.push(m.chargePointId);
  if (m.latencyMs !== null && m.latencyMs !== undefined) parts.push(`lat ${m.latencyMs}ms`);
  if (m.parseError) parts.push(`parse:${m.parseError}`);
  if (m.errorCode) parts.push(m.errorCode);
  return parts.join(' · ');
}

/* --- Filtros ------------------------------------------------------------ */
function currentFilters() {
  return {
    chargePointId: $('f-charger').value || undefined,
    direction: $('f-direction').value || undefined,
    messageType: $('f-type').value || undefined,
    action: $('f-action').value.trim() || undefined,
    search: $('f-search').value.trim() || undefined,
    onlyErrors: $('f-errors').checked || undefined,
  };
}
function matchesFilters(m) {
  const f = currentFilters();
  if (f.chargePointId && m.chargePointId !== f.chargePointId) return false;
  if (f.direction && m.direction !== f.direction) return false;
  if (f.messageType && m.messageType !== f.messageType) return false;
  if (f.onlyErrors && m.status !== 'failed') return false;
  if (f.action && !(m.action || '').toLowerCase().includes(f.action.toLowerCase())) return false;
  if (f.search) {
    const hay = `${m.id} ${m.uniqueId || ''} ${m.action || ''} ${m.chargePointId} ${JSON.stringify(m.payload)}`.toLowerCase();
    if (!hay.includes(f.search.toLowerCase())) return false;
  }
  return true;
}

/* --- Terminal ----------------------------------------------------------- */
function buildRow(m) {
  const row = el('div', `line ${m.status === 'failed' ? 'failed' : ''}`);
  row.dataset.id = m.id;
  row.appendChild(el('span', 'ts', fmtTime(m.observedAt)));
  row.appendChild(el('span', `dir ${m.direction}`, m.direction === 'received' ? '⬇ IN' : '⬆ OUT'));
  row.appendChild(el('span', 'type', `${m.messageType}${m.messageTypeId ? '' : ''}`));
  row.appendChild(el('span', 'action', m.action || '—'));
  const summary = el('span', 'summary', summaryOf(m));
  row.appendChild(summary);

  row.title = 'Clique para ver o payload';
  row.addEventListener('click', () => {
    const open = row.querySelector('.payload');
    if (open) {
      open.remove();
      return;
    }
    const pre = el('pre', 'payload');
    pre.textContent = JSON.stringify(
      {
        id: m.id,
        connectionId: m.connectionId,
        uniqueId: m.uniqueId,
        messageTypeId: m.messageTypeId,
        observedAt: m.observedAt,
        observedAtLocal: m.observedAtLocal,
        processedAt: m.processedAt,
        respondedAt: m.respondedAt,
        latencyMs: m.latencyMs,
        status: m.status,
        errorCode: m.errorCode,
        errorMessage: m.errorMessage,
        messageBytes: m.messageBytes,
        parseError: m.parseError,
        payload: m.payload,
      },
      null,
      2,
    );
    row.appendChild(pre);
  });
  return row;
}

function updateRow(m) {
  const existing = state.rowEls.get(m.id);
  if (!existing) return;
  const replacement = buildRow(m);
  existing.replaceWith(replacement);
  state.rowEls.set(m.id, replacement);
}

function prependMessage(m) {
  const terminal = $('terminal');
  const empty = terminal.querySelector('.empty');
  if (empty) empty.remove();
  const atTop = terminal.scrollTop < 40;
  const row = buildRow(m);
  terminal.insertBefore(row, terminal.firstChild);
  state.rowEls.set(m.id, row);
  if ($('autoscroll').checked && atTop) terminal.scrollTop = 0;
  updateTerminalCount();
}

function ingestMessage(m) {
  if ($('pause').checked) return;
  const known = state.byId.get(m.id);
  if (known) {
    Object.assign(known, m); // reemissao com latencia/status preenchidos (upsert)
    if (matchesFilters(known)) updateRow(known);
    else {
      const elRow = state.rowEls.get(m.id);
      if (elRow) {
        elRow.remove();
        state.rowEls.delete(m.id);
      }
    }
    return;
  }
  state.byId.set(m.id, m);
  state.messages.unshift(m);
  while (state.messages.length > MAX_MESSAGES) {
    const dropped = state.messages.pop();
    if (!dropped) break;
    state.byId.delete(dropped.id);
    const row = state.rowEls.get(dropped.id);
    if (row) row.remove();
    state.rowEls.delete(dropped.id);
  }
  if (matchesFilters(m)) prependMessage(m);
}

function renderTerminal() {
  const terminal = $('terminal');
  terminal.textContent = '';
  state.rowEls.clear();
  const visible = state.messages.filter(matchesFilters);
  if (visible.length === 0) {
    terminal.appendChild(el('p', 'empty', 'Nenhuma mensagem para os filtros atuais.'));
    updateTerminalCount();
    return;
  }
  const frag = document.createDocumentFragment();
  for (const m of visible) {
    const row = buildRow(m);
    state.rowEls.set(m.id, row);
    frag.appendChild(row);
  }
  terminal.appendChild(frag);
  updateTerminalCount();
}

function updateTerminalCount() {
  $('terminal-count').textContent = `${state.rowEls.size} visíveis / ${state.messages.length} em memória`;
}

/* --- Carregadores ------------------------------------------------------- */
function chargerCard(entry) {
  const id = entry.chargePointId;
  const conn = entry.connection;
  const hb = entry.heartbeat || state.heartbeat.get(id) || null;
  const charger = entry.charger || null;
  const card = el('div', `charger ${state.selected === id ? 'selected' : ''}`);
  card.dataset.id = id;

  const row1 = el('div', 'row1');
  row1.appendChild(el('span', 'id', id));
  row1.appendChild(badge(entry.connected ? 'ONLINE' : 'OFFLINE', entry.connected ? 'badge-on' : 'badge-crit'));
  card.appendChild(row1);

  const row2 = el('div', 'row2');
  if (hb) row2.appendChild(badge(`HB ${hb.classification}`, classificationClass(hb.classification)));
  if (conn) {
    row2.appendChild(badge(conn.transport, conn.transport === 'CONNECTED' ? 'badge-ok' : 'badge-muted'));
    row2.appendChild(badge(conn.registration, conn.registration === 'ACCEPTED' ? 'badge-ok' : 'badge-warn'));
    if (conn.usedFallback) row2.appendChild(badge('fallback subprotocolo', 'badge-warn'));
  }
  // Frota conhecida: destaca o carregador que NAO voltou apos a janela de tolerancia.
  const fleet = entry.fleet || null;
  if (fleet && fleet.status === 'AWAITING_RECONNECTION') {
    const secs = fleet.awaitingMs != null ? ` ${Math.round(fleet.awaitingMs / 1000)}s` : '';
    row2.appendChild(badge(`aguardando reconexão${secs}`, 'badge-crit'));
  }
  card.appendChild(row2);

  const meta = el('div', 'meta');
  if (hb) {
    meta.appendChild(metaItem('intervalo', `${hb.negotiatedIntervalSeconds}s`));
    meta.appendChild(metaItem('último HB', hb.lastHeartbeatAt ? timeAgo(hb.lastHeartbeatAt) : 'nunca'));
    meta.appendChild(metaItem('HBs', hb.heartbeatCount));
    meta.appendChild(metaItem('atrasados', hb.delayedCount));
    if (hb.averageLatencyMs !== null) meta.appendChild(metaItem('lat.média', `${hb.averageLatencyMs}ms`));
  }
  if (conn) meta.appendChild(metaItem('última msg', conn.lastMessageAt ? timeAgo(conn.lastMessageAt) : '—'));
  if (fleet) {
    meta.appendChild(metaItem('frota', fleet.status));
    if (fleet.lastSeenAt) meta.appendChild(metaItem('visto há', timeAgo(fleet.lastSeenAt)));
  }
  const connectors = charger?.connectors || [];
  for (const connector of connectors.slice(0, 3)) {
    meta.appendChild(metaItem(`conector ${connector.connectorId}`, `${connector.status} / ${connector.errorCode}`));
  }
  const readings = charger?.meterReadings || [];
  for (const measurand of ['Power.Active.Import', 'Temperature']) {
    const reading = [...readings].reverse().find((item) => item.measurand === measurand);
    if (reading) {
      meta.appendChild(metaItem(measurand, `${reading.value}${reading.unit ? ` ${reading.unit}` : ''}`));
    }
  }
  card.appendChild(meta);

  card.addEventListener('click', () => selectCharger(id));
  return card;
}
function metaItem(label, value) {
  const span = el('span');
  span.appendChild(document.createTextNode(`${label} `));
  span.appendChild(el('b', null, String(value)));
  return span;
}

function renderChargers() {
  const container = $('chargers');
  container.textContent = '';
  const entries = Array.from(state.chargers.values()).sort((a, b) => a.chargePointId.localeCompare(b.chargePointId));
  $('chargers-count').textContent = `${entries.length}`;
  if (entries.length === 0) {
    container.appendChild(el('p', 'empty', 'Nenhum carregador conectado ainda.'));
    return;
  }
  for (const entry of entries) container.appendChild(chargerCard(entry));
}

function renderKpis() {
  const stats = state.stats || { received: 0, sent: 0, errors: 0, buffered: 0 };
  const connected = Array.from(state.chargers.values()).filter((c) => c.connected).length;
  const fleet = state.fleetSummary;
  const awaitingReconnection = fleet
    ? fleet.awaitingReconnection
    : Array.from(state.chargers.values()).filter((c) => c.fleet && c.fleet.status === 'AWAITING_RECONNECTION').length;
  const kpis = [
    { label: 'Carregadores online', value: connected },
    { label: 'Frota aguardando reconexão', value: awaitingReconnection, crit: awaitingReconnection > 0 },
    { label: 'Mensagens recebidas', value: stats.received },
    { label: 'Mensagens enviadas', value: stats.sent },
    { label: 'Erros / CALLERROR', value: stats.errors, crit: stats.errors > 0 },
    { label: 'Em buffer', value: stats.buffered },
  ];
  const container = $('kpis');
  container.textContent = '';
  for (const k of kpis) {
    const box = el('div', 'kpi');
    box.appendChild(el('div', 'label', k.label));
    box.appendChild(el('div', `value${k.crit ? ' crit' : ''}`, k.value));
    container.appendChild(box);
  }
}

/* --- Diagnostico / detalhe --------------------------------------------- */
async function selectCharger(id) {
  state.selected = id;
  renderChargers();
  $('detail-title').textContent = `Diagnóstico — ${id}`;
  const detail = $('detail');
  detail.textContent = '';
  detail.appendChild(el('p', 'empty', 'carregando...'));
  try {
    const [diagnosis, full] = await Promise.all([
      apiFetch(`/api/chargers/${encodeURIComponent(id)}/diagnosis`),
      apiFetch(`/api/chargers/${encodeURIComponent(id)}`),
    ]);
    renderDetail(id, diagnosis, full);
  } catch (err) {
    detail.textContent = '';
    detail.appendChild(el('p', 'empty', `Falha ao carregar diagnóstico: ${err.message}`));
  }
}

function renderDetail(id, diagnosis, full) {
  const detail = $('detail');
  detail.textContent = '';
  $('detail-sub').textContent = `severidade: ${diagnosis.severity}`;

  const findings = el('div', 'findings');
  for (const f of diagnosis.findings || []) {
    const node = el('div', `finding ${f.severity}`);
    node.appendChild(el('div', 'code', `${f.severity} · ${f.code}`));
    node.appendChild(el('div', 'msg', f.message));
    findings.appendChild(node);
  }
  detail.appendChild(findings);

  const charger = full.charger;
  detail.appendChild(el('h3', null, 'estado operacional reportado'));
  if (charger) {
    const statuses = el('div', 'probe-statuses');
    statuses.appendChild(badge(full.online ? 'WebSocket conectado' : 'WebSocket desconectado', full.online ? 'badge-ok' : 'badge-crit'));
    for (const connector of charger.connectors || []) {
      statuses.appendChild(
        badge(
          `conector ${connector.connectorId}: ${connector.status} · ${connector.errorCode}`,
          connector.errorCode === 'NoError' ? 'badge-ok' : 'badge-crit',
        ),
      );
    }
    detail.appendChild(statuses);
    const readings = charger.meterReadings || [];
    if (readings.length) {
      const list = el('div', 'probe-readings');
      for (const reading of readings) {
        list.appendChild(
          el(
            'div',
            'probe-reading',
            `conector ${reading.connectorId} · ${reading.measurand}: ${reading.value}${reading.unit ? ` ${reading.unit}` : ''} · ${fmtTime(reading.timestamp)}`,
          ),
        );
      }
      detail.appendChild(list);
    } else {
      detail.appendChild(el('p', 'empty', 'Nenhum MeterValues recebido ainda; potência e temperatura dependem do suporte/configuração do carregador.'));
    }
  } else {
    detail.appendChild(el('p', 'empty', 'Aguardando BootNotification e mensagens operacionais.'));
  }

  const probe = full.availabilityProbe;
  detail.appendChild(el('h3', null, 'sondagem TriggerMessage'));
  const probeSummary = el('div', 'probe-toolbar');
  probeSummary.appendChild(
    el(
      'span',
      null,
      probe?.enabled
        ? `Automática a cada ${Math.round(probe.intervalMs / 1000)}s · timeout ${probe.timeoutMs}ms`
        : 'Sondagem automática desabilitada',
    ),
  );
  const probeButton = el('button', 'btn', probe?.running ? 'sondagem em andamento...' : 'Sondar agora');
  probeButton.disabled = !full.online || Boolean(probe?.running);
  const probeResult = el('span', 'hint');
  probeButton.addEventListener('click', async () => {
    probeButton.disabled = true;
    probeResult.textContent = 'solicitando...';
    try {
      await apiFetch(`/api/chargers/${encodeURIComponent(id)}/probe`, { method: 'POST' });
      probeResult.textContent = 'sondagem iniciada';
    } catch (err) {
      probeResult.textContent = `falha: ${err.message}`;
    } finally {
      setTimeout(() => {
        probeButton.disabled = false;
        void selectCharger(id);
      }, 1000);
    }
  });
  probeSummary.appendChild(probeButton);
  probeSummary.appendChild(probeResult);
  // Sondagem DIRECIONADA: pede somente StatusNotification (TriggerMessage) e
  // devolve o estado dos conectores reportado agora pelo carregador.
  const statusButton = el('button', 'btn', 'Sondar status');
  statusButton.disabled = !full.online;
  const statusResult = el('span', 'hint');
  statusButton.addEventListener('click', async () => {
    statusButton.disabled = true;
    statusResult.textContent = 'solicitando StatusNotification...';
    try {
      const res = await apiFetch(`/api/chargers/${encodeURIComponent(id)}/probe-status`, { method: 'POST' });
      const action = res.probe && res.probe.outcome ? res.probe : null;
      statusResult.textContent = action ? `resultado: ${action.outcome}` : 'sem resposta (probe desabilitada/ocupada)';
    } catch (err) {
      statusResult.textContent = `falha: ${err.message}`;
    } finally {
      setTimeout(() => {
        statusButton.disabled = false;
        void selectCharger(id);
      }, 800);
    }
  });
  probeSummary.appendChild(statusButton);
  probeSummary.appendChild(statusResult);
  detail.appendChild(probeSummary);
  const probeActions = el('div', 'probe-statuses');
  for (const action of ['Heartbeat', 'StatusNotification', 'MeterValues']) {
    const result = probe?.actions?.[action];
    const label = result?.outcome || 'idle';
    const cls =
      label === 'received' ? 'badge-ok' : ['unsupported', 'rejected'].includes(label) ? 'badge-warn' : ['report_timeout', 'trigger_timeout', 'error', 'disconnected'].includes(label) ? 'badge-crit' : 'badge-muted';
    probeActions.appendChild(badge(`${action}: ${label}`, cls));
    if (result?.detail) probeActions.appendChild(el('span', 'hint', result.detail));
  }
  detail.appendChild(probeActions);

  const st = diagnosis.recentMessageStats;
  if (st) {
    detail.appendChild(el('h3', null, `mensagens recentes (${st.received} in / ${st.sent} out / ${st.errors} erros)`));
  }

  // Comandos CSMS -> carregador
  detail.appendChild(el('h3', null, 'enviar comando ao carregador'));
  const form = el('div', 'cmd-form');
  const select = el('select');
  for (const action of COMMON_ACTIONS) {
    const opt = el('option', null, action);
    opt.value = action;
    select.appendChild(opt);
  }
  const textarea = el('textarea');
  textarea.value = '{\n  \n}';
  const btn = el('button', 'btn primary', 'Enviar');
  const result = el('div', 'cmd-result');
  btn.addEventListener('click', async () => {
    let payload = {};
    try {
      payload = textarea.value.trim() ? JSON.parse(textarea.value) : {};
    } catch {
      result.textContent = 'payload JSON inválido';
      return;
    }
    btn.disabled = true;
    result.textContent = 'enviando...';
    try {
      const res = await apiFetch(`/api/chargers/${encodeURIComponent(id)}/commands`, {
        method: 'POST',
        headers: apiHeaders({ 'content-type': 'application/json' }),
        body: JSON.stringify({ action: select.value, payload }),
      });
      result.textContent = JSON.stringify(res, null, 2);
    } catch (err) {
      result.textContent = `falha: ${err.message}`;
    } finally {
      btn.disabled = false;
    }
  });
  form.appendChild(select);
  form.appendChild(textarea);
  form.appendChild(btn);
  form.appendChild(result);
  detail.appendChild(form);

  // Ultimas mensagens do carregador
  detail.appendChild(el('h3', null, 'últimas mensagens'));
  const list = el('div');
  for (const m of (full.recentMessages || []).slice(0, 20)) {
    list.appendChild(buildRow(m));
  }
  detail.appendChild(list);
}

/* --- Export ------------------------------------------------------------- */
function exportQuery(format) {
  const f = currentFilters();
  const params = new URLSearchParams();
  params.set('format', format);
  params.set('limit', '5000');
  for (const [key, value] of Object.entries(f)) {
    if (value === undefined || value === false || value === '') continue;
    params.set(key, value === true ? '1' : String(value));
  }
  return params.toString();
}

async function download(format) {
  try {
    const res = await fetch(`/api/messages/export?${exportQuery(format)}`, { headers: apiHeaders() });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `ocpp-messages.${format}`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  } catch (err) {
    alert(`Falha ao exportar: ${err.message}`);
  }
}

/* --- Estado / tempo real (SSE) ----------------------------------------- */
function setStreamState(connected) {
  const node = $('stream-state');
  node.textContent = connected ? 'tempo real ativo' : 'desconectado';
  node.className = `badge ${connected ? 'badge-on' : 'badge-off'}`;
}

function applyOverview(payload) {
  if (Array.isArray(payload.chargers)) {
    // Preserva o status de frota recebido por SSE quando o snapshot nao o traz.
    for (const entry of payload.chargers) {
      if (!entry.fleet) {
        const previous = state.chargers.get(entry.chargePointId);
        if (previous && previous.fleet) entry.fleet = previous.fleet;
      }
    }
    state.chargers = new Map(payload.chargers.map((c) => [c.chargePointId, c]));
  }
  if (Array.isArray(payload.fleet) && payload.chargers === undefined) {
    for (const member of payload.fleet) {
      const entry = state.chargers.get(member.chargePointId);
      if (entry) entry.fleet = member;
    }
  }
  if (payload.fleetSummary) state.fleetSummary = payload.fleetSummary;
  if (Array.isArray(payload.heartbeat)) {
    state.heartbeat = new Map(payload.heartbeat.map((h) => [h.chargePointId, h]));
    for (const entry of state.chargers.values()) {
      const hb = state.heartbeat.get(entry.chargePointId);
      if (hb) entry.heartbeat = hb;
    }
  }
  if (payload.messageStats) state.stats = payload.messageStats;
  syncChargerFilter();
  renderKpis();
  renderChargers();
}

function syncChargerFilter() {
  const select = $('f-charger');
  const current = select.value;
  const ids = Array.from(state.chargers.keys()).sort();
  const known = Array.from(select.options).map((o) => o.value);
  for (const id of ids) {
    if (!known.includes(id)) {
      const opt = el('option', null, id);
      opt.value = id;
      select.appendChild(opt);
    }
  }
  if (current && !ids.includes(current)) select.value = '';
}

async function refreshOverview() {
  try {
    const data = await apiFetch('/api/chargers');
    applyOverview(data);
  } catch {
    /* ignora falha pontual */
  }
}

async function loadRecentMessages() {
  try {
    const data = await apiFetch('/api/messages?limit=500');
    for (const m of (data.messages || []).reverse()) ingestMessage(m); // mais antigo -> mais novo
    if (data.messages) {
      state.stats = data.stats;
      renderKpis();
      renderTerminal();
    }
  } catch {
    /* ignora */
  }
}

function connectStream() {
  if (state.source) state.source.close();
  const key = apiKey();
  const url = `/api/stream${key ? `?api_key=${encodeURIComponent(key)}` : ''}`;
  const source = new EventSource(url);
  state.source = source;

  source.addEventListener('open', () => {
    state.retryMs = SSE_RETRY_BASE_MS;
    setStreamState(true);
  });
  source.addEventListener('error', () => {
    setStreamState(false);
    source.close();
    state.source = null;
    const delay = Math.min(state.retryMs, 15000);
    state.retryMs = Math.min(state.retryMs * 2, 15000);
    setTimeout(connectStream, delay);
  });
  source.addEventListener('snapshot', (ev) => {
    try {
      applyOverview(JSON.parse(ev.data));
    } catch {
      /* noop */
    }
  });
  source.addEventListener('ocpp-message', (ev) => {
    try {
      ingestMessage(JSON.parse(ev.data));
    } catch {
      /* noop */
    }
  });
  source.addEventListener('connection', () => {
    void refreshOverview();
  });
  source.addEventListener('charger-state', (ev) => {
    try {
      const update = JSON.parse(ev.data);
      const entry = state.chargers.get(update.chargePointId);
      if (entry) entry.charger = update.charger;
      renderChargers();
      if (state.selected === update.chargePointId) void selectCharger(update.chargePointId);
    } catch {
      /* noop */
    }
  });
  source.addEventListener('availability-probe', (ev) => {
    try {
      const update = JSON.parse(ev.data);
      const entry = state.chargers.get(update.snapshot.chargePointId);
      if (entry) entry.availabilityProbe = update.snapshot;
      renderChargers();
      if (state.selected === update.snapshot.chargePointId && update.event.event === 'cycle_completed') {
        void selectCharger(update.snapshot.chargePointId);
      }
    } catch {
      /* noop */
    }
  });
  source.addEventListener('fleet-status', (ev) => {
    try {
      const update = JSON.parse(ev.data);
      const entry = state.chargers.get(update.chargePointId);
      if (entry) {
        entry.fleet = Object.assign({}, entry.fleet || {}, {
          chargePointId: update.chargePointId,
          status: update.status,
          awaitingSince: update.awaitingSince,
          lastSeenAt: update.metadata ? update.metadata.lastSeenAt : (entry.fleet || {}).lastSeenAt,
        });
      }
      renderChargers();
      renderKpis();
    } catch {
      /* noop */
    }
  });
  source.addEventListener('heartbeat', (ev) => {
    try {
      const list = JSON.parse(ev.data);
      state.heartbeat = new Map(list.map((h) => [h.chargePointId, h]));
      for (const entry of state.chargers.values()) {
        const hb = state.heartbeat.get(entry.chargePointId);
        if (hb) entry.heartbeat = hb;
      }
      renderChargers();
    } catch {
      /* noop */
    }
  });
}

/* --- Bootstrap ---------------------------------------------------------- */
function bindUi() {
  $('api-key').value = apiKey();
  $('api-key').addEventListener('change', () => {
    localStorage.setItem('csms_api_key', $('api-key').value.trim());
    connectStream();
    void refreshOverview();
    void loadRecentMessages();
  });
  $('btn-reconnect').addEventListener('click', () => connectStream());
  $('clear-log').addEventListener('click', () => {
    state.messages = [];
    state.byId.clear();
    renderTerminal();
  });
  $('export-json').addEventListener('click', () => void download('json'));
  $('export-csv').addEventListener('click', () => void download('csv'));
  for (const id of ['f-charger', 'f-direction', 'f-type', 'f-errors']) {
    $(id).addEventListener('change', renderTerminal);
  }
  for (const id of ['f-action', 'f-search']) {
    $(id).addEventListener('input', debounce(renderTerminal, 250));
  }
  $('pause').addEventListener('change', () => {
    if (!$('pause').checked) void loadRecentMessages();
  });
}
function debounce(fn, ms) {
  let timer = null;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), ms);
  };
}

function start() {
  bindUi();
  $('terminal').appendChild(el('p', 'empty', 'aguardando mensagens...'));
  connectStream();
  void refreshOverview();
  void loadRecentMessages();
  setInterval(() => void refreshOverview(), OVERVIEW_REFRESH_MS);
  setInterval(() => {
    // Atualiza os "time ago" sem depender de novo evento.
    renderChargers();
  }, 5000);
}

document.addEventListener('DOMContentLoaded', start);
