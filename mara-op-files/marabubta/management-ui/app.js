// Marabunta - Licensed under the MIT License.
// ============================================================================
// Marabunta Swarm Management Console — Application
// Pure ES2022, zero dependencies. Datadog/Grafana-tier management dashboard.
// ============================================================================

'use strict';

// ============================================================================
// ICONS — SVG icon library
// ============================================================================

const ICONS = {
    nodes: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="8" cy="4" r="2.5"/><circle cx="3" cy="12" r="2.5"/><circle cx="13" cy="12" r="2.5"/><line x1="8" y1="6.5" x2="3" y2="9.5"/><line x1="8" y1="6.5" x2="13" y2="9.5"/></svg>',
    jobs: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="2" width="12" height="12" rx="2"/><line x1="5" y1="6" x2="11" y2="6"/><line x1="5" y1="8.5" x2="11" y2="8.5"/><line x1="5" y1="11" x2="8" y2="11"/></svg>',
    events: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M2 3h12M2 6.5h12M2 10h12M2 13.5h12"/></svg>',
    alerts: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M8 1.5L14.5 13H1.5z"/><line x1="8" y1="6" x2="8" y2="9"/><circle cx="8" cy="11" r="0.5" fill="currentColor"/></svg>',
    fleet: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="1" y="3" width="6" height="4" rx="1"/><rect x="9" y="3" width="6" height="4" rx="1"/><rect x="1" y="9" width="6" height="4" rx="1"/><rect x="9" y="9" width="6" height="4" rx="1"/></svg>',
    sla: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="8" cy="8" r="6.5"/><path d="M8 4v4l3 2"/></svg>',
    capacity: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="8" width="3" height="6" rx="0.5"/><rect x="6.5" y="5" width="3" height="9" rx="0.5"/><rect x="11" y="2" width="3" height="12" rx="0.5"/></svg>',
    constellation: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="8" cy="3" r="2"/><circle cx="3" cy="11" r="2"/><circle cx="13" cy="11" r="2"/><line x1="8" y1="5" x2="4" y2="9.5"/><line x1="8" y1="5" x2="12" y2="9.5"/><line x1="5" y1="11" x2="11" y2="11"/></svg>',
    psyche: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="8" cy="8" r="6.5"/><path d="M8 3v5l4 2"/><circle cx="8" cy="8" r="1" fill="currentColor"/></svg>',
    audit: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M4 2h8a1 1 0 011 1v10a1 1 0 01-1 1H4a1 1 0 01-1-1V3a1 1 0 011-1z"/><path d="M6 5h4M6 7.5h4M6 10h2"/></svg>',
    metrics: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><polyline points="2,12 5,7 8,9 11,4 14,6"/></svg>',
    settings: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="8" cy="8" r="2.5"/><path d="M8 1v2M8 13v2M1 8h2M13 8h2M3 3l1.5 1.5M11.5 11.5L13 13M3 13l1.5-1.5M11.5 4.5L13 3"/></svg>',
    search: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="7" cy="7" r="4.5"/><line x1="10.5" y1="10.5" x2="14" y2="14"/></svg>',
    close: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><line x1="4" y1="4" x2="12" y2="12"/><line x1="12" y1="4" x2="4" y2="12"/></svg>',
    refresh: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M2 8a6 6 0 0110.5-4M14 8a6 6 0 01-10.5 4"/><polyline points="12,1 13,4 10,5"/><polyline points="4,15 3,12 6,11"/></svg>',
    check: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3,8 7,12 13,4"/></svg>',
    warning: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M8 1.5L14.5 13H1.5z"/><line x1="8" y1="6" x2="8" y2="9"/><circle cx="8" cy="11" r="0.5" fill="currentColor"/></svg>',
    error: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="8" cy="8" r="6.5"/><line x1="5.5" y1="5.5" x2="10.5" y2="10.5"/><line x1="10.5" y1="5.5" x2="5.5" y2="10.5"/></svg>',
    info: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="8" cy="8" r="6.5"/><line x1="8" y1="7" x2="8" y2="11.5"/><circle cx="8" cy="4.5" r="0.5" fill="currentColor"/></svg>',
    chevronRight: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6,3 11,8 6,13"/></svg>',
    chevronDown: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3,6 8,11 13,6"/></svg>',
    drain: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M8 2v8M5 7l3 3 3-3"/><line x1="3" y1="13" x2="13" y2="13"/></svg>',
    cordon: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="8" cy="8" r="6.5"/><line x1="3" y1="3" x2="13" y2="13"/></svg>',
    quarantine: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M8 1.5L14.5 8 8 14.5 1.5 8z"/><line x1="8" y1="5" x2="8" y2="8.5"/><circle cx="8" cy="10.5" r="0.5" fill="currentColor"/></svg>',
    download: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M8 2v8M5 7l3 3 3-3"/><path d="M2 12v2h12v-2"/></svg>',
    copy: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="5" y="5" width="8" height="8" rx="1"/><path d="M3 11V3a1 1 0 011-1h8"/></svg>',
    play: '<svg viewBox="0 0 16 16" width="16" height="16" fill="currentColor"><polygon points="4,2 14,8 4,14"/></svg>',
    pause: '<svg viewBox="0 0 16 16" width="16" height="16" fill="currentColor"><rect x="3" y="2" width="3.5" height="12" rx="0.5"/><rect x="9.5" y="2" width="3.5" height="12" rx="0.5"/></svg>',
    rollback: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M4 8h8a3 3 0 010 6H8"/><polyline points="7,5 4,8 7,11"/></svg>',
    economy: '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="8" cy="8" r="7"/><path d="M8 5v6M5 8h6M6 6l4 4M6 10l4-4"/></svg>',
};

// ============================================================================
// UTILITIES
// ============================================================================

function escapeHtml(str) {
    const el = document.createElement('span');
    el.textContent = str;
    return el.innerHTML;
}

function truncateId(id, len = 8) {
    if (!id) return '—';
    return id.length > len ? id.slice(0, len) + '...' : id;
}

function formatDuration(secs) {
    if (secs == null) return '—';
    if (secs < 1) return '< 1s';
    if (secs < 60) return `${Math.floor(secs)}s`;
    if (secs < 3600) return `${Math.floor(secs / 60)}m ${Math.floor(secs % 60)}s`;
    if (secs < 86400) return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
    return `${Math.floor(secs / 86400)}d ${Math.floor((secs % 86400) / 3600)}h`;
}

function formatTimeAgo(dateStr) {
    if (!dateStr) return '—';
    const date = typeof dateStr === 'string' ? new Date(dateStr) : dateStr;
    const now = Date.now();
    const diff = (now - date.getTime()) / 1000;
    if (diff < 10) return 'just now';
    if (diff < 60) return `${Math.floor(diff)}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    if (diff < 604800) return `${Math.floor(diff / 86400)}d ago`;
    return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

function formatNumber(n) {
    if (n == null) return '—';
    if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e4) return (n / 1e3).toFixed(1) + 'K';
    return n.toLocaleString();
}

function formatBytes(bytes) {
    if (bytes == null) return '—';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
    return (bytes / 1073741824).toFixed(1) + ' GB';
}

function formatPercent(value, decimals = 1) {
    if (value == null) return '—';
    return value.toFixed(decimals) + '%';
}

function debounce(fn, ms) {
    let t;
    return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

function throttle(fn, ms) {
    let last = 0;
    return (...args) => {
        const now = Date.now();
        if (now - last >= ms) { last = now; fn(...args); }
    };
}

function generateId() {
    return Math.random().toString(36).slice(2, 10);
}

function severityColor(sev) {
    const map = { critical: 'var(--error)', error: 'var(--error)', warning: 'var(--warning)', notice: 'var(--info)', info: 'var(--success)', debug: 'var(--fg-muted)' };
    return map[sev] || 'var(--fg-muted)';
}

function statusClass(status) {
    const s = (status || '').toLowerCase();
    if (['alive', 'healthy', 'active', 'running'].includes(s)) return 'badge--alive';
    if (['suspect', 'draining', 'degraded'].includes(s)) return 'badge--suspect';
    if (['dead', 'failed', 'quarantined', 'unreachable'].includes(s)) return 'badge--dead';
    if (['cordoned'].includes(s)) return 'badge--cordoned';
    if (['updating'].includes(s)) return 'badge--updating';
    return 'badge--draining';
}

function el(tag, attrs = {}, children = []) {
    const e = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
        if (k === 'className') e.className = v;
        else if (k === 'innerHTML') e.innerHTML = v;
        else if (k === 'textContent') e.textContent = v;
        else if (k.startsWith('on')) e.addEventListener(k.slice(2).toLowerCase(), v);
        else if (k === 'style' && typeof v === 'object') Object.assign(e.style, v);
        else if (k === 'dataset') Object.assign(e.dataset, v);
        else e.setAttribute(k, v);
    }
    for (const c of (Array.isArray(children) ? children : [children])) {
        if (typeof c === 'string') e.appendChild(document.createTextNode(c));
        else if (c instanceof Node) e.appendChild(c);
    }
    return e;
}

function removeChildren(e) {
    while (e.firstChild) e.removeChild(e.firstChild);
}

function copyToClipboard(text) {
    if (navigator.clipboard) return navigator.clipboard.writeText(text);
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.left = '-9999px';
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
}

function announceToSR(msg) {
    const sr = document.getElementById('sr-announcements');
    if (sr) { sr.textContent = ''; requestAnimationFrame(() => { sr.textContent = msg; }); }
}

function levelColor(level) {
    const map = { 1: 'var(--level-1)', 2: 'var(--level-2)', 3: 'var(--level-3)', 4: 'var(--level-4)', 5: 'var(--level-5)' };
    return map[level] || 'var(--fg-muted)';
}

function levelName(level) {
    const map = { 1: 'Critical', 2: 'Stressed', 3: 'Normal', 4: 'Healthy', 5: 'Thriving' };
    return map[level] || 'Unknown';
}

// ============================================================================
// SwarmAPI — REST client with retry and caching
// ============================================================================

class SwarmAPI {
    constructor(baseUrl = '') {
        this.baseUrl = baseUrl.replace(/\/$/, '') || (location.origin + '/api/v1');
        this._cache = new Map();
        this._inflight = new Map();
        this._cacheTTL = 5000;
        this._timeout = 30000;
        this._maxRetries = 3;
    }

    async _fetch(method, path, body = null, params = null) {
        let url = `${this.baseUrl}${path}`;
        if (params) {
            const sp = new URLSearchParams();
            for (const [k, v] of Object.entries(params)) { if (v != null) sp.set(k, String(v)); }
            const qs = sp.toString();
            if (qs) url += '?' + qs;
        }
        const cacheKey = method === 'GET' ? url : null;
        if (cacheKey) {
            const cached = this._cache.get(cacheKey);
            if (cached && Date.now() - cached.ts < this._cacheTTL) return cached.data;
            const inflight = this._inflight.get(cacheKey);
            if (inflight) return inflight;
        }

        const doFetch = async () => {
            let lastErr;
            for (let attempt = 0; attempt <= this._maxRetries; attempt++) {
                if (attempt > 0) await new Promise(r => setTimeout(r, Math.min(1000 * 2 ** (attempt - 1), 10000)));
                try {
                    const ctrl = new AbortController();
                    const timer = setTimeout(() => ctrl.abort(), this._timeout);
                    const opts = { method, signal: ctrl.signal, headers: { 'Accept': 'application/json' } };
                    if (body) { opts.headers['Content-Type'] = 'application/json'; opts.body = JSON.stringify(body); }
                    const res = await fetch(url, opts);
                    clearTimeout(timer);
                    if (res.ok) {
                        const ct = res.headers.get('content-type') || '';
                        const data = ct.includes('json') ? await res.json() : await res.text();
                        if (cacheKey) this._cache.set(cacheKey, { data, ts: Date.now() });
                        return data;
                    }
                    if (res.status === 429 || res.status >= 500) {
                        lastErr = new Error(`HTTP ${res.status}`);
                        continue;
                    }
                    if (res.status === 404) return null;
                    throw new Error(`HTTP ${res.status}: ${await res.text().catch(() => '')}`);
                } catch (e) {
                    lastErr = e;
                    if (e.name === 'AbortError') lastErr = new Error('Request timeout');
                    if (attempt === this._maxRetries) break;
                }
            }
            throw lastErr;
        };

        if (cacheKey) {
            const p = doFetch().finally(() => this._inflight.delete(cacheKey));
            this._inflight.set(cacheKey, p);
            return p;
        }
        return doFetch();
    }

    get(path, params) { return this._fetch('GET', path, null, params); }
    post(path, body) { return this._fetch('POST', path, body); }
    del(path) { return this._fetch('DELETE', path); }

    // Health
    async healthCheck() { try { await this.get('/health'); return true; } catch { return false; } }
    serverInfo() { return this.get('/info'); }

    // Psyche
    psyche() { return this.get('/psyche'); }
    psycheHistory(limit = 100) { return this.get('/psyche/history', { limit }); }
    psycheBreakdown() { return this.get('/psyche/breakdown'); }
    psycheForecast(minutes = 30) { return this.get('/psyche/forecast', { minutes_ahead: minutes }); }
    psycheTrends() { return this.get('/psyche/trends'); }

    // Events
    events(filter = {}) { return this.get('/events', filter); }
    eventStats() { return this.get('/events/stats'); }

    // Alerts
    alerts() { return this.get('/alerts'); }
    alertRules() { return this.get('/alerts/rules'); }
    alertSummary() { return this.get('/alerts/summary'); }
    acknowledgeAlert(name) { return this.post(`/alerts/${encodeURIComponent(name)}/acknowledge`); }
    silenceAlert(name, secs, reason) { return this.post(`/alerts/${encodeURIComponent(name)}/silence`, { duration_secs: secs, reason }); }

    // Fleet
    fleetStatus() { return this.get('/fleet'); }
    drainNode(id, timeout, reason) { return this.post(`/fleet/drain/${encodeURIComponent(id)}`, { timeout_secs: timeout, reason }); }
    cordonNode(id, reason) { return this.post(`/fleet/cordon/${encodeURIComponent(id)}`, { reason }); }
    uncordonNode(id) { return this.post(`/fleet/uncordon/${encodeURIComponent(id)}`); }
    quarantineNode(id, reason) { return this.post(`/fleet/quarantine/${encodeURIComponent(id)}`, { reason }); }
    updateStatus() { return this.get('/fleet/update'); }
    startUpdate(plan) { return this.post('/fleet/update', plan); }
    pauseUpdate(reason) { return this.post('/fleet/update/pause', { reason }); }
    resumeUpdate() { return this.post('/fleet/update/resume'); }
    rollbackUpdate(reason) { return this.post('/fleet/update/rollback', { reason }); }
    cancelUpdate() { return this.post('/fleet/update/cancel'); }
    fleetHealth() { return this.get('/fleet/health'); }

    // Nodes
    nodes() { return this.get('/nodes'); }
    node(id) { return this.get(`/nodes/${encodeURIComponent(id)}`); }

    // Jobs
    jobs() { return this.get('/jobs'); }
    job(id) { return this.get(`/jobs/${encodeURIComponent(id)}`); }
    diagnoseJob(id) { return this.get(`/jobs/${encodeURIComponent(id)}/diagnose`); }

    // SLA
    slaStatuses() { return this.get('/sla'); }
    slaReport(name, hours = 24) { return this.get(`/sla/${encodeURIComponent(name)}/report`, { period_hours: hours }); }

    // Capacity
    capacity() { return this.get('/capacity'); }
    capacityForecast() { return this.get('/capacity/forecast'); }
    capacityBottlenecks() { return this.get('/capacity/bottlenecks'); }

    // Multi-swarm
    constellation() { return this.get('/constellation'); }
    sovereignty() { return this.get('/sovereignty'); }
    membranes() { return this.get('/membranes'); }
    membraneCrossings(id, limit = 50) { return this.get(`/membranes/${encodeURIComponent(id)}/crossings`, { limit }); }
    treaties() { return this.get('/treaties'); }

    // Visions
    visions() { return this.get('/visions'); }

    // Audit
    auditLog(filter = {}) { return this.get('/audit', filter); }
    auditVerify() { return this.get('/audit/verify'); }

    // Metrics
    metricsRaw() { return this.get('/metrics'); }

    // Config
    configFull() { return this.get('/config'); }
    configSchema() { return this.get('/config/schema'); }
    configUpdate(patch) { return this._fetch('PATCH', '/config', patch); }
    configValidate(patch) { return this.post('/config/validate', patch); }
    configHistory(params = {}) { return this.get('/config/history', params); }
    configCompliance() { return this.get('/config/compliance'); }

    // Compliance
    complianceManifest() { return this.get('/compliance/manifest'); }
    compliancePosture() { return this.get('/compliance/posture'); }
    compliancePostureAt(ts) { return this.get(`/compliance/posture/at/${encodeURIComponent(ts)}`); }
    complianceReport(format = 'json') { return this.get('/compliance/report', { format }); }
    complianceConsent() { return this.get('/compliance/consent'); }
    complianceViolations() { return this.get('/compliance/violations'); }

    // Time-travel
    timeTravelConfig(at) { return this.get('/timetravel/config', { at }); }
    timeTravelSwarm(at) { return this.get('/timetravel/swarm', { at }); }
    timeTravelNode(id, from, to) { return this.get(`/timetravel/nodes/${encodeURIComponent(id)}`, { from, to }); }
    timeTravelJob(id) { return this.get(`/timetravel/jobs/${encodeURIComponent(id)}`); }
}

// ============================================================================
// EventStream — SSE client with auto-reconnect
// ============================================================================

class EventStream {
    constructor(baseUrl, handlers = {}) {
        this.baseUrl = baseUrl.replace(/\/$/, '') || (location.origin + '/api/v1');
        this.onEvent = handlers.onEvent || (() => {});
        this.onConnect = handlers.onConnect || (() => {});
        this.onDisconnect = handlers.onDisconnect || (() => {});
        this._es = null;
        this._attempt = 0;
        this._maxDelay = 30000;
        this._timer = null;
        this._connected = false;
    }

    connect(filter = {}) {
        this.disconnect();
        const params = new URLSearchParams();
        for (const [k, v] of Object.entries(filter)) { if (v != null) params.set(k, String(v)); }
        const qs = params.toString();
        const url = `${this.baseUrl}/events/stream${qs ? '?' + qs : ''}`;
        try {
            this._es = new EventSource(url);
            this._es.onopen = () => { this._attempt = 0; this._connected = true; this.onConnect(); };
            this._es.onmessage = (e) => { try { this.onEvent(JSON.parse(e.data)); } catch {} };
            this._es.onerror = () => { this._connected = false; this.onDisconnect(); this._reconnect(); };
        } catch {
            this._reconnect();
        }
    }

    disconnect() {
        clearTimeout(this._timer);
        if (this._es) { this._es.close(); this._es = null; }
        this._connected = false;
    }

    get connected() { return this._connected; }

    _reconnect() {
        if (this._es) { this._es.close(); this._es = null; }
        const delay = Math.min(1000 * 2 ** this._attempt, this._maxDelay);
        this._attempt++;
        this._timer = setTimeout(() => this.connect(), delay);
    }
}

// ============================================================================
// State — Reactive state store
// ============================================================================

class State {
    constructor() {
        this._data = {
            psyche: null, psycheHistory: [], psycheBreakdown: null, psycheTrends: null,
            nodes: [], nodeHealth: [],
            jobs: [],
            events: [], eventStats: null,
            alerts: [], alertRules: [], alertSummary: null,
            fleet: null, updateStatus: null,
            sla: [],
            capacity: null, capacityForecast: [], bottlenecks: [],
            constellation: null, membranes: [], treaties: [],
            visions: [],
            audit: [], auditVerification: null,
            configSchema: null, configFull: null, configHistory: [],
            complianceManifest: null, compliancePosture: null, complianceConsent: [], complianceViolations: [],
            ui: { theme: 'dark', sidebarCollapsed: false, detailOpen: false, loading: true, connected: false, vision: 'noc-operator' },
        };
        this._listeners = {};
    }

    get(key) { return key.includes('.') ? key.split('.').reduce((o, k) => o?.[k], this._data) : this._data[key]; }

    set(key, value) {
        if (key.includes('.')) {
            const parts = key.split('.');
            const last = parts.pop();
            const parent = parts.reduce((o, k) => o[k] = o[k] || {}, this._data);
            parent[last] = value;
        } else {
            this._data[key] = value;
        }
        this._notify(key, value);
    }

    subscribe(key, cb) {
        (this._listeners[key] = this._listeners[key] || []).push(cb);
        return () => { this._listeners[key] = this._listeners[key].filter(f => f !== cb); };
    }

    _notify(key, value) {
        const root = key.split('.')[0];
        for (const k of [key, root]) {
            for (const cb of (this._listeners[k] || [])) { try { cb(value); } catch (e) { console.error('State listener error:', e); } }
        }
    }
}

// ============================================================================
// ThemeManager
// ============================================================================

class ThemeManager {
    constructor() {
        this._themes = ['dark', 'light'];
        this._idx = 0;
        const saved = localStorage.getItem('swarm-theme');
        if (saved && this._themes.includes(saved)) this._idx = this._themes.indexOf(saved);
        this.apply();
        window.matchMedia('(prefers-color-scheme: light)').addEventListener('change', () => this.apply());
    }

    toggle() {
        this._idx = (this._idx + 1) % this._themes.length;
        this.apply();
        localStorage.setItem('swarm-theme', this.current);
    }

    get current() { return this._themes[this._idx]; }

    apply() {
        document.documentElement.setAttribute('data-theme', this.current);
    }
}

// ============================================================================
// Toast — Notification system
// ============================================================================

class ToastManager {
    constructor() {
        this._container = document.getElementById('toast-container');
        this._queue = [];
        this._visible = 0;
        this._maxVisible = 5;
    }

    show(message, severity = 'info', durationMs = null) {
        if (durationMs == null) {
            durationMs = { info: 3000, success: 3000, warning: 5000, error: 10000 }[severity] || 5000;
        }
        const toast = el('div', { className: `toast toast--${severity}`, role: 'alert' }, [
            el('span', { className: 'toast-icon', innerHTML: ICONS[severity] || ICONS.info }),
            el('span', { className: 'toast-message', textContent: message }),
            el('button', { className: 'toast-dismiss', innerHTML: ICONS.close, 'aria-label': 'Dismiss', onClick: () => this._remove(toast) }),
        ]);
        if (durationMs > 0) {
            const prog = el('div', { className: 'toast-progress', style: { animationDuration: durationMs + 'ms' } });
            toast.appendChild(prog);
            setTimeout(() => this._remove(toast), durationMs);
        }
        if (this._visible >= this._maxVisible) {
            this._queue.push(toast);
        } else {
            this._show(toast);
        }
    }

    _show(toast) {
        this._container.appendChild(toast);
        this._visible++;
    }

    _remove(toast) {
        if (!toast.parentNode) return;
        toast.style.animation = 'fadeOut 0.2s ease forwards';
        setTimeout(() => {
            toast.remove();
            this._visible--;
            if (this._queue.length) this._show(this._queue.shift());
        }, 200);
    }

    info(msg) { this.show(msg, 'info'); }
    success(msg) { this.show(msg, 'success'); }
    warn(msg) { this.show(msg, 'warning'); }
    error(msg) { this.show(msg, 'error'); }
}

// ============================================================================
// Modal — Dialog manager
// ============================================================================

class ModalManager {
    constructor() {
        this._dialog = document.getElementById('modal-container');
        this._title = document.getElementById('modal-title');
        this._body = document.getElementById('modal-body');
        this._footer = document.getElementById('modal-footer');
        this._dialog.querySelector('.modal-close').addEventListener('click', () => this.close());
        this._dialog.addEventListener('click', (e) => { if (e.target === this._dialog || e.target.classList.contains('modal-backdrop')) this.close(); });
    }

    open(title, bodyContent, actions = []) {
        this._title.textContent = title;
        removeChildren(this._body);
        removeChildren(this._footer);
        if (typeof bodyContent === 'string') this._body.innerHTML = bodyContent;
        else if (bodyContent instanceof Node) this._body.appendChild(bodyContent);
        for (const a of actions) {
            const btn = el('button', { className: `btn ${a.primary ? 'btn-primary' : ''} ${a.danger ? 'btn-danger' : ''}`, textContent: a.label, onClick: () => { a.action(); if (a.close !== false) this.close(); } });
            this._footer.appendChild(btn);
        }
        this._dialog.showModal();
    }

    confirm(title, message, onConfirm, danger = false) {
        this.open(title, el('p', { textContent: message }), [
            { label: 'Cancel', action: () => {}, close: true },
            { label: 'Confirm', action: onConfirm, close: true, primary: !danger, danger },
        ]);
    }

    close() {
        this._dialog.close();
    }
}

// ============================================================================
// DetailPanel — Slide-in panel
// ============================================================================


class EconomyPanel {
    constructor(state, api) {
        this.state = state;
        this.api = api;
        this._el = null;
    }
    render() {
        const card = el('section', { className: 'card', id: 'economy' }, [
            el('header', { className: 'card-header' }, [
                el('div', { className: 'flex items-center gap-2' }, [
                    el('span', { className: 'card-icon', innerHTML: ICONS.economy }),
                    el('h2', { className: 'card-title', textContent: 'MMX Settlement Ledger' }),
                ]),
                el('div', { className: 'card-actions' }, [
                    el('button', { className: 'btn btn-sm', textContent: 'Export CSV', onclick: () => this.exportCsv() }),
                ])
            ]),
            el('div', { className: 'card-body' }, [
                el('table', { className: 'table' }, [
                    el('thead', {}, [
                        el('tr', {}, [
                            el('th', { textContent: 'Chunk ID' }),
                            el('th', { textContent: 'Worker' }),
                            el('th', { textContent: 'Amount' }),
                            el('th', { textContent: 'Verified At' }),
                        ])
                    ]),
                    el('tbody', { id: 'ledger-body' }, [
                        el('tr', {}, [el('td', { colspan: 4, className: 'text-center', textContent: 'Loading ledger...' })])
                    ])
                ])
            ])
        ]);
        this._el = card;
        this.update();
        this.state.subscribe('ledger', () => this.update());
        return card;
    }
    async update() {
        const ledger = this.state.get('ledger') || [];
        const body = document.getElementById('ledger-body');
        if (!body) return;
        removeChildren(body);
        if (ledger.length === 0) {
            body.appendChild(el('tr', {}, [el('td', { colspan: 4, className: 'text-center', textContent: 'No settlement records found.' })]));
            return;
        }
        for (const p of ledger) {
            body.appendChild(el('tr', {}, [
                el('td', { className: 'font-mono text-xs', textContent: truncateId(p.chunk_id, 12) }),
                el('td', { className: 'font-mono text-xs', textContent: truncateId(p.worker, 12) }),
                el('td', { className: 'text-success font-bold', textContent: `${p.amount} MMX` }),
                el('td', { textContent: formatTimeAgo(new Date(p.verified_at * 1000)) }),
            ]));
        }
    }
    async exportCsv() {
        const fullLedger = [];
        let offset = 0;
        const limit = 500;
        
        try {
            // 🛡️ SOVEREIGN YEAR-END AUDIT: Programmatically page through the entire history.
            // This ensures the Norwegian Tax Authority receives mathematical proof for 
            // every single micro-transaction, not just the cached view.
            while (true) {
                const page = await this.api.ledger({ offset, limit });
                if (!page || page.length === 0) break;
                fullLedger.push(...page);
                if (page.length < limit) break;
                offset += limit;
            }

            let csv = 'chunk_id,worker,amount,verified_at,signature,public_key,federation_id
';
            for (const p of fullLedger) {
                csv += `${p.chunk_id},${p.worker},${p.amount},${new Date(p.verified_at * 1000).toISOString()},${p.signature},${p.public_key},${p.federation_id}
`;
            }
            const blob = new Blob([csv], { type: 'text/csv' });
            const url = URL.createObjectURL(blob);
            const a = el('a', { href: url, download: `mmx-ledger-${new Date().toISOString().split('T')[0]}.csv` });
            a.click();
            URL.revokeObjectURL(url);
        } catch (e) {
            console.error('Failed to export full ledger:', e);
            alert('Full audit history unavailable. Exporting cached view.');
            const ledger = this.state.get('ledger') || [];
            let csv = 'chunk_id,worker,amount,verified_at
';
            for (const p of ledger) csv += `${p.chunk_id},${p.worker},${p.amount},${new Date(p.verified_at * 1000).toISOString()}
`;
            const blob = new Blob([csv], { type: 'text/csv' });
            const url = URL.createObjectURL(blob);
            const a = el('a', { href: url, download: 'marabunta-mmx-ledger-cached.csv' });
            a.click();
        }
    }
}

class DetailPanelManager {
    constructor() {
        this._panel = document.getElementById('detail-panel');
        this._title = document.getElementById('detail-panel-title');
        this._content = document.getElementById('detail-panel-content');
        this._footer = document.getElementById('detail-panel-footer');
        this._panel.querySelector('.detail-panel-close').addEventListener('click', () => this.close());
    }

    open(title, content, footerContent = null) {
        this._title.textContent = title;
        removeChildren(this._content);
        removeChildren(this._footer);
        if (typeof content === 'string') this._content.innerHTML = content;
        else if (content instanceof Node) this._content.appendChild(content);
        if (footerContent instanceof Node) this._footer.appendChild(footerContent);
        this._panel.setAttribute('aria-hidden', 'false');
    }

    close() {
        this._panel.setAttribute('aria-hidden', 'true');
    }

    get isOpen() {
        return this._panel.getAttribute('aria-hidden') === 'false';
    }
}

// ============================================================================
// CommandPalette
// ============================================================================

class CommandPalette {
    constructor(app) {
        this._app = app;
        this._dialog = document.getElementById('cmd-palette');
        this._input = document.getElementById('cmd-search');
        this._results = document.getElementById('cmd-results');
        this._selectedIdx = -1;
        this._commands = [];

        this._input.addEventListener('input', debounce(() => this._search(), 150));
        this._input.addEventListener('keydown', (e) => this._handleKey(e));
        document.getElementById('cmd-palette-btn').addEventListener('click', () => this.toggle());
    }

    _buildCommands() {
        const cmds = [];
        const visions = this._app.state.get('visions') || [];
        for (const v of visions) {
            cmds.push({ label: `Switch to ${v.name || v}`, category: 'Vision', icon: 'psyche', action: () => this._app.switchVision(v.name || v) });
        }
        cmds.push({ label: 'Toggle theme', category: 'UI', icon: 'settings', action: () => this._app.theme.toggle() });
        cmds.push({ label: 'Refresh data', category: 'Action', icon: 'refresh', action: () => this._app.dataManager.fetchAll() });
        cmds.push({ label: 'Show keyboard shortcuts', category: 'Help', icon: 'info', action: () => document.getElementById('keyboard-help').showModal() });
        const nodes = this._app.state.get('nodes') || [];
        for (const n of nodes.slice(0, 20)) {
            const nid = n.id || n.node_id || '';
            cmds.push({ label: `Node ${truncateId(nid)}`, category: 'Node', icon: 'nodes', action: () => this._app.showNodeDetail(nid) });
        }
        this._commands = cmds;
    }

    toggle() {
        if (this._dialog.open) { this._dialog.close(); return; }
        this._buildCommands();
        this._input.value = '';
        this._selectedIdx = -1;
        this._renderResults(this._commands.slice(0, 10));
        this._dialog.showModal();
        this._input.focus();
    }

    _search() {
        const q = this._input.value.toLowerCase().trim();
        if (!q) { this._renderResults(this._commands.slice(0, 10)); return; }
        const scored = this._commands.map(c => {
            const label = c.label.toLowerCase();
            let score = 0;
            if (label === q) score = 100;
            else if (label.startsWith(q)) score = 80;
            else if (label.includes(q)) score = 50;
            else {
                let qi = 0;
                for (const ch of label) { if (qi < q.length && ch === q[qi]) qi++; }
                if (qi === q.length) score = 30;
            }
            return { ...c, score };
        }).filter(c => c.score > 0).sort((a, b) => b.score - a.score);
        this._selectedIdx = scored.length ? 0 : -1;
        this._renderResults(scored.slice(0, 10));
    }

    _renderResults(items) {
        removeChildren(this._results);
        if (!items.length) {
            this._results.appendChild(el('li', { className: 'cmd-empty-state', role: 'option', 'aria-disabled': 'true' }, [
                el('span', { className: 'cmd-empty-text', textContent: 'No results found' }),
            ]));
            return;
        }
        items.forEach((cmd, i) => {
            const li = el('li', {
                className: 'cmd-result-item',
                role: 'option',
                'aria-selected': i === this._selectedIdx ? 'true' : 'false',
                onClick: () => { cmd.action(); this._dialog.close(); },
            }, [
                el('span', { className: 'result-icon', innerHTML: ICONS[cmd.icon] || '' }),
                el('span', { className: 'result-label', textContent: cmd.label }),
                el('span', { className: 'result-hint', textContent: cmd.category }),
            ]);
            this._results.appendChild(li);
        });
    }

    _handleKey(e) {
        const items = this._results.querySelectorAll('.cmd-result-item');
        if (e.key === 'ArrowDown') {
            e.preventDefault();
            this._selectedIdx = Math.min(this._selectedIdx + 1, items.length - 1);
            this._updateSelection(items);
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            this._selectedIdx = Math.max(this._selectedIdx - 1, 0);
            this._updateSelection(items);
        } else if (e.key === 'Enter' && this._selectedIdx >= 0 && items[this._selectedIdx]) {
            e.preventDefault();
            items[this._selectedIdx].click();
        } else if (e.key === 'Escape') {
            this._dialog.close();
        }
    }

    _updateSelection(items) {
        items.forEach((it, i) => it.setAttribute('aria-selected', i === this._selectedIdx ? 'true' : 'false'));
    }
}

// ============================================================================
// KeyboardManager
// ============================================================================

class KeyboardManager {
    constructor(app) {
        this._app = app;
        document.addEventListener('keydown', (e) => this._handle(e));
    }

    _handle(e) {
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
        if ((e.ctrlKey || e.metaKey) && e.key === 'k') { e.preventDefault(); this._app.cmdPalette.toggle(); return; }
        if (e.key === 'Escape') {
            if (this._app.detailPanel.isOpen) { this._app.detailPanel.close(); return; }
            const kh = document.getElementById('keyboard-help');
            if (kh.open) { kh.close(); return; }
        }
        if (e.key === '?') { document.getElementById('keyboard-help').showModal(); return; }
        if (e.key === 't' || e.key === 'T') { this._app.theme.toggle(); return; }
        if (e.key === 'r' || e.key === 'R') { this._app.dataManager.fetchAll(); this._app.toast.info('Refreshing...'); return; }
        if (e.key >= '1' && e.key <= '8') {
            const visions = this._app.state.get('visions') || [];
            const idx = parseInt(e.key) - 1;
            if (visions[idx]) this._app.switchVision(visions[idx].name || visions[idx]);
        }
    }
}

// ============================================================================
// COMPONENT: PsychePanel — 7-facet psyche visualization
// ============================================================================

class PsychePanel {
    constructor(state) {
        this._state = state;
        this.el = el('div', { className: 'card' });
    }

    render() {
        const psyche = this._state.get('psyche');
        removeChildren(this.el);
        const header = el('div', { className: 'card-header' }, [
            el('div', {}, [
                el('h3', { className: 'card-title', textContent: 'Swarm Psyche' }),
                el('p', { className: 'card-subtitle', textContent: psyche?.archetype ? `Archetype: ${psyche.archetype}` : 'Measuring...' }),
            ]),
        ]);
        this.el.appendChild(header);

        const panel = el('div', { className: 'psyche-panel' });
        const facets = psyche?.facets || {};
        const facetNames = ['resilience', 'efficiency', 'coherence', 'vitality', 'intelligence', 'reach', 'integrity'];
        const trends = this._state.get('psycheTrends') || {};

        for (const name of facetNames) {
            const f = facets[name] || {};
            const level = f.level || 0;
            const score = f.score != null ? f.score : 0;
            const pct = Math.min(100, Math.max(0, score * 100));
            const trend = trends[name] || '';
            const trendArrow = trend === 'rising' ? ' ^' : trend === 'falling' ? ' v' : '';

            const row = el('div', { className: 'psyche-facet' }, [
                el('span', { className: 'facet-label', textContent: name.charAt(0).toUpperCase() + name.slice(1) }),
                el('div', { className: 'facet-bar-container' }, [
                    el('div', { className: 'facet-bar', dataset: { facet: name }, style: { width: pct + '%' }, title: `Level ${level}: ${levelName(level)} (${formatPercent(pct, 0)})` }),
                ]),
                el('span', { className: 'facet-value', textContent: `L${level}${trendArrow}` }),
            ]);
            panel.appendChild(row);
        }
        this.el.appendChild(panel);
        return this.el;
    }

    updateMiniBar() {
        const psyche = this._state.get('psyche');
        const facets = psyche?.facets || {};
        const segments = document.querySelectorAll('.psyche-mini-segment');
        const facetNames = ['resilience', 'efficiency', 'coherence', 'vitality', 'intelligence', 'reach', 'integrity'];
        segments.forEach((seg, i) => {
            const name = facetNames[i];
            const f = facets[name] || {};
            seg.setAttribute('data-level', f.level || 0);
            seg.title = `${name}: L${f.level || 0}`;
        });
        const badge = document.getElementById('archetype-badge');
        if (badge) badge.textContent = psyche?.archetype || '';
    }
}

// ============================================================================
// COMPONENT: EventLog — Scrollable event list
// ============================================================================

class EventLog {
    constructor(state) {
        this._state = state;
        this.el = el('div', { className: 'card' });
        this._autoScroll = true;
    }

    render() {
        removeChildren(this.el);
        const header = el('div', { className: 'card-header' }, [
            el('h3', { className: 'card-title', textContent: 'Events' }),
            el('span', { className: 'text-muted', textContent: `${(this._state.get('events') || []).length} events` }),
        ]);
        this.el.appendChild(header);

        this._log = el('div', { className: 'event-log' });
        this._log.addEventListener('scroll', () => {
            const { scrollTop, scrollHeight, clientHeight } = this._log;
            this._autoScroll = scrollHeight - scrollTop - clientHeight < 50;
        });

        const events = (this._state.get('events') || []).slice(-200);
        for (const ev of events) {
            this._log.appendChild(this._renderEvent(ev));
        }
        this.el.appendChild(this._log);
        if (this._autoScroll) this._log.scrollTop = this._log.scrollHeight;
        return this.el;
    }

    addEvent(ev) {
        if (!this._log) return;
        this._log.appendChild(this._renderEvent(ev));
        while (this._log.children.length > 500) this._log.removeChild(this._log.firstChild);
        if (this._autoScroll) this._log.scrollTop = this._log.scrollHeight;
    }

    _renderEvent(ev) {
        const severity = (ev.severity || 'info').toLowerCase();
        return el('div', { className: 'event-item' }, [
            el('span', { className: `event-severity event-severity--${severity}`, title: severity }),
            el('span', { className: 'event-time', textContent: formatTimeAgo(ev.timestamp || ev.ts) }),
            el('span', { className: 'event-summary', textContent: ev.summary || ev.message || JSON.stringify(ev) }),
            el('span', { className: 'event-domain', textContent: ev.domain || 'system' }),
        ]);
    }
}

// ============================================================================
// COMPONENT: NodeTable — Sortable, filterable node table
// ============================================================================

class NodeTable {
    constructor(state, app) {
        this._state = state;
        this._app = app;
        this.el = el('div', { className: 'card' });
        this._sort = { key: 'status', dir: 'asc' };
        this._filter = { status: '', search: '' };
        this._page = 0;
        this._pageSize = 25;
    }

    render() {
        removeChildren(this.el);
        const header = el('div', { className: 'card-header' }, [
            el('h3', { className: 'card-title', textContent: 'Nodes' }),
        ]);
        this.el.appendChild(header);

        // Filter bar
        const filterBar = el('div', { className: 'filter-bar' });
        const searchInput = el('input', { className: 'filter-input', type: 'text', placeholder: 'Search nodes...', 'aria-label': 'Search nodes' });
        searchInput.addEventListener('input', debounce(() => { this._filter.search = searchInput.value.toLowerCase(); this._page = 0; this._renderTable(); }, 300));

        const statusSelect = el('select', { className: 'filter-select', 'aria-label': 'Filter by status' });
        statusSelect.innerHTML = '<option value="">All statuses</option><option value="alive">Alive</option><option value="suspect">Suspect</option><option value="dead">Dead</option><option value="draining">Draining</option><option value="cordoned">Cordoned</option><option value="quarantined">Quarantined</option>';
        statusSelect.addEventListener('change', () => { this._filter.status = statusSelect.value; this._page = 0; this._renderTable(); });

        filterBar.append(searchInput, statusSelect);
        this.el.appendChild(filterBar);

        this._tableWrap = el('div', { className: 'data-table-wrapper' });
        this.el.appendChild(this._tableWrap);

        this._paginationEl = el('div', { className: 'pagination' });
        this.el.appendChild(this._paginationEl);

        this._renderTable();
        return this.el;
    }

    _getFiltered() {
        let nodes = this._state.get('nodes') || [];
        if (this._filter.status) nodes = nodes.filter(n => (n.status || '').toLowerCase() === this._filter.status);
        if (this._filter.search) nodes = nodes.filter(n => {
            const id = (n.id || n.node_id || '').toLowerCase();
            const region = (n.region || '').toLowerCase();
            return id.includes(this._filter.search) || region.includes(this._filter.search);
        });
        // Sort
        const k = this._sort.key;
        const dir = this._sort.dir === 'asc' ? 1 : -1;
        nodes.sort((a, b) => {
            const av = a[k] ?? '', bv = b[k] ?? '';
            if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * dir;
            return String(av).localeCompare(String(bv)) * dir;
        });
        return nodes;
    }

    _renderTable() {
        const nodes = this._getFiltered();
        const totalPages = Math.max(1, Math.ceil(nodes.length / this._pageSize));
        this._page = Math.min(this._page, totalPages - 1);
        const start = this._page * this._pageSize;
        const pageNodes = nodes.slice(start, start + this._pageSize);

        removeChildren(this._tableWrap);
        const table = el('table', { className: 'data-table' });
        const cols = [
            { key: 'id', label: 'ID' }, { key: 'status', label: 'Status' },
            { key: 'load', label: 'Load' }, { key: 'region', label: 'Region' },
            { key: 'uptime', label: 'Uptime' },
        ];
        const thead = el('thead');
        const headRow = el('tr');
        for (const col of cols) {
            const th = el('th', { onClick: () => this._toggleSort(col.key) }, [
                document.createTextNode(col.label),
                el('span', { className: 'sort-arrow', textContent: this._sort.key === col.key ? (this._sort.dir === 'asc' ? ' \u25B2' : ' \u25BC') : '' }),
            ]);
            if (this._sort.key === col.key) th.setAttribute('aria-sort', this._sort.dir === 'asc' ? 'ascending' : 'descending');
            headRow.appendChild(th);
        }
        thead.appendChild(headRow);
        table.appendChild(thead);

        const tbody = el('tbody');
        if (!pageNodes.length) {
            tbody.appendChild(el('tr', {}, [el('td', { colSpan: String(cols.length), className: 'empty-state', textContent: 'No nodes found' })]));
        } else {
            for (const n of pageNodes) {
                const nid = n.id || n.node_id || '';
                const status = (n.status || 'unknown').toLowerCase();
                const load = n.load != null ? n.load : (n.cpu_load != null ? n.cpu_load : null);
                const loadPct = load != null ? Math.round(load * 100) : null;
                const tr = el('tr', { style: { cursor: 'pointer' }, onClick: () => this._app.showNodeDetail(nid) }, [
                    el('td', { className: 'text-mono', textContent: truncateId(nid, 12), title: nid }),
                    el('td', {}, [el('span', { className: `badge ${statusClass(status)}`, textContent: status })]),
                    el('td', {}, loadPct != null ? [
                        el('div', { className: 'progress', style: { width: '80px', display: 'inline-block' } }, [
                            el('div', { className: `progress-bar ${loadPct > 90 ? 'progress-bar--error' : loadPct > 70 ? 'progress-bar--warning' : 'progress-bar--success'}`, style: { width: loadPct + '%' } }),
                        ]),
                        el('span', { className: 'text-muted', style: { marginLeft: '4px', fontSize: 'var(--text-xs)' }, textContent: loadPct + '%' }),
                    ] : [el('span', { className: 'text-muted', textContent: '—' })]),
                    el('td', { textContent: n.region || '—' }),
                    el('td', { textContent: n.uptime_secs != null ? formatDuration(n.uptime_secs) : '—' }),
                ]);
                tbody.appendChild(tr);
            }
        }
        table.appendChild(tbody);
        this._tableWrap.appendChild(table);

        // Pagination
        removeChildren(this._paginationEl);
        if (totalPages > 1) {
            const info = el('span', { className: 'page-info', textContent: `${start + 1}-${Math.min(start + this._pageSize, nodes.length)} of ${nodes.length}` });
            const prev = el('button', { className: 'page-btn', textContent: 'Prev', disabled: this._page === 0, onClick: () => { this._page--; this._renderTable(); } });
            const next = el('button', { className: 'page-btn', textContent: 'Next', disabled: this._page >= totalPages - 1, onClick: () => { this._page++; this._renderTable(); } });
            this._paginationEl.append(prev, info, next);
        }
    }

    _toggleSort(key) {
        if (this._sort.key === key) this._sort.dir = this._sort.dir === 'asc' ? 'desc' : 'asc';
        else { this._sort.key = key; this._sort.dir = 'asc'; }
        this._renderTable();
    }
}

// ============================================================================
// COMPONENT: AlertPanel
// ============================================================================

class AlertPanel {
    constructor(state, app) {
        this._state = state;
        this._app = app;
        this.el = el('div', { className: 'card' });
    }

    render() {
        removeChildren(this.el);
        const alerts = this._state.get('alerts') || [];
        const summary = this._state.get('alertSummary');

        const header = el('div', { className: 'card-header' }, [
            el('h3', { className: 'card-title', textContent: 'Alerts' }),
            el('span', { className: alerts.length ? 'text-error' : 'text-muted', textContent: `${alerts.length} active` }),
        ]);
        this.el.appendChild(header);

        if (!alerts.length) {
            this.el.appendChild(el('div', { className: 'empty-state' }, [
                el('span', { innerHTML: ICONS.check }),
                el('p', { textContent: 'No active alerts' }),
            ]));
            return this.el;
        }

        const list = el('div', { className: 'flex flex-col gap-2' });
        const sorted = [...alerts].sort((a, b) => {
            const sevOrder = { critical: 0, error: 1, warning: 2, info: 3 };
            return (sevOrder[a.severity] ?? 9) - (sevOrder[b.severity] ?? 9);
        });

        for (const alert of sorted.slice(0, 20)) {
            const sev = (alert.severity || 'warning').toLowerCase();
            const sevClass = sev === 'critical' || sev === 'error' ? 'alert-card--critical' : sev === 'warning' ? 'alert-card--warning' : 'alert-card--info';
            const card = el('div', { className: `alert-card ${sevClass}` }, [
                el('span', { className: 'alert-icon', innerHTML: ICONS.warning }),
                el('div', { className: 'alert-body' }, [
                    el('div', { className: 'alert-title', textContent: alert.name || alert.rule_name || 'Alert' }),
                    el('div', { className: 'alert-desc', textContent: alert.summary || alert.message || '' }),
                    el('div', { className: 'alert-actions' }, [
                        el('button', { className: 'btn btn-sm', textContent: 'Acknowledge', onClick: () => {
                            this._app.api.acknowledgeAlert(alert.name || alert.rule_name).then(() => this._app.toast.success('Alert acknowledged')).catch(() => this._app.toast.error('Failed to acknowledge'));
                        }}),
                        el('button', { className: 'btn btn-sm', textContent: 'Silence 1h', onClick: () => {
                            this._app.api.silenceAlert(alert.name || alert.rule_name, 3600, 'Silenced from UI').then(() => this._app.toast.success('Alert silenced')).catch(() => this._app.toast.error('Failed to silence'));
                        }}),
                    ]),
                ]),
            ]);
            list.appendChild(card);
        }
        this.el.appendChild(list);
        return this.el;
    }
}

// ============================================================================
// COMPONENT: FleetPanel
// ============================================================================

class FleetPanel {
    constructor(state, app) {
        this._state = state;
        this._app = app;
        this.el = el('div', { className: 'card' });
    }

    render() {
        removeChildren(this.el);
        const fleet = this._state.get('fleet') || {};
        const update = this._state.get('updateStatus');

        const header = el('div', { className: 'card-header' }, [
            el('h3', { className: 'card-title', textContent: 'Fleet Management' }),
        ]);
        this.el.appendChild(header);

        // Summary cards
        const stats = el('div', { className: 'grid-4 mb-4' });
        const items = [
            { label: 'Total', value: fleet.total_managed ?? '—', color: '' },
            { label: 'Normal', value: fleet.normal ?? '—', color: 'text-success' },
            { label: 'Draining', value: fleet.draining ?? 0, color: 'text-warning' },
            { label: 'Quarantined', value: fleet.quarantined ?? 0, color: 'text-error' },
        ];
        for (const item of items) {
            stats.appendChild(el('div', { className: 'card', style: { textAlign: 'center', padding: 'var(--space-3)' } }, [
                el('div', { className: `text-3xl ${item.color}`, textContent: String(item.value) }),
                el('div', { className: 'text-muted', style: { fontSize: 'var(--text-sm)' }, textContent: item.label }),
            ]));
        }
        this.el.appendChild(stats);

        // Rolling update section
        if (update && update.phase) {
            const updateSection = el('div', { className: 'mt-4' });
            updateSection.appendChild(el('h4', { className: 'mb-2', textContent: 'Rolling Update' }));
            const pct = update.progress != null ? Math.round(update.progress * 100) : 0;
            updateSection.appendChild(el('div', { className: 'progress-label' }, [
                el('span', { textContent: `Phase: ${update.phase}` }),
                el('span', { textContent: formatPercent(pct, 0) }),
            ]));
            updateSection.appendChild(el('div', { className: 'progress' }, [
                el('div', { className: 'progress-bar', style: { width: pct + '%' } }),
            ]));
            const actions = el('div', { className: 'flex gap-2 mt-2' });
            if (update.phase !== 'paused') {
                actions.appendChild(el('button', { className: 'btn btn-sm', textContent: 'Pause', onClick: () => this._app.modal.confirm('Pause Update', 'Pause the rolling update?', () => this._app.api.pauseUpdate('Paused from UI')) }));
            } else {
                actions.appendChild(el('button', { className: 'btn btn-sm btn-primary', textContent: 'Resume', onClick: () => this._app.api.resumeUpdate() }));
            }
            actions.appendChild(el('button', { className: 'btn btn-sm btn-danger', textContent: 'Rollback', onClick: () => this._app.modal.confirm('Rollback Update', 'Roll back the current update?', () => this._app.api.rollbackUpdate('Rollback from UI'), true) }));
            updateSection.appendChild(actions);
            this.el.appendChild(updateSection);
        } else {
            this.el.appendChild(el('div', { className: 'empty-state', style: { padding: 'var(--space-4)' } }, [
                el('p', { className: 'text-muted', textContent: 'No rolling update in progress' }),
            ]));
        }
        return this.el;
    }
}

// ============================================================================
// COMPONENT: SlaPanel
// ============================================================================

class SlaPanel {
    constructor(state) {
        this._state = state;
        this.el = el('div', { className: 'card' });
    }

    render() {
        removeChildren(this.el);
        const slas = this._state.get('sla') || [];

        this.el.appendChild(el('div', { className: 'card-header' }, [
            el('h3', { className: 'card-title', textContent: 'SLA Status' }),
        ]));

        if (!slas.length) {
            this.el.appendChild(el('div', { className: 'empty-state' }, [el('p', { textContent: 'No SLAs configured' })]));
            return this.el;
        }

        for (const sla of slas) {
            const compliance = sla.current_compliance ?? sla.compliance ?? 0;
            const target = sla.target ?? 99.9;
            const ok = compliance >= target;
            const row = el('div', { className: 'gauge-row' }, [
                el('span', { className: 'gauge-label', textContent: sla.name || sla.sla_name || 'SLA' }),
                el('div', { className: 'gauge-bar' }, [
                    el('div', { className: 'progress' }, [
                        el('div', { className: `progress-bar ${ok ? 'progress-bar--success' : 'progress-bar--error'}`, style: { width: Math.min(100, compliance) + '%' } }),
                    ]),
                ]),
                el('span', { className: `gauge-value ${ok ? 'text-success' : 'text-error'}`, textContent: formatPercent(compliance) }),
            ]);
            this.el.appendChild(row);
        }
        return this.el;
    }
}

// ============================================================================
// COMPONENT: CapacityPanel
// ============================================================================

class CapacityPanel {
    constructor(state) {
        this._state = state;
        this.el = el('div', { className: 'card' });
    }

    render() {
        removeChildren(this.el);
        const cap = this._state.get('capacity') || {};

        this.el.appendChild(el('div', { className: 'card-header' }, [
            el('h3', { className: 'card-title', textContent: 'Capacity' }),
        ]));

        const resources = [
            { label: 'CPU', value: cap.cpu_pct ?? cap.cpu ?? null },
            { label: 'Memory', value: cap.memory_pct ?? cap.memory ?? null },
            { label: 'Disk', value: cap.disk_pct ?? cap.disk ?? null },
            { label: 'Bandwidth', value: cap.bandwidth_pct ?? cap.bandwidth ?? null },
        ];

        for (const r of resources) {
            const pct = r.value != null ? Math.round(r.value * (r.value <= 1 ? 100 : 1)) : null;
            const barClass = pct != null ? (pct > 90 ? 'progress-bar--error' : pct > 70 ? 'progress-bar--warning' : 'progress-bar--success') : '';
            this.el.appendChild(el('div', { className: 'gauge-row' }, [
                el('span', { className: 'gauge-label', textContent: r.label }),
                el('div', { className: 'gauge-bar' }, [
                    el('div', { className: 'progress' }, [
                        pct != null ? el('div', { className: `progress-bar ${barClass}`, style: { width: pct + '%' } }) : el('div'),
                    ]),
                ]),
                el('span', { className: 'gauge-value', textContent: pct != null ? formatPercent(pct, 0) : '—' }),
            ]));
        }

        // Bottlenecks
        const bottlenecks = this._state.get('bottlenecks') || [];
        if (bottlenecks.length) {
            this.el.appendChild(el('div', { className: 'mt-4' }, [
                el('h4', { className: 'mb-2 text-warning', textContent: 'Bottlenecks' }),
                ...bottlenecks.map(b => el('div', { className: 'alert-card alert-card--warning', style: { marginBottom: '4px' } }, [
                    el('span', { className: 'alert-icon', innerHTML: ICONS.warning }),
                    el('div', { className: 'alert-body' }, [
                        el('div', { className: 'alert-title', textContent: b.resource || b.name || 'Resource' }),
                        el('div', { className: 'alert-desc', textContent: b.message || `Exhaustion in ${b.days_to_exhaustion ?? '?'} days` }),
                    ]),
                ])),
            ]));
        }
        return this.el;
    }
}

// ============================================================================
// COMPONENT: ConstellationGraph — SVG multi-swarm visualization
// ============================================================================

class ConstellationGraph {
    constructor(state) {
        this._state = state;
        this.el = el('div', { className: 'card' });
    }

    render() {
        removeChildren(this.el);
        const data = this._state.get('constellation');

        this.el.appendChild(el('div', { className: 'card-header' }, [
            el('h3', { className: 'card-title', textContent: 'Constellation' }),
        ]));

        if (!data || !data.swarms || !data.swarms.length) {
            this.el.appendChild(el('div', { className: 'empty-state' }, [
                el('span', { innerHTML: ICONS.constellation }),
                el('p', { textContent: 'No remote swarms connected' }),
            ]));
            return this.el;
        }

        const container = el('div', { className: 'constellation-container', style: { height: '350px' } });
        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('class', 'constellation-svg');
        svg.setAttribute('viewBox', '0 0 600 350');

        const swarms = data.swarms || [];
        const cx = 300, cy = 175;
        const positions = swarms.map((_, i) => {
            const angle = (2 * Math.PI * i) / swarms.length - Math.PI / 2;
            const r = swarms.length === 1 ? 0 : 120;
            return { x: cx + r * Math.cos(angle), y: cy + r * Math.sin(angle) };
        });

        // Draw edges (membranes)
        const membranes = data.membranes || [];
        for (const m of membranes) {
            const fromIdx = swarms.findIndex(s => (s.id || s.swarm_id) === m.from);
            const toIdx = swarms.findIndex(s => (s.id || s.swarm_id) === m.to);
            if (fromIdx >= 0 && toIdx >= 0) {
                const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                line.setAttribute('x1', positions[fromIdx].x);
                line.setAttribute('y1', positions[fromIdx].y);
                line.setAttribute('x2', positions[toIdx].x);
                line.setAttribute('y2', positions[toIdx].y);
                line.setAttribute('class', `constellation-edge ${m.status === 'active' ? 'constellation-edge--active' : ''}`);
                svg.appendChild(line);
            }
        }

        // Draw nodes
        swarms.forEach((s, i) => {
            const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            g.setAttribute('class', 'constellation-node');
            g.setAttribute('transform', `translate(${positions[i].x},${positions[i].y})`);

            const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            circle.setAttribute('r', '20');
            const health = (s.health || s.status || '').toLowerCase();
            const color = health === 'healthy' || health === 'alive' ? 'var(--success)' : health === 'degraded' ? 'var(--warning)' : health === 'unreachable' ? 'var(--fg-muted)' : 'var(--accent)';
            circle.setAttribute('fill', color);
            circle.setAttribute('opacity', '0.2');
            circle.setAttribute('stroke', color);
            g.appendChild(circle);

            const dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            dot.setAttribute('r', '6');
            dot.setAttribute('fill', color);
            g.appendChild(dot);

            const label = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            label.setAttribute('y', '32');
            label.textContent = s.name || truncateId(s.id || s.swarm_id);
            g.appendChild(label);

            if (s.is_local) {
                const ring = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                ring.setAttribute('r', '26');
                ring.setAttribute('fill', 'none');
                ring.setAttribute('stroke', 'var(--accent)');
                ring.setAttribute('stroke-width', '2');
                ring.setAttribute('stroke-dasharray', '4 2');
                g.appendChild(ring);
            }
            svg.appendChild(g);
        });

        container.appendChild(svg);
        this.el.appendChild(container);
        return this.el;
    }
}

// ============================================================================
// COMPONENT: AuditViewer
// ============================================================================

class AuditViewer {
    constructor(state) {
        this._state = state;
        this.el = el('div', { className: 'card' });
    }

    render() {
        removeChildren(this.el);
        const entries = this._state.get('audit') || [];
        const verification = this._state.get('auditVerification');

        this.el.appendChild(el('div', { className: 'card-header' }, [
            el('div', {}, [
                el('h3', { className: 'card-title', textContent: 'Audit Trail' }),
                verification ? el('span', {
                    className: verification.valid ? 'text-success' : 'text-error',
                    style: { fontSize: 'var(--text-xs)', marginLeft: '8px' },
                    textContent: verification.valid ? 'Chain verified' : 'Chain broken!',
                }) : null,
            ].filter(Boolean)),
        ]));

        if (!entries.length) {
            this.el.appendChild(el('div', { className: 'empty-state' }, [el('p', { textContent: 'No audit entries' })]));
            return this.el;
        }

        const wrap = el('div', { className: 'data-table-wrapper' });
        const table = el('table', { className: 'data-table' });
        table.innerHTML = '<thead><tr><th>Time</th><th>Actor</th><th>Action</th><th>Target</th><th>Outcome</th></tr></thead>';
        const tbody = el('tbody');
        for (const e of entries.slice(-100)) {
            tbody.appendChild(el('tr', {}, [
                el('td', { className: 'event-time', textContent: formatTimeAgo(e.timestamp) }),
                el('td', { textContent: truncateId(e.actor || e.actor_id || '—', 12) }),
                el('td', { textContent: e.action || '—' }),
                el('td', { textContent: truncateId(e.target || e.target_id || '—', 16) }),
                el('td', {}, [el('span', { className: `badge ${e.outcome === 'success' ? 'badge--alive' : 'badge--dead'}`, textContent: e.outcome || '—' })]),
            ]));
        }
        table.appendChild(tbody);
        wrap.appendChild(table);
        this.el.appendChild(wrap);
        return this.el;
    }
}

// ============================================================================
// COMPONENT: MetricCard
// ============================================================================

class MetricCard {
    constructor(label, value, opts = {}) {
        this.el = el('div', { className: 'card', style: { textAlign: 'center', padding: 'var(--space-3)' } }, [
            el('div', { className: `text-3xl ${opts.color || ''}`, textContent: String(value ?? '—') }),
            el('div', { className: 'text-muted', style: { fontSize: 'var(--text-sm)', marginTop: '2px' }, textContent: label }),
        ]);
    }
}

// ============================================================================
// COMPONENT: JobList
// ============================================================================

class JobList {
    constructor(state) {
        this._state = state;
        this.el = el('div', { className: 'card' });
    }

    render() {
        removeChildren(this.el);
        const jobs = this._state.get('jobs') || [];

        this.el.appendChild(el('div', { className: 'card-header' }, [
            el('h3', { className: 'card-title', textContent: 'Jobs' }),
            el('span', { className: 'text-muted', textContent: `${jobs.length} jobs` }),
        ]));

        if (!jobs.length) {
            this.el.appendChild(el('div', { className: 'empty-state' }, [el('p', { textContent: 'No jobs' })]));
            return this.el;
        }

        const wrap = el('div', { className: 'data-table-wrapper' });
        const table = el('table', { className: 'data-table' });
        table.innerHTML = '<thead><tr><th>ID</th><th>Status</th><th>Progress</th><th>Chunks</th><th>Created</th></tr></thead>';
        const tbody = el('tbody');
        for (const j of jobs.slice(0, 50)) {
            const jid = j.id || j.job_id || '';
            const status = (j.status || 'unknown').toLowerCase();
            const progress = j.progress != null ? Math.round(j.progress * 100) : null;
            tbody.appendChild(el('tr', {}, [
                el('td', { className: 'text-mono', textContent: truncateId(jid, 12), title: jid }),
                el('td', {}, [el('span', { className: `badge ${statusClass(status)}`, textContent: status })]),
                el('td', {}, progress != null ? [
                    el('div', { className: 'progress', style: { width: '80px', display: 'inline-block' } }, [
                        el('div', { className: 'progress-bar progress-bar--success', style: { width: progress + '%' } }),
                    ]),
                    el('span', { className: 'text-muted', style: { marginLeft: '4px', fontSize: 'var(--text-xs)' }, textContent: progress + '%' }),
                ] : [el('span', { className: 'text-muted', textContent: '—' })]),
                el('td', { textContent: `${j.completed_chunks ?? '?'}/${j.total_chunks ?? '?'}` }),
                el('td', { textContent: formatTimeAgo(j.created_at || j.submitted_at) }),
            ]));
        }
        table.appendChild(tbody);
        wrap.appendChild(table);
        this.el.appendChild(wrap);
        return this.el;
    }
}

// ============================================================================
// VISION RENDERERS
// ============================================================================

class VisionRenderer {
    constructor(container, state, api, app) {
        this.container = container;
        this.state = state;
        this.api = api;
        this.app = app;
        this._components = [];
        this._refreshTimer = null;
    }

    render() { removeChildren(this.container); }
    update() { for (const c of this._components) if (c.render) c.render(); }

    startAutoRefresh(ms) {
        this.stopAutoRefresh();
        this._refreshTimer = setInterval(() => this.update(), ms);
    }

    stopAutoRefresh() {
        if (this._refreshTimer) { clearInterval(this._refreshTimer); this._refreshTimer = null; }
    }

    destroy() {
        this.stopAutoRefresh();
        this._components = [];
    }
}

// NOC Operator Vision — primary monitoring dashboard
class NocVisionRenderer extends VisionRenderer {
    render() {
        super.render();
        const nodes = this.state.get('nodes') || [];
        const alerts = this.state.get('alerts') || [];
        const jobs = this.state.get('jobs') || [];
        const alive = nodes.filter(n => (n.status || '').toLowerCase() === 'alive').length;

        const metricsRow = el('div', { className: 'grid-4' });
        metricsRow.append(
            new MetricCard('Total Nodes', formatNumber(nodes.length)).el,
            new MetricCard('Alive', formatNumber(alive), { color: 'text-success' }).el,
            new MetricCard('Active Jobs', formatNumber(jobs.filter(j => (j.status || '').toLowerCase() === 'running').length), { color: 'text-info' }).el,
            new MetricCard('Active Alerts', formatNumber(alerts.length), { color: alerts.length ? 'text-error' : 'text-success' }).el,
        );
        this.container.appendChild(metricsRow);

        this._psyche = new PsychePanel(this.state);
        this.container.appendChild(this._psyche.render());
        this._psyche.updateMiniBar();
        this._components.push(this._psyche);

        const grid = el('div', { className: 'grid-2' });
        this._events = new EventLog(this.state);
        grid.appendChild(this._events.render());
        this._components.push(this._events);
        this._alerts = new AlertPanel(this.state, this.app);
        grid.appendChild(this._alerts.render());
        this._components.push(this._alerts);
        this.container.appendChild(grid);

        this._nodes = new NodeTable(this.state, this.app);
        this.container.appendChild(this._nodes.render());
        this._components.push(this._nodes);
        this.startAutoRefresh(5000);
    }

    handleSSEEvent(ev) { if (this._events) this._events.addEvent(ev); }
}

class DeveloperVisionRenderer extends VisionRenderer {
    render() {
        super.render();
        const grid = el('div', { className: 'grid-2' });
        this._jobs = new JobList(this.state);
        grid.appendChild(this._jobs.render());
        this._components.push(this._jobs);
        this._events = new EventLog(this.state);
        grid.appendChild(this._events.render());
        this._components.push(this._events);
        this.container.appendChild(grid);
        this.startAutoRefresh(10000);
    }

    handleSSEEvent(ev) { if (this._events) this._events.addEvent(ev); }
}

class ExecutiveVisionRenderer extends VisionRenderer {
    render() {
        super.render();
        const psyche = this.state.get('psyche');
        this.container.appendChild(el('div', { className: 'card', style: { textAlign: 'center', padding: 'var(--space-6)' } }, [
            el('h2', { style: { fontSize: 'var(--text-2xl)', marginBottom: 'var(--space-2)' }, textContent: psyche?.archetype || 'Measuring...' }),
            el('p', { className: 'text-muted', textContent: psyche?.description || 'Swarm archetype analysis in progress' }),
        ]));
        const nodes = this.state.get('nodes') || [];
        const slas = this.state.get('sla') || [];
        const avgCompliance = slas.length ? slas.reduce((s, sla) => s + (sla.current_compliance ?? sla.compliance ?? 0), 0) / slas.length : null;
        const metrics = el('div', { className: 'grid-4' });
        metrics.append(
            new MetricCard('Nodes', formatNumber(nodes.length)).el,
            new MetricCard('Active Jobs', formatNumber((this.state.get('jobs') || []).length)).el,
            new MetricCard('SLA Compliance', avgCompliance != null ? formatPercent(avgCompliance) : '—', { color: avgCompliance >= 99 ? 'text-success' : 'text-warning' }).el,
            new MetricCard('Alerts', formatNumber((this.state.get('alerts') || []).length), { color: (this.state.get('alerts') || []).length ? 'text-error' : 'text-success' }).el,
        );
        this.container.appendChild(metrics);
        this._sla = new SlaPanel(this.state);
        this.container.appendChild(this._sla.render());
        this._components.push(this._sla);
        this.startAutoRefresh(300000);
    }
}

class CapacityPlannerVisionRenderer extends VisionRenderer {
    render() {
        super.render();
        this._capacity = new CapacityPanel(this.state);
        this.container.appendChild(this._capacity.render());
        this._components.push(this._capacity);
        const forecast = this.state.get('capacityForecast') || [];
        if (forecast.length) {
            const card = el('div', { className: 'card' });
            card.appendChild(el('div', { className: 'card-header' }, [el('h3', { className: 'card-title', textContent: 'Forecast' })]));
            for (const f of forecast) {
                card.appendChild(el('div', { className: 'gauge-row' }, [
                    el('span', { className: 'gauge-label', textContent: f.resource || '?' }),
                    el('span', { className: 'gauge-value', textContent: f.days_to_exhaustion != null ? `${f.days_to_exhaustion}d` : 'OK' }),
                ]));
            }
            this.container.appendChild(card);
        }
        this.startAutoRefresh(30000);
    }
}

class SecurityAuditorVisionRenderer extends VisionRenderer {
    render() {
        super.render();
        this._audit = new AuditViewer(this.state);
        this.container.appendChild(this._audit.render());
        this._components.push(this._audit);
        this._alerts = new AlertPanel(this.state, this.app);
        this.container.appendChild(this._alerts.render());
        this._components.push(this._alerts);
        this.startAutoRefresh(60000);
    }
}

class ConstellationVisionRenderer extends VisionRenderer {
    render() {
        super.render();
        this._graph = new ConstellationGraph(this.state);
        this.container.appendChild(this._graph.render());
        this._components.push(this._graph);
        const membranes = this.state.get('membranes') || [];
        const treaties = this.state.get('treaties') || [];
        if (membranes.length) {
            const card = el('div', { className: 'card' });
            card.appendChild(el('div', { className: 'card-header' }, [el('h3', { className: 'card-title', textContent: 'Membranes' })]));
            const wrap = el('div', { className: 'data-table-wrapper' });
            const table = el('table', { className: 'data-table' });
            table.innerHTML = '<thead><tr><th>Name</th><th>Status</th><th>Crossings</th></tr></thead>';
            const tbody = el('tbody');
            for (const m of membranes) {
                tbody.appendChild(el('tr', {}, [
                    el('td', { textContent: m.name || m.membrane_id || '—' }),
                    el('td', {}, [el('span', { className: `badge ${statusClass(m.status)}`, textContent: m.status || '—' })]),
                    el('td', { textContent: formatNumber(m.crossing_count ?? 0) }),
                ]));
            }
            table.appendChild(tbody); wrap.appendChild(table); card.appendChild(wrap);
            this.container.appendChild(card);
        }
        if (treaties.length) {
            const card = el('div', { className: 'card' });
            card.appendChild(el('div', { className: 'card-header' }, [el('h3', { className: 'card-title', textContent: 'Treaties' })]));
            const wrap = el('div', { className: 'data-table-wrapper' });
            const table = el('table', { className: 'data-table' });
            table.innerHTML = '<thead><tr><th>Name</th><th>Parties</th><th>Status</th></tr></thead>';
            const tbody = el('tbody');
            for (const t of treaties) {
                tbody.appendChild(el('tr', {}, [
                    el('td', { textContent: t.name || t.agreement_id || '—' }),
                    el('td', { textContent: (t.parties || []).map(p => truncateId(p, 8)).join(', ') || '—' }),
                    el('td', {}, [el('span', { className: `badge ${statusClass(t.status)}`, textContent: t.status || '—' })]),
                ]));
            }
            table.appendChild(tbody); wrap.appendChild(table); card.appendChild(wrap);
            this.container.appendChild(card);
        }
        this.startAutoRefresh(10000);
    }
}

class SwarmWhispererVisionRenderer extends VisionRenderer {
    render() {
        super.render();
        this._psyche = new PsychePanel(this.state);
        this.container.appendChild(this._psyche.render());
        this._psyche.updateMiniBar();
        this._components.push(this._psyche);

        const grid1 = el('div', { className: 'grid-2' });
        this._events = new EventLog(this.state);
        grid1.appendChild(this._events.render());
        this._components.push(this._events);
        this._alerts = new AlertPanel(this.state, this.app);
        grid1.appendChild(this._alerts.render());
        this._components.push(this._alerts);
        this.container.appendChild(grid1);

        this._nodes = new NodeTable(this.state, this.app);
        this.container.appendChild(this._nodes.render());
        this._components.push(this._nodes);

        this._jobs = new JobList(this.state);
        this.container.appendChild(this._jobs.render());
        this._components.push(this._jobs);

        const grid2 = el('div', { className: 'grid-2' });
        this._fleet = new FleetPanel(this.state, this.app);
        grid2.appendChild(this._fleet.render());
        this._components.push(this._fleet);
        this._sla = new SlaPanel(this.state);
        grid2.appendChild(this._sla.render());
        this._components.push(this._sla);
        this.container.appendChild(grid2);

        const grid3 = el('div', { className: 'grid-2' });
        this._capacity = new CapacityPanel(this.state);
        grid3.appendChild(this._capacity.render());
        this._components.push(this._capacity);
        this._constellation = new ConstellationGraph(this.state);
        grid3.appendChild(this._constellation.render());
        this._components.push(this._constellation);
        this.container.appendChild(grid3);

        this._audit = new AuditViewer(this.state);
        this.container.appendChild(this._audit.render());
        this._components.push(this._audit);
        this.startAutoRefresh(3000);
    }

    handleSSEEvent(ev) { if (this._events) this._events.addEvent(ev); }
}

// ============================================================================
// SettingsVisionRenderer — Configuration & Compliance Settings
// ============================================================================


class EconomyVisionRenderer extends VisionRenderer {
    render() {
        super.render();
        this._economy = new EconomyPanel(this.state, this.api);
        this.container.appendChild(this._economy.render());
        this._components.push(this._economy);
        this.startAutoRefresh(10000);
    }
}

class SettingsVisionRenderer extends VisionRenderer {
    render() {
        super.render();

        const layout = el('div', { className: 'settings-layout' });

        // --- Left sidebar: category navigation ---
        this._categories = [
            { id: 'network', label: 'Network', filter: 'gossip,transport,failure' },
            { id: 'execution', label: 'Execution', filter: 'work,sandbox' },
            { id: 'marketplace', label: 'Marketplace', filter: 'marketplace,reputation' },
            { id: 'admission', label: 'Admission', filter: 'admission' },
            { id: 'security', label: 'Security', filter: 'security' },
            { id: 'neuromancer', label: 'Neuromancer', filter: 'neuromancer' },
            { id: 'postgres', label: 'PostgreSQL', filter: 'postgres' },
            { id: 'plugins', label: 'Plugins', filter: 'plugin' },
            { id: 'compliance', label: 'Compliance', filter: '__compliance__' },
            { id: 'history', label: 'History', filter: '__history__' },
        ];
        this._activeCategory = 'network';

        const sidebar = el('nav', { className: 'settings-sidebar' });
        for (const cat of this._categories) {
            const btn = el('button', {
                className: 'settings-category-btn' + (cat.id === this._activeCategory ? ' active' : ''),
                textContent: cat.label,
            });
            btn.dataset.catId = cat.id;
            btn.addEventListener('click', () => this._switchCategory(cat.id));
            sidebar.appendChild(btn);
        }
        layout.appendChild(sidebar);

        // --- Main content area ---
        this._mainArea = el('div', { className: 'settings-main' });
        layout.appendChild(this._mainArea);
        this.container.appendChild(layout);

        this._renderCategory(this._activeCategory);
    }

    _switchCategory(catId) {
        this._activeCategory = catId;
        const btns = this.container.querySelectorAll('.settings-category-btn');
        btns.forEach(b => b.classList.toggle('active', b.dataset.catId === catId));
        this._renderCategory(catId);
    }

    _renderCategory(catId) {
        removeChildren(this._mainArea);
        const cat = this._categories.find(c => c.id === catId);
        if (!cat) return;

        if (cat.filter === '__compliance__') { this._renderCompliance(); return; }
        if (cat.filter === '__history__') { this._renderHistory(); return; }

        const schema = this.state.get('configSchema');
        const configFull = this.state.get('configFull');
        if (!schema || !Array.isArray(schema)) {
            this._mainArea.appendChild(el('p', { className: 'text-muted', textContent: 'Loading configuration schema...' }));
            return;
        }

        const filterKeys = cat.filter.split(',');
        const settings = schema.filter(s => filterKeys.some(f => s.key && s.key.startsWith(f)));

        if (settings.length === 0) {
            this._mainArea.appendChild(el('p', { className: 'text-muted', textContent: 'No settings in this category.' }));
            return;
        }

        const header = el('h2', { textContent: cat.label + ' Settings' });
        this._mainArea.appendChild(header);

        for (const setting of settings) {
            this._mainArea.appendChild(this._renderSettingRow(setting, configFull));
        }
    }

    _renderSettingRow(setting, configFull) {
        const row = el('div', { className: 'setting-row' });

        // Tier badge
        const tier = setting.tier || 'runtime';
        const tierBadge = el('span', {
            className: 'tier-badge tier-' + tier.toLowerCase(),
            textContent: tier.toUpperCase(),
        });

        // Setting key + description
        const info = el('div', { className: 'setting-info' });
        const keyLine = el('div', { className: 'setting-key' });
        keyLine.appendChild(el('code', { textContent: setting.key }));
        keyLine.appendChild(tierBadge);
        if (setting.compliance_tags && setting.compliance_tags.length) {
            for (const tag of setting.compliance_tags) {
                keyLine.appendChild(el('span', { className: 'compliance-tag', textContent: tag }));
            }
        }
        info.appendChild(keyLine);
        if (setting.description) {
            info.appendChild(el('div', { className: 'setting-description', textContent: setting.description }));
        }
        row.appendChild(info);

        // Current value + edit control
        const control = el('div', { className: 'setting-control' });
        const currentVal = this._extractValue(configFull, setting.key);
        const isReadonly = tier.toLowerCase() === 'hardwired' || tier.toLowerCase() === 'startup';

        if (setting.value_type === 'Bool' || typeof currentVal === 'boolean') {
            const toggle = el('label', { className: 'toggle-switch' });
            const input = el('input', { type: 'checkbox' });
            input.checked = !!currentVal;
            input.disabled = isReadonly;
            if (!isReadonly) {
                input.addEventListener('change', () => this._applyChange(setting.key, input.checked));
            }
            toggle.appendChild(input);
            toggle.appendChild(el('span', { className: 'toggle-slider' }));
            control.appendChild(toggle);
        } else if (setting.value_type && setting.value_type.Enum) {
            const select = el('select', { className: 'setting-select' });
            select.disabled = isReadonly;
            for (const variant of (setting.value_type.Enum.variants || [])) {
                const opt = el('option', { textContent: variant });
                opt.value = variant;
                if (String(currentVal) === variant) opt.selected = true;
                select.appendChild(opt);
            }
            if (!isReadonly) {
                select.addEventListener('change', () => this._applyChange(setting.key, select.value));
            }
            control.appendChild(select);
        } else {
            const input = el('input', { type: 'text', className: 'setting-input' });
            input.value = currentVal != null ? String(currentVal) : '';
            input.disabled = isReadonly;
            if (!isReadonly) {
                const btn = el('button', { className: 'btn btn-sm', textContent: 'Apply' });
                btn.addEventListener('click', () => {
                    let val = input.value;
                    if (setting.value_type === 'Int') val = parseInt(val, 10);
                    else if (setting.value_type === 'Float') val = parseFloat(val);
                    this._applyChange(setting.key, val);
                });
                control.appendChild(input);
                control.appendChild(btn);
            } else {
                control.appendChild(input);
            }
        }
        row.appendChild(control);
        return row;
    }

    _extractValue(config, key) {
        if (!config) return null;
        const parts = key.split('.');
        if (parts.length === 2) {
            // Try flat: gossip.fanout -> gossip_fanout
            const flat = parts[0] + '_' + parts[1];
            if (config[flat] !== undefined) return config[flat];
            // Try nested
            if (config[parts[0]] && config[parts[0]][parts[1]] !== undefined) return config[parts[0]][parts[1]];
            // Try bare
            if (config[parts[1]] !== undefined) return config[parts[1]];
        }
        return config[key] !== undefined ? config[key] : null;
    }

    async _applyChange(key, value) {
        try {
            const patch = {};
            patch[key] = value;
            await this.api.configUpdate(patch);
            this.app.toast.ok(`Updated ${key}`);
            await this.app.dataManager.fetchConfig();
            this._renderCategory(this._activeCategory);
        } catch (e) {
            this.app.toast.error(`Failed to update ${key}: ${e.message || e}`);
        }
    }

    _renderCompliance() {
        const posture = this.state.get('compliancePosture');
        const manifest = this.state.get('complianceManifest');
        const violations = this.state.get('complianceViolations') || [];
        const consent = this.state.get('complianceConsent') || [];

        // --- Posture overview ---
        const header = el('h2', { textContent: 'Compliance Posture' });
        this._mainArea.appendChild(header);

        if (posture) {
            const overall = posture.overall_status || 'unknown';
            const statusEl = el('div', { className: 'posture-overall posture-' + overall });
            statusEl.appendChild(el('span', { className: 'posture-dot' }));
            statusEl.appendChild(el('span', { textContent: `Overall: ${overall.toUpperCase()}` }));
            if (posture.profile_name) {
                statusEl.appendChild(el('span', { className: 'posture-profile', textContent: ` (${posture.profile_name} v${posture.profile_version || '?'})` }));
            }
            this._mainArea.appendChild(statusEl);

            const counts = el('div', { className: 'posture-counts' });
            counts.appendChild(el('span', { className: 'count-passing', textContent: `${posture.passing_count || 0} passing` }));
            counts.appendChild(el('span', { className: 'count-warning', textContent: `${posture.warning_count || 0} warnings` }));
            counts.appendChild(el('span', { className: 'count-violation', textContent: `${posture.violation_count || 0} violations` }));
            this._mainArea.appendChild(counts);

            // --- Control grid ---
            if (posture.controls && posture.controls.length) {
                const grid = el('div', { className: 'posture-grid' });
                for (const ctrl of posture.controls) {
                    const card = el('div', { className: 'posture-control posture-control--' + (ctrl.status || 'unknown') });
                    card.appendChild(el('div', { className: 'control-key', innerHTML: `<code>${escapeHtml(ctrl.key)}</code>` }));
                    card.appendChild(el('div', { className: 'control-constraint', textContent: `${ctrl.constraint_type}: ${JSON.stringify(ctrl.required_value)}` }));
                    card.appendChild(el('div', { className: 'control-current', textContent: `Current: ${JSON.stringify(ctrl.current_value)}` }));
                    card.appendChild(el('div', { className: 'control-justification text-muted', textContent: ctrl.justification }));
                    grid.appendChild(card);
                }
                this._mainArea.appendChild(grid);
            }
        } else {
            this._mainArea.appendChild(el('p', { className: 'text-muted', textContent: 'No compliance data available.' }));
        }

        // --- Profile info ---
        if (manifest && manifest.profile_name) {
            this._mainArea.appendChild(el('h3', { textContent: 'Active Profile' }));
            const info = el('div', { className: 'manifest-info' });
            info.appendChild(el('div', { textContent: `Name: ${manifest.profile_name}` }));
            info.appendChild(el('div', { textContent: `Version: ${manifest.profile_version || 'N/A'}` }));
            info.appendChild(el('div', { textContent: `Description: ${manifest.profile_description || ''}` }));
            info.appendChild(el('div', { textContent: `Overrides: ${manifest.overrides_count || 0}` }));
            if (manifest.build_features && manifest.build_features.length) {
                info.appendChild(el('div', { textContent: `Build features: ${manifest.build_features.join(', ')}` }));
            }
            this._mainArea.appendChild(info);
        }

        // --- Violations ---
        if (violations.length) {
            this._mainArea.appendChild(el('h3', { textContent: `Violations (${violations.length})` }));
            const table = el('table', { className: 'data-table' });
            table.appendChild(el('thead', {}, [
                el('tr', {}, [
                    el('th', { textContent: 'Detected' }),
                    el('th', { textContent: 'Control' }),
                    el('th', { textContent: 'Required' }),
                    el('th', { textContent: 'Current' }),
                ]),
            ]));
            const tbody = el('tbody');
            for (const v of violations.slice(0, 50)) {
                tbody.appendChild(el('tr', {}, [
                    el('td', { textContent: v.detected_at ? new Date(v.detected_at).toLocaleString() : '—' }),
                    el('td', { innerHTML: `<code>${escapeHtml(v.control?.key || '?')}</code>` }),
                    el('td', { textContent: JSON.stringify(v.control?.required_value) }),
                    el('td', { textContent: JSON.stringify(v.control?.current_value) }),
                ]));
            }
            table.appendChild(tbody);
            this._mainArea.appendChild(table);
        }

        // --- Consent records ---
        if (consent.length) {
            this._mainArea.appendChild(el('h3', { textContent: `Consent Records (${consent.length})` }));
            const table = el('table', { className: 'data-table' });
            table.appendChild(el('thead', {}, [
                el('tr', {}, [
                    el('th', { textContent: 'Node' }),
                    el('th', { textContent: 'Accepted' }),
                    el('th', { textContent: 'Auto' }),
                    el('th', { textContent: 'Recorded' }),
                ]),
            ]));
            const tbody = el('tbody');
            for (const c of consent.slice(0, 50)) {
                tbody.appendChild(el('tr', {}, [
                    el('td', { textContent: truncateId(c.node_id) }),
                    el('td', { textContent: c.accepted ? 'Yes' : 'No' }),
                    el('td', { textContent: c.auto_accepted ? 'Yes' : 'No' }),
                    el('td', { textContent: c.recorded_at ? new Date(c.recorded_at).toLocaleString() : '—' }),
                ]));
            }
            table.appendChild(tbody);
            this._mainArea.appendChild(table);
        }

        // --- Report generation ---
        this._mainArea.appendChild(el('h3', { textContent: 'Reports' }));
        const reportBtns = el('div', { className: 'report-buttons' });
        const jsonBtn = el('button', { className: 'btn', textContent: 'Download JSON Report' });
        jsonBtn.addEventListener('click', async () => {
            try {
                const report = await this.api.complianceReport('json');
                const blob = new Blob([JSON.stringify(report, null, 2)], { type: 'application/json' });
                const url = URL.createObjectURL(blob);
                const a = el('a', { href: url, download: 'compliance-report.json' });
                a.click();
                URL.revokeObjectURL(url);
            } catch (e) { this.app.toast.error('Failed to generate report'); }
        });
        reportBtns.appendChild(jsonBtn);

        const htmlBtn = el('button', { className: 'btn', textContent: 'Download HTML Report' });
        htmlBtn.addEventListener('click', async () => {
            try {
                const resp = await fetch(this.api.baseUrl + '/compliance/report?format=html');
                const html = await resp.text();
                const blob = new Blob([html], { type: 'text/html' });
                const url = URL.createObjectURL(blob);
                const a = el('a', { href: url, download: 'compliance-report.html' });
                a.click();
                URL.revokeObjectURL(url);
            } catch (e) { this.app.toast.error('Failed to generate HTML report'); }
        });
        reportBtns.appendChild(htmlBtn);
        this._mainArea.appendChild(reportBtns);
    }

    _renderHistory() {
        const history = this.state.get('configHistory') || [];

        const header = el('h2', { textContent: 'Configuration History' });
        this._mainArea.appendChild(header);

        // --- Import/Export buttons ---
        const ioBtns = el('div', { className: 'io-buttons' });
        const exportBtn = el('button', { className: 'btn', textContent: 'Export Config (JSON)' });
        exportBtn.addEventListener('click', async () => {
            try {
                const config = await this.api.configFull();
                const blob = new Blob([JSON.stringify(config, null, 2)], { type: 'application/json' });
                const url = URL.createObjectURL(blob);
                const a = el('a', { href: url, download: 'swarm-config.json' });
                a.click();
                URL.revokeObjectURL(url);
            } catch (e) { this.app.toast.error('Failed to export config'); }
        });
        ioBtns.appendChild(exportBtn);
        this._mainArea.appendChild(ioBtns);

        // --- Timeline ---
        if (history.length === 0) {
            this._mainArea.appendChild(el('p', { className: 'text-muted', textContent: 'No configuration history available.' }));
            return;
        }

        const timeline = el('div', { className: 'config-timeline' });
        for (const entry of history.slice(0, 100)) {
            const item = el('div', { className: 'timeline-item' });
            item.appendChild(el('div', { className: 'timeline-dot' }));
            const content = el('div', { className: 'timeline-content' });
            content.appendChild(el('div', { className: 'timeline-time', textContent: entry.changed_at ? new Date(entry.changed_at).toLocaleString() : entry.valid_from ? new Date(entry.valid_from).toLocaleString() : '—' }));
            content.appendChild(el('div', { className: 'timeline-key', innerHTML: `<code>${escapeHtml(entry.key || '?')}</code> by ${escapeHtml(entry.changed_by || entry.source || '?')}` }));
            if (entry.old_value !== undefined || entry.prev_value !== undefined) {
                const old = entry.old_value !== undefined ? entry.old_value : entry.prev_value;
                content.appendChild(el('div', { className: 'timeline-diff', textContent: `${JSON.stringify(old)} → ${JSON.stringify(entry.new_value !== undefined ? entry.new_value : entry.value_json)}` }));
            }
            item.appendChild(content);
            timeline.appendChild(item);
        }
        this._mainArea.appendChild(timeline);
    }
}

// ============================================================================
// DataManager — Centralized data fetching
// ============================================================================

class DataManager {
    constructor(api, state, toast) {
        this.api = api;
        this.state = state;
        this.toast = toast;
        this._polling = null;
    }

    
    async fetchLedger() {
        try {
            const l = await this.api.ledger();
            if (l) this.state.set('ledger', l);
        } catch {}
    }

    async fetchAll() {
        const results = await Promise.allSettled([
            this.fetchPsyche(), this.fetchNodes(), this.fetchJobs(), this.fetchEvents(),
            this.fetchAlerts(), this.fetchFleet(), this.fetchSla(), this.fetchCapacity(),
            this.fetchConstellation(), this.fetchAudit(),
        ]);
        const failed = results.filter(r => r.status === 'rejected');
        if (failed.length && failed.length < results.length) this.toast.warn(`${failed.length} data sources unavailable`);
    }

    async fetchForVision(vision) {
        const map = {
            'noc-operator': ['Psyche', 'Nodes', 'Events', 'Alerts', 'Fleet'],
            'developer': ['Jobs', 'Events', 'Psyche'],
            'executive': ['Psyche', 'Sla', 'Capacity', 'Nodes', 'Jobs', 'Alerts'],
            'capacity-planner': ['Capacity', 'Nodes'],
            'security-auditor': ['Audit', 'Alerts'],
            'constellation': ['Constellation'],
            'swarm-whisperer': ['Psyche', 'Nodes', 'Jobs', 'Events', 'Alerts', 'Fleet', 'Sla', 'Capacity', 'Constellation', 'Audit'],
            'settings': ['Config'],
        };
        await Promise.allSettled((map[vision] || map['noc-operator']).map(f => this[`fetch${f}`]()));
    }

    startPolling(vision, ms = 10000) { this.stopPolling(); this._polling = setInterval(() => this.fetchForVision(vision), ms); }
    stopPolling() { if (this._polling) { clearInterval(this._polling); this._polling = null; } }

    async fetchPsyche() {
        try { const [p, t] = await Promise.all([this.api.psyche(), this.api.psycheTrends()]); if (p) this.state.set('psyche', p); if (t) this.state.set('psycheTrends', t); } catch {}
    }
    async fetchNodes() {
        try { const d = await this.api.nodes(); if (d) this.state.set('nodes', Array.isArray(d) ? d : d.items || []); } catch {}
    }
    async fetchJobs() {
        try { const d = await this.api.jobs(); if (d) this.state.set('jobs', Array.isArray(d) ? d : d.items || []); } catch {}
    }
    async fetchEvents() {
        try { const d = await this.api.events({ limit: 200 }); if (d) this.state.set('events', Array.isArray(d) ? d : d.items || []); } catch {}
    }
    async fetchAlerts() {
        try {
            const [a, s] = await Promise.all([this.api.alerts(), this.api.alertSummary()]);
            if (a) this.state.set('alerts', Array.isArray(a) ? a : a.items || []);
            if (s) this.state.set('alertSummary', s);
        } catch {}
    }
    async fetchFleet() {
        try { const [f, u] = await Promise.all([this.api.fleetStatus(), this.api.updateStatus()]); if (f) this.state.set('fleet', f); if (u) this.state.set('updateStatus', u); } catch {}
    }
    async fetchSla() {
        try { const d = await this.api.slaStatuses(); if (d) this.state.set('sla', Array.isArray(d) ? d : d.items || []); } catch {}
    }
    async fetchCapacity() {
        try {
            const [c, f, b] = await Promise.all([this.api.capacity(), this.api.capacityForecast(), this.api.capacityBottlenecks()]);
            if (c) this.state.set('capacity', c); if (f) this.state.set('capacityForecast', Array.isArray(f) ? f : []); if (b) this.state.set('bottlenecks', Array.isArray(b) ? b : []);
        } catch {}
    }
    async fetchConstellation() {
        try {
            const [c, m, t] = await Promise.all([this.api.constellation(), this.api.membranes(), this.api.treaties()]);
            if (c) this.state.set('constellation', c); if (m) this.state.set('membranes', Array.isArray(m) ? m : []); if (t) this.state.set('treaties', Array.isArray(t) ? t : []);
        } catch {}
    }
    async fetchAudit() {
        try { const [e, v] = await Promise.all([this.api.auditLog({ limit: 100 }), this.api.auditVerify()]); if (e) this.state.set('audit', Array.isArray(e) ? e : e.items || []); if (v) this.state.set('auditVerification', v); } catch {}
    }

    async fetchConfig() {
        try {
            const [schema, full, history, posture, manifest, consent, violations] = await Promise.allSettled([
                this.api.configSchema(),
                this.api.configFull(),
                this.api.configHistory({ limit: 50 }),
                this.api.compliancePosture(),
                this.api.complianceManifest(),
                this.api.complianceConsent(),
                this.api.complianceViolations(),
            ]);
            if (schema.status === 'fulfilled' && schema.value) this.state.set('configSchema', schema.value);
            if (full.status === 'fulfilled' && full.value) this.state.set('configFull', full.value);
            if (history.status === 'fulfilled' && history.value) this.state.set('configHistory', Array.isArray(history.value) ? history.value : history.value.items || []);
            if (posture.status === 'fulfilled' && posture.value) this.state.set('compliancePosture', posture.value);
            if (manifest.status === 'fulfilled' && manifest.value) this.state.set('complianceManifest', manifest.value);
            if (consent.status === 'fulfilled' && consent.value) this.state.set('complianceConsent', consent.value.consent_records || []);
            if (violations.status === 'fulfilled' && violations.value) this.state.set('complianceViolations', violations.value.violations || []);
        } catch {}
    }
}

// ============================================================================
// App — Main application controller
// ============================================================================

class App {
    constructor() {
        this.state = new State();
        this.api = new SwarmAPI();
        this.theme = new ThemeManager();
        this.toast = new ToastManager();
        this.modal = new ModalManager();
        this.detailPanel = new DetailPanelManager();
        this.dataManager = new DataManager(this.api, this.state, this.toast);
        this.cmdPalette = new CommandPalette(this);
        this.keyboard = new KeyboardManager(this);
        this._visionRenderers = {
            'noc-operator': NocVisionRenderer, 'developer': DeveloperVisionRenderer,
            'executive': ExecutiveVisionRenderer, 'capacity-planner': CapacityPlannerVisionRenderer,
            'security-auditor': SecurityAuditorVisionRenderer, 'constellation': ConstellationVisionRenderer,
            'swarm-whisperer': SwarmWhispererVisionRenderer,
            'settings': SettingsVisionRenderer, 'economy': EconomyVisionRenderer,
        };
        this._currentRenderer = null;
        this._contentEl = document.getElementById('vision-content');
        this._visionSelect = document.getElementById('vision-select');
        this._stream = new EventStream(this.api.baseUrl, {
            onEvent: (ev) => this._handleSSE(ev),
            onConnect: () => this._setConnection(true),
            onDisconnect: () => this._setConnection(false),
        });
        this._visionSelect.addEventListener('change', () => this.switchVision(this._visionSelect.value));
        document.getElementById('theme-toggle').addEventListener('click', () => this.theme.toggle());
        document.getElementById('sidebar-toggle')?.addEventListener('click', () => document.getElementById('sidebar').classList.toggle('open'));
        document.getElementById('keyboard-help').querySelector('.modal-close')?.addEventListener('click', () => document.getElementById('keyboard-help').close());
    }

    async init() {
        const healthy = await this.api.healthCheck();
        if (!healthy) this.toast.warn('API may be unreachable');
        try {
            const info = await this.api.serverInfo();
            if (info) {
                document.getElementById('swarm-name').textContent = info.swarm_name || info.name || 'Swarm';
                document.title = `${info.swarm_name || info.name || 'Swarm'} — Management Console`;
                const ver = document.getElementById('version-badge')?.querySelector('span');
                if (ver && info.version) ver.textContent = `v${info.version}`;
            }
        } catch {}

        const defaultVisions = [
            { name: 'noc-operator', label: 'NOC Operator' }, { name: 'developer', label: 'Developer' },
            { name: 'executive', label: 'Executive' }, { name: 'capacity-planner', label: 'Capacity Planner' },
            { name: 'security-auditor', label: 'Security Auditor' }, { name: 'constellation', label: 'Constellation' },
            { name: 'swarm-whisperer', label: 'Swarm Whisperer' },
            { name: 'settings', label: 'Settings' },
        ];
        try {
            const sv = await this.api.visions();
            this.state.set('visions', Array.isArray(sv) && sv.length ? sv : defaultVisions);
        } catch { this.state.set('visions', defaultVisions); }

        this._populateVisionSelect();
        await this.dataManager.fetchAll();
        this.switchVision(localStorage.getItem('swarm-vision') || 'noc-operator');
        this._stream.connect();
        document.getElementById('loading-skeleton').style.display = 'none';
        document.body.classList.add('loaded');
        this._updateSidebarCounts();
        this.state.subscribe('nodes', () => this._updateSidebarCounts());
        this.state.subscribe('alerts', () => this._updateSidebarCounts());
    }

    switchVision(name) {
        if (this._currentRenderer) { this._currentRenderer.destroy(); this._currentRenderer = null; }
        this.dataManager.stopPolling();
        const Cls = this._visionRenderers[name] || NocVisionRenderer;
        this._currentRenderer = new Cls(this._contentEl, this.state, this.api, this);
        this._currentRenderer.render();
        this._visionSelect.value = name;
        localStorage.setItem('swarm-vision', name);
        this.state.set('ui.vision', name);
        this._updateSidebar(name);
        const intervals = { 'noc-operator': 5000, 'developer': 10000, 'executive': 300000, 'capacity-planner': 30000, 'security-auditor': 60000, 'constellation': 10000, 'swarm-whisperer': 3000 };
        this.dataManager.startPolling(name, intervals[name] || 10000);
        announceToSR(`Switched to ${name} vision`);
    }

    showNodeDetail(nodeId) {
        const node = (this.state.get('nodes') || []).find(n => (n.id || n.node_id) === nodeId);
        if (!node) { this.toast.warn('Node not found'); return; }
        const content = el('div', { className: 'flex flex-col gap-3' });
        for (const [label, value] of [
            ['ID', node.id || node.node_id || '—'], ['Status', node.status || '—'], ['Region', node.region || '—'],
            ['Load', node.load != null ? formatPercent(node.load * 100) : '—'],
            ['Uptime', node.uptime_secs != null ? formatDuration(node.uptime_secs) : '—'],
            ['Traits', (node.traits || []).join(', ') || '—'],
            ['Memory', node.memory_mb != null ? formatBytes(node.memory_mb * 1048576) : '—'],
        ]) {
            content.appendChild(el('div', { className: 'flex justify-between' }, [
                el('span', { className: 'text-muted', textContent: label }), el('span', { textContent: value }),
            ]));
        }
        const footer = el('div', { className: 'flex gap-2' }, [
            el('button', { className: 'btn btn-sm', textContent: 'Drain', onClick: () => this.modal.confirm('Drain Node', `Drain ${truncateId(nodeId)}?`, () => this.api.drainNode(nodeId, 300, 'UI drain').then(() => this.toast.success('Drain started')).catch(() => this.toast.error('Drain failed'))) }),
            el('button', { className: 'btn btn-sm', textContent: 'Cordon', onClick: () => this.api.cordonNode(nodeId, 'UI cordon').then(() => this.toast.success('Cordoned')).catch(() => this.toast.error('Cordon failed')) }),
        ]);
        this.detailPanel.open(`Node ${truncateId(nodeId)}`, content, footer);
    }

    _handleSSE(ev) {
        const events = this.state.get('events') || [];
        events.push(ev);
        if (events.length > 500) events.splice(0, events.length - 500);
        this.state.set('events', events);
        if (this._currentRenderer?.handleSSEEvent) this._currentRenderer.handleSSEEvent(ev);
        if (ev.severity === 'critical' || ev.severity === 'error') this.toast.show(ev.summary || ev.message || 'Critical event', 'error', 10000);
        if (ev.type === 'PsycheBroadcast' && ev.psyche) {
            this.state.set('psyche', ev.psyche);
            if (this._currentRenderer?._psyche) this._currentRenderer._psyche.updateMiniBar();
        }
    }

    _setConnection(connected) {
        const dot = document.getElementById('connection-status')?.querySelector('.status-dot');
        const text = document.getElementById('connection-status')?.querySelector('.status-text');
        if (dot) dot.className = `status-dot ${connected ? 'status-dot--alive' : 'status-dot--dead'}`;
        if (text) text.textContent = connected ? 'Connected' : 'Disconnected';
    }

    _populateVisionSelect() {
        this._visionSelect.innerHTML = '';
        for (const v of (this.state.get('visions') || [])) {
            this._visionSelect.appendChild(el('option', { value: v.name || v, textContent: v.label || v.name || v }));
        }
    }

    _updateSidebar(vision) {
        const navList = document.getElementById('nav-list');
        removeChildren(navList);
        const map = {
            'noc-operator': [['psyche','psyche','Psyche'],['events','events','Events'],['alerts','alerts','Alerts'],['nodes','nodes','Nodes']],
            'developer': [['jobs','jobs','Jobs'],['events','events','Events']],
            'executive': [['overview','psyche','Overview'],['economy','economy','Economy'],['sla','sla','SLA']],
            'capacity-planner': [['capacity','capacity','Capacity']],
            'security-auditor': [['audit','audit','Audit'],['alerts','alerts','Alerts']],
            'constellation': [['graph','constellation','Constellation']],
            'swarm-whisperer': [['psyche','psyche','Psyche'],['economy','economy','Economy'],['events','events','Events'],['alerts','alerts','Alerts'],['nodes','nodes','Nodes'],['jobs','jobs','Jobs'],['fleet','fleet','Fleet'],['sla','sla','SLA'],['capacity','capacity','Capacity'],['constellation','constellation','Constellation'],['audit','audit','Audit']],
            'settings': [['network','nodes','Network'],['execution','jobs','Execution'],['security','alerts','Security'],['compliance','audit','Compliance'],['history','events','History']],
        };
        for (const [id, icon, label] of (map[vision] || map['noc-operator'])) {
            const li = el('li', { className: 'nav-item', role: 'none' });
            li.appendChild(el('a', { href: `#${id}`, className: 'nav-link', role: 'menuitem' }, [
                el('span', { className: 'nav-icon', innerHTML: ICONS[icon] || '' }),
                el('span', { className: 'nav-label', textContent: label }),
            ]));
            navList.appendChild(li);
        }
    }

    _updateSidebarCounts() {
        const nodeEl = document.getElementById('node-count-badge')?.querySelector('span');
        const alertEl = document.getElementById('alert-count-badge')?.querySelector('span');
        if (nodeEl) nodeEl.textContent = `${(this.state.get('nodes') || []).length} nodes`;
        if (alertEl) alertEl.textContent = `${(this.state.get('alerts') || []).length} alerts`;
    }
}

// ============================================================================
// BOOT
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    const app = new App();
    window.__swarmApp = app;
    app.init().catch(err => {
        console.error('Boot failed:', err);
        const skeleton = document.getElementById('loading-skeleton');
        if (skeleton) skeleton.innerHTML = `<div class="error-state"><h2 style="margin-bottom:8px">Failed to connect to swarm</h2><p style="margin-bottom:16px">${escapeHtml(err.message || 'Unknown error')}</p><button class="btn btn-primary" onclick="location.reload()">Retry</button></div>`;
    });
});
