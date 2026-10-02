// Marabunta - Licensed under the MIT License.
/**
 * Marabunta Radiation — Job Submission UI
 *
 * Single-page application for managing distributed compute jobs.
 * Communicates with the swarm API at /api/v1/combo/*.
 */

(function () {
  'use strict';

  // ========================================================================
  // Configuration
  // ========================================================================

  const API_BASE = '/api/v1';
  const SSE_KEEPALIVE_MS = 15000;
  const POLL_INTERVAL_MS = 5000;
  const MAX_LOG_ENTRIES = 200;

  // ========================================================================
  // State
  // ========================================================================

  const DEFAULT_SETTINGS = {
    defaultChunkSize: 'auto',
    defaultRetries: 3,
    defaultTimeout: 300,
    preferredRegion: 'any',
    defaultPriority: 'standard',
    costCurrency: 'USD',
    favoriteModules: [],
    recentModules: [],
  };

  function loadSettings() {
    try {
      const saved = localStorage.getItem('cr-settings');
      return saved ? Object.assign({}, DEFAULT_SETTINGS, JSON.parse(saved)) : Object.assign({}, DEFAULT_SETTINGS);
    } catch (_) {
      return Object.assign({}, DEFAULT_SETTINGS);
    }
  }

  function saveSettings(settings) {
    try {
      localStorage.setItem('cr-settings', JSON.stringify(settings));
    } catch (_) { /* localStorage unavailable */ }
  }

  const state = {
    currentView: 'module-browser',
    modules: [],
    selectedModule: null,
    uploadedFiles: [],
    currentStep: 1,
    totalSteps: 5,
    activeJobId: null,
    sseSource: null,
    pollTimer: null,
    history: [],
    settings: loadSettings(),
  };

  // ========================================================================
  // DOM references
  // ========================================================================

  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

  // ========================================================================
  // Initialisation
  // ========================================================================

  document.addEventListener('DOMContentLoaded', init);

  function init() {
    // Mobile nav toggle
    const navToggle = document.getElementById('nav-toggle');
    const mainNav = document.querySelector('.main-nav');
    if (navToggle && mainNav) {
        navToggle.addEventListener('click', () => {
            mainNav.classList.toggle('open');
            navToggle.setAttribute('aria-expanded', mainNav.classList.contains('open'));
        });
    }

    setupNavigation();
    setupThemeToggle();
    setupModuleBrowser();
    setupMultiStepForm();
    setupFileUpload();
    setupJobStatus();
    setupHistory();
    setupSettings();
    applyDefaultsToForm();
    loadModules();
  }

  // ========================================================================
  // Navigation
  // ========================================================================

  function setupNavigation() {
    $$('[data-view]').forEach(el => {
      el.addEventListener('click', (e) => {
        e.preventDefault();
        showView(el.dataset.view);
      });
    });
  }

  function showView(viewId) {
    $$('.view').forEach(v => v.classList.add('hidden'));
    const target = document.getElementById(viewId);
    if (target) target.classList.remove('hidden');

    $$('.nav-link').forEach(a => {
      a.classList.toggle('active', a.dataset.view === viewId);
      if (a.dataset.view === viewId) {
        a.setAttribute('aria-current', 'page');
      } else {
        a.removeAttribute('aria-current');
      }
    });

    state.currentView = viewId;

    if (viewId === 'job-history') loadHistory();
  }

  // ========================================================================
  // Theme toggle
  // ========================================================================

  function setupThemeToggle() {
    const btn = $('#theme-toggle');
    if (!btn) return;

    const saved = localStorage.getItem('cr-theme');
    if (saved) document.documentElement.dataset.theme = saved;

    btn.addEventListener('click', () => {
      const current = document.documentElement.dataset.theme;
      const next = current === 'dark' ? 'light' : 'dark';
      document.documentElement.dataset.theme = next;
      localStorage.setItem('cr-theme', next);
    });
  }

  // ========================================================================
  // Module browser
  // ========================================================================

  function setupModuleBrowser() {
    const search = $('#module-search');
    if (search) {
      search.addEventListener('input', () => renderModules(search.value.trim()));
    }

    $$('.category-chip').forEach(chip => {
      chip.addEventListener('click', () => {
        $$('.category-chip').forEach(c => c.classList.remove('selected'));
        chip.classList.add('selected');
        const cat = chip.dataset.category || '';
        renderModules($('#module-search')?.value || '', cat);
      });
    });
  }

  async function loadModules() {
    try {
      const resp = await api('GET', '/combo/modules');
      state.modules = resp;
      renderModules();
    } catch (err) {
      console.error('Failed to load modules:', err);
      renderModuleError();
    }
  }

  function renderModules(search = '', category = '') {
    const grid = $('#module-grid');
    if (!grid) return;

    const lower = search.toLowerCase();
    const filtered = state.modules.filter(m => {
      const matchSearch = !lower ||
        m.id.toLowerCase().includes(lower) ||
        m.name.toLowerCase().includes(lower) ||
        m.description.toLowerCase().includes(lower);
      const matchCat = !category || category === 'all' || m.category === category;
      return matchSearch && matchCat;
    });

    if (filtered.length === 0) {
      grid.innerHTML = '<p class="empty-state">No modules match your search.</p>';
      return;
    }

    grid.innerHTML = filtered.map(m => `
      <button class="module-card" data-module-id="${esc(m.id)}" type="button">
        <div class="module-card-header">
          <span class="module-icon">${categoryIcon(m.category)}</span>
          <span class="module-category">${esc(m.category)}</span>
        </div>
        <h3 class="module-name">${esc(m.name)}</h3>
        <p class="module-desc">${esc(m.description)}</p>
        <span class="module-version">v${esc(m.version)}</span>
      </button>
    `).join('');

    grid.querySelectorAll('.module-card').forEach(card => {
      card.addEventListener('click', () => selectModule(card.dataset.moduleId));
    });
  }

  function renderModuleError() {
    const grid = $('#module-grid');
    if (grid) {
      grid.innerHTML = '<p class="empty-state error">Failed to load modules. Check API connection.</p>';
    }
  }

  async function selectModule(moduleId) {
    try {
      const detail = await api('GET', `/combo/modules/${moduleId}`);
      state.selectedModule = detail;

      const nameEl = $('#selected-module-name');
      const descEl = $('#selected-module-desc');
      if (nameEl) nameEl.textContent = detail.name;
      if (descEl) descEl.textContent = detail.description;

      trackModuleUsage(moduleId, detail.name);
      state.currentStep = 1;
      updateSteps();
      applyDefaultsToForm();
      showView('submit-form');
    } catch (err) {
      console.error('Failed to load module detail:', err);
    }
  }

  // ========================================================================
  // Multi-step form
  // ========================================================================

  function setupMultiStepForm() {
    const nextBtn = $('#next-step');
    const prevBtn = $('#prev-step');

    if (nextBtn) nextBtn.addEventListener('click', nextStep);
    if (prevBtn) prevBtn.addEventListener('click', prevStep);

    const form = $('#job-form');
    if (form) {
      form.addEventListener('submit', (e) => {
        e.preventDefault();
        submitJob();
      });
    }

    // Priority cards
    $$('.priority-card').forEach(card => {
      card.addEventListener('click', () => {
        $$('.priority-card').forEach(c => c.classList.remove('selected'));
        card.classList.add('selected');
      });
    });
  }

  function nextStep() {
    if (state.currentStep < state.totalSteps) {
      state.currentStep++;
      updateSteps();
      if (state.currentStep === state.totalSteps) populateReview();
    }
  }

  function prevStep() {
    if (state.currentStep > 1) {
      state.currentStep--;
      updateSteps();
    }
  }

  function updateSteps() {
    const step = state.currentStep;

    $$('.form-step').forEach(fs => {
      const s = parseInt(fs.dataset.step, 10);
      fs.classList.toggle('hidden', s !== step);
    });

    $$('.stepper .step').forEach(s => {
      const n = parseInt(s.dataset.step, 10);
      s.classList.toggle('active', n === step);
      s.classList.toggle('completed', n < step);
      if (n === step) {
        s.setAttribute('aria-current', 'step');
      } else {
        s.removeAttribute('aria-current');
      }
    });

    const prev = $('#prev-step');
    const next = $('#next-step');
    if (prev) prev.disabled = step <= 1;
    if (next) next.classList.toggle('hidden', step >= state.totalSteps);
  }

  function populateReview() {
    setText('#review-module', state.selectedModule?.name || '--');
    setText('#review-name', val('#param-name') || '--');
    setText('#review-files', state.uploadedFiles.length > 0
      ? state.uploadedFiles.map(f => f.name).join(', ')
      : 'None');
    setText('#review-chunk', val('#param-chunk-size') || 'Auto');
    setText('#review-priority', getSelectedPriority());
    setText('#review-region', val('#param-region') || 'Any');
    setText('#review-retries', val('#param-retries') || '3');
    setText('#review-timeout', (val('#param-timeout') || '300') + 's');

    estimateCost();
  }

  async function estimateCost() {
    if (!state.selectedModule) return;

    try {
      const body = {
        module_id: state.selectedModule.id,
        total_items: state.uploadedFiles.length || 1,
        priority: getSelectedPriority(),
      };
      const est = await api('POST', '/combo/estimate', body);

      setText('#cost-compute', '$' + (est.estimated_cost_usd || 0).toFixed(2));
      setText('#cost-total', '$' + (est.estimated_cost_usd || 0).toFixed(2));

      const prio = getSelectedPriority();
      const mult = prio === 'rush' ? '3.0x' : prio === 'economy' ? '0.5x' : '1.0x';
      setText('#cost-multiplier', mult);
    } catch (err) {
      setText('#cost-total', 'Estimate unavailable');
    }
  }

  function getSelectedPriority() {
    const selected = document.querySelector('.priority-card.selected');
    return selected ? (selected.dataset.priority || 'standard') : 'standard';
  }

  // ========================================================================
  // File upload
  // ========================================================================

  function setupFileUpload() {
    const dropZone = $('#drop-zone');
    const fileInput = $('#file-input');

    if (!dropZone || !fileInput) return;

    dropZone.addEventListener('click', () => fileInput.click());
    dropZone.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        fileInput.click();
      }
    });

    dropZone.addEventListener('dragover', (e) => {
      e.preventDefault();
      dropZone.classList.add('dragover');
    });
    dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
    dropZone.addEventListener('drop', (e) => {
      e.preventDefault();
      dropZone.classList.remove('dragover');
      handleFiles(e.dataTransfer.files);
    });

    fileInput.addEventListener('change', () => handleFiles(fileInput.files));
  }

  function handleFiles(fileList) {
    for (const file of fileList) {
      state.uploadedFiles.push(file);
    }
    renderFileList();
  }

  function renderFileList() {
    const list = $('#file-list');
    if (!list) return;

    list.innerHTML = state.uploadedFiles.map((f, i) => `
      <li class="file-item">
        <span class="file-name">${esc(f.name)}</span>
        <span class="file-size">${formatBytes(f.size)}</span>
        <button class="btn btn-ghost btn-xs file-remove" data-index="${i}" type="button" aria-label="Remove ${esc(f.name)}">×</button>
      </li>
    `).join('');

    list.querySelectorAll('.file-remove').forEach(btn => {
      btn.addEventListener('click', () => {
        state.uploadedFiles.splice(parseInt(btn.dataset.index, 10), 1);
        renderFileList();
      });
    });
  }

  // ========================================================================
  // Job submission
  // ========================================================================

  async function submitJob() {
    if (!state.selectedModule) return;

    const btn = $('#submit-btn');
    if (btn) { btn.disabled = true; btn.textContent = 'Submitting...'; }

    try {
      const formData = new FormData();

      const params = {
        module: state.selectedModule.id,
        name: val('#param-name') || 'untitled',
        priority: getSelectedPriority(),
        chunk_size: val('#param-chunk-size') || 'auto',
        max_retries: parseInt(val('#param-retries') || '3', 10),
        chunk_timeout: parseInt(val('#param-timeout') || '300', 10),
        min_nodes: parseInt(val('#param-min-nodes') || '1', 10),
        preferred_region: val('#param-region') || 'any',
        env_vars: parseEnvVars(val('#param-env') || ''),
      };

      formData.append('params', JSON.stringify(params));

      for (const file of state.uploadedFiles) {
        formData.append('file', file, file.name);
      }

      const resp = await apiRaw('POST', '/combo/jobs', formData);
      const result = await resp.json();

      state.activeJobId = result.job_id;
      showView('job-status');
      startTracking(result.job_id);

    } catch (err) {
      console.error('Submission failed:', err);
      alert('Job submission failed: ' + err.message);
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = 'Submit Job'; }
    }
  }

  // ========================================================================
  // Job status & tracking
  // ========================================================================

  function setupJobStatus() {
    const trackBtn = document.querySelector('#job-status .btn-primary');
    const input = $('#status-job-input');

    if (trackBtn && input) {
      trackBtn.addEventListener('click', () => {
        const jobId = input.value.trim();
        if (jobId) startTracking(jobId);
      });
      input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          const jobId = input.value.trim();
          if (jobId) startTracking(jobId);
        }
      });
    }
  }

  function startTracking(jobId) {
    stopTracking();
    state.activeJobId = jobId;

    const input = $('#status-job-input');
    if (input) input.value = jobId;

    // Try SSE first, fall back to polling.
    trySSE(jobId);
    pollJobStatus(jobId);
    state.pollTimer = setInterval(() => pollJobStatus(jobId), POLL_INTERVAL_MS);
  }

  function stopTracking() {
    if (state.sseSource) {
      state.sseSource.close();
      state.sseSource = null;
    }
    if (state.pollTimer) {
      clearInterval(state.pollTimer);
      state.pollTimer = null;
    }
  }

  function trySSE(jobId) {
    const url = `${API_BASE}/combo/jobs/${jobId}/stream`;
    const source = new EventSource(url);
    state.sseSource = source;

    source.addEventListener('chunk_completed', (e) => {
      addLogEntry('success', 'Chunk completed: ' + e.data);
    });
    source.addEventListener('chunk_failed', (e) => {
      addLogEntry('error', 'Chunk failed: ' + e.data);
    });
    source.addEventListener('progress', (e) => {
      try {
        const data = JSON.parse(e.data);
        updateProgressFromSSE(data);
      } catch (_) { /* ignore parse errors */ }
    });
    source.addEventListener('job_completed', () => {
      addLogEntry('success', 'Job completed!');
      stopTracking();
      pollJobStatus(jobId);
    });
    source.addEventListener('job_failed', (e) => {
      addLogEntry('error', 'Job failed: ' + (e.data || 'unknown error'));
      stopTracking();
    });
    source.onerror = () => {
      source.close();
      state.sseSource = null;
    };
  }

  async function pollJobStatus(jobId) {
    try {
      const status = await api('GET', `/combo/jobs/${jobId}`);
      updateStatusUI(status);
    } catch (err) {
      console.error('Poll failed:', err);
    }
  }

  function updateStatusUI(status) {
    setText('#stat-completion', (status.progress_pct || 0).toFixed(0) + '%');
    setText('#stat-nodes', status.active_nodes || '--');
    setText('#stat-cost', '$' + (status.cost_usd || 0).toFixed(2));
    setText('#stat-eta', status.eta || '--');
    setText('#stat-retry-rate', (status.retry_rate_pct || 0).toFixed(1) + '%');
    setText('#stat-throughput', status.throughput || '--');

    // Update progress bars.
    const total = status.chunks_total || 1;
    const done = status.chunks_completed || 0;
    const running = status.chunks_running || 0;
    const queued = status.chunks_queued || 0;
    const retrying = status.chunks_retrying || 0;

    setProgress('.progress-done', (done / total) * 100, `${done} / ${total}`);
    setProgress('.progress-running', (running / total) * 100, running);
    setProgress('.progress-queued', (queued / total) * 100, queued);
    setProgress('.progress-retrying', (retrying / total) * 100, retrying);

    if (status.status === 'completed' || status.status === 'failed') {
      stopTracking();
    }
  }

  function updateProgressFromSSE(data) {
    if (data.progress_pct !== undefined) {
      setText('#stat-completion', data.progress_pct.toFixed(0) + '%');
    }
    if (data.cost_usd !== undefined) {
      setText('#stat-cost', '$' + data.cost_usd.toFixed(2));
    }
  }

  function setProgress(selector, pct, label) {
    const row = document.querySelector(selector)?.closest('.progress-row');
    if (!row) return;
    const fill = row.querySelector('.progress-fill');
    const val = row.querySelector('.progress-value');
    if (fill) {
      fill.style.width = pct.toFixed(1) + '%';
      fill.setAttribute('aria-valuenow', Math.round(pct));
    }
    if (val) val.textContent = label;
  }

  // ========================================================================
  // Event log
  // ========================================================================

  function addLogEntry(level, message) {
    const log = $('#event-log');
    if (!log) return;

    const now = new Date();
    const time = now.toTimeString().slice(0, 8);
    const cls = level === 'error' ? 'log-error' : level === 'success' ? 'log-success' : level === 'warn' ? 'log-warn' : 'log-info';

    const entry = document.createElement('div');
    entry.className = 'log-entry ' + cls;
    entry.innerHTML = `<time>${esc(time)}</time><span>${esc(message)}</span>`;

    log.appendChild(entry);
    log.scrollTop = log.scrollHeight;

    // Cap log size.
    while (log.children.length > MAX_LOG_ENTRIES) {
      log.removeChild(log.firstChild);
    }
  }

  // ========================================================================
  // Job history
  // ========================================================================

  function setupHistory() {
    const filter = $('#history-status-filter');
    const search = $('#history-search');

    if (filter) filter.addEventListener('change', () => renderHistory());
    if (search) search.addEventListener('input', () => renderHistory());
  }

  async function loadHistory() {
    try {
      const jobs = await api('GET', '/combo/jobs?limit=50');
      state.history = Array.isArray(jobs) ? jobs : [];
      renderHistory();
    } catch (err) {
      console.error('Failed to load history:', err);
    }
  }

  function renderHistory() {
    const tbody = document.querySelector('#job-history tbody');
    if (!tbody) return;

    const statusFilter = val('#history-status-filter') || 'all';
    const search = (val('#history-search') || '').toLowerCase();

    const filtered = state.history.filter(j => {
      const matchStatus = statusFilter === 'all' || j.status === statusFilter;
      const matchSearch = !search ||
        (j.job_id || '').toLowerCase().includes(search) ||
        (j.name || '').toLowerCase().includes(search);
      return matchStatus && matchSearch;
    });

    tbody.innerHTML = filtered.map(j => `
      <tr>
        <td class="mono">${esc(j.job_id || '--')}</td>
        <td>${esc(j.name || '--')}</td>
        <td>${esc(j.module_id || '--')}</td>
        <td>${formatDate(j.created_at)}</td>
        <td>${j.duration || '--'}</td>
        <td>$${(j.cost_usd || 0).toFixed(2)}</td>
        <td><span class="status-badge status-${esc(j.status || 'unknown')}">${esc(j.status || '--')}</span></td>
        <td><button class="btn btn-ghost btn-sm" onclick="window.crApp.trackJob('${esc(j.job_id)}')">Track</button></td>
      </tr>
    `).join('');
  }

  // ========================================================================
  // API helpers
  // ========================================================================

  async function api(method, path, body) {
    const opts = {
      method,
      headers: { 'Accept': 'application/json' },
    };
    if (body && method !== 'GET') {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    }

    const token = localStorage.getItem('cr-token');
    if (token) opts.headers['Authorization'] = 'Bearer ' + token;

    const resp = await fetch(API_BASE + path, opts);
    if (!resp.ok) throw new Error(`API ${resp.status}: ${await resp.text()}`);
    return resp.json();
  }

  async function apiRaw(method, path, body) {
    const opts = { method, body };
    const token = localStorage.getItem('cr-token');
    if (token) {
      opts.headers = { 'Authorization': 'Bearer ' + token };
    }
    const resp = await fetch(API_BASE + path, opts);
    if (!resp.ok) throw new Error(`API ${resp.status}: ${await resp.text()}`);
    return resp;
  }

  // ========================================================================
  // Utility
  // ========================================================================

  function esc(str) {
    const div = document.createElement('div');
    div.textContent = str || '';
    return div.innerHTML;
  }

  function val(sel) {
    const el = $(sel);
    return el ? el.value : '';
  }

  function setText(sel, text) {
    const el = $(sel);
    if (el) el.textContent = text;
  }

  function formatBytes(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
    return (bytes / 1073741824).toFixed(2) + ' GB';
  }

  function formatDate(iso) {
    if (!iso) return '--';
    try {
      const d = new Date(iso);
      const now = new Date();
      const isToday = d.toDateString() === now.toDateString();
      const yesterday = new Date(now);
      yesterday.setDate(yesterday.getDate() - 1);
      const isYesterday = d.toDateString() === yesterday.toDateString();

      const time = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
      if (isToday) return 'Today, ' + time;
      if (isYesterday) return 'Yesterday, ' + time;
      return d.toLocaleDateString([], { month: 'short', day: 'numeric' }) + ', ' + time;
    } catch (_) {
      return iso;
    }
  }

  function categoryIcon(cat) {
    const icons = {
      computation: '&#x2699;',
      data: '&#x1F4CA;',
      media: '&#x1F3AC;',
      science: '&#x1F52C;',
      ai: '&#x1F916;',
      finance: '&#x1F4B0;',
      enterprise: '&#x1F3E2;',
    };
    return icons[cat] || '&#x2699;';
  }

  function parseEnvVars(text) {
    const vars = {};
    text.split('\n').forEach(line => {
      const eq = line.indexOf('=');
      if (eq > 0) {
        vars[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
      }
    });
    return vars;
  }

  // ========================================================================
  // Settings
  // ========================================================================

  function setupSettings() {
    const saveBtn = $('#save-settings');
    const resetBtn = $('#reset-settings');

    if (saveBtn) saveBtn.addEventListener('click', saveSettingsFromForm);
    if (resetBtn) resetBtn.addEventListener('click', resetSettingsToDefaults);

    // Populate settings form from current state
    populateSettingsForm();
    renderFavoriteModules();
    renderRecentModules();
  }

  function populateSettingsForm() {
    const s = state.settings;
    setVal('#setting-chunk-size', s.defaultChunkSize);
    setInputVal('#setting-retries', s.defaultRetries);
    setInputVal('#setting-timeout', s.defaultTimeout);
    setVal('#setting-region', s.preferredRegion);
    setVal('#setting-priority', s.defaultPriority);
    setVal('#setting-currency', s.costCurrency);
  }

  function saveSettingsFromForm() {
    state.settings.defaultChunkSize = val('#setting-chunk-size') || 'auto';
    state.settings.defaultRetries = parseInt(val('#setting-retries') || '3', 10);
    state.settings.defaultTimeout = parseInt(val('#setting-timeout') || '300', 10);
    state.settings.preferredRegion = val('#setting-region') || 'any';
    state.settings.defaultPriority = val('#setting-priority') || 'standard';
    state.settings.costCurrency = val('#setting-currency') || 'USD';

    saveSettings(state.settings);
    showToast('Settings saved');
  }

  function resetSettingsToDefaults() {
    state.settings = Object.assign({}, DEFAULT_SETTINGS);
    saveSettings(state.settings);
    populateSettingsForm();
    showToast('Settings reset to defaults');
  }

  function applyDefaultsToForm() {
    const s = state.settings;
    setVal('#param-chunk-size', s.defaultChunkSize);
    setInputVal('#param-retries', s.defaultRetries);
    setInputVal('#param-timeout', s.defaultTimeout);
    setVal('#param-region', s.preferredRegion);
  }

  function trackModuleUsage(moduleId, moduleName) {
    const maxRecent = 10;
    const entry = { id: moduleId, name: moduleName || moduleId, usedAt: new Date().toISOString() };

    // Remove existing entry with same id
    state.settings.recentModules = state.settings.recentModules.filter(m => m.id !== moduleId);
    // Prepend
    state.settings.recentModules.unshift(entry);
    // Trim
    if (state.settings.recentModules.length > maxRecent) {
      state.settings.recentModules = state.settings.recentModules.slice(0, maxRecent);
    }

    saveSettings(state.settings);
    renderRecentModules();
  }

  function renderFavoriteModules() {
    const list = $('#favorite-modules-list');
    if (!list) return;

    if (state.settings.favoriteModules.length === 0) {
      list.innerHTML = '<li class="settings-empty">No favorites yet. Browse modules and click the star icon to pin.</li>';
      return;
    }

    list.innerHTML = state.settings.favoriteModules.map((m, i) => `
      <li class="settings-module-item">
        <span class="settings-module-name">${esc(m.name || m.id)}</span>
        <button class="btn btn-ghost btn-xs" data-fav-index="${i}" type="button" aria-label="Remove from favorites">&times;</button>
      </li>
    `).join('');

    list.querySelectorAll('[data-fav-index]').forEach(btn => {
      btn.addEventListener('click', () => {
        state.settings.favoriteModules.splice(parseInt(btn.dataset.favIndex, 10), 1);
        saveSettings(state.settings);
        renderFavoriteModules();
      });
    });
  }

  function renderRecentModules() {
    const list = $('#recent-modules-list');
    if (!list) return;

    if (state.settings.recentModules.length === 0) {
      list.innerHTML = '<li class="settings-empty">No recent modules.</li>';
      return;
    }

    list.innerHTML = state.settings.recentModules.map(m => `
      <li class="settings-module-item">
        <span class="settings-module-name">${esc(m.name || m.id)}</span>
        <span class="settings-module-time">${formatDate(m.usedAt)}</span>
      </li>
    `).join('');
  }

  function showToast(message) {
    const container = $('#toast-container');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = 'toast toast-success';
    toast.textContent = message;
    container.appendChild(toast);

    setTimeout(() => {
      toast.classList.add('toast-exit');
      setTimeout(() => toast.remove(), 300);
    }, 3000);
  }

  function setVal(sel, value) {
    const el = $(sel);
    if (el) el.value = value;
  }

  function setInputVal(sel, value) {
    const el = $(sel);
    if (el) el.value = String(value);
  }

  // ========================================================================
  // Public API (for inline onclick handlers in rendered HTML)
  // ========================================================================

  window.crApp = {
    trackJob(jobId) {
      showView('job-status');
      startTracking(jobId);
    },
  };

})();
