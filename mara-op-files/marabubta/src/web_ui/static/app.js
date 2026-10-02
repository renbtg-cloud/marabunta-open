// Marabunta - Licensed under the MIT License.
/**
 * Marabunta Compute Dashboard - Main JavaScript
 *
 * This file provides the client-side functionality for the Marabunta Compute
 * web dashboard, including API communication, state management, and UI updates.
 */

// ============================================================================
// Configuration
// ============================================================================

const CONFIG = {
    API_BASE: '',  // Same origin
    REFRESH_INTERVAL: 3000,  // 3 seconds
    JOBS_PAGE_SIZE: 20,
    NODES_PAGE_SIZE: 50,
};

// ============================================================================
// State Management
// ============================================================================

const state = {
    jobs: [],
    nodes: [],
    instances: [],
    stats: null,
    activity: [],
    filters: {
        jobStatus: 'all',
        nodeStatus: 'all',
        search: '',
    },
    pagination: {
        jobs: { page: 1, total: 0 },
        nodes: { page: 1, total: 0 },
    },
    refreshTimer: null,
};

// ============================================================================
// API Client
// ============================================================================

const api = {
    async fetch(endpoint, options = {}) {
        try {
            const response = await fetch(`${CONFIG.API_BASE}${endpoint}`, {
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers,
                },
                ...options,
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            return await response.json();
        } catch (error) {
            console.error(`API Error (${endpoint}):`, error);
            throw error;
        }
    },

    // Dashboard stats
    async getStats() {
        return this.fetch('/api/dashboard/stats');
    },

    // Recent activity
    async getActivity() {
        return this.fetch('/api/dashboard/activity');
    },

    // Jobs
    async getJobs(params = {}) {
        const query = new URLSearchParams(params).toString();
        return this.fetch(`/api/jobs${query ? '?' + query : ''}`);
    },

    async getJob(jobId) {
        return this.fetch(`/api/jobs/${jobId}`);
    },

    async submitJob(jobSpec) {
        return this.fetch('/api/jobs', {
            method: 'POST',
            body: JSON.stringify(jobSpec),
        });
    },

    async cancelJob(jobId) {
        return this.fetch(`/api/jobs/${jobId}/cancel`, {
            method: 'POST',
        });
    },

    // Nodes
    async getNodes(params = {}) {
        const query = new URLSearchParams(params).toString();
        return this.fetch(`/api/nodes${query ? '?' + query : ''}`);
    },

    async getNode(nodeId) {
        return this.fetch(`/api/nodes/${nodeId}`);
    },
};

// ============================================================================
// UI Components
// ============================================================================

const ui = {
    // Format numbers with commas
    formatNumber(num) {
        if (num === undefined || num === null) return '-';
        return num.toLocaleString();
    },

    // Format bytes to human readable
    formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    },

    // Format duration in seconds to human readable
    formatDuration(seconds) {
        if (seconds < 60) return `${seconds}s`;
        if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        return `${hours}h ${minutes}m`;
    },

    // Format timestamp to relative time
    formatRelativeTime(timestamp) {
        const date = new Date(timestamp);
        const now = new Date();
        const diffMs = now - date;
        const diffSecs = Math.floor(diffMs / 1000);
        const diffMins = Math.floor(diffSecs / 60);
        const diffHours = Math.floor(diffMins / 60);
        const diffDays = Math.floor(diffHours / 24);

        if (diffSecs < 60) return 'just now';
        if (diffMins < 60) return `${diffMins}m ago`;
        if (diffHours < 24) return `${diffHours}h ago`;
        return `${diffDays}d ago`;
    },

    // Get status badge class
    getStatusBadgeClass(status) {
        const statusMap = {
            running: 'badge-info',
            completed: 'badge-success',
            failed: 'badge-error',
            pending: 'badge-neutral',
            scheduled: 'badge-warning',
            cancelled: 'badge-neutral',
            ready: 'badge-success',
            busy: 'badge-warning',
            offline: 'badge-error',
            draining: 'badge-warning',
        };
        return statusMap[status?.toLowerCase()] || 'badge-neutral';
    },

    // Get utilization class
    getUtilizationClass(percent) {
        if (percent < 50) return 'low';
        if (percent < 80) return 'medium';
        return 'high';
    },

    // Show notification toast
    showToast(message, type = 'info') {
        // Create toast element
        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.textContent = message;

        // Add to DOM
        let container = document.getElementById('toast-container');
        if (!container) {
            container = document.createElement('div');
            container.id = 'toast-container';
            container.style.cssText = 'position: fixed; bottom: 20px; right: 20px; z-index: 9999;';
            document.body.appendChild(container);
        }
        container.appendChild(toast);

        // Animate in
        toast.style.cssText = `
            padding: 12px 20px;
            margin-top: 10px;
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            color: var(--text-primary);
            opacity: 0;
            transform: translateX(100%);
            transition: all 0.3s ease;
        `;

        requestAnimationFrame(() => {
            toast.style.opacity = '1';
            toast.style.transform = 'translateX(0)';
        });

        // Remove after delay
        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateX(100%)';
            setTimeout(() => toast.remove(), 300);
        }, 3000);
    },

    // Open modal
    openModal(modalId) {
        const modal = document.getElementById(modalId);
        if (modal) {
            modal.classList.add('active');
            document.body.style.overflow = 'hidden';
        }
    },

    // Close modal
    closeModal(modalId) {
        const modal = document.getElementById(modalId);
        if (modal) {
            modal.classList.remove('active');
            document.body.style.overflow = '';
        }
    },
};

// ============================================================================
// Dashboard Page
// ============================================================================

const dashboard = {
    async init() {
        await this.refresh();
        this.startAutoRefresh();
    },

    async refresh() {
        try {
            const [stats, activity] = await Promise.all([
                api.getStats(),
                api.getActivity(),
            ]);

            state.stats = stats;
            state.activity = activity;

            this.render();
        } catch (error) {
            console.error('Failed to refresh dashboard:', error);
        }
    },

    render() {
        this.renderStats();
        this.renderActivity();
    },

    renderStats() {
        const stats = state.stats;
        if (!stats) return;

        // Update stat cards
        this.updateStatCard('total-jobs', stats.total_jobs);
        this.updateStatCard('running-jobs', stats.running_jobs);
        this.updateStatCard('total-nodes', stats.total_nodes);
        this.updateStatCard('healthy-nodes', stats.healthy_nodes);
        this.updateStatCard('tasks-per-second', stats.tasks_per_second?.toFixed(1));
        this.updateStatCard('cluster-utilization', (stats.cluster_utilization * 100).toFixed(0) + '%');

        // Update uptime
        const uptimeEl = document.getElementById('uptime');
        if (uptimeEl) {
            uptimeEl.textContent = ui.formatDuration(stats.uptime_secs);
        }

        // Update utilization bar
        const utilizationBar = document.getElementById('utilization-bar');
        if (utilizationBar) {
            const percent = stats.cluster_utilization * 100;
            utilizationBar.style.width = percent + '%';
            utilizationBar.className = 'fill ' + ui.getUtilizationClass(percent);
        }
    },

    updateStatCard(id, value) {
        const el = document.getElementById(id);
        if (el) {
            el.textContent = ui.formatNumber(value);
        }
    },

    renderActivity() {
        const container = document.getElementById('activity-list');
        if (!container || !state.activity.length) return;

        const html = state.activity.map(item => {
            const iconClass = this.getActivityIconClass(item.event_type);
            return `
                <li class="activity-item">
                    <div class="activity-icon ${iconClass}">
                        ${this.getActivityIcon(item.event_type)}
                    </div>
                    <div class="activity-content">
                        <div class="title">${this.escapeHtml(item.description)}</div>
                        <div class="time">${ui.formatRelativeTime(item.timestamp)}</div>
                    </div>
                </li>
            `;
        }).join('');

        container.innerHTML = html;
    },

    getActivityIconClass(eventType) {
        const map = {
            job_completed: 'success',
            job_submitted: 'info',
            job_failed: 'error',
            job_cancelled: 'warning',
            node_joined: 'info',
            node_left: 'warning',
        };
        return map[eventType] || 'info';
    },

    getActivityIcon(eventType) {
        const map = {
            job_completed: '&#10003;',
            job_submitted: '&#9889;',
            job_failed: '&#10007;',
            job_cancelled: '&#8722;',
            node_joined: '&#43;',
            node_left: '&#8722;',
        };
        return map[eventType] || '&#8226;';
    },

    escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    },

    startAutoRefresh() {
        if (state.refreshTimer) {
            clearInterval(state.refreshTimer);
        }
        state.refreshTimer = setInterval(() => this.refresh(), CONFIG.REFRESH_INTERVAL);
    },

    stopAutoRefresh() {
        if (state.refreshTimer) {
            clearInterval(state.refreshTimer);
            state.refreshTimer = null;
        }
    },
};

// ============================================================================
// Jobs Page
// ============================================================================

const jobs = {
    async init() {
        this.bindEvents();
        await this.refresh();
        this.startAutoRefresh();
    },

    bindEvents() {
        // Filter buttons
        document.querySelectorAll('[data-filter-status]').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const status = e.target.dataset.filterStatus;
                state.filters.jobStatus = status;
                this.render();

                // Update active state
                document.querySelectorAll('[data-filter-status]').forEach(b => {
                    b.classList.toggle('active', b.dataset.filterStatus === status);
                });
            });
        });

        // Search input
        const searchInput = document.getElementById('job-search');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                state.filters.search = e.target.value.toLowerCase();
                this.render();
            });
        }

        // Submit job button
        const submitBtn = document.getElementById('submit-job-btn');
        if (submitBtn) {
            submitBtn.addEventListener('click', () => ui.openModal('submit-job-modal'));
        }

        // Submit job form
        const submitForm = document.getElementById('submit-job-form');
        if (submitForm) {
            submitForm.addEventListener('submit', async (e) => {
                e.preventDefault();
                await this.handleSubmitJob();
            });
        }

        // Modal close buttons
        document.querySelectorAll('.modal-close, [data-close-modal]').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const modal = e.target.closest('.modal-overlay');
                if (modal) {
                    modal.classList.remove('active');
                    document.body.style.overflow = '';
                }
            });
        });
    },

    async refresh() {
        try {
            const resp = await fetch('/api/jobs?limit=500');
            if (resp.ok) {
                const data = await resp.json();
                state.jobs = data.items || data;
            }
            this.render();
        } catch (e) {
            console.error('Failed to fetch jobs:', e);
        }
    },

    render() {
        const container = document.getElementById('jobs-table-body');
        if (!container) return;

        let filtered = [...state.jobs];

        // Apply status filter
        if (state.filters.jobStatus !== 'all') {
            filtered = filtered.filter(j => j.status === state.filters.jobStatus);
        }

        // Apply search filter
        if (state.filters.search) {
            filtered = filtered.filter(j =>
                j.name.toLowerCase().includes(state.filters.search) ||
                j.id.toLowerCase().includes(state.filters.search)
            );
        }

        if (filtered.length === 0) {
            container.innerHTML = `
                <tr>
                    <td colspan="6" class="empty-state">
                        <p>No jobs found</p>
                    </td>
                </tr>
            `;
            return;
        }

        container.innerHTML = filtered.map(job => `
            <tr>
                <td>
                    <a href="#" onclick="jobs.showDetails('${job.id}')">${job.id.substring(0, 12)}</a>
                </td>
                <td>${this.escapeHtml(job.name)}</td>
                <td>
                    <span class="badge ${ui.getStatusBadgeClass(job.status)}">${job.status}</span>
                </td>
                <td>
                    <div class="progress-bar" style="width: 100px;">
                        <div class="fill" style="width: ${job.progress}%"></div>
                    </div>
                    <span style="margin-left: 8px; font-size: 0.75rem;">${job.progress}%</span>
                </td>
                <td>${job.tasks_completed}/${job.tasks_total}</td>
                <td>
                    ${job.status === 'running' || job.status === 'pending' ? `
                        <button class="btn btn-sm btn-secondary" onclick="jobs.cancelJob('${job.id}')">Cancel</button>
                    ` : ''}
                </td>
            </tr>
        `).join('');
    },

    escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    },

    async handleSubmitJob() {
        const nameInput = document.getElementById('job-name');
        const specInput = document.getElementById('job-spec');

        if (!nameInput?.value || !specInput?.value) {
            ui.showToast('Please fill in all fields', 'error');
            return;
        }

        try {
            const spec = JSON.parse(specInput.value);
            spec.name = nameInput.value;

            const resp = await fetch('/api/jobs', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(spec),
            });
            if (resp.ok) {
                ui.closeModal('submit-job-modal');
                ui.showToast('Job submitted', 'success');
                await this.refresh();
            } else {
                ui.showToast('Submit failed: ' + resp.statusText, 'error');
            }
        } catch (e) {
            ui.showToast('Submit error: ' + e.message, 'error');
        }
    },

    async cancelJob(jobId) {
        if (!confirm(`Cancel job ${jobId}?`)) return;

        try {
            await fetch('/api/jobs/' + jobId + '/cancel', { method: 'POST' });
            ui.showToast('Job cancelled', 'success');
            await this.refresh();
        } catch (e) {
            ui.showToast('Cancel error: ' + e.message, 'error');
        }
    },

    showDetails(jobId) {
        // In production, fetch job details and show modal
        console.log('Show details for:', jobId);
    },

    startAutoRefresh() {
        if (state.refreshTimer) {
            clearInterval(state.refreshTimer);
        }
        state.refreshTimer = setInterval(() => this.refresh(), CONFIG.REFRESH_INTERVAL);
    },
};

// ============================================================================
// Nodes Page
// ============================================================================

const nodes = {
    async init() {
        this.bindEvents();
        await this.refresh();
        this.startAutoRefresh();
    },

    bindEvents() {
        // Filter buttons
        document.querySelectorAll('[data-filter-node-status]').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const status = e.target.dataset.filterNodeStatus;
                state.filters.nodeStatus = status;
                this.render();

                document.querySelectorAll('[data-filter-node-status]').forEach(b => {
                    b.classList.toggle('active', b.dataset.filterNodeStatus === status);
                });
            });
        });

        // Search input
        const searchInput = document.getElementById('node-search');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                state.filters.search = e.target.value.toLowerCase();
                this.render();
            });
        }
    },

    async refresh() {
        try {
            const resp = await fetch('/api/nodes?limit=500');
            if (resp.ok) {
                const data = await resp.json();
                state.nodes = data.items || data;
            }
            this.render();
        } catch (e) {
            console.error('Failed to fetch nodes:', e);
        }
    },

    render() {
        const container = document.getElementById('nodes-table-body');
        if (!container) return;

        let filtered = [...state.nodes];

        // Apply status filter
        if (state.filters.nodeStatus !== 'all') {
            filtered = filtered.filter(n => n.status === state.filters.nodeStatus);
        }

        // Apply search filter
        if (state.filters.search) {
            filtered = filtered.filter(n =>
                n.id.toLowerCase().includes(state.filters.search) ||
                n.region?.toLowerCase().includes(state.filters.search) ||
                n.node_type?.toLowerCase().includes(state.filters.search)
            );
        }

        if (filtered.length === 0) {
            container.innerHTML = `
                <tr>
                    <td colspan="8" class="empty-state">
                        <p>No nodes found</p>
                    </td>
                </tr>
            `;
            return;
        }

        container.innerHTML = filtered.map(node => {
            const loadPercent = (node.current_load * 100).toFixed(0);
            const statusDotClass = {
                ready: 'online',
                busy: 'busy',
                offline: 'offline',
                draining: 'busy',
            }[node.status] || 'offline';

            const thermalBadge = this.getThermalBadge(node.thermal_state);

            return `
                <tr>
                    <td>
                        <div class="status-indicator">
                            <span class="status-dot ${statusDotClass}"></span>
                            ${node.id}
                        </div>
                    </td>
                    <td>${node.node_type}</td>
                    <td>${node.cores}</td>
                    <td>${ui.formatBytes(node.memory_bytes)}</td>
                    <td>${node.region || '-'}</td>
                    <td>
                        <div class="utilization-meter">
                            <div class="bar">
                                <div class="fill ${ui.getUtilizationClass(loadPercent)}" style="width: ${loadPercent}%"></div>
                            </div>
                            <span class="percent">${loadPercent}%</span>
                        </div>
                    </td>
                    <td>${thermalBadge}</td>
                    <td>-</td>
                </tr>
            `;
        }).join('');

        // Update summary stats
        this.updateSummary(filtered);
    },

    getThermalBadge(thermalState) {
        const state = (thermalState || 'normal').toLowerCase();
        const badgeMap = {
            normal: '<span class="badge badge-success">normal</span>',
            elevated: '<span class="badge badge-warning">elevated</span>',
            high: '<span class="badge badge-error">high</span>',
            critical: '<span class="badge badge-error" style="animation: pulse 1s infinite;">critical</span>',
        };
        return badgeMap[state] || `<span class="badge badge-neutral">${state}</span>`;
    },

    updateSummary(nodes) {
        const totalNodes = document.getElementById('total-nodes-count');
        const onlineNodes = document.getElementById('online-nodes-count');
        const totalCores = document.getElementById('total-cores');
        const totalMemory = document.getElementById('total-memory');

        if (totalNodes) totalNodes.textContent = nodes.length;
        if (onlineNodes) {
            const online = nodes.filter(n => n.status === 'ready' || n.status === 'busy').length;
            onlineNodes.textContent = online;
        }
        if (totalCores) {
            const cores = nodes.reduce((sum, n) => sum + n.cores, 0);
            totalCores.textContent = cores;
        }
        if (totalMemory) {
            const memory = nodes.reduce((sum, n) => sum + n.memory_bytes, 0);
            totalMemory.textContent = ui.formatBytes(memory);
        }
    },

    startAutoRefresh() {
        if (state.refreshTimer) {
            clearInterval(state.refreshTimer);
        }
        state.refreshTimer = setInterval(() => this.refresh(), CONFIG.REFRESH_INTERVAL);
    },
};

// ============================================================================
// Instances Page
// ============================================================================

const instances = {
    async init() {
        this.bindEvents();
        await this.refresh();
        this.startAutoRefresh();
    },

    bindEvents() {
        // Create instance button
        const createBtn = document.getElementById('create-instance-btn');
        if (createBtn) {
            createBtn.addEventListener('click', () => ui.openModal('create-instance-modal'));
        }

        // Create instance form
        const createForm = document.getElementById('create-instance-form');
        if (createForm) {
            createForm.addEventListener('submit', async (e) => {
                e.preventDefault();
                await this.handleCreateInstance();
            });
        }

        // Search input
        const searchInput = document.getElementById('instance-search');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                state.filters.search = e.target.value.toLowerCase();
                this.render();
            });
        }

        // Modal close buttons
        document.querySelectorAll('.modal-close, [data-close-modal]').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const modal = e.target.closest('.modal-overlay');
                if (modal) {
                    modal.classList.remove('active');
                    document.body.style.overflow = '';
                }
            });
        });
    },

    async refresh() {
        try {
            const resp = await fetch('/api/instances');
            if (resp.ok) {
                state.instances = await resp.json();
            } else {
                state.instances = [];
            }
        } catch (error) {
            console.error('Failed to fetch instances:', error);
            state.instances = [];
        }
        this.render();
    },

    render() {
        const container = document.getElementById('instances-table-body');
        if (!container) return;

        let filtered = [...(state.instances || [])];

        // Apply search filter
        if (state.filters.search) {
            filtered = filtered.filter(i =>
                (i.name || '').toLowerCase().includes(state.filters.search)
            );
        }

        // Update summary stats
        this.updateSummary(filtered);

        if (filtered.length === 0) {
            container.innerHTML = `
                <tr>
                    <td colspan="6" class="empty-state">
                        <div class="icon">&#x2b21;</div>
                        <h4>No instances found</h4>
                        <p>Create a PostgreSQL instance to get started.</p>
                    </td>
                </tr>
            `;
            return;
        }

        container.innerHTML = filtered.map(inst => {
            const statusClass = inst.status === 'running' ? 'online' : (inst.status === 'stopped' ? 'offline' : 'busy');
            const badgeClass = inst.status === 'running' ? 'badge-success' : (inst.status === 'stopped' ? 'badge-error' : 'badge-warning');
            return `
                <tr>
                    <td>
                        <div class="status-indicator">
                            <span class="status-dot ${statusClass}"></span>
                            ${this.escapeHtml(inst.name || '-')}
                        </div>
                    </td>
                    <td style="font-family: monospace;">${inst.port || '-'}</td>
                    <td>${inst.shard_count || 0}</td>
                    <td>${inst.node_count || 0}</td>
                    <td><span class="badge badge-info">${inst.consistency_mode || 'eventual'}</span></td>
                    <td><span class="badge ${badgeClass}">${inst.status || 'unknown'}</span></td>
                </tr>
            `;
        }).join('');
    },

    updateSummary(instancesList) {
        const totalInstances = document.getElementById('total-instances');
        const totalShards = document.getElementById('total-shards');
        const totalHostingNodes = document.getElementById('total-hosting-nodes');

        if (totalInstances) totalInstances.textContent = instancesList.length;
        if (totalShards) {
            const shards = instancesList.reduce((sum, i) => sum + (i.shard_count || 0), 0);
            totalShards.textContent = shards;
        }
        if (totalHostingNodes) {
            const nodes = instancesList.reduce((sum, i) => sum + (i.node_count || 0), 0);
            totalHostingNodes.textContent = nodes;
        }
    },

    escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    },

    async handleCreateInstance() {
        const nameInput = document.getElementById('instance-name');
        const shardCountInput = document.getElementById('shard-count');
        const replicationFactorInput = document.getElementById('replication-factor');
        const consistencyModeInput = document.getElementById('consistency-mode');

        if (!nameInput?.value) {
            ui.showToast('Please provide an instance name', 'error');
            return;
        }

        const payload = {
            name: nameInput.value,
            shard_count: parseInt(shardCountInput.value, 10) || 16,
            replication_factor: parseInt(replicationFactorInput.value, 10) || 1,
            consistency_mode: consistencyModeInput.value || 'eventual',
        };

        try {
            const resp = await fetch('/api/instances', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });

            if (resp.ok) {
                ui.closeModal('create-instance-modal');
                ui.showToast('Instance created successfully', 'success');
                nameInput.value = '';
                shardCountInput.value = '16';
                replicationFactorInput.value = '1';
                consistencyModeInput.value = 'eventual';
                await this.refresh();
            } else {
                const err = await resp.text();
                ui.showToast('Failed to create instance: ' + err, 'error');
            }
        } catch (error) {
            ui.showToast('Failed to create instance: ' + error.message, 'error');
        }
    },

    startAutoRefresh() {
        if (state.refreshTimer) {
            clearInterval(state.refreshTimer);
        }
        state.refreshTimer = setInterval(() => this.refresh(), CONFIG.REFRESH_INTERVAL);
    },
};

// ============================================================================
// Alert Polling
// ============================================================================

async function pollAlerts() {
    try {
        const resp = await fetch('/api/alerts');
        if (resp.ok) {
            const alerts = await resp.json();
            renderAlerts(alerts);
        }
    } catch(e) {}
}

function renderAlerts(alerts) {
    let container = document.getElementById('alerts-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'alerts-container';
        container.style.cssText = 'position:fixed;top:0;left:0;right:0;z-index:1000;';
        document.body.prepend(container);
    }
    if (!alerts || alerts.length === 0) {
        container.innerHTML = '';
        return;
    }
    container.innerHTML = alerts.map(a => {
        const colors = { critical: '#ef4444', warning: '#f59e0b', info: '#3b82f6' };
        const bg = { critical: '#1f1012', warning: '#1a1806', info: '#0c1524' };
        return `<div style="background:${bg[a.severity]||bg.info};border-bottom:1px solid ${colors[a.severity]||colors.info};padding:8px 16px;font-size:0.85rem;color:${colors[a.severity]||colors.info};">
            <strong>${a.severity?.toUpperCase() || 'ALERT'}</strong>: ${a.message || a.description || 'Alert'}
        </div>`;
    }).join('');
}

setInterval(pollAlerts, 5000);

// ============================================================================
// Page Initialization
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    // Determine which page we're on and initialize accordingly
    const path = window.location.pathname;

    if (path === '/' || path === '/index.html' || path.endsWith('/')) {
        dashboard.init();
    } else if (path === '/jobs.html') {
        jobs.init();
    } else if (path === '/nodes.html') {
        nodes.init();
    } else if (path === '/instances.html') {
        instances.init();
    }

    // Initial alert poll
    pollAlerts();

    // Set up modal close on overlay click
    document.querySelectorAll('.modal-overlay').forEach(overlay => {
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) {
                overlay.classList.remove('active');
                document.body.style.overflow = '';
            }
        });
    });

    // Set up keyboard shortcuts
    document.addEventListener('keydown', (e) => {
        // ESC to close modals
        if (e.key === 'Escape') {
            document.querySelectorAll('.modal-overlay.active').forEach(modal => {
                modal.classList.remove('active');
                document.body.style.overflow = '';
            });
        }
    });
});

// Export for use in HTML onclick handlers
window.jobs = jobs;
window.nodes = nodes;
window.instances = instances;
window.ui = ui;
