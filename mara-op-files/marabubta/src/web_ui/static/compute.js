// Marabunta - Licensed under the MIT License.
/**
 * Marabunta Compute - Browser Swarm Worker Engine
 *
 * This is the client-side engine that connects a browser tab to the
 * Marabunta Swarm as an actual compute node via WebSocket. It manages
 * Web Worker pools for task execution, handles the wire protocol, and
 * updates the UI in real time.
 */

'use strict';

// ============================================================================
// Configuration
// ============================================================================

var HEARTBEAT_INTERVAL = 5000;      // Send heartbeat every 5s
var RECONNECT_DELAY = 3000;         // Retry connect after 3s
var MAX_RECONNECT_ATTEMPTS = 20;    // Give up after 20 attempts
var TASK_TIMEOUT = 30000;           // 30s per task
var LOG_MAX_ENTRIES = 200;          // Max task log entries to keep

// ============================================================================
// SwarmWorker — the main worker engine
// ============================================================================

function SwarmWorker(serverUrl) {
    this.serverUrl = serverUrl;
    this.ws = null;
    this.nodeId = null;
    this.connected = false;
    this.connecting = false;
    this.reconnectAttempts = 0;
    this.heartbeatTimer = null;

    // Stats
    this.tasksCompleted = 0;
    this.tasksFailed = 0;
    this.tasksActive = 0;
    this.connectTime = null;
    this.swarmSize = 0;
    this.totalBrowsers = 0;
    this.jobsActive = 0;
    this.tasksPerSecond = 0;

    // Web Worker pool
    this.workers = [];
    this.workerQueue = [];
    this.activeWorkerTasks = new Map(); // taskId -> { worker, timeout }

    // Task log
    this.taskLog = [];

    // UI callbacks (set externally)
    this.onConnected = null;
    this.onDisconnected = null;
    this.onTask = null;
    this.onTaskComplete = null;
    this.onStats = null;
    this.onLog = null;
}

// ============================================================================
// Connection management
// ============================================================================

SwarmWorker.prototype.connect = function() {
    if (this.connected || this.connecting) return;
    this.connecting = true;

    var self = this;
    var wsUrl = this.serverUrl.replace(/^http/, 'ws') + '/ws/compute';

    this.addLog('system', 'Connecting to ' + wsUrl + '...');

    try {
        this.ws = new WebSocket(wsUrl);
    } catch (err) {
        this.addLog('error', 'Failed to create WebSocket: ' + err.message);
        this.connecting = false;
        this.scheduleReconnect();
        return;
    }

    this.ws.onopen = function() {
        self.connecting = false;
        self.reconnectAttempts = 0;
        self.addLog('system', 'WebSocket connected, sending join...');

        // Create the worker pool before sending join.
        self.createWorkerPool();

        // Send join message.
        var cores = navigator.hardwareConcurrency || 2;
        var capabilities = ['js'];

        // Detect WebAssembly support.
        if (typeof WebAssembly !== 'undefined') {
            capabilities.push('wasm');
        }

        self.send({
            type: 'join',
            cores: cores,
            capabilities: capabilities,
            userAgent: navigator.userAgent
        });
    };

    this.ws.onmessage = function(event) {
        try {
            var msg = JSON.parse(event.data);
            self.handleMessage(msg);
        } catch (err) {
            self.addLog('error', 'Failed to parse server message: ' + err.message);
        }
    };

    this.ws.onerror = function(event) {
        self.addLog('error', 'WebSocket error');
    };

    this.ws.onclose = function(event) {
        var wasConnected = self.connected;
        self.connected = false;
        self.connecting = false;
        self.stopHeartbeat();

        if (wasConnected) {
            self.addLog('system', 'Disconnected from swarm (code: ' + event.code + ')');
        }

        if (self.onDisconnected) {
            self.onDisconnected();
        }

        // Auto-reconnect if we were previously connected.
        if (wasConnected) {
            self.scheduleReconnect();
        }
    };
};

SwarmWorker.prototype.disconnect = function() {
    this.reconnectAttempts = MAX_RECONNECT_ATTEMPTS; // Prevent auto-reconnect.
    this.stopHeartbeat();
    this.destroyWorkerPool();

    if (this.ws) {
        this.ws.close(1000, 'user disconnect');
        this.ws = null;
    }

    this.connected = false;
    this.connecting = false;
    this.nodeId = null;

    if (this.onDisconnected) {
        this.onDisconnected();
    }

    this.addLog('system', 'Disconnected by user');
};

SwarmWorker.prototype.scheduleReconnect = function() {
    if (this.reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
        this.addLog('error', 'Max reconnect attempts reached, giving up');
        return;
    }

    var self = this;
    this.reconnectAttempts++;
    var delay = RECONNECT_DELAY * Math.min(this.reconnectAttempts, 5);

    this.addLog('system', 'Reconnecting in ' + (delay / 1000) + 's (attempt ' + this.reconnectAttempts + ')...');

    setTimeout(function() {
        if (!self.connected && !self.connecting) {
            self.connect();
        }
    }, delay);
};

SwarmWorker.prototype.send = function(msg) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify(msg));
    }
};

// ============================================================================
// Message handling
// ============================================================================

SwarmWorker.prototype.handleMessage = function(msg) {
    switch (msg.type) {
        case 'welcome':
            this.nodeId = msg.nodeId;
            this.swarmSize = msg.swarmSize;
            this.connected = true;
            this.connectTime = Date.now();
            this.startHeartbeat();

            this.addLog('system', 'Joined swarm as ' + msg.nodeId.substring(0, 8) +
                '... (swarm size: ' + msg.swarmSize + ')');

            if (this.onConnected) {
                this.onConnected(msg.nodeId, msg.swarmSize);
            }
            break;

        case 'task':
            this.handleTask(msg);
            break;

        case 'stats':
            this.swarmSize = msg.totalNodes;
            this.totalBrowsers = msg.totalBrowsers;
            this.jobsActive = msg.jobsActive;
            this.tasksPerSecond = msg.tasksPerSecond;

            if (this.onStats) {
                this.onStats(msg);
            }
            break;

        case 'ping':
            // Server ping -- no response needed, it's just keeping the connection alive.
            break;

        default:
            this.addLog('warning', 'Unknown message type: ' + msg.type);
    }
};

// ============================================================================
// Task execution
// ============================================================================

SwarmWorker.prototype.handleTask = function(task) {
    this.tasksActive++;
    this.addLog('task', 'Received task ' + task.taskId.substring(0, 8) + '... [' + task.taskType + ']');

    if (this.onTask) {
        this.onTask(task);
    }

    var self = this;
    var startTime = Date.now();

    // Find an available worker.
    var worker = this.getAvailableWorker();
    if (!worker) {
        // No workers available -- report error.
        this.send({
            type: 'error',
            taskId: task.taskId,
            error: 'no workers available'
        });
        this.tasksFailed++;
        this.tasksActive--;
        this.addLog('error', 'No workers available for task ' + task.taskId.substring(0, 8));
        return;
    }

    // Set up timeout.
    var timeoutId = setTimeout(function() {
        self.handleWorkerTimeout(task.taskId, worker);
    }, TASK_TIMEOUT);

    this.activeWorkerTasks.set(task.taskId, { worker: worker, timeout: timeoutId });

    // Set up the message handler for this specific task.
    worker.busy = true;
    worker.currentTaskId = task.taskId;

    worker.instance.onmessage = function(e) {
        clearTimeout(timeoutId);
        self.activeWorkerTasks.delete(task.taskId);
        worker.busy = false;
        worker.currentTaskId = null;

        var durationMs = Date.now() - startTime;

        if (e.data.error) {
            self.send({
                type: 'error',
                taskId: task.taskId,
                error: e.data.error
            });
            self.tasksFailed++;
            self.addLog('error', 'Task ' + task.taskId.substring(0, 8) +
                '... failed: ' + e.data.error);
        } else {
            self.send({
                type: 'result',
                taskId: task.taskId,
                output: e.data.result || '',
                durationMs: durationMs
            });
            self.tasksCompleted++;
            self.addLog('success', 'Task ' + task.taskId.substring(0, 8) +
                '... completed in ' + durationMs + 'ms');
        }

        self.tasksActive--;

        if (self.onTaskComplete) {
            self.onTaskComplete(task.taskId, !e.data.error, durationMs);
        }

        // Process queued tasks.
        self.processQueue();
    };

    worker.instance.onerror = function(err) {
        clearTimeout(timeoutId);
        self.activeWorkerTasks.delete(task.taskId);
        worker.busy = false;
        worker.currentTaskId = null;

        self.send({
            type: 'error',
            taskId: task.taskId,
            error: 'worker error: ' + (err.message || 'unknown')
        });
        self.tasksFailed++;
        self.tasksActive--;
        self.addLog('error', 'Worker error on task ' + task.taskId.substring(0, 8));

        // Replace the broken worker.
        self.replaceWorker(worker);
    };

    // Post the task to the worker.
    worker.instance.postMessage({
        taskId: task.taskId,
        taskType: task.taskType,
        code: task.code,
        input: task.input || null,
        moduleUrl: task.moduleUrl || null,
        funcName: task.funcName || null
    });
};

SwarmWorker.prototype.handleWorkerTimeout = function(taskId, worker) {
    this.activeWorkerTasks.delete(taskId);

    // Terminate and replace the timed-out worker.
    worker.instance.terminate();
    worker.busy = false;
    worker.currentTaskId = null;

    this.send({
        type: 'error',
        taskId: taskId,
        error: 'task execution timed out (' + (TASK_TIMEOUT / 1000) + 's)'
    });

    this.tasksFailed++;
    this.tasksActive--;
    this.addLog('error', 'Task ' + taskId.substring(0, 8) + '... timed out');

    this.replaceWorker(worker);
};

// ============================================================================
// Web Worker pool management
// ============================================================================

SwarmWorker.prototype.createWorkerPool = function() {
    var poolSize = Math.max(1, navigator.hardwareConcurrency || 2);
    this.addLog('system', 'Creating worker pool with ' + poolSize + ' threads');

    for (var i = 0; i < poolSize; i++) {
        this.workers.push(this.createWorker());
    }
};

SwarmWorker.prototype.createWorker = function() {
    // Create a blob URL for the worker script so we don't need an external file
    // served from the exact same origin. We use the embedded worker-exec.js.
    var workerUrl = '/static/worker-exec.js';
    var instance = new Worker(workerUrl);

    return {
        instance: instance,
        busy: false,
        currentTaskId: null
    };
};

SwarmWorker.prototype.getAvailableWorker = function() {
    for (var i = 0; i < this.workers.length; i++) {
        if (!this.workers[i].busy) {
            return this.workers[i];
        }
    }
    return null;
};

SwarmWorker.prototype.replaceWorker = function(worker) {
    var idx = this.workers.indexOf(worker);
    if (idx !== -1) {
        try { worker.instance.terminate(); } catch (_) {}
        this.workers[idx] = this.createWorker();
    }
};

SwarmWorker.prototype.destroyWorkerPool = function() {
    // Cancel all active tasks.
    var self = this;
    this.activeWorkerTasks.forEach(function(info, taskId) {
        clearTimeout(info.timeout);
        self.send({ type: 'error', taskId: taskId, error: 'disconnected' });
    });
    this.activeWorkerTasks.clear();

    // Terminate all workers.
    for (var i = 0; i < this.workers.length; i++) {
        try { this.workers[i].instance.terminate(); } catch (_) {}
    }
    this.workers = [];
    this.workerQueue = [];
};

SwarmWorker.prototype.processQueue = function() {
    // Future: if tasks are queued while all workers are busy, process them here.
};

// ============================================================================
// Heartbeat
// ============================================================================

SwarmWorker.prototype.startHeartbeat = function() {
    this.stopHeartbeat();
    var self = this;
    this.heartbeatTimer = setInterval(function() {
        var load = self.workers.length > 0
            ? self.tasksActive / self.workers.length
            : 0;
        self.send({
            type: 'heartbeat',
            load: Math.min(1.0, load),
            tasksActive: self.tasksActive
        });
    }, HEARTBEAT_INTERVAL);
};

SwarmWorker.prototype.stopHeartbeat = function() {
    if (this.heartbeatTimer) {
        clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = null;
    }
};

// ============================================================================
// Task log
// ============================================================================

SwarmWorker.prototype.addLog = function(type, message) {
    var entry = {
        time: new Date().toISOString(),
        type: type,
        message: message
    };

    this.taskLog.unshift(entry);
    if (this.taskLog.length > LOG_MAX_ENTRIES) {
        this.taskLog.pop();
    }

    if (this.onLog) {
        this.onLog(entry);
    }
};

// ============================================================================
// Utility: compute uptime string
// ============================================================================

SwarmWorker.prototype.getUptime = function() {
    if (!this.connectTime) return '0s';
    var secs = Math.floor((Date.now() - this.connectTime) / 1000);
    if (secs < 60) return secs + 's';
    if (secs < 3600) return Math.floor(secs / 60) + 'm ' + (secs % 60) + 's';
    var hours = Math.floor(secs / 3600);
    var mins = Math.floor((secs % 3600) / 60);
    return hours + 'h ' + mins + 'm';
};

// ============================================================================
// Demo task generator
// ============================================================================

SwarmWorker.prototype.runDemo = function() {
    if (!this.connected) {
        this.addLog('error', 'Not connected to swarm');
        return;
    }

    this.addLog('system', '--- Starting Demo: distributed computation across all browsers ---');

    var demoTasks = [
        {
            name: 'Prime Sieve',
            code: 'var limit = 100000; var sieve = new Array(limit + 1).fill(true); sieve[0] = sieve[1] = false; for (var i = 2; i * i <= limit; i++) { if (sieve[i]) { for (var j = i * i; j <= limit; j += i) sieve[j] = false; } } var count = sieve.filter(Boolean).length; return { primes_up_to: limit, count: count };'
        },
        {
            name: 'Fibonacci (40th)',
            code: 'function fib(n) { if (n <= 1) return n; var a = 0, b = 1; for (var i = 2; i <= n; i++) { var t = a + b; a = b; b = t; } return b; } return { n: 40, result: fib(40) };'
        },
        {
            name: 'SHA-256 Hash',
            code: 'var data = ""; for (var i = 0; i < 10000; i++) data += "marabunta-compute-" + i; var hash = 0; for (var i = 0; i < data.length; i++) { var ch = data.charCodeAt(i); hash = ((hash << 5) - hash) + ch; hash = hash & hash; } return { input_length: data.length, hash_value: hash };'
        },
        {
            name: 'Matrix Multiply',
            code: 'var N = 100; function rand() { return Math.random(); } var A = []; var B = []; for (var i = 0; i < N; i++) { A[i] = []; B[i] = []; for (var j = 0; j < N; j++) { A[i][j] = rand(); B[i][j] = rand(); } } var C = []; for (var i = 0; i < N; i++) { C[i] = []; for (var j = 0; j < N; j++) { var s = 0; for (var k = 0; k < N; k++) s += A[i][k] * B[k][j]; C[i][j] = s; } } return { size: N + "x" + N, trace: C.reduce(function(s, r, i) { return s + r[i]; }, 0).toFixed(4) };'
        },
        {
            name: 'Pi Estimation (Monte Carlo)',
            code: 'var n = 1000000; var inside = 0; for (var i = 0; i < n; i++) { var x = Math.random(); var y = Math.random(); if (x*x + y*y <= 1) inside++; } var pi = 4.0 * inside / n; return { samples: n, pi_estimate: pi.toFixed(6), error_pct: (Math.abs(pi - Math.PI) / Math.PI * 100).toFixed(4) + "%" };'
        },
        {
            name: 'Sort Benchmark',
            code: 'var arr = []; for (var i = 0; i < 100000; i++) arr.push(Math.random()); var start = Date.now(); arr.sort(function(a, b) { return a - b; }); var elapsed = Date.now() - start; return { elements: arr.length, sort_time_ms: elapsed, sorted: arr[0] <= arr[arr.length - 1] };'
        },
        {
            name: 'String Processing',
            code: 'var words = "the quick brown fox jumps over the lazy dog".split(" "); var result = {}; for (var rep = 0; rep < 50000; rep++) { for (var i = 0; i < words.length; i++) { var w = words[i]; result[w] = (result[w] || 0) + 1; } } return { unique_words: Object.keys(result).length, total_counts: Object.values(result).reduce(function(a,b){return a+b;}, 0) };'
        },
        {
            name: 'Collatz Conjecture',
            code: 'var maxSteps = 0; var maxN = 0; for (var n = 1; n <= 100000; n++) { var x = n; var steps = 0; while (x !== 1) { x = x % 2 === 0 ? x / 2 : 3 * x + 1; steps++; } if (steps > maxSteps) { maxSteps = steps; maxN = n; } } return { range: "1-100000", longest_chain: maxSteps, starting_number: maxN };'
        }
    ];

    // Execute each task.
    var self = this;
    var totalStart = Date.now();

    for (var i = 0; i < demoTasks.length; i++) {
        (function(task, index) {
            // Create a synthetic task message as if it came from the server.
            var taskMsg = {
                taskId: 'demo-' + Date.now() + '-' + index,
                taskType: 'js',
                code: task.code,
                input: null
            };

            self.addLog('task', 'Demo: launching "' + task.name + '"');
            self.handleTask(taskMsg);
        })(demoTasks[i], i);
    }
};

// ============================================================================
// Export for use in compute.html
// ============================================================================

// Make SwarmWorker available globally.
if (typeof window !== 'undefined') {
    window.SwarmWorker = SwarmWorker;
}
