// Marabunta - Licensed under the MIT License.
/**
 * Marabunta Compute - Browser Worker Execution Script
 *
 * This Web Worker script runs inside a dedicated thread to execute
 * JavaScript and WebAssembly tasks without blocking the main UI thread.
 *
 * Protocol:
 *   Main thread posts: { taskId, taskType, code, input, moduleUrl, funcName }
 *   Worker responds:   { taskId, result } on success
 *                      { taskId, error }  on failure
 */

'use strict';

// Task execution timeout (30 seconds).
var TASK_TIMEOUT_MS = 30000;

self.onmessage = function(e) {
    var data = e.data;
    var taskId = data.taskId;
    var taskType = data.taskType;
    var code = data.code || '';
    var input = data.input || null;

    try {
        var result;

        if (taskType === 'js') {
            result = executeJS(code, input);
            self.postMessage({ taskId: taskId, result: JSON.stringify(result) });

        } else if (taskType === 'wasm') {
            // WebAssembly tasks are async -- we handle them with a promise.
            executeWASM(data.moduleUrl, data.funcName, input)
                .then(function(wasmResult) {
                    self.postMessage({ taskId: taskId, result: JSON.stringify(wasmResult) });
                })
                .catch(function(err) {
                    self.postMessage({ taskId: taskId, error: err.message || String(err) });
                });
            return; // Don't fall through to the synchronous postMessage below.

        } else {
            self.postMessage({ taskId: taskId, error: 'unknown task type: ' + taskType });
            return;
        }

    } catch (err) {
        self.postMessage({
            taskId: taskId,
            error: err.message || String(err)
        });
    }
};

/**
 * Execute a JavaScript code string with the given input.
 *
 * The code is wrapped in a Function constructor, so it has access to
 * an `input` parameter. The return value of the function is the result.
 */
function executeJS(code, input) {
    // Parse input if it's a JSON string.
    var parsedInput = input;
    if (typeof input === 'string') {
        try {
            parsedInput = JSON.parse(input);
        } catch (_) {
            // Keep as string if not valid JSON.
        }
    }

    // Create a sandboxed function. The code has access to:
    //   - input: the parsed input data
    //   - Math, JSON, Date, Array, Object, String, Number, etc.
    var fn = new Function('input', code);
    return fn(parsedInput);
}

/**
 * Execute a WebAssembly module by URL.
 *
 * Fetches the .wasm module, instantiates it, and calls the named export.
 */
function executeWASM(moduleUrl, funcName, input) {
    if (!moduleUrl) {
        return Promise.reject(new Error('no moduleUrl provided for wasm task'));
    }
    funcName = funcName || 'main';

    return fetch(moduleUrl)
        .then(function(response) {
            if (!response.ok) {
                throw new Error('failed to fetch wasm module: HTTP ' + response.status);
            }
            return response.arrayBuffer();
        })
        .then(function(bytes) {
            return WebAssembly.instantiate(bytes, {
                env: {
                    // Minimal imports -- can be extended as needed.
                    memory: new WebAssembly.Memory({ initial: 256 }),
                    abort: function() { throw new Error('wasm abort'); }
                }
            });
        })
        .then(function(wasmModule) {
            var exports = wasmModule.instance.exports;
            if (typeof exports[funcName] !== 'function') {
                throw new Error('wasm export "' + funcName + '" is not a function');
            }

            // Parse numeric input for wasm calls.
            var arg = 0;
            if (input !== null && input !== undefined) {
                var parsed = typeof input === 'string' ? parseFloat(input) : input;
                if (!isNaN(parsed)) {
                    arg = parsed;
                }
            }

            return exports[funcName](arg);
        });
}
