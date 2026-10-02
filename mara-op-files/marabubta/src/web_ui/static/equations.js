// Marabunta - Licensed under the MIT License.
/**
 * Marabunta Compute - Equation Switchboard UI
 *
 * Handles expression submission, handler selection, and result display
 * for the /api/switchboard/* endpoints.
 */

'use strict';

// ============================================================================
// State
// ============================================================================

var eqState = {
    format: 'plain_text',
    sessionId: null,
    submitting: false,
    executing: false
};

// ============================================================================
// Format selection
// ============================================================================

function selectFormat(el) {
    var tabs = document.querySelectorAll('.format-tab');
    for (var i = 0; i < tabs.length; i++) {
        tabs[i].classList.remove('active');
    }
    el.classList.add('active');
    eqState.format = el.getAttribute('data-format');
    updatePreview();
}

// ============================================================================
// Live preview
// ============================================================================

function updatePreview() {
    var input = document.getElementById('expr-input').value.trim();
    var preview = document.getElementById('preview');

    if (!input) {
        preview.className = 'preview-area empty';
        preview.textContent = 'Type an expression to see a preview';
        return;
    }

    preview.className = 'preview-area';

    if ((eqState.format === 'latex' || eqState.format === 'plain_text') && typeof katex !== 'undefined') {
        try {
            var texInput = eqState.format === 'latex' ? input : plainToLatex(input);
            katex.render(texInput, preview, { throwOnError: false, displayMode: true });
            return;
        } catch (_) {
            // Fall through to plain text display
        }
    }

    preview.textContent = input;
}

/**
 * Best-effort plain text to LaTeX conversion for preview.
 */
function plainToLatex(text) {
    return text
        .replace(/\bsqrt\(([^)]+)\)/g, '\\sqrt{$1}')
        .replace(/\bsin\b/g, '\\sin')
        .replace(/\bcos\b/g, '\\cos')
        .replace(/\btan\b/g, '\\tan')
        .replace(/\blog\b/g, '\\log')
        .replace(/\bln\b/g, '\\ln')
        .replace(/\bexp\b/g, '\\exp')
        .replace(/\bpi\b/g, '\\pi')
        .replace(/\binf\b/g, '\\infty')
        .replace(/\*\*/g, '^')
        .replace(/\*/g, '\\cdot ')
        .replace(/(\w)\^(\w)/g, '$1^{$2}')
        .replace(/(\w)\^{(\w+)}/g, '$1^{$2}');
}

// ============================================================================
// Submit expression
// ============================================================================

function submitExpression() {
    var input = document.getElementById('expr-input').value.trim();
    if (!input || eqState.submitting) return;

    eqState.submitting = true;
    hideError();
    hideResult();
    hideOptionMenu();

    var btn = document.getElementById('solve-btn');
    var status = document.getElementById('solve-status');
    btn.disabled = true;
    status.innerHTML = '<span class="spinner"></span>Classifying expression...';

    var body = JSON.stringify({
        format: eqState.format,
        content: input,
        metadata: {}
    });

    fetch('/api/switchboard/submit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: body
    })
    .then(function(resp) {
        if (!resp.ok) {
            return resp.text().then(function(t) { throw new Error(t || 'HTTP ' + resp.status); });
        }
        return resp.json();
    })
    .then(function(data) {
        eqState.sessionId = data.session_id;
        showOptionMenu(data);
        status.textContent = 'Select a solver below';
    })
    .catch(function(err) {
        showError('Submission failed: ' + err.message);
        status.textContent = '';
    })
    .finally(function() {
        eqState.submitting = false;
        btn.disabled = false;
    });
}

// ============================================================================
// Option menu
// ============================================================================

function showOptionMenu(data) {
    var section = document.getElementById('option-menu');
    var grid = document.getElementById('handler-grid');
    var info = document.getElementById('classification-info');

    // Show classification info
    if (data.confidence !== undefined) {
        info.textContent = 'Confidence: ' + (data.confidence * 100).toFixed(0) + '%';
    } else {
        info.textContent = '';
    }

    // Build handler cards
    var options = (data.menu && data.menu.options) || [];
    grid.innerHTML = '';

    for (var i = 0; i < options.length; i++) {
        var opt = options[i];
        var card = document.createElement('div');
        card.className = 'handler-card' + (opt.applicability === 'Recommended' ? ' recommended' : '');
        card.setAttribute('data-handler-id', opt.handler_id);
        card.onclick = (function(handlerId) {
            return function() { executeHandler(handlerId); };
        })(opt.handler_id);

        var badgeClass = 'badge-applicable';
        if (opt.applicability === 'Recommended') badgeClass = 'badge-recommended';
        else if (opt.applicability === 'Marginal') badgeClass = 'badge-marginal';

        card.innerHTML =
            '<div class="handler-name">' + escapeHtml(opt.name) + '</div>' +
            '<div class="handler-desc">' + escapeHtml(opt.description) + '</div>' +
            '<div class="handler-meta">' +
                '<span class="handler-badge ' + badgeClass + '">' + escapeHtml(opt.applicability) + '</span>' +
                '<span class="handler-time">~' + opt.estimated_time_ms + 'ms</span>' +
                (opt.requires_api_key ? '<span class="handler-badge badge-marginal">API Key</span>' : '') +
            '</div>';

        grid.appendChild(card);
    }

    section.classList.add('visible');
}

function hideOptionMenu() {
    document.getElementById('option-menu').classList.remove('visible');
}

// ============================================================================
// Execute handler
// ============================================================================

function executeHandler(handlerId) {
    if (!eqState.sessionId || eqState.executing) return;

    eqState.executing = true;
    hideError();
    hideResult();

    var status = document.getElementById('solve-status');
    status.innerHTML = '<span class="spinner"></span>Computing...';

    // Highlight selected card
    var cards = document.querySelectorAll('.handler-card');
    for (var i = 0; i < cards.length; i++) {
        cards[i].style.opacity = cards[i].getAttribute('data-handler-id') === handlerId ? '1' : '0.4';
    }

    var body = JSON.stringify({
        session_id: eqState.sessionId,
        handler_id: handlerId,
        params: {}
    });

    fetch('/api/switchboard/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: body
    })
    .then(function(resp) {
        if (!resp.ok) {
            return resp.text().then(function(t) { throw new Error(t || 'HTTP ' + resp.status); });
        }
        return resp.json();
    })
    .then(function(data) {
        showResult(data);
        status.textContent = 'Done';
    })
    .catch(function(err) {
        showError('Execution failed: ' + err.message);
        status.textContent = '';
    })
    .finally(function() {
        eqState.executing = false;
        // Restore card opacity
        var cards = document.querySelectorAll('.handler-card');
        for (var i = 0; i < cards.length; i++) {
            cards[i].style.opacity = '1';
        }
    });
}

// ============================================================================
// Result display
// ============================================================================

function showResult(data) {
    var section = document.getElementById('result-section');
    var latexEl = document.getElementById('result-latex');
    var plainEl = document.getElementById('result-plain');
    var stepsEl = document.getElementById('result-steps');
    var metaEl = document.getElementById('result-meta');

    // Render LaTeX result
    if (data.result_latex && typeof katex !== 'undefined') {
        try {
            katex.render(data.result_latex, latexEl, { throwOnError: false, displayMode: true });
            latexEl.style.display = 'block';
        } catch (_) {
            latexEl.textContent = data.result_latex;
            latexEl.style.display = 'block';
        }
    } else {
        latexEl.style.display = 'none';
    }

    // Plain text result
    if (data.result_plain) {
        plainEl.textContent = data.result_plain;
        plainEl.style.display = 'block';
    } else {
        plainEl.style.display = 'none';
    }

    // Steps
    stepsEl.innerHTML = '';
    var steps = data.steps || [];
    for (var i = 0; i < steps.length; i++) {
        var step = steps[i];
        var div = document.createElement('div');
        div.className = 'step-item';
        div.innerHTML =
            '<div class="step-number">Step ' + step.step_number + '</div>' +
            '<div class="step-desc">' + escapeHtml(step.description || '') + '</div>' +
            (step.expression ? '<div class="step-expr">' + escapeHtml(step.expression) + '</div>' : '');
        stepsEl.appendChild(div);
    }

    // Meta
    var metaParts = [];
    if (data.duration_ms !== undefined) metaParts.push('Duration: ' + data.duration_ms + 'ms');
    if (data.backend) metaParts.push('Backend: ' + data.backend);
    metaEl.innerHTML = metaParts.map(function(p) { return '<span>' + escapeHtml(p) + '</span>'; }).join('');

    section.classList.add('visible');
}

function hideResult() {
    document.getElementById('result-section').classList.remove('visible');
}

// ============================================================================
// Error handling
// ============================================================================

function showError(msg) {
    var banner = document.getElementById('error-banner');
    banner.textContent = msg;
    banner.classList.add('visible');
}

function hideError() {
    document.getElementById('error-banner').classList.remove('visible');
}

// ============================================================================
// Utility
// ============================================================================

function escapeHtml(str) {
    var div = document.createElement('div');
    div.textContent = str || '';
    return div.innerHTML;
}

// ============================================================================
// Init
// ============================================================================

document.addEventListener('DOMContentLoaded', function() {
    // Focus the input
    var input = document.getElementById('expr-input');
    if (input) input.focus();

    // Enter key submits
    input.addEventListener('keydown', function(e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            submitExpression();
        }
    });
});
