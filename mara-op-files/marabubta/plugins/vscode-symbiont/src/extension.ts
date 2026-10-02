// Marabunta - Licensed under the MIT License.
import * as vscode from 'vscode';
import * as child_process from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

export function activate(context: vscode.ExtensionContext) {
    console.log('[MARABUNTA SYMBIONT] Extension Active.');

    let analyzeCmd = vscode.commands.registerCommand('marabunta.analyzeWorkspace', () => {
        const workspacePath = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
        if (!workspacePath) {
            vscode.window.showErrorMessage('No workspace open to scan for Hidden Gems.');
            return;
        }

        vscode.window.withProgress({
            location: vscode.ProgressLocation.Notification,
            title: "Marabunta Symbiont",
            cancellable: false
        }, async (progress) => {
            progress.report({ message: "Hunting for blocking bottlenecks and GIL-locks..." });

            return new Promise<void>((resolve, reject) => {
                // Call the Rust analyzer backend we built!
                const scannerPath = 'symbiont-scanner'; 
                child_process.exec(`${scannerPath} ${workspacePath}`, (error, stdout, stderr) => {
                    if (error) {
                        vscode.window.showErrorMessage(`Symbiont Scan failed: ${stderr}`);
                        reject();
                        return;
                    }
                    
                    vscode.window.showInformationMessage('Symbiont Scan complete. Opening GUI Editor.');
                    vscode.commands.executeCommand('marabunta.openSymbiontGUI');
                    resolve();
                });
            });
        });
    });

    let guiCmd = vscode.commands.registerCommand('marabunta.openSymbiontGUI', () => {
        const panel = vscode.window.createWebviewPanel(
            'marabuntaSymbiont',
            'Marabunta Symbiont Opportunities',
            vscode.ViewColumn.One,
            { enableScripts: true }
        );

        // Normally we load the HTML/JS, here we just show the brutalist stub.
        panel.webview.html = getWebviewContent();
    });

    
    // --- MARABUNTA FIN-OPS CODELENS ---
    vscode.languages.registerCodeLensProvider({ language: 'python' }, {
        provideCodeLenses(document) {
            let lenses = [];
            for (let i = 0; i < document.lineCount; i++) {
                let line = document.lineAt(i);
                if (line.text.includes('@workflow')) {
                    lenses.push(new vscode.CodeLens(line.range, {
                        title: "💡 Marabunta: ~ $0.02 | AWS: $0.85 (85% Savings)",
                        command: "marabunta.openSymbiontGUI"
                    }));
                }
            }
            return lenses;
        }
    });

    context.subscriptions.push(analyzeCmd, guiCmd);
}

function getWebviewContent() {
    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Marabunta Symbiont Editor</title>
    <style>
        body { font-family: monospace; background: #fff; color: #000; padding: 20px; }
        h1 { background: #000; color: #fff; padding: 10px; text-transform: uppercase; }
        .box { border: 4px solid #000; padding: 15px; margin-bottom: 20px; box-shadow: 8px 8px 0px #000; }
        .btn { background: #ff0000; color: #fff; border: 2px solid #000; padding: 10px; font-weight: bold; cursor: pointer; text-transform: uppercase; }
        .code { background: #000; color: #0f0; padding: 10px; white-space: pre; }
    </style>
</head>
<body>
    <h1>MARABUNTA SYMBIONT: OPPORTUNITY SCANNER</h1>
    <p>We found 3 "Hidden Gems" in your workspace. Select a block to Auto-Refactor to the Swarm.</p>
    
    <div class="box">
        <h3>[!] GIL-Locked Bottleneck Detected</h3>
        <p><strong>File:</strong> src/batch/nightly.py:42</p>
        <p><strong>Heuristic:</strong> Synchronous compute-heavy inference inside standard Python for-loop.</p>
        <div class="code">for img in images:
    res = model.predict(img)
    processed.append(res)</div>
        <button class="btn" onclick="alert('Auto-Refactoring...')">⚡ Rip out inference & distribute via Swarm</button>
    </div>
    
    <script>
        // JS interop here to talk back to VSCode Extension API
    </script>
</body>
</html>`;
}

export function deactivate() {}
