// Marabunta - Licensed under the MIT License.
use std::io::BufRead;

fn main() {
    let stdin = std::io::stdin();
    let mut _stdout = std::io::stdout();
    let mut reader = stdin.lock();

    eprintln!("MARABUNTA DAP: Initializing Holographic Debugger (Time-Travel Enabled)...");

    let mut line = String::new();
    while reader.read_line(&mut line).is_ok() {
        if line.is_empty() { break; }
        
        if line.contains("initialize") {
            eprintln!("MARABUNTA DAP: Capabilities advertised: [supportsStepBack: true]");
        } else if line.contains("stepBack") {
            eprintln!("MARABUNTA DAP: ALIEN FEATURE TRIGGERED: Space-Time Reversal.");
            eprintln!("MARABUNTA DAP: Resetting WASM VM to State 0...");
            eprintln!("MARABUNTA DAP: Fast-forwarding instructions to T-100ms...");
        }
        
        line.clear();
    }
}
