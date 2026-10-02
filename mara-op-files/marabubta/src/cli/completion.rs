// Marabunta - Licensed under the MIT License.
//! Shell completion scripts for bash, zsh, and fish
//!
//! Generates completion scripts that can be installed for various shells.
//!
//! # Installation
//!
//! ## Bash
//! ```bash
//! marabunta completions bash > ~/.bash_completion.d/marabunta
//! source ~/.bash_completion.d/marabunta
//! ```
//!
//! ## Zsh
//! ```bash
//! marabunta completions zsh > ~/.zsh/completions/_marabunta
//! ```
//!
//! ## Fish
//! ```bash
//! marabunta completions fish > ~/.config/fish/completions/marabunta.fish
//! ```

use crate::cli::types::CliError;

// ─────────────────────────────────────────────────────────────────────────────
// SHELL TYPE
// ─────────────────────────────────────────────────────────────────────────────

/// Supported shell types for completion scripts
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shell {
    Bash,
    Zsh,
    Fish,
    PowerShell,
}

impl Shell {
    /// Parse shell name from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "bash" => Some(Shell::Bash),
            "zsh" => Some(Shell::Zsh),
            "fish" => Some(Shell::Fish),
            "powershell" | "pwsh" => Some(Shell::PowerShell),
            _ => None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// COMPLETION GENERATION
// ─────────────────────────────────────────────────────────────────────────────

/// Generate completion script for the specified shell
pub fn generate_completion(shell: Shell) -> Result<String, CliError> {
    match shell {
        Shell::Bash => Ok(generate_bash_completion()),
        Shell::Zsh => Ok(generate_zsh_completion()),
        Shell::Fish => Ok(generate_fish_completion()),
        Shell::PowerShell => Ok(generate_powershell_completion()),
    }
}

/// Generate bash completion script
fn generate_bash_completion() -> String {
    r##"# Marabunta Compute CLI Bash Completion
# Installation: marabunta completions bash >> ~/.bashrc
# Or: marabunta completions bash > /etc/bash_completion.d/marabunta

_marabunta_completions() {
    local cur prev words cword
    _init_completion || return

    local commands="submit status results cancel nodes tokens config maintenance workflow backup policy info dashboard shell completions"
    local submit_opts="--name --runtime --samples --samples-per-task --converge --memory-min --disk-min --arch --timeout --memory-prefer --cores-prefer --prefer-local --target-cores --target-memory --node-quality --strategy --exclude-memory-below --exclude-reliability-below --exclude-battery --wait --output --priority --preset --dry-run --no-plan"
    local status_opts="--watch --interval --format --extended"
    local results_opts="--format --output --partial --tasks"
    local cancel_opts="--force --quiet"
    local nodes_opts="--runtime --memory-min --region --arch --status --verbose --format --sort --reverse --limit --summary-only"
    local tokens_opts="balance history claim"
    local config_opts="set get list init reset path"
    local workflow_opts="submit status list run show delete cancel runs"
    local backup_opts="create restore list schedule"
    local policy_opts="validate apply list show delete template"
    local global_opts="--coordinator --format --verbose --quiet --help --version"

    # Handle subcommands
    case "${words[1]}" in
        submit)
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "$submit_opts $global_opts" -- "$cur"))
            else
                # Complete file names
                _filedir '@(py|wasm|lua|js|rb|pl|jl|R|f90|sh)'
            fi
            ;;
        status)
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "$status_opts $global_opts" -- "$cur"))
            else
                # Try to complete job IDs
                _marabunta_complete_jobs
            fi
            ;;
        results)
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "$results_opts $global_opts" -- "$cur"))
            else
                _marabunta_complete_jobs
            fi
            ;;
        cancel)
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "$cancel_opts $global_opts" -- "$cur"))
            else
                _marabunta_complete_jobs
            fi
            ;;
        nodes)
            COMPREPLY=($(compgen -W "$nodes_opts $global_opts" -- "$cur"))
            ;;
        tokens)
            if [[ $cword -eq 2 ]]; then
                COMPREPLY=($(compgen -W "$tokens_opts" -- "$cur"))
            fi
            ;;
        config)
            if [[ $cword -eq 2 ]]; then
                COMPREPLY=($(compgen -W "$config_opts" -- "$cur"))
            elif [[ $cword -eq 3 && "${words[2]}" == "set" ]]; then
                COMPREPLY=($(compgen -W "coordinator_url org_key default_runtime default_timeout output_format color default_region auto_confirm max_concurrent_uploads http_timeout_secs verbose" -- "$cur"))
            fi
            ;;
        workflow)
            if [[ $cword -eq 2 ]]; then
                COMPREPLY=($(compgen -W "$workflow_opts" -- "$cur"))
            fi
            ;;
        backup)
            if [[ $cword -eq 2 ]]; then
                COMPREPLY=($(compgen -W "$backup_opts" -- "$cur"))
            fi
            ;;
        policy)
            if [[ $cword -eq 2 ]]; then
                COMPREPLY=($(compgen -W "$policy_opts" -- "$cur"))
            fi
            ;;
        completions)
            COMPREPLY=($(compgen -W "bash zsh fish powershell" -- "$cur"))
            ;;
        *)
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "$global_opts" -- "$cur"))
            else
                COMPREPLY=($(compgen -W "$commands" -- "$cur"))
            fi
            ;;
    esac

    return 0
}

# Helper function to complete job IDs
_marabunta_complete_jobs() {
    # Try to fetch recent job IDs from a cache file
    local cache_file="${XDG_CACHE_HOME:-$HOME/.cache}/marabunta/job_ids"
    if [[ -f "$cache_file" ]]; then
        local jobs=$(cat "$cache_file" 2>/dev/null)
        COMPREPLY=($(compgen -W "$jobs" -- "$cur"))
    fi
}

# Helper function to complete node IDs
_marabunta_complete_nodes() {
    local cache_file="${XDG_CACHE_HOME:-$HOME/.cache}/marabunta/node_ids"
    if [[ -f "$cache_file" ]]; then
        local nodes=$(cat "$cache_file" 2>/dev/null)
        COMPREPLY=($(compgen -W "$nodes" -- "$cur"))
    fi
}

# Register the completion function
complete -F _marabunta_completions marabunta
"##.to_string()
}

/// Generate zsh completion script
fn generate_zsh_completion() -> String {
    r##"#compdef marabunta

# Marabunta Compute CLI Zsh Completion
# Installation: marabunta completions zsh > "${fpath[1]}/_marabunta"

_marabunta() {
    local context state state_descr line
    typeset -A opt_args

    local -a commands
    commands=(
        'submit:Submit a new job for distributed execution'
        'status:Check the status of a job'
        'results:Get the results of a completed job'
        'cancel:Cancel a running or pending job'
        'nodes:List available compute nodes'
        'tokens:Manage contribution tokens'
        'config:Configure CLI settings'
        'maintenance:Manage maintenance windows for nodes'
        'workflow:Manage multi-job workflows'
        'backup:Backup and restore cluster state'
        'policy:Manage placement policies'
        'info:Show cluster information'
        'dashboard:Open the TUI dashboard'
        'shell:Run interactive shell'
        'completions:Generate shell completion scripts'
    )

    local -a global_opts
    global_opts=(
        '--coordinator[Coordinator URL]:url:'
        '--format[Output format]:format:(human json csv yaml)'
        '(-v --verbose)'{-v,--verbose}'[Enable verbose output]'
        '(-q --quiet)'{-q,--quiet}'[Suppress non-essential output]'
        '--help[Show help]'
        '--version[Show version]'
    )

    _arguments -C \
        $global_opts \
        '1: :->command' \
        '*:: :->args'

    case $state in
        command)
            _describe -t commands 'marabunta commands' commands
            ;;
        args)
            case $line[1] in
                submit)
                    _arguments \
                        '--name[Job name]:name:' \
                        '--runtime[Runtime to use]:runtime:(python3 wasm lua native nodejs)' \
                        '--samples[Total number of samples]:samples:' \
                        '--samples-per-task[Samples per task]:count:' \
                        '--converge[Convergence threshold]:threshold:' \
                        '--memory-min[Minimum memory per task]:memory:' \
                        '--disk-min[Minimum disk space]:disk:' \
                        '--arch[Required architectures]:arch:(x86_64 arm64 aarch64)' \
                        '--timeout[Task timeout]:timeout:' \
                        '--memory-prefer[Preferred memory]:memory:' \
                        '--cores-prefer[Preferred cores]:cores:' \
                        '--prefer-local[Prefer local nodes]' \
                        '--target-cores[Target total cores]:cores:' \
                        '--target-memory[Target total memory]:memory:' \
                        '--node-quality[Node quality preference]:quality:(fewer_better more_smaller balanced)' \
                        '--strategy[Placement strategy]:strategy:(throughput latency cost balanced)' \
                        '--exclude-battery[Exclude battery-powered nodes]' \
                        '--wait[Wait for job completion]' \
                        '--output[Save results to file]:file:_files' \
                        '--priority[Priority level]:priority:(low normal high critical)' \
                        '--preset[Use preset configuration]:preset:(beefy swarm balanced cheap)' \
                        '--dry-run[Show what would be submitted]' \
                        '--no-plan[Do not show placement plan]' \
                        '*:file:_files -g "*.py *.wasm *.lua *.js *.rb *.pl *.jl *.R *.f90 *.sh"'
                    ;;
                status)
                    _arguments \
                        '(-w --watch)'{-w,--watch}'[Watch mode]' \
                        '--interval[Watch interval]:seconds:' \
                        '--format[Output format]:format:(human json csv yaml)' \
                        '(-e --extended)'{-e,--extended}'[Show extended details]' \
                        '*:job_id:_marabunta_jobs'
                    ;;
                results)
                    _arguments \
                        '--format[Output format]:format:(human json csv yaml)' \
                        '--output[Save to file]:file:_files' \
                        '--partial[Get partial results]' \
                        '--tasks[Show task details]' \
                        '*:job_id:_marabunta_jobs'
                    ;;
                cancel)
                    _arguments \
                        '(-f --force)'{-f,--force}'[Force cancel]' \
                        '(-q --quiet)'{-q,--quiet}'[Quiet mode]' \
                        '*:job_id:_marabunta_jobs'
                    ;;
                nodes)
                    _arguments \
                        '--runtime[Filter by runtime]:runtime:' \
                        '--memory-min[Filter by minimum memory]:memory:' \
                        '--region[Filter by region]:region:' \
                        '--arch[Filter by architecture]:arch:(x86_64 arm64)' \
                        '--status[Filter by status]:status:(ready busy offline draining)' \
                        '(-v --verbose)'{-v,--verbose}'[Show detailed info]' \
                        '--format[Output format]:format:(human json csv yaml)' \
                        '--sort[Sort by field]:field:(cores memory reliability load type status tasks)' \
                        '--reverse[Reverse sort order]' \
                        '--limit[Limit results]:limit:' \
                        '--summary-only[Show only summary]'
                    ;;
                tokens)
                    local -a token_commands
                    token_commands=(
                        'balance:Show token balance'
                        'history:Show transaction history'
                        'claim:Claim pending rewards'
                    )
                    _describe -t token_commands 'token commands' token_commands
                    ;;
                config)
                    local -a config_commands
                    config_commands=(
                        'set:Set a configuration value'
                        'get:Get a configuration value'
                        'list:List all configuration values'
                        'init:Interactive setup wizard'
                        'reset:Reset to defaults'
                        'path:Show config file path'
                    )
                    _describe -t config_commands 'config commands' config_commands
                    ;;
                workflow)
                    local -a workflow_commands
                    workflow_commands=(
                        'submit:Submit a workflow definition'
                        'status:Get workflow execution status'
                        'list:List workflows'
                        'run:Run a workflow'
                        'show:Show workflow details'
                        'delete:Delete a workflow'
                        'cancel:Cancel a running workflow'
                        'runs:List active runs'
                    )
                    _describe -t workflow_commands 'workflow commands' workflow_commands
                    ;;
                backup)
                    local -a backup_commands
                    backup_commands=(
                        'create:Create a new backup'
                        'restore:Restore from backup'
                        'list:List available backups'
                        'schedule:Manage backup schedules'
                    )
                    _describe -t backup_commands 'backup commands' backup_commands
                    ;;
                policy)
                    local -a policy_commands
                    policy_commands=(
                        'validate:Validate a policy file'
                        'apply:Apply a policy'
                        'list:List active policies'
                        'show:Show policy details'
                        'delete:Delete a policy'
                        'template:Generate policy template'
                    )
                    _describe -t policy_commands 'policy commands' policy_commands
                    ;;
                completions)
                    _arguments '1:shell:(bash zsh fish powershell)'
                    ;;
            esac
            ;;
    esac
}

# Helper function to complete job IDs
_marabunta_jobs() {
    local cache_file="${XDG_CACHE_HOME:-$HOME/.cache}/marabunta/job_ids"
    if [[ -f "$cache_file" ]]; then
        local jobs
        jobs=(${(f)"$(cat $cache_file 2>/dev/null)"})
        _describe -t jobs 'job IDs' jobs
    fi
}

# Helper function to complete node IDs
_marabunta_nodes() {
    local cache_file="${XDG_CACHE_HOME:-$HOME/.cache}/marabunta/node_ids"
    if [[ -f "$cache_file" ]]; then
        local nodes
        nodes=(${(f)"$(cat $cache_file 2>/dev/null)"})
        _describe -t nodes 'node IDs' nodes
    fi
}

_marabunta "$@"
"##.to_string()
}

/// Generate fish completion script
fn generate_fish_completion() -> String {
    r##"# Marabunta Compute CLI Fish Completion
# Installation: marabunta completions fish > ~/.config/fish/completions/marabunta.fish

# Disable file completions by default
complete -c marabunta -f

# Main commands
complete -c marabunta -n "__fish_use_subcommand" -a "submit" -d "Submit a new job for distributed execution"
complete -c marabunta -n "__fish_use_subcommand" -a "status" -d "Check the status of a job"
complete -c marabunta -n "__fish_use_subcommand" -a "results" -d "Get the results of a completed job"
complete -c marabunta -n "__fish_use_subcommand" -a "cancel" -d "Cancel a running or pending job"
complete -c marabunta -n "__fish_use_subcommand" -a "nodes" -d "List available compute nodes"
complete -c marabunta -n "__fish_use_subcommand" -a "tokens" -d "Manage contribution tokens"
complete -c marabunta -n "__fish_use_subcommand" -a "config" -d "Configure CLI settings"
complete -c marabunta -n "__fish_use_subcommand" -a "maintenance" -d "Manage maintenance windows"
complete -c marabunta -n "__fish_use_subcommand" -a "workflow" -d "Manage multi-job workflows"
complete -c marabunta -n "__fish_use_subcommand" -a "backup" -d "Backup and restore cluster state"
complete -c marabunta -n "__fish_use_subcommand" -a "policy" -d "Manage placement policies"
complete -c marabunta -n "__fish_use_subcommand" -a "info" -d "Show cluster information"
complete -c marabunta -n "__fish_use_subcommand" -a "dashboard" -d "Open the TUI dashboard"
complete -c marabunta -n "__fish_use_subcommand" -a "shell" -d "Run interactive shell"
complete -c marabunta -n "__fish_use_subcommand" -a "completions" -d "Generate shell completion scripts"

# Global options
complete -c marabunta -l coordinator -d "Coordinator URL"
complete -c marabunta -l format -d "Output format" -xa "human json csv yaml"
complete -c marabunta -s v -l verbose -d "Enable verbose output"
complete -c marabunta -s q -l quiet -d "Suppress non-essential output"
complete -c marabunta -s h -l help -d "Show help"
complete -c marabunta -l version -d "Show version"

# Submit command options
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l name -d "Job name"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l runtime -d "Runtime" -xa "python3 wasm lua native nodejs"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l samples -d "Total number of samples"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l samples-per-task -d "Samples per task"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l converge -d "Convergence threshold"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l memory-min -d "Minimum memory per task"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l disk-min -d "Minimum disk space"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l arch -d "Required architectures" -xa "x86_64 arm64 aarch64"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l timeout -d "Task timeout"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l memory-prefer -d "Preferred memory"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l cores-prefer -d "Preferred cores"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l prefer-local -d "Prefer local nodes"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l target-cores -d "Target total cores"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l target-memory -d "Target total memory"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l node-quality -d "Node quality preference" -xa "fewer_better more_smaller balanced"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l strategy -d "Placement strategy" -xa "throughput latency cost balanced"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l exclude-battery -d "Exclude battery-powered nodes"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l wait -d "Wait for job completion"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l output -d "Save results to file" -r
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l priority -d "Priority level" -xa "low normal high critical"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l preset -d "Use preset configuration" -xa "beefy swarm balanced cheap"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l dry-run -d "Show what would be submitted"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -l no-plan -d "Don't show placement plan"
complete -c marabunta -n "__fish_seen_subcommand_from submit" -F -d "Job file"

# Status command options
complete -c marabunta -n "__fish_seen_subcommand_from status" -s w -l watch -d "Watch mode"
complete -c marabunta -n "__fish_seen_subcommand_from status" -l interval -d "Watch interval in seconds"
complete -c marabunta -n "__fish_seen_subcommand_from status" -l format -d "Output format" -xa "human json csv yaml"
complete -c marabunta -n "__fish_seen_subcommand_from status" -s e -l extended -d "Show extended details"

# Results command options
complete -c marabunta -n "__fish_seen_subcommand_from results" -l format -d "Output format" -xa "human json csv yaml"
complete -c marabunta -n "__fish_seen_subcommand_from results" -l output -d "Save to file" -r
complete -c marabunta -n "__fish_seen_subcommand_from results" -l partial -d "Get partial results"
complete -c marabunta -n "__fish_seen_subcommand_from results" -l tasks -d "Show task details"

# Cancel command options
complete -c marabunta -n "__fish_seen_subcommand_from cancel" -s f -l force -d "Force cancel without confirmation"
complete -c marabunta -n "__fish_seen_subcommand_from cancel" -s q -l quiet -d "Quiet mode"

# Nodes command options
complete -c marabunta -n "__fish_seen_subcommand_from nodes" -l runtime -d "Filter by runtime"
complete -c marabunta -n "__fish_seen_subcommand_from nodes" -l memory-min -d "Filter by minimum memory"
complete -c marabunta -n "__fish_seen_subcommand_from nodes" -l region -d "Filter by region"
complete -c marabunta -n "__fish_seen_subcommand_from nodes" -l arch -d "Filter by architecture" -xa "x86_64 arm64"
complete -c marabunta -n "__fish_seen_subcommand_from nodes" -l status -d "Filter by status" -xa "ready busy offline draining"
complete -c marabunta -n "__fish_seen_subcommand_from nodes" -s v -l verbose -d "Show detailed info"
complete -c marabunta -n "__fish_seen_subcommand_from nodes" -l sort -d "Sort by field" -xa "cores memory reliability load type status tasks"
complete -c marabunta -n "__fish_seen_subcommand_from nodes" -l reverse -d "Reverse sort order"
complete -c marabunta -n "__fish_seen_subcommand_from nodes" -l limit -d "Limit results"
complete -c marabunta -n "__fish_seen_subcommand_from nodes" -l summary-only -d "Show only summary"

# Tokens subcommands
complete -c marabunta -n "__fish_seen_subcommand_from tokens; and not __fish_seen_subcommand_from balance history claim" -a "balance" -d "Show token balance"
complete -c marabunta -n "__fish_seen_subcommand_from tokens; and not __fish_seen_subcommand_from balance history claim" -a "history" -d "Show transaction history"
complete -c marabunta -n "__fish_seen_subcommand_from tokens; and not __fish_seen_subcommand_from balance history claim" -a "claim" -d "Claim pending rewards"

# Config subcommands
complete -c marabunta -n "__fish_seen_subcommand_from config; and not __fish_seen_subcommand_from set get list init reset path" -a "set" -d "Set a configuration value"
complete -c marabunta -n "__fish_seen_subcommand_from config; and not __fish_seen_subcommand_from set get list init reset path" -a "get" -d "Get a configuration value"
complete -c marabunta -n "__fish_seen_subcommand_from config; and not __fish_seen_subcommand_from set get list init reset path" -a "list" -d "List all configuration values"
complete -c marabunta -n "__fish_seen_subcommand_from config; and not __fish_seen_subcommand_from set get list init reset path" -a "init" -d "Interactive setup wizard"
complete -c marabunta -n "__fish_seen_subcommand_from config; and not __fish_seen_subcommand_from set get list init reset path" -a "reset" -d "Reset to defaults"
complete -c marabunta -n "__fish_seen_subcommand_from config; and not __fish_seen_subcommand_from set get list init reset path" -a "path" -d "Show config file path"

# Config set key completions
complete -c marabunta -n "__fish_seen_subcommand_from config; and __fish_seen_subcommand_from set" -a "coordinator_url org_key default_runtime default_timeout output_format color default_region auto_confirm max_concurrent_uploads http_timeout_secs verbose" -d "Config key"

# Workflow subcommands
complete -c marabunta -n "__fish_seen_subcommand_from workflow; and not __fish_seen_subcommand_from submit status list run show delete cancel runs" -a "submit" -d "Submit a workflow definition"
complete -c marabunta -n "__fish_seen_subcommand_from workflow; and not __fish_seen_subcommand_from submit status list run show delete cancel runs" -a "status" -d "Get workflow execution status"
complete -c marabunta -n "__fish_seen_subcommand_from workflow; and not __fish_seen_subcommand_from submit status list run show delete cancel runs" -a "list" -d "List workflows"
complete -c marabunta -n "__fish_seen_subcommand_from workflow; and not __fish_seen_subcommand_from submit status list run show delete cancel runs" -a "run" -d "Run a workflow"
complete -c marabunta -n "__fish_seen_subcommand_from workflow; and not __fish_seen_subcommand_from submit status list run show delete cancel runs" -a "show" -d "Show workflow details"
complete -c marabunta -n "__fish_seen_subcommand_from workflow; and not __fish_seen_subcommand_from submit status list run show delete cancel runs" -a "delete" -d "Delete a workflow"
complete -c marabunta -n "__fish_seen_subcommand_from workflow; and not __fish_seen_subcommand_from submit status list run show delete cancel runs" -a "cancel" -d "Cancel a running workflow"
complete -c marabunta -n "__fish_seen_subcommand_from workflow; and not __fish_seen_subcommand_from submit status list run show delete cancel runs" -a "runs" -d "List active runs"

# Backup subcommands
complete -c marabunta -n "__fish_seen_subcommand_from backup; and not __fish_seen_subcommand_from create restore list schedule" -a "create" -d "Create a new backup"
complete -c marabunta -n "__fish_seen_subcommand_from backup; and not __fish_seen_subcommand_from create restore list schedule" -a "restore" -d "Restore from backup"
complete -c marabunta -n "__fish_seen_subcommand_from backup; and not __fish_seen_subcommand_from create restore list schedule" -a "list" -d "List available backups"
complete -c marabunta -n "__fish_seen_subcommand_from backup; and not __fish_seen_subcommand_from create restore list schedule" -a "schedule" -d "Manage backup schedules"

# Policy subcommands
complete -c marabunta -n "__fish_seen_subcommand_from policy; and not __fish_seen_subcommand_from validate apply list show delete template" -a "validate" -d "Validate a policy file"
complete -c marabunta -n "__fish_seen_subcommand_from policy; and not __fish_seen_subcommand_from validate apply list show delete template" -a "apply" -d "Apply a policy"
complete -c marabunta -n "__fish_seen_subcommand_from policy; and not __fish_seen_subcommand_from validate apply list show delete template" -a "list" -d "List active policies"
complete -c marabunta -n "__fish_seen_subcommand_from policy; and not __fish_seen_subcommand_from validate apply list show delete template" -a "show" -d "Show policy details"
complete -c marabunta -n "__fish_seen_subcommand_from policy; and not __fish_seen_subcommand_from validate apply list show delete template" -a "delete" -d "Delete a policy"
complete -c marabunta -n "__fish_seen_subcommand_from policy; and not __fish_seen_subcommand_from validate apply list show delete template" -a "template" -d "Generate policy template"

# Completions subcommand
complete -c marabunta -n "__fish_seen_subcommand_from completions" -a "bash" -d "Bash completion script"
complete -c marabunta -n "__fish_seen_subcommand_from completions" -a "zsh" -d "Zsh completion script"
complete -c marabunta -n "__fish_seen_subcommand_from completions" -a "fish" -d "Fish completion script"
complete -c marabunta -n "__fish_seen_subcommand_from completions" -a "powershell" -d "PowerShell completion script"

# Dynamic job ID completion (reads from cache if available)
function __marabunta_complete_jobs
    set -l cache_file "$HOME/.cache/marabunta/job_ids"
    if test -f $cache_file
        cat $cache_file 2>/dev/null
    end
end

# Dynamic node ID completion
function __marabunta_complete_nodes
    set -l cache_file "$HOME/.cache/marabunta/node_ids"
    if test -f $cache_file
        cat $cache_file 2>/dev/null
    end
end
"##.to_string()
}

/// Generate PowerShell completion script
fn generate_powershell_completion() -> String {
    r##"# Marabunta Compute CLI PowerShell Completion
# Installation: marabunta completions powershell >> $PROFILE

using namespace System.Management.Automation
using namespace System.Management.Automation.Language

Register-ArgumentCompleter -Native -CommandName marabunta -ScriptBlock {
    param($wordToComplete, $commandAst, $cursorPosition)

    $commands = @{
        'submit' = @{
            Description = 'Submit a new job for distributed execution'
            Options = @(
                @{ Name = '--name'; Description = 'Job name' }
                @{ Name = '--runtime'; Description = 'Runtime to use'; Values = @('python3', 'wasm', 'lua', 'native', 'nodejs') }
                @{ Name = '--samples'; Description = 'Total number of samples' }
                @{ Name = '--samples-per-task'; Description = 'Samples per task' }
                @{ Name = '--converge'; Description = 'Convergence threshold' }
                @{ Name = '--memory-min'; Description = 'Minimum memory per task' }
                @{ Name = '--wait'; Description = 'Wait for job completion' }
                @{ Name = '--output'; Description = 'Save results to file' }
                @{ Name = '--priority'; Description = 'Priority level'; Values = @('low', 'normal', 'high', 'critical') }
                @{ Name = '--preset'; Description = 'Use preset configuration'; Values = @('beefy', 'swarm', 'balanced', 'cheap') }
                @{ Name = '--dry-run'; Description = 'Show what would be submitted' }
            )
        }
        'status' = @{
            Description = 'Check the status of a job'
            Options = @(
                @{ Name = '--watch'; Description = 'Watch mode' }
                @{ Name = '--interval'; Description = 'Watch interval' }
                @{ Name = '--format'; Description = 'Output format'; Values = @('human', 'json', 'csv', 'yaml') }
                @{ Name = '--extended'; Description = 'Show extended details' }
            )
        }
        'results' = @{
            Description = 'Get the results of a completed job'
            Options = @(
                @{ Name = '--format'; Description = 'Output format'; Values = @('human', 'json', 'csv', 'yaml') }
                @{ Name = '--output'; Description = 'Save to file' }
                @{ Name = '--partial'; Description = 'Get partial results' }
                @{ Name = '--tasks'; Description = 'Show task details' }
            )
        }
        'cancel' = @{
            Description = 'Cancel a running or pending job'
            Options = @(
                @{ Name = '--force'; Description = 'Force cancel' }
                @{ Name = '--quiet'; Description = 'Quiet mode' }
            )
        }
        'nodes' = @{
            Description = 'List available compute nodes'
            Options = @(
                @{ Name = '--runtime'; Description = 'Filter by runtime' }
                @{ Name = '--memory-min'; Description = 'Filter by minimum memory' }
                @{ Name = '--region'; Description = 'Filter by region' }
                @{ Name = '--arch'; Description = 'Filter by architecture'; Values = @('x86_64', 'arm64') }
                @{ Name = '--status'; Description = 'Filter by status'; Values = @('ready', 'busy', 'offline', 'draining') }
                @{ Name = '--verbose'; Description = 'Show detailed info' }
                @{ Name = '--format'; Description = 'Output format'; Values = @('human', 'json', 'csv', 'yaml') }
                @{ Name = '--sort'; Description = 'Sort by field'; Values = @('cores', 'memory', 'reliability', 'load', 'type', 'status', 'tasks') }
            )
        }
        'tokens' = @{
            Description = 'Manage contribution tokens'
            Subcommands = @('balance', 'history', 'claim')
        }
        'config' = @{
            Description = 'Configure CLI settings'
            Subcommands = @('set', 'get', 'list', 'init', 'reset', 'path')
        }
        'workflow' = @{
            Description = 'Manage multi-job workflows'
            Subcommands = @('submit', 'status', 'list', 'run', 'show', 'delete', 'cancel', 'runs')
        }
        'backup' = @{
            Description = 'Backup and restore cluster state'
            Subcommands = @('create', 'restore', 'list', 'schedule')
        }
        'policy' = @{
            Description = 'Manage placement policies'
            Subcommands = @('validate', 'apply', 'list', 'show', 'delete', 'template')
        }
        'info' = @{ Description = 'Show cluster information' }
        'dashboard' = @{ Description = 'Open the TUI dashboard' }
        'shell' = @{ Description = 'Run interactive shell' }
        'completions' = @{
            Description = 'Generate shell completion scripts'
            Subcommands = @('bash', 'zsh', 'fish', 'powershell')
        }
    }

    $elements = $commandAst.CommandElements
    $command = $null

    for ($i = 1; $i -lt $elements.Count; $i++) {
        $element = $elements[$i]
        if ($element.Extent.Text -notmatch '^-') {
            $command = $element.Extent.Text
            break
        }
    }

    if (-not $command) {
        # Complete main commands
        $commands.Keys | Where-Object { $_ -like "$wordToComplete*" } | ForEach-Object {
            [CompletionResult]::new($_, $_, 'ParameterValue', $commands[$_].Description)
        }
        return
    }

    if ($commands.ContainsKey($command)) {
        $cmdInfo = $commands[$command]

        # Complete subcommands
        if ($cmdInfo.Subcommands -and $wordToComplete -notmatch '^-') {
            $cmdInfo.Subcommands | Where-Object { $_ -like "$wordToComplete*" } | ForEach-Object {
                [CompletionResult]::new($_, $_, 'ParameterValue', $_)
            }
        }

        # Complete options
        if ($cmdInfo.Options) {
            $cmdInfo.Options | Where-Object { $_.Name -like "$wordToComplete*" } | ForEach-Object {
                [CompletionResult]::new($_.Name, $_.Name, 'ParameterName', $_.Description)
            }
        }
    }

    # Global options
    @('--coordinator', '--format', '--verbose', '--quiet', '--help', '--version') |
        Where-Object { $_ -like "$wordToComplete*" } | ForEach-Object {
            [CompletionResult]::new($_, $_, 'ParameterName', 'Global option')
        }
}
"##.to_string()
}

// ─────────────────────────────────────────────────────────────────────────────
// TESTS
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shell_from_str() {
        assert_eq!(Shell::from_str("bash"), Some(Shell::Bash));
        assert_eq!(Shell::from_str("ZSH"), Some(Shell::Zsh));
        assert_eq!(Shell::from_str("Fish"), Some(Shell::Fish));
        assert_eq!(Shell::from_str("pwsh"), Some(Shell::PowerShell));
        assert_eq!(Shell::from_str("unknown"), None);
    }

    #[test]
    fn test_generate_bash() {
        let script = generate_bash_completion();
        assert!(script.contains("_marabunta_completions"));
        assert!(script.contains("complete -F"));
        assert!(script.contains("submit"));
        assert!(script.contains("status"));
    }

    #[test]
    fn test_generate_zsh() {
        let script = generate_zsh_completion();
        assert!(script.contains("#compdef marabunta"));
        assert!(script.contains("_marabunta"));
        assert!(script.contains("commands"));
    }

    #[test]
    fn test_generate_fish() {
        let script = generate_fish_completion();
        assert!(script.contains("complete -c marabunta"));
        assert!(script.contains("__fish_use_subcommand"));
        assert!(script.contains("submit"));
    }

    #[test]
    fn test_generate_powershell() {
        let script = generate_powershell_completion();
        assert!(script.contains("Register-ArgumentCompleter"));
        assert!(script.contains("CompletionResult"));
    }
}
