// Marabunta - Licensed under the MIT License.
//! Backup CLI commands
//!
//! Provides CLI commands for backup and restore operations:
//! - `marabunta backup create` - Create a new backup
//! - `marabunta backup list` - List available backups
//! - `marabunta backup restore` - Restore from a backup
//! - `marabunta backup delete` - Delete a backup

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Args, Subcommand, ValueEnum};
use console::style;

use crate::backup::{
    Backup, BackupCreator, BackupError, BackupRestorer, BackupStatus, BackupTarget, BackupType,
    RestoreOptions,
};
use crate::cli::CliError;
use crate::storage::SqliteBackend;

/// Backup command arguments
#[derive(Args)]
pub struct BackupArgs {
    /// Backup subcommand
    #[command(subcommand)]
    pub command: BackupCommand,
}

/// Backup subcommands
#[derive(Subcommand)]
pub enum BackupCommand {
    /// Create a new backup
    Create(CreateArgs),

    /// List available backups
    List(ListArgs),

    /// Restore from a backup
    Restore(RestoreArgs),

    /// Delete a backup
    Delete(DeleteArgs),

    /// Show backup details
    Show(ShowArgs),
}

/// Arguments for creating a backup
#[derive(Args)]
pub struct CreateArgs {
    /// Type of backup to create
    #[arg(long, short = 't', default_value = "full")]
    pub backup_type: BackupTypeArg,

    /// Backup directory (defaults to ./backups)
    #[arg(long, short = 'd')]
    pub backup_dir: Option<PathBuf>,

    /// Database path to backup from
    #[arg(long, default_value = "./data/marabunta.db")]
    pub database: PathBuf,

    /// Description for the backup
    #[arg(long)]
    pub description: Option<String>,

    /// Only backup specific namespaces (comma-separated)
    #[arg(long)]
    pub namespaces: Option<String>,
}

/// Arguments for listing backups
#[derive(Args)]
pub struct ListArgs {
    /// Backup directory to scan
    #[arg(long, short = 'd')]
    pub backup_dir: Option<PathBuf>,

    /// Show only the N most recent backups
    #[arg(long, short = 'n')]
    pub limit: Option<usize>,

    /// Filter by backup type
    #[arg(long, short = 't')]
    pub backup_type: Option<BackupTypeArg>,

    /// Show detailed output
    #[arg(long, short = 'v')]
    pub verbose: bool,
}

/// Arguments for restoring from a backup
#[derive(Args)]
pub struct RestoreArgs {
    /// Backup ID or file path to restore from
    pub backup_id: String,

    /// Database path to restore to
    #[arg(long, default_value = "./data/marabunta.db")]
    pub database: PathBuf,

    /// Backup directory (if using backup ID)
    #[arg(long, short = 'd')]
    pub backup_dir: Option<PathBuf>,

    /// Clear existing data before restore
    #[arg(long)]
    pub clear_existing: bool,

    /// Only restore specific namespaces (comma-separated)
    #[arg(long)]
    pub namespaces: Option<String>,

    /// Skip checksum validation (not recommended)
    #[arg(long)]
    pub skip_checksum: bool,

    /// Dry run - validate but don't restore
    #[arg(long)]
    pub dry_run: bool,

    /// Force restore without confirmation
    #[arg(long, short = 'f')]
    pub force: bool,
}

/// Arguments for deleting a backup
#[derive(Args)]
pub struct DeleteArgs {
    /// Backup ID to delete
    pub backup_id: String,

    /// Backup directory
    #[arg(long, short = 'd')]
    pub backup_dir: Option<PathBuf>,

    /// Force delete without confirmation
    #[arg(long, short = 'f')]
    pub force: bool,
}

/// Arguments for showing backup details
#[derive(Args)]
pub struct ShowArgs {
    /// Backup ID or file path
    pub backup_id: String,

    /// Backup directory (if using backup ID)
    #[arg(long, short = 'd')]
    pub backup_dir: Option<PathBuf>,
}

/// Backup type argument for CLI
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum BackupTypeArg {
    /// Full backup of all data
    Full,
    /// Incremental backup (changes only)
    Incremental,
    /// Point-in-time snapshot
    Snapshot,
}

impl From<BackupTypeArg> for BackupType {
    fn from(arg: BackupTypeArg) -> Self {
        match arg {
            BackupTypeArg::Full => BackupType::Full,
            BackupTypeArg::Incremental => BackupType::Incremental,
            BackupTypeArg::Snapshot => BackupType::Snapshot,
        }
    }
}

/// Execute backup command
pub async fn execute_backup(args: BackupArgs) -> Result<(), CliError> {
    match args.command {
        BackupCommand::Create(create_args) => execute_create(create_args).await,
        BackupCommand::List(list_args) => execute_list(list_args).await,
        BackupCommand::Restore(restore_args) => execute_restore(restore_args).await,
        BackupCommand::Delete(delete_args) => execute_delete(delete_args).await,
        BackupCommand::Show(show_args) => execute_show(show_args).await,
    }
}

/// Execute backup create command
async fn execute_create(args: CreateArgs) -> Result<(), CliError> {
    let backup_dir = args
        .backup_dir
        .unwrap_or_else(|| PathBuf::from("./backups"));
    let backup_type: BackupType = args.backup_type.into();

    println!();
    println!(
        "{} Creating {} backup...",
        style("Backup").cyan().bold(),
        style(backup_type.to_string()).yellow()
    );

    // Open the database
    let backend = Arc::new(SqliteBackend::new(&args.database).map_err(|e| {
        CliError::Io(std::io::Error::other(
            e.to_string(),
        ))
    })?);

    let target = BackupTarget::local(&backup_dir);
    let creator = BackupCreator::new(backend, target);

    // Create the backup
    let backup = if let Some(namespaces) = args.namespaces {
        let ns_list: Vec<String> = namespaces
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        creator
            .create_backup_for_namespaces(backup_type, ns_list)
            .await
            .map_err(|e| CliError::Client(e.to_string()))?
    } else {
        creator
            .create_backup(backup_type, None)
            .await
            .map_err(|e| CliError::Client(e.to_string()))?
    };

    print_backup_summary(&backup);

    Ok(())
}

/// Execute backup list command
async fn execute_list(args: ListArgs) -> Result<(), CliError> {
    let backup_dir = args
        .backup_dir
        .unwrap_or_else(|| PathBuf::from("./backups"));

    println!();
    println!(
        "{} Scanning {}...",
        style("Backups").cyan().bold(),
        backup_dir.display()
    );

    // Scan backup directory for backup files
    let backups = scan_backup_directory(&backup_dir)?;

    if backups.is_empty() {
        println!();
        println!("  No backups found in {}", backup_dir.display());
        println!();
        return Ok(());
    }

    // Filter by type if specified
    let mut filtered: Vec<_> = if let Some(backup_type) = args.backup_type {
        let bt: BackupType = backup_type.into();
        backups
            .into_iter()
            .filter(|b| b.backup_type == bt)
            .collect()
    } else {
        backups
    };

    // Sort by timestamp descending
    filtered.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

    // Limit if specified
    if let Some(limit) = args.limit {
        filtered.truncate(limit);
    }

    println!();
    println!("  {} {}", style("ID").bold().underlined(), " ".repeat(30));
    println!("  {:-<50} {:-<12} {:-<10} {:-<10}", "", "", "", "");

    for backup in &filtered {
        print_backup_row(backup, args.verbose);
    }

    println!();
    println!("  Total: {} backup(s)", style(filtered.len()).green());
    println!();

    Ok(())
}

/// Execute backup restore command
async fn execute_restore(args: RestoreArgs) -> Result<(), CliError> {
    let backup_dir = args
        .backup_dir
        .unwrap_or_else(|| PathBuf::from("./backups"));

    // Find the backup file
    let backup_path = if args.backup_id.ends_with(".gz") || args.backup_id.contains('/') {
        PathBuf::from(&args.backup_id)
    } else {
        find_backup_file(&backup_dir, &args.backup_id)?
    };

    if !backup_path.exists() {
        return Err(CliError::NotFound(format!(
            "Backup not found: {}",
            backup_path.display()
        )));
    }

    println!();
    println!(
        "{} Restoring from {}...",
        style("Restore").cyan().bold(),
        style(backup_path.display()).yellow()
    );

    // Confirm unless forced
    if !args.force && !args.dry_run {
        println!();
        if args.clear_existing {
            println!(
                "  {} This will clear existing data!",
                style("WARNING:").red().bold()
            );
        }
        println!("  Target database: {}", args.database.display());
        println!();

        use dialoguer::Confirm;
        let confirmed = Confirm::new()
            .with_prompt("  Continue with restore?")
            .default(false)
            .interact()
            .map_err(|e| {
                CliError::Io(std::io::Error::other(
                    e.to_string(),
                ))
            })?;

        if !confirmed {
            println!("  Restore cancelled.");
            return Ok(());
        }
    }

    // Open target database
    let backend = Arc::new(SqliteBackend::new(&args.database).map_err(|e| {
        CliError::Io(std::io::Error::other(
            e.to_string(),
        ))
    })?);

    // Build restore options
    let mut options = RestoreOptions::new();
    if args.clear_existing {
        options = options.with_clear_existing();
    }
    if args.dry_run {
        options = options.with_dry_run();
    }
    if args.skip_checksum {
        options.skip_checksum = true;
    }
    if let Some(namespaces) = args.namespaces {
        let ns_list: Vec<String> = namespaces
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        options = options.with_namespaces(ns_list);
    }

    // Create a minimal backup struct for the restore
    let backup = crate::backup::Backup::new(
        BackupType::Full,
        crate::backup::BackupLocation::Local {
            path: backup_path.to_string_lossy().to_string(),
        },
    );

    // Perform restore
    let restorer = BackupRestorer::new(backend);
    let result = restorer
        .restore_with_options(&backup, options)
        .await
        .map_err(|e| CliError::Client(e.to_string()))?;

    println!();
    if args.dry_run {
        println!(
            "  {} Dry run complete (no data modified)",
            style("OK").green().bold()
        );
    } else {
        println!("  {} Restore complete!", style("OK").green().bold());
    }
    println!();
    println!("  Namespaces: {}", result.namespaces_restored);
    println!("  Items:      {}", result.total_items);
    println!("  Duration:   {} ms", result.duration_ms);

    if !result.warnings.is_empty() {
        println!();
        println!("  {} Warnings:", style("!").yellow().bold());
        for warning in &result.warnings {
            println!("    - {}", warning);
        }
    }

    println!();

    Ok(())
}

/// Execute backup delete command
async fn execute_delete(args: DeleteArgs) -> Result<(), CliError> {
    let backup_dir = args
        .backup_dir
        .unwrap_or_else(|| PathBuf::from("./backups"));

    let backup_path = if args.backup_id.ends_with(".gz") || args.backup_id.contains('/') {
        PathBuf::from(&args.backup_id)
    } else {
        find_backup_file(&backup_dir, &args.backup_id)?
    };

    if !backup_path.exists() {
        return Err(CliError::NotFound(format!(
            "Backup not found: {}",
            backup_path.display()
        )));
    }

    println!();
    println!(
        "{} Deleting backup: {}",
        style("Delete").red().bold(),
        backup_path.display()
    );

    // Confirm unless forced
    if !args.force {
        use dialoguer::Confirm;
        let confirmed = Confirm::new()
            .with_prompt("  Are you sure you want to delete this backup?")
            .default(false)
            .interact()
            .map_err(|e| {
                CliError::Io(std::io::Error::other(
                    e.to_string(),
                ))
            })?;

        if !confirmed {
            println!("  Delete cancelled.");
            return Ok(());
        }
    }

    std::fs::remove_file(&backup_path).map_err(|e| {
        CliError::Io(std::io::Error::other(
            e.to_string(),
        ))
    })?;

    println!();
    println!("  {} Backup deleted.", style("OK").green().bold());
    println!();

    Ok(())
}

/// Execute backup show command
async fn execute_show(args: ShowArgs) -> Result<(), CliError> {
    let backup_dir = args
        .backup_dir
        .unwrap_or_else(|| PathBuf::from("./backups"));

    let backup_path = if args.backup_id.ends_with(".gz") || args.backup_id.contains('/') {
        PathBuf::from(&args.backup_id)
    } else {
        find_backup_file(&backup_dir, &args.backup_id)?
    };

    if !backup_path.exists() {
        return Err(CliError::NotFound(format!(
            "Backup not found: {}",
            backup_path.display()
        )));
    }

    // Load and parse the backup file to get details
    let file = std::fs::File::open(&backup_path).map_err(CliError::Io)?;

    let reader = std::io::BufReader::new(file);
    let decoder = flate2::read::GzDecoder::new(reader);
    let mut json = Vec::new();
    std::io::Read::read_to_end(&mut std::io::BufReader::new(decoder), &mut json)
        .map_err(CliError::Io)?;

    let backup_data: crate::backup::BackupData =
        serde_json::from_slice(&json).map_err(|e| CliError::Parse(e.to_string()))?;

    println!();
    println!("{} Backup Details", style("Backup").cyan().bold());
    println!("{}", style("-".repeat(50)).dim());
    println!();
    println!("  ID:          {}", backup_data.backup.id);
    println!("  Type:        {}", backup_data.backup.backup_type);
    println!("  Status:      {}", backup_data.backup.status);
    println!("  Created:     {}", backup_data.backup.timestamp);
    println!("  Size:        {} bytes", backup_data.backup.size_bytes);
    println!("  Format:      v{}", backup_data.format_version);

    if let Some(checksum) = &backup_data.backup.checksum {
        println!("  Checksum:    {}...", &checksum[..16.min(checksum.len())]);
    }

    if let Some(desc) = &backup_data.backup.description {
        println!("  Description: {}", desc);
    }

    println!();
    println!("  {} Namespaces:", style("Data").yellow());
    for (namespace, ns_data) in &backup_data.data {
        println!("    - {} ({} items)", namespace, ns_data.item_count);
    }

    if !backup_data.backup.metadata.is_empty() {
        println!();
        println!("  {} Metadata:", style("Extra").dim());
        for (key, value) in &backup_data.backup.metadata {
            println!("    {}: {}", key, value);
        }
    }

    println!();

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPER FUNCTIONS
// ─────────────────────────────────────────────────────────────────────────────

/// Print a backup summary
fn print_backup_summary(backup: &Backup) {
    println!();
    println!(
        "  {} Backup created successfully!",
        style("OK").green().bold()
    );
    println!();
    println!("  ID:       {}", style(&backup.id).cyan());
    println!("  Type:     {}", backup.backup_type);
    println!("  Status:   {}", backup.status);
    println!("  Size:     {} bytes", backup.size_bytes);

    if let Some(path) = backup.file_path() {
        println!("  Location: {}", path.display());
    }

    if let Some(checksum) = &backup.checksum {
        println!("  Checksum: {}...", &checksum[..16.min(checksum.len())]);
    }

    println!();
}

/// Print a backup row in list view
fn print_backup_row(backup: &Backup, verbose: bool) {
    let status_icon = match backup.status {
        BackupStatus::Completed => style("*").green(),
        BackupStatus::InProgress => style("~").yellow(),
        BackupStatus::Failed => style("x").red(),
        _ => style("-").dim(),
    };

    let size = if backup.size_bytes > 1_000_000 {
        format!("{:.1} MB", backup.size_bytes as f64 / 1_000_000.0)
    } else if backup.size_bytes > 1_000 {
        format!("{:.1} KB", backup.size_bytes as f64 / 1_000.0)
    } else {
        format!("{} B", backup.size_bytes)
    };

    println!(
        "  {} {} {:12} {:>10} {}",
        status_icon,
        style(&backup.id).cyan(),
        backup.backup_type.to_string(),
        size,
        backup.timestamp.format("%Y-%m-%d %H:%M")
    );

    if verbose {
        if let Some(desc) = &backup.description {
            println!("      {}", style(desc).dim());
        }
    }
}

/// Scan backup directory for backup files
fn scan_backup_directory(dir: &PathBuf) -> Result<Vec<Backup>, CliError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut backups = Vec::new();

    for entry in std::fs::read_dir(dir).map_err(CliError::Io)? {
        let entry = entry.map_err(CliError::Io)?;
        let path = entry.path();

        if path.extension().map(|e| e == "gz").unwrap_or(false)
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.contains(".marabunta.bak"))
                .unwrap_or(false)
        {
            // Try to parse backup info from file
            if let Ok(backup) = parse_backup_file(&path) {
                backups.push(backup);
            }
        }
    }

    Ok(backups)
}

/// Parse backup metadata from a file
fn parse_backup_file(path: &PathBuf) -> Result<Backup, BackupError> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let decoder = flate2::read::GzDecoder::new(reader);
    let mut json = Vec::new();
    std::io::Read::read_to_end(&mut std::io::BufReader::new(decoder), &mut json)?;

    let backup_data: crate::backup::BackupData = serde_json::from_slice(&json)?;
    Ok(backup_data.backup)
}

/// Find a backup file by ID prefix
fn find_backup_file(dir: &PathBuf, id_prefix: &str) -> Result<PathBuf, CliError> {
    if !dir.exists() {
        return Err(CliError::NotFound(format!(
            "Backup directory not found: {}",
            dir.display()
        )));
    }

    for entry in std::fs::read_dir(dir).map_err(CliError::Io)? {
        let entry = entry.map_err(CliError::Io)?;
        let path = entry.path();

        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with(id_prefix) && name.contains(".marabunta.bak") {
                return Ok(path);
            }
        }
    }

    Err(CliError::NotFound(format!(
        "Backup with ID starting with '{}' not found in {}",
        id_prefix,
        dir.display()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backup_type_conversion() {
        assert_eq!(BackupType::from(BackupTypeArg::Full), BackupType::Full);
        assert_eq!(
            BackupType::from(BackupTypeArg::Incremental),
            BackupType::Incremental
        );
        assert_eq!(
            BackupType::from(BackupTypeArg::Snapshot),
            BackupType::Snapshot
        );
    }
}
