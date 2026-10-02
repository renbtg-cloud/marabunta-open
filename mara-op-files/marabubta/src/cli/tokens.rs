// Marabunta - Licensed under the MIT License.
//! Token management command
//!
//! Manages contribution tokens - view balance, history, and claim rewards.

use clap::{Args, Subcommand};

use crate::cli::client::CoordinatorClient;
use crate::cli::config::Config;
use crate::cli::display::{print_success, print_token_balance, print_token_transactions};
use crate::cli::types::{CliError, OutputFormat, TokenTransaction};

// ─────────────────────────────────────────────────────────────────────────────
// TOKENS ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for the tokens command
#[derive(Args)]
pub struct TokensArgs {
    /// Tokens subcommand
    #[command(subcommand)]
    pub command: Option<TokensCommand>,
}

/// Tokens subcommands
#[derive(Subcommand)]
pub enum TokensCommand {
    /// Show current balance
    Balance,
    /// Show transaction history
    History {
        /// Number of transactions to show
        #[arg(long, short, default_value = "20")]
        limit: u32,
        /// Output format (human, json, csv)
        #[arg(long, value_enum)]
        format: Option<OutputFormat>,
    },
    /// Claim pending rewards
    Claim,
    /// Show earnings breakdown
    Earnings {
        /// Time period (day, week, month, all)
        #[arg(long, default_value = "week")]
        period: String,
    },
    /// Show spending breakdown
    Spending {
        /// Time period (day, week, month, all)
        #[arg(long, default_value = "week")]
        period: String,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// EXECUTE TOKENS
// ─────────────────────────────────────────────────────────────────────────────

/// Execute the tokens command
pub async fn execute_tokens(args: TokensArgs, config: &Config) -> Result<(), CliError> {
    let client = CoordinatorClient::from_config(config).await?;

    match args.command {
        Some(TokensCommand::Balance) | None => execute_balance(&client, config).await,
        Some(TokensCommand::History { limit, format }) => {
            execute_history(&client, config, limit, format).await
        }
        Some(TokensCommand::Claim) => execute_claim(&client).await,
        Some(TokensCommand::Earnings { period }) => {
            execute_earnings(&client, config, &period).await
        }
        Some(TokensCommand::Spending { period }) => {
            execute_spending(&client, config, &period).await
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BALANCE
// ─────────────────────────────────────────────────────────────────────────────

/// Show token balance
async fn execute_balance(client: &CoordinatorClient, config: &Config) -> Result<(), CliError> {
    let balance = client.get_token_balance().await?;

    match config.output_format {
        OutputFormat::Human => {
            print_token_balance(&balance);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&balance)?);
        }
        OutputFormat::Csv => {
            println!("balance,pending,total_earned,total_spent");
            println!(
                "{},{},{},{}",
                balance.balance, balance.pending, balance.total_earned, balance.total_spent
            );
        }
        OutputFormat::Yaml => {
            println!("balance: {}", balance.balance);
            println!("pending: {}", balance.pending);
            println!("total_earned: {}", balance.total_earned);
            println!("total_spent: {}", balance.total_spent);
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// HISTORY
// ─────────────────────────────────────────────────────────────────────────────

/// Show transaction history
async fn execute_history(
    client: &CoordinatorClient,
    config: &Config,
    limit: u32,
    format: Option<OutputFormat>,
) -> Result<(), CliError> {
    let transactions = client.get_token_transactions(Some(limit)).await?;
    let format = format.unwrap_or(config.output_format);

    match format {
        OutputFormat::Human => {
            print_transaction_history(&transactions);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&transactions)?);
        }
        OutputFormat::Csv => {
            print_transactions_csv(&transactions);
        }
        OutputFormat::Yaml => {
            print_transactions_yaml(&transactions);
        }
    }

    Ok(())
}

/// Print transaction history in human format
fn print_transaction_history(transactions: &[TokenTransaction]) {
    use console::style;

    println!();
    println!("{} Transaction History", style("Marabunta").yellow().bold());
    println!("{}", style("─".repeat(70)).dim());

    if transactions.is_empty() {
        println!("   No transactions found.");
        return;
    }

    print_token_transactions(transactions);

    // Summary
    let total_in: f64 = transactions
        .iter()
        .filter(|t| t.amount > 0.0)
        .map(|t| t.amount)
        .sum();
    let total_out: f64 = transactions
        .iter()
        .filter(|t| t.amount < 0.0)
        .map(|t| t.amount.abs())
        .sum();

    println!();
    println!("{}", style("─".repeat(70)).dim());
    println!(
        "   Total In:  {} | Total Out: {}",
        style(format!("+{:.4}", total_in)).green(),
        style(format!("-{:.4}", total_out)).red()
    );
}

/// Print transactions as CSV
fn print_transactions_csv(transactions: &[TokenTransaction]) {
    println!("id,type,amount,timestamp,description");
    for tx in transactions {
        println!(
            "{},{},{},{},\"{}\"",
            tx.id,
            tx.tx_type,
            tx.amount,
            tx.timestamp,
            tx.description.replace('"', "\\\"")
        );
    }
}

/// Print transactions as YAML
fn print_transactions_yaml(transactions: &[TokenTransaction]) {
    println!("transactions:");
    for tx in transactions {
        println!("  - id: {}", tx.id);
        println!("    type: {}", tx.tx_type);
        println!("    amount: {}", tx.amount);
        println!("    timestamp: {}", tx.timestamp);
        println!("    description: \"{}\"", tx.description);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CLAIM
// ─────────────────────────────────────────────────────────────────────────────

/// Claim pending rewards
async fn execute_claim(client: &CoordinatorClient) -> Result<(), CliError> {
    use console::style;

    // Check balance first
    let balance = client.get_token_balance().await?;

    if balance.pending <= 0.0 {
        println!();
        println!("{} No pending rewards to claim.", style("INFO").cyan());
        return Ok(());
    }

    println!();
    println!(
        "Claiming {} pending rewards...",
        style(format!("{:.4}", balance.pending)).yellow()
    );

    let claimed = client.claim_rewards().await?;

    print_success(&format!(
        "Claimed {:.4} tokens! New balance: {:.4}",
        claimed,
        balance.balance + claimed
    ));

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// EARNINGS
// ─────────────────────────────────────────────────────────────────────────────

/// Show earnings breakdown
async fn execute_earnings(
    client: &CoordinatorClient,
    _config: &Config,
    period: &str,
) -> Result<(), CliError> {
    use console::style;

    // Get transactions and filter for earnings
    let limit = match period {
        "day" => 50,
        "week" => 200,
        "month" => 500,
        _ => 1000,
    };

    let transactions = client.get_token_transactions(Some(limit)).await?;
    let earnings: Vec<&TokenTransaction> = transactions.iter().filter(|t| t.amount > 0.0).collect();

    println!();
    println!(
        "{} Earnings Breakdown ({})",
        style("Marabunta").yellow().bold(),
        period
    );
    println!("{}", style("─".repeat(50)).dim());

    if earnings.is_empty() {
        println!("   No earnings in this period.");
        return Ok(());
    }

    // Group by type
    let mut by_type: std::collections::HashMap<&str, f64> = std::collections::HashMap::new();
    for tx in &earnings {
        *by_type.entry(&tx.tx_type).or_default() += tx.amount;
    }

    // Print breakdown
    let total: f64 = earnings.iter().map(|t| t.amount).sum();

    for (tx_type, amount) in &by_type {
        let pct = (amount / total) * 100.0;
        println!(
            "   {:<20} {:<12} ({:.1}%)",
            tx_type,
            style(format!("+{:.4}", amount)).green(),
            pct
        );
    }

    println!("{}", style("─".repeat(50)).dim());
    println!(
        "   {:<20} {}",
        style("Total").bold(),
        style(format!("+{:.4}", total)).green().bold()
    );

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// SPENDING
// ─────────────────────────────────────────────────────────────────────────────

/// Show spending breakdown
async fn execute_spending(
    client: &CoordinatorClient,
    _config: &Config,
    period: &str,
) -> Result<(), CliError> {
    use console::style;

    // Get transactions and filter for spending
    let limit = match period {
        "day" => 50,
        "week" => 200,
        "month" => 500,
        _ => 1000,
    };

    let transactions = client.get_token_transactions(Some(limit)).await?;
    let spending: Vec<&TokenTransaction> = transactions.iter().filter(|t| t.amount < 0.0).collect();

    println!();
    println!(
        "{} Spending Breakdown ({})",
        style("Marabunta").yellow().bold(),
        period
    );
    println!("{}", style("─".repeat(50)).dim());

    if spending.is_empty() {
        println!("   No spending in this period.");
        return Ok(());
    }

    // Group by type
    let mut by_type: std::collections::HashMap<&str, f64> = std::collections::HashMap::new();
    for tx in &spending {
        *by_type.entry(&tx.tx_type).or_default() += tx.amount.abs();
    }

    // Print breakdown
    let total: f64 = spending.iter().map(|t| t.amount.abs()).sum();

    for (tx_type, amount) in &by_type {
        let pct = (amount / total) * 100.0;
        println!(
            "   {:<20} {:<12} ({:.1}%)",
            tx_type,
            style(format!("-{:.4}", amount)).red(),
            pct
        );
    }

    println!("{}", style("─".repeat(50)).dim());
    println!(
        "   {:<20} {}",
        style("Total").bold(),
        style(format!("-{:.4}", total)).red().bold()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokens_command_parse() {
        // Just verify the enum compiles correctly
        let _balance = TokensCommand::Balance;
        let _history = TokensCommand::History {
            limit: 10,
            format: Some(OutputFormat::Json),
        };
        let _claim = TokensCommand::Claim;
    }
}
