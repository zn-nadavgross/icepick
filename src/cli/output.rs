//! Output formatting for CLI commands

use clap::ValueEnum;
use serde::Serialize;

/// Output format for CLI commands
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable text output (AWS CLI style)
    #[default]
    Text,
    /// JSON output for scripting
    Json,
}

/// Trait for types that can be output in both text and JSON format
pub trait Outputable: Serialize {
    /// Format as human-readable text
    fn to_text(&self) -> String;
}

/// Print an outputable item in the specified format
pub fn print<T: Outputable>(item: &T, format: OutputFormat) {
    match format {
        OutputFormat::Text => println!("{}", item.to_text()),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(item)
                    .unwrap_or_else(|e| format!("{{\"error\": \"{}\"}}", e))
            );
        }
    }
}

/// Print an error message
pub fn print_error(message: &str) {
    eprintln!("Error: {}", message);
}

/// Print a success message (text mode only)
pub fn print_success(message: &str, format: OutputFormat) {
    match format {
        OutputFormat::Text => println!("{}", message),
        OutputFormat::Json => {} // JSON output should be self-contained
    }
}

/// Format bytes in human-readable format
pub fn format_bytes(bytes: u64) -> String {
    bytesize::ByteSize(bytes).to_string_as(true)
}

/// Format a number with thousands separators
pub fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Calculate percentage
pub fn format_percentage(numerator: u64, denominator: u64) -> String {
    if denominator == 0 {
        return "0%".to_string();
    }
    let pct = (numerator as f64 / denominator as f64) * 100.0;
    format!("{:.1}%", pct)
}
