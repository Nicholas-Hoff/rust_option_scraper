mod optionscraper;
mod riskfreerate_lib;

use optionscraper::OptionScraper;
use std::io::{self, Write};

fn main() -> Result<(), String> {
    // Prompt the user for a ticker symbol instead of using a hard-coded value
    print!("Enter ticker symbol: ");
    io::stdout().flush().map_err(|e| e.to_string())?;
    let mut ticker = String::new();
    io::stdin()
        .read_line(&mut ticker)
        .map_err(|e| e.to_string())?;
    let ticker = ticker.trim();
    if ticker.is_empty() {
        return Err("ticker cannot be empty".to_string());
    }

    let sc = OptionScraper::new(ticker)?;
    let rows = sc.scrape_data()?;

    // Print all fields for each option row
    for r in rows.iter() {
        println!("{:#?}", r);
    }
    Ok(())
}
