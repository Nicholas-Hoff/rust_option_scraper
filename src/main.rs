mod riskfreerate_lib;
mod optionscraper;

use optionscraper::OptionScraper;

fn main() -> Result<(), String> {
    let sc = OptionScraper::new("SPY")?;
    let rows = sc.scrape_data()?;

    // Print a few rows
    for r in rows.iter() {
        println!(
            "{} {} {} exp={} rfr={:.4} spot={:.2} ts_cst={}",
            r.ticker, r.contract_type, r.strike_price, r.expiration_date, r.risk_free_rate, r.spot_price, r.data_update_time_cst
        );
    }
    Ok(())
}
