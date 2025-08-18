mod optionscraper;
mod riskfreerate_lib;

use chrono::Local;
use optionscraper::{OptionRow, OptionScraper};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

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

    // Save results and capture changes
    let file_to_print = save_with_change_capture(ticker, &rows)?;
    let text = fs::read_to_string(file_to_print).map_err(|e| e.to_string())?;
    println!("{}", text);

    Ok(())
}

fn save_with_change_capture(ticker: &str, rows: &[OptionRow]) -> Result<PathBuf, String> {
    let out_dir = Path::new("data").join(ticker);
    fs::create_dir_all(&out_dir).map_err(|e| e.to_string())?;

    // locate previous run before writing new files
    let prev_file = find_latest_file(&out_dir, ticker);
    let prev_rows: Option<Vec<OptionRow>> = if let Some(prev_path) = &prev_file {
        let prev_text = fs::read_to_string(prev_path).map_err(|e| e.to_string())?;
        Some(serde_json::from_str(&prev_text).map_err(|e| e.to_string())?)
    } else {
        None
    };

    // compute diffs against previous run without altering previous file yet
    let mut changes: Vec<ChangeRecord> = Vec::new();
    if let Some(prev_rows) = &prev_rows {
        let prev_map: HashMap<String, OptionRow> = prev_rows
            .iter()
            .cloned()
            .map(|r| (r.option.clone(), r))
            .collect();
        let curr_map: HashMap<String, OptionRow> = rows
            .iter()
            .cloned()
            .map(|r| (r.option.clone(), r))
            .collect();
        let keys: HashSet<_> = prev_map
            .keys()
            .cloned()
            .chain(curr_map.keys().cloned())
            .collect();
        for k in keys {
            match (prev_map.get(&k), curr_map.get(&k)) {
                (Some(old), Some(new)) => {
                    let diffs = diff_rows(old, new);
                    if !diffs.is_empty() {
                        changes.extend(diffs);
                    }
                }
                (None, Some(new)) => {
                    changes.push(ChangeRecord {
                        option: k.clone(),
                        field: "record".to_string(),
                        old_value: Value::Null,
                        new_value: serde_json::to_value(new).unwrap_or(Value::Null),
                        old_active: false,
                        new_active: true,
                    });
                }
                (Some(old), None) => {
                    changes.push(ChangeRecord {
                        option: k.clone(),
                        field: "record".to_string(),
                        old_value: serde_json::to_value(old).unwrap_or(Value::Null),
                        new_value: Value::Null,
                        old_active: false,
                        new_active: false,
                    });
                }
                (None, None) => {}
            }
        }
    }

    // If no changes and previous file exists, reuse it
    if changes.is_empty() {
        if let Some(prev_path) = prev_file {
            return Ok(prev_path);
        }
    }

    // Otherwise mark previous inactive and write new file
    if let (Some(prev_path), Some(mut prev_rows)) = (prev_file, prev_rows) {
        for r in prev_rows.iter_mut() {
            r.active = false;
        }
        let prev_json = serde_json::to_string_pretty(&prev_rows).map_err(|e| e.to_string())?;
        fs::write(prev_path, prev_json).map_err(|e| e.to_string())?;
    }

    let ts = Local::now().format("%Y%m%d_%H%M").to_string();
    let raw_path = out_dir.join(format!("{}_{}.json", ticker, ts));
    let json = serde_json::to_string_pretty(rows).map_err(|e| e.to_string())?;
    fs::write(&raw_path, json).map_err(|e| e.to_string())?;

    if !changes.is_empty() {
        let cdc_path = out_dir.join(format!("{}_{}_cdc.json", ticker, ts));
        let cdc_json = serde_json::to_string_pretty(&changes).map_err(|e| e.to_string())?;
        fs::write(cdc_path, cdc_json).map_err(|e| e.to_string())?;
    }

    Ok(raw_path)
}

#[derive(Serialize, Deserialize)]
struct ChangeRecord {
    option: String,
    field: String,
    old_value: Value,
    new_value: Value,
    old_active: bool,
    new_active: bool,
}

const IGNORED_FIELDS: &[&str] = &[
    "data_update_time",
    "data_update_time_cst",
    "years_till_expire",
    "risk_free_rate",
    "active",
];

fn diff_rows(old: &OptionRow, new: &OptionRow) -> Vec<ChangeRecord> {
    let old_val = serde_json::to_value(old).unwrap_or(Value::Null);
    let new_val = serde_json::to_value(new).unwrap_or(Value::Null);
    let mut diffs = Vec::new();
    if let (Value::Object(o_map), Value::Object(n_map)) = (old_val, new_val) {
        for (k, o_v) in o_map.iter() {
            if IGNORED_FIELDS.contains(&k.as_str()) {
                continue;
            }
            if let Some(n_v) = n_map.get(k) {
                if o_v != n_v {
                    diffs.push(ChangeRecord {
                        option: new.option.clone(),
                        field: k.clone(),
                        old_value: o_v.clone(),
                        new_value: n_v.clone(),
                        old_active: old.active,
                        new_active: new.active,
                    });
                }
            }
        }
    }
    diffs
}

fn find_latest_file(dir: &Path, ticker: &str) -> Option<PathBuf> {
    fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|ft| ft.is_file()).unwrap_or(false))
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.starts_with(&format!("{}_", ticker)) && !s.contains("_cdc"))
                .unwrap_or(false)
        })
        .max_by_key(|p| p.file_name().and_then(|n| n.to_str()).map(|s| s.to_owned()))
}
