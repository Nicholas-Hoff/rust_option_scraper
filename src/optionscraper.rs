// src/optionscraper.rs

use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, NaiveDateTime, Utc};
use chrono_tz::America::Chicago;
use regex::Regex;
use reqwest::blocking::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

use crate::riskfreerate_lib::RiskFreeRate;

// =============== Public API ===============

#[derive(Debug, Clone)]
pub struct OptionScraper {
    pub ticker: String,
    http: Client,
    rfr: RiskFreeRate,
    // Precompiled regexes to mirror Python .str.extract calls
    re_type: Regex,
    re_strike: Regex,
    re_exp: Regex,
    re_ticker: Regex,
}

impl OptionScraper {
    pub fn new<T: Into<String>>(ticker: T) -> Result<Self, String> {
        use reqwest::header::{ACCEPT, USER_AGENT};

        let http = Client::builder()
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15")
            .gzip(true)
            .brotli(true)
            .timeout(Duration::from_secs(20))
            .default_headers({
                let mut h = reqwest::header::HeaderMap::new();
                h.insert(ACCEPT, "application/json".parse().unwrap());
                h.insert(USER_AGENT, "Mozilla/5.0 (Macintosh; Intel Mac OS X) Safari/605.1.15".parse().unwrap());
                h
            })
            .build()
            .map_err(err)?;

        let rfr = RiskFreeRate::load_default().unwrap_or_default();

        let re_type = Regex::new(r"\d([A-Z])\d").unwrap();         // C/P
        let re_strike = Regex::new(r"\d[A-Z](\d+)\d{3}").unwrap(); // strike dollars (drop last 3)
        let re_exp = Regex::new(r"[A-Z](\d{6})").unwrap();         // YYMMDD after C/P
        let re_ticker = Regex::new(r"^(\D*)").unwrap();            // leading non-digits

        Ok(Self {
            ticker: ticker.into(),
            http,
            rfr,
            re_type,
            re_strike,
            re_exp,
            re_ticker,
        })
    }

    /// Flexible UTC string -> America/Chicago.
    fn flex_to_chicago(&self, s: &str) -> Result<String, String> {
        // Try common formats we've seen in Cboe payloads
        let fmts = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S UTC",
            "%Y-%m-%dT%H:%M:%SZ",
        ];
        for fmt in fmts {
            if let Ok(naive) = NaiveDateTime::parse_from_str(s, fmt) {
                let dt_utc = chrono::DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
                let cst = dt_utc.with_timezone(&Chicago);
                return Ok(cst.format("%Y-%m-%d %H:%M:%S").to_string());
            }
        }
        Err(format!("unrecognized timestamp format: {s}"))
    }

    /// Try several likely timestamp fields; fall back to None.
    fn pick_timestamp_utc(&self, data: &Value) -> Option<String> {
        for k in ["timestamp", "time", "updated", "last_updated", "as_of", "quote_time"] {
            if let Some(s) = data.get(k).and_then(|v| v.as_str()) {
                if !s.is_empty() {
                    return Some(s.to_string());
                }
            }
        }
        if let Some(q) = data.get("quote") {
            for k in ["timestamp", "time", "updated", "last_updated", "as_of", "quote_time"] {
                if let Some(s) = q.get(k).and_then(|v| v.as_str()) {
                    if !s.is_empty() {
                        return Some(s.to_string());
                    }
                }
            }
        }
        None
    }

    /// Convert "YYYY-mm-dd HH:MM:SS" UTC -> America/Chicago time string (strict form)
    pub fn convert_utc_timestamp_cst(&self, utc_str: &str) -> Result<String, String> {
        let naive = NaiveDateTime::parse_from_str(utc_str, "%Y-%m-%d %H:%M:%S").map_err(err)?;
        let dt_utc = chrono::DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
        let cst = dt_utc.with_timezone(&Chicago);
        Ok(cst.format("%Y-%m-%d %H:%M:%S").to_string())
    }

    /// True if date is the third Friday of its month (helper)
    pub fn is_third_friday(&self, d: NaiveDate) -> bool {
        d.weekday().number_from_monday() == 5 && (15..=21).contains(&d.day())
    }

    /// Pull + shape option rows like the Python version
    pub fn scrape_data(&self) -> Result<Vec<OptionRow>, String> {
        // ---- fetch JSON from Cboe (try plain, then underscore form) ----
        let t = self.ticker.to_uppercase();
        let urls = [
            format!("https://cdn.cboe.com/api/global/delayed_quotes/options/{}.json", t),
            format!("https://cdn.cboe.com/api/global/delayed_quotes/options/_{}.json", t),
        ];

        let raw_text = {
            let mut last_err = String::new();
            let mut body: Option<String> = None;
            for u in urls.iter() {
                match self.http.get(u).send().map_err(err) {
                    Ok(resp) => {
                        let status = resp.status();
                        match resp.text() {
                            Ok(txt) => {
                                if status.is_success() && txt.trim_start().starts_with('{') {
                                    body = Some(txt);
                                    break;
                                } else {
                                    last_err = format!(
                                        "HTTP {} with non-JSON body (first 120 chars): {}",
                                        status,
                                        &txt.chars().take(120).collect::<String>()
                                    );
                                }
                            }
                            Err(e) => last_err = format!("read body error: {e}"),
                        }
                    }
                    Err(e) => last_err = e,
                }
            }
            body.ok_or_else(|| format!("Cboe fetch failed. Last error: {last_err}"))?
        };

        // Decode JSON to a Value so unknown fields never break decoding
        let root: Value = serde_json::from_str(&raw_text).map_err(|e| {
            format!(
                "JSON decode error: {e}. First 240 chars: {}",
                &raw_text.chars().take(240).collect::<String>()
            )
        })?;

        // Shape it into the fields we need
        let data = root.get("data").ok_or("missing 'data' key")?;

        // Spot price: try several names
        let spot_price = data
            .get("current_price")
            .and_then(|v| v.as_f64())
            .or_else(|| data.get("last").and_then(|v| v.as_f64()))
            .or_else(|| data.get("mark").and_then(|v| v.as_f64()))
            .ok_or("missing numeric 'current_price'/'last'/'mark'")?;

        // Timestamp (robust) -> CST string
        let ts_raw = self
            .pick_timestamp_utc(data)
            .unwrap_or_else(|| Utc::now().format("%Y-%m-%d %H:%M:%S").to_string());
        let ts_cst = self.flex_to_chicago(&ts_raw).unwrap_or_else(|_| ts_raw.clone());

        // Options array
        let opts = data
            .get("options")
            .and_then(|v| v.as_array())
            .ok_or("missing 'options' array")?;

        // ---- parse each option into prelim rows ----
        let mut unique_exps: HashMap<NaiveDate, ()> = HashMap::new();
        let mut prelim: Vec<PrelimRow> = Vec::with_capacity(opts.len());

        for v in opts.iter() {
            let symbol = v
                .get("option")
                .and_then(|v| v.as_str())
                .ok_or("option.symbol missing")?;

            let contract_type = self
                .re_type
                .captures(symbol)
                .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
                .unwrap_or_default();

            let strike_price: i64 = self
                .re_strike
                .captures(symbol)
                .and_then(|c| c.get(1))
                .and_then(|m| m.as_str().parse::<i64>().ok())
                .unwrap_or(0);

            // YYMMDD -> "20" + YYMMDD -> %Y%m%d
            let exp_date = self
                .re_exp
                .captures(symbol)
                .and_then(|c| c.get(1))
                .map(|m| format!("20{}", m.as_str()))
                .ok_or_else(|| format!("expiration parse failed for {symbol}"))?;

            let expiration = NaiveDate::parse_from_str(&exp_date, "%Y%m%d").map_err(err)?;

            let ticker = self
                .re_ticker
                .captures(symbol)
                .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
                .unwrap_or_default();

            let iv = v.get("iv").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let gamma = v.get("gamma").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let delta = v.get("delta").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let open_interest = v.get("open_interest").and_then(|x| x.as_u64()).unwrap_or(0);

            unique_exps.insert(expiration, ());
            prelim.push(PrelimRow {
                symbol: symbol.to_string(),
                contract_type,
                strike_price,
                expiration,
                ticker,
                iv,
                gamma,
                delta,
                open_interest,
            });
        }

        // ---- per-exp table: business days / 252, RFR, monthly/quarterly ----
        let today = Utc::now().date_naive();
        let mut per_exp: HashMap<NaiveDate, PerExpiration> = HashMap::new();

        for (&exp, _) in unique_exps.iter() {
            let busdays = business_days_between(today, exp);
            let years_frac = (busdays as f64) / 252.0;
            let risk_free_rate = self.rfr.rate_at_years(years_frac);
            let monthly_exp = next_third_friday_on_or_after(exp);
            let quarterly_expiration = business_quarter_end_on_or_after(exp);
            per_exp.insert(
                exp,
                PerExpiration {
                    years_frac,
                    risk_free_rate,
                    monthly_exp,
                    quarterly_expiration,
                },
            );
        }

        // ---- merge per-exp + mirrored call/put columns + globals ----
        let mut rows = Vec::with_capacity(prelim.len());
        for r in prelim.into_iter() {
            let extra = per_exp
                .get(&r.expiration)
                .ok_or_else(|| format!("missing per-exp fields for {}", r.expiration))?;

            let (call_iv, put_iv) = if r.contract_type == "C" {
                (r.iv, 0.0)
            } else if r.contract_type == "P" {
                (0.0, r.iv)
            } else {
                (0.0, 0.0)
            };

            let (call_gamma, put_gamma) = if r.contract_type == "C" {
                (r.gamma, 0.0)
            } else if r.contract_type == "P" {
                (0.0, r.gamma)
            } else {
                (0.0, 0.0)
            };

            let (call_delta, put_delta) = if r.contract_type == "C" {
                (r.delta, 0.0)
            } else if r.contract_type == "P" {
                (0.0, r.delta)
            } else {
                (0.0, 0.0)
            };

            let (call_oi, put_oi) = if r.contract_type == "C" {
                (r.open_interest, 0)
            } else if r.contract_type == "P" {
                (0, r.open_interest)
            } else {
                (0, 0)
            };

            rows.push(OptionRow {
                option: r.symbol,
                contract_type: r.contract_type,
                strike_price: r.strike_price,
                expiration_date: r.expiration,
                ticker: r.ticker,
                iv: r.iv,
                gamma: r.gamma,
                delta: r.delta,
                open_interest: r.open_interest,

                years_till_expire: extra.years_frac,
                risk_free_rate: extra.risk_free_rate,
                monthly_exp: extra.monthly_exp,
                quarterly_expiration: extra.quarterly_expiration,

                call_iv,
                put_iv,
                call_gamma,
                put_gamma,
                call_delta,
                put_delta,
                call_open_interest: call_oi,
                put_open_interest: put_oi,

                spot_price,
                data_update_time: ts_raw.clone(),        // <— clone per row
                data_update_time_cst: ts_cst.clone(),    // <— clone per row
            });
        }

        Ok(rows)
    }
}

// =============== Return row (like a DataFrame row) ===============

#[derive(Debug, Clone)]
pub struct OptionRow {
    pub option: String,
    pub contract_type: String, // "C" or "P"
    pub strike_price: i64,     // integer dollars (truncated like your regex)
    pub expiration_date: NaiveDate,
    pub ticker: String,

    pub iv: f64,
    pub gamma: f64,
    pub delta: f64,
    pub open_interest: u64,

    pub years_till_expire: f64, // busday_count/252 (Python called this days_till_expire)
    pub risk_free_rate: f64,
    pub monthly_exp: NaiveDate, // next 3rd Friday >= expiration
    pub quarterly_expiration: NaiveDate,

    // per-type mirrors (only one side non-zero per row, like your pandas columns)
    pub call_iv: f64,
    pub put_iv: f64,
    pub call_gamma: f64,
    pub put_gamma: f64,
    pub call_delta: f64,
    pub put_delta: f64,
    pub call_open_interest: u64,
    pub put_open_interest: u64,

    // globals copied to each row
    pub spot_price: f64,
    pub data_update_time: String,
    pub data_update_time_cst: String,
}

// =============== Internal shaping structs ===============

#[derive(Debug)]
struct PrelimRow {
    symbol: String,
    contract_type: String,
    strike_price: i64,
    expiration: NaiveDate,
    ticker: String,
    iv: f64,
    gamma: f64,
    delta: f64,
    open_interest: u64,
}

#[derive(Debug)]
struct PerExpiration {
    years_frac: f64,
    risk_free_rate: f64,
    monthly_exp: NaiveDate,
    quarterly_expiration: NaiveDate,
}

// =============== Helpers ===============

fn err<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

/// Count business days (Mon–Fri) between `start` (inclusive) and `end` (exclusive)
fn business_days_between(start: NaiveDate, end: NaiveDate) -> i32 {
    if end <= start {
        return 0;
    }
    let mut d = start;
    let mut count = 0;
    while d < end {
        let wd = d.weekday().number_from_monday(); // 1=Mon .. 7=Sun
        if wd <= 5 {
            count += 1;
        }
        d = d.succ_opt().unwrap();
    }
    count
}

/// Next 3rd Friday on or after the given date.
fn next_third_friday_on_or_after(mut d: NaiveDate) -> NaiveDate {
    loop {
        let third_fri = third_friday_of_month(d.year(), d.month());
        if d <= third_fri {
            return third_fri;
        }
        // move to 1st of next month
        let (mut y, mut m) = (d.year(), d.month() as i32 + 1);
        if m > 12 {
            m = 1;
            y += 1;
        }
        d = NaiveDate::from_ymd_opt(y, m as u32, 1).unwrap();
    }
}

fn third_friday_of_month(year: i32, month: u32) -> NaiveDate {
    // Find first Friday, then add 14 days
    let first = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
    let mut d = first;
    while d.weekday().number_from_monday() != 5 {
        d = d.succ_opt().unwrap();
    }
    d + ChronoDuration::days(14)
}

/// Business quarter end (last weekday of Mar/Jun/Sep/Dec) on or after `d`.
fn business_quarter_end_on_or_after(d: NaiveDate) -> NaiveDate {
    let qe = next_quarter_end(d);
    last_weekday_on_or_before(qe)
}

fn next_quarter_end(d: NaiveDate) -> NaiveDate {
    let (y, m) = (d.year(), d.month());
    let (qy, qm) = match m {
        1..=3 => (y, 3),
        4..=6 => (y, 6),
        7..=9 => (y, 9),
        _ => (y, 12),
    };
    let last_day = last_day_of_month(qy, qm);
    NaiveDate::from_ymd_opt(qy, qm, last_day).unwrap()
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    let first_next_month = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
    };
    let last = first_next_month - ChronoDuration::days(1);
    last.day()
}

fn last_weekday_on_or_before(mut d: NaiveDate) -> NaiveDate {
    while d.weekday().number_from_monday() > 5 {
        d = d.pred_opt().unwrap();
    }
    d
}
