//! Risk-free rate loader with simple linear interpolation over Treasury points.
//! Mirrors the Python behavior:
//!   - Fetch yield.xml
//!   - Extract latest row: 0y, 1–6M, 1–5Y (percent→decimal)
//!   - Cache to disk for 24h
//!   - Provide linear interpolation across those points
//!   - On failure: fallback constant 0.02

use once_cell::sync::Lazy;
use quick_xml::events::Event;
use quick_xml::Reader;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use thiserror::Error;

pub const DEFAULT_CACHE_PATH: &str = "interp_cache.json";
pub const TREASURY_URL: &str =
    "https://home.treasury.gov/sites/default/files/interest-rates/yield.xml";
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60); // 24h

static HTTP: Lazy<Client> = Lazy::new(|| {
    Client::builder()
        .user_agent("riskfreerate/0.1 (rust)")
        .gzip(true)
        .brotli(true)
        .timeout(Duration::from_secs(20))
        .build()
        .expect("http client")
});

#[derive(Debug, Error)]
pub enum RfrError {
    #[error("http: {0}")]
    Http(String),
    #[error("xml: {0}")]
    Xml(String),
    #[error("io: {0}")]
    Io(String),
    #[error("json: {0}")]
    Json(String),
    #[error("no data")]
    NoData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheFile {
    years: Vec<f64>,
    rates: Vec<f64>,
}

/// Public API: identical spirit to your Python class.
#[derive(Debug, Clone)]
pub struct RiskFreeRate {
    years: Vec<f64>,
    rates: Vec<f64>,
}

impl RiskFreeRate {
    /// Load from 24h cache or fetch latest yield.xml.
    pub fn load_default() -> Result<Self, RfrError> {
        let path = PathBuf::from(DEFAULT_CACHE_PATH);
        if let Some(r) = load_cache_if_fresh(&path, DEFAULT_CACHE_TTL) {
            return Ok(r);
        }

        // Fetch body; don’t reuse `resp` after consuming to avoid E0382.
        let mut resp = HTTP
            .get(TREASURY_URL)
            .send()
            .map_err(to_http)?
            .error_for_status()
            .map_err(to_http)?;
        let mut xml = String::new();
        resp.read_to_string(&mut xml).map_err(to_io)?;

        let (years, rates) = parse_latest_curve_points(&xml)?;
        let r = Self { years, rates };

        // Best-effort cache write
        let _ = save_cache(&path, &r);
        Ok(r)
    }

    /// Linear interpolation over **years**.
    pub fn rate_at_years(&self, x_years: f64) -> f64 {
        lin_interp_clamped(&self.years, &self.rates, x_years)
    }

    /// Convenience: interpolate by **days** (Act/365).
    pub fn rate_at_days(&self, days: f64) -> f64 {
        self.rate_at_years((days / 365.0).max(0.0))
    }

    /// Optional: return a callable like Python’s `interp1d`.
    pub fn interp_fn(&self) -> impl Fn(f64) -> f64 + '_ {
        move |x_years| self.rate_at_years(x_years)
    }
}

/// Fallback: constant 2% if everything fails.
impl Default for RiskFreeRate {
    fn default() -> Self {
        Self {
            years: vec![0.0, 5.0],
            rates: vec![0.02, 0.02],
        }
    }
}

// ----------------- Cache helpers -----------------

fn load_cache_if_fresh(path: &Path, ttl: Duration) -> Option<RiskFreeRate> {
    let meta = fs::metadata(path).ok()?;
    let modified = meta.modified().ok()?;
    let age = SystemTime::now().duration_since(modified).ok()?;
    if age <= ttl {
        let data = fs::read(path).ok()?;
        let cf: CacheFile = serde_json::from_slice(&data).ok()?;
        return Some(RiskFreeRate {
            years: cf.years,
            rates: cf.rates,
        });
    }
    None
}

fn save_cache(path: &Path, r: &RiskFreeRate) -> Result<(), RfrError> {
    let cf = CacheFile {
        years: r.years.clone(),
        rates: r.rates.clone(),
    };
    let data =
        serde_json::to_vec_pretty(&cf).map_err(|e| RfrError::Json(format!("encode: {e}")))?;
    let tmp = tmp_path(path);
    fs::write(&tmp, &data).map_err(to_io)?;
    fs::rename(tmp, path).map_err(to_io)?;
    Ok(())
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut p = path.to_path_buf();
    let fname = path.file_name().and_then(|s| s.to_str()).unwrap_or("cache");
    p.set_file_name(format!("{fname}.tmp"));
    p
}

// ----------------- XML parsing -----------------

/// Extract (years, rates) from the **latest** <G_BC_CAT>.
/// years = (0, 1/12, 2/12, 3/12, 4/12, 6/12, 12/12, 24/12, 36/12, 60/12)
/// rates = (0, m1/100, m2/100, m3/100, m4/100, m6/100, y1/100, y2/100, y3/100, y5/100)
fn parse_latest_curve_points(xml: &str) -> Result<(Vec<f64>, Vec<f64>), RfrError> {
    let mut reader = Reader::from_str(xml); // no trim_text/config_mut calls (version-agnostic)

    let mut buf = Vec::new();
    let mut in_block = false;
    let mut current = String::new();
    let mut last_block: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                if e.name().as_ref() == b"G_BC_CAT" {
                    in_block = true;
                    current.clear();
                    current.push_str("<G_BC_CAT>");
                } else if in_block {
                    current.push('<');
                    current.push_str(&String::from_utf8_lossy(e.name().as_ref()));
                    current.push('>');
                }
            }
            Ok(Event::Text(t)) => {
                if in_block {
                    let s = t.unescape().unwrap_or_default();
                    if !s.trim().is_empty() {
                        current.push_str(&s);
                    }
                }
            }
            Ok(Event::End(e)) => {
                if e.name().as_ref() == b"G_BC_CAT" {
                    current.push_str("</G_BC_CAT>");
                    last_block = Some(current.clone());
                    in_block = false;
                } else if in_block {
                    current.push_str("</");
                    current.push_str(&String::from_utf8_lossy(e.name().as_ref()));
                    current.push('>');
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(RfrError::Xml(format!("read: {e}"))),
            _ => {}
        }
        buf.clear();
    }

    let last = last_block.ok_or(RfrError::NoData)?;

    let get = |tag: &str| -> Result<f64, RfrError> {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let start = last
            .find(&open)
            .ok_or_else(|| RfrError::Xml(format!("missing {tag}")))?;
        let end = last[start..]
            .find(&close)
            .ok_or_else(|| RfrError::Xml(format!("missing close {tag}")))?
            + start;
        let text = &last[start + open.len()..end];
        text.trim()
            .parse::<f64>()
            .map_err(|e| RfrError::Xml(format!("{tag} parse: {e}")))
    };

    let m1 = get("BC_1MONTH")? / 100.0;
    let m2 = get("BC_2MONTH")? / 100.0;
    let m3 = get("BC_3MONTH")? / 100.0;
    let m4 = get("BC_4MONTH")? / 100.0;
    let m6 = get("BC_6MONTH")? / 100.0;
    let y1 = get("BC_1YEAR")? / 100.0;
    let y2 = get("BC_2YEAR")? / 100.0;
    let y3 = get("BC_3YEAR")? / 100.0;
    let y5 = get("BC_5YEAR")? / 100.0;

    let years = vec![
        0.0,
        1.0 / 12.0,
        2.0 / 12.0,
        3.0 / 12.0,
        4.0 / 12.0,
        6.0 / 12.0,
        12.0 / 12.0,
        24.0 / 12.0,
        36.0 / 12.0,
        60.0 / 12.0,
    ];
    let rates = vec![0.0, m1, m2, m3, m4, m6, y1, y2, y3, y5];

    Ok((years, rates))
}

// ----------------- Linear interpolation -----------------

fn lin_interp_clamped(xs: &[f64], ys: &[f64], x: f64) -> f64 {
    debug_assert_eq!(xs.len(), ys.len());
    if xs.is_empty() {
        return 0.02;
    }
    if xs.len() == 1 {
        return ys[0];
    }
    if x <= xs[0] {
        return ys[0];
    }
    if x >= xs[xs.len() - 1] {
        return ys[ys.len() - 1];
    }

    // binary search for interval
    let (mut lo, mut hi) = (0usize, xs.len() - 1);
    while hi - lo > 1 {
        let mid = (lo + hi) / 2;
        if xs[mid] <= x {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    let (x0, x1) = (xs[lo], xs[hi]);
    let (y0, y1) = (ys[lo], ys[hi]);
    if (x1 - x0).abs() < 1e-12 {
        return y0;
    }
    let t = (x - x0) / (x1 - x0);
    y0 + t * (y1 - y0)
}

// ----------------- error helpers -----------------

fn to_http<E: std::fmt::Display>(e: E) -> RfrError {
    RfrError::Http(e.to_string())
}
fn to_io<E: std::fmt::Display>(e: E) -> RfrError {
    RfrError::Io(e.to_string())
}
