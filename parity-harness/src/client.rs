//! HTTP send + capture.

use crate::corpus::Entry;
use reqwest::Method;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Captured {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body_text: String,
    pub error: Option<String>,
}

impl Captured {
    pub fn error(err: impl ToString) -> Self {
        Self {
            status: 0,
            headers: BTreeMap::new(),
            body_text: String::new(),
            error: Some(err.to_string()),
        }
    }
}

pub async fn send(http: &reqwest::Client, base: &str, entry: &Entry) -> Captured {
    let url = format!("{}{}", base.trim_end_matches('/'), entry.path);
    let method = match Method::from_bytes(entry.method.as_bytes()) {
        Ok(m) => m,
        Err(e) => return Captured::error(format!("bad method: {e}")),
    };

    let mut req = http.request(method, &url);
    for (k, v) in &entry.headers {
        req = req.header(k, v);
    }
    if let Some(body) = &entry.body {
        // If the body is a {"base64": "..."} wrapper, send the decoded bytes.
        // Otherwise, send as JSON.
        if let Some(obj) = body.as_object() {
            if let Some(b64) = obj.get("base64").and_then(|v| v.as_str()) {
                match base64_decode(b64) {
                    Ok(bytes) => req = req.body(bytes),
                    Err(e) => return Captured::error(format!("bad base64 body: {e}")),
                }
            } else {
                req = req.json(body);
            }
        } else {
            req = req.json(body);
        }
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => return Captured::error(format!("request failed: {e}")),
    };
    let status = resp.status().as_u16();
    let headers: BTreeMap<String, String> = resp
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();
    let body_text = match resp.text().await {
        Ok(t) => t,
        Err(e) => return Captured::error(format!("body read failed: {e}")),
    };
    Captured {
        status,
        headers,
        body_text,
        error: None,
    }
}

fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    // Minimal std-lib base64 decode (we don't want to pull the base64 crate
    // just for corpus payloads). Accepts url-safe and standard alphabets.
    let clean: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    let padded = match clean.len() % 4 {
        0 => clean,
        n => {
            let mut c = clean;
            c.push_str(&"=".repeat(4 - n));
            c
        }
    };
    let bytes = padded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut buf: u32 = 0;
    let mut bits = 0u32;
    for &b in bytes {
        let v: u32 = match b {
            b'A'..=b'Z' => (b - b'A') as u32,
            b'a'..=b'z' => (b - b'a' + 26) as u32,
            b'0'..=b'9' => (b - b'0' + 52) as u32,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            b'=' => {
                break;
            }
            _ => return Err(format!("bad base64 char: {}", b as char)),
        };
        buf = (buf << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buf >> bits) & 0xff) as u8);
        }
    }
    Ok(out)
}
