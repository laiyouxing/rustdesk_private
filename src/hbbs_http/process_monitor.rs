use crate::hbbs_http::create_http_client_with_url;
use hbb_common::{config::Config, config::LocalConfig, log};
use serde::Serialize;
use std::collections::HashSet;
use std::thread;
use std::time::Duration;

const DEFAULT_INTERVAL: u64 = 30;
const CONFIG_PATH: &str = "/api/process/config";
const STATUS_PATH: &str = "/api/process/status";

#[derive(Serialize)]
struct ReportItem {
    name: String,
    #[serde(rename = "type")]
    typ: String,
    target: String,
    running: bool,
}

#[derive(Serialize)]
struct ReportBody {
    peer_id: String,
    items: Vec<ReportItem>,
}

// 后台线程：定期拉取监控配置 -> 检测进程/端口 -> 上报状态（带 Bearer 鉴权）
pub fn run() {
    std::thread::spawn(move || {
        loop {
            let api_server = crate::get_api_server(
                Config::get_option("api-server"),
                Config::get_option("custom-rendezvous-server"),
            );
            if api_server.is_empty() {
                thread::sleep(Duration::from_secs(60));
                continue;
            }
            // 与审计上报一致：未关联 api-server 账号(access_token 为空)则不上报，定时重试
            let token = LocalConfig::get_option("access_token");
            if token.is_empty() {
                thread::sleep(Duration::from_secs(60));
                continue;
            }
            let peer_id = Config::get_id();
            let client = create_http_client_with_url(&format!("{}{}", api_server, CONFIG_PATH));

            // 拉取本设备监控配置（后台集中下发）
            let rules = match client
                .get(&format!("{}{}", api_server, CONFIG_PATH))
                .query(&[("peer_id", peer_id.clone())])
                .header("Authorization", format!("Bearer {}", token))
                .send()
            {
                Ok(resp) => match resp.json::<serde_json::Value>() {
                    Ok(v) => v,
                    Err(_) => serde_json::json!({"rules": []}),
                },
                Err(e) => {
                    log::warn!("process_monitor fetch config failed: {}", e);
                    serde_json::json!({"rules": []})
                }
            };
            let rule_list = rules
                .get("rules")
                .and_then(|r| r.as_array())
                .cloned()
                .unwrap_or_default();
            if rule_list.is_empty() {
                thread::sleep(Duration::from_secs(60));
                continue;
            }
            let interval = rule_list
                .iter()
                .filter_map(|r| r.get("interval").and_then(|i| i.as_i64()))
                .filter(|&i| i > 0)
                .min()
                .unwrap_or(DEFAULT_INTERVAL as i64) as u64;

            if let Err(e) = detect_and_report(&client, &api_server, &token, &peer_id, &rule_list) {
                log::warn!("process_monitor report failed: {}", e);
            }
            thread::sleep(Duration::from_secs(interval));
        }
    });
}

fn detect_and_report(
    client: &reqwest::blocking::Client,
    api_server: &str,
    token: &str,
    peer_id: &str,
    rules: &[serde_json::Value],
) -> Result<(), Box<dyn std::error::Error>> {
    let processes = running_process_names();
    let ports = listening_ports();
    let mut items = Vec::new();
    for r in rules {
        let typ = r
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("process")
            .to_string();
        let target = r
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let name = r
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if target.is_empty() {
            continue;
        }
        let running = if typ == "port" {
            target
                .parse::<u16>()
                .map(|p| ports.contains(&p))
                .unwrap_or(false)
        } else {
            let mut t = target.to_lowercase();
            if t.ends_with(".exe") {
                t.truncate(t.len() - 4);
            }
            processes.contains(&t)
        };
        items.push(ReportItem {
            name,
            typ,
            target,
            running,
        });
    }
    let body = ReportBody {
        peer_id: peer_id.to_string(),
        items,
    };
    let resp = client
        .post(&format!("{}{}", api_server, STATUS_PATH))
        .header("Authorization", format!("Bearer {}", token))
        .json(&body)
        .send()?;
    if !resp.status().is_success() {
        log::warn!("process_monitor status http {}", resp.status());
    }
    Ok(())
}

// ===== Windows 检测实现 =====

#[cfg(windows)]
fn running_process_names() -> HashSet<String> {
    use windows::Win32::System::Diagnostics::ToolHelp::*;
    let mut set = HashSet::new();
    unsafe {
        let snap = match CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) {
            Ok(h) => h,
            Err(_) => return set,
        };
        let mut entry = PROCESSENTRY32::default();
        entry.dwSize = std::mem::size_of::<PROCESSENTRY32>() as u32;
        if Process32First(snap, &mut entry).is_ok() {
            loop {
                let raw: Vec<u8> = entry
                    .szExeFile
                    .iter()
                    .take_while(|&&c| c != 0)
                    .map(|&c| c as u8)
                    .collect();
                let mut name = String::from_utf8_lossy(&raw).to_lowercase();
                if name.ends_with(".exe") {
                    name.truncate(name.len() - 4);
                }
                set.insert(name);
                if Process32Next(snap, &mut entry).is_err() {
                    break;
                }
            }
        }
    }
    set
}

#[cfg(not(windows))]
fn running_process_names() -> HashSet<String> {
    HashSet::new()
}

#[cfg(windows)]
fn listening_ports() -> HashSet<u16> {
    use std::process::Command;
    let mut set = HashSet::new();
    if let Ok(out) = Command::new("netstat").args(["-ano", "-p", "tcp"]).output() {
        let s = String::from_utf8_lossy(&out.stdout);
        for line in s.lines() {
            let cols: Vec<&str> = line.split_whitespace().collect();
            // 形如: TCP    0.0.0.0:8080    0.0.0.0:0    LISTENING    1234
            if cols.len() >= 4 && cols[3].eq_ignore_ascii_case("LISTENING") {
                let addr = cols[1].trim_end_matches(']');
                if let Some(port_str) = addr.rsplit(':').next() {
                    if let Ok(p) = port_str.parse::<u16>() {
                        set.insert(p);
                    }
                }
            }
        }
    }
    set
}

#[cfg(not(windows))]
fn listening_ports() -> HashSet<u16> {
    HashSet::new()
}
