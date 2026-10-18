//! 通过 api-server 交换打洞地址（UDP 打洞前置）。
//!
//! 背景：原来双方通过已建立的中继连接交换公网地址（PunchPeerAddr），
//! 导致必须"先建中继 → 再打洞 → 再换流"，产生双连接与换流竞态。
//! 本模块把地址交换改走 api-server：设备周期上报自己的 STUN 映射地址，
//! 控制端连接前即可拉取对端地址并立即打洞，成功则直接用直连建立会话。
//!
//! 鉴权方式与 process_monitor 一致：已登录带 access_token（Bearer），
//! 未登录设备带 X-Rustdesk-Key（服务器共享 key）。

use hbb_common::{anyhow::anyhow, config::Config, config::LocalConfig, ResultType};
use serde::{Deserialize, Serialize};

const PUNCH_ADDR_PATH: &str = "/api/punch/addr";
const PUNCH_INBOX_PATH: &str = "/api/punch/addr/inbox";

/// 一个可打洞地址（addr 形如 "1.2.3.4:5678"）
#[derive(Debug, Clone)]
pub struct PunchAddr {
    pub addr: String,
    /// "udp" | "udp6" | "tcp"
    pub addr_type: String,
    /// 对称 NAT 端口增量（0 = 锥形 NAT，端口恒定）
    pub delta: i32,
    /// NTP 对齐的双方约定起始时刻（0 = 不使用）
    pub sync_at_ms: i64,
}

/// inbox 里的一条连接请求
#[derive(Debug, Clone)]
pub struct InboxEntry {
    pub peer_id: String,
    pub addrs: Vec<PunchAddr>,
}

#[derive(Serialize)]
struct ReportAddr {
    addr: String,
    addr_type: String,
    delta: i32,
    sync_at_ms: i64,
}

#[derive(Serialize)]
struct ReportBody {
    peer_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    target: String,
    addrs: Vec<ReportAddr>,
}

#[derive(Deserialize)]
struct PeerAddrRaw {
    addr: String,
    #[serde(default)]
    addr_type: String,
    #[serde(default)]
    delta: i32,
    #[serde(default)]
    sync_at_ms: i64,
}

/// (api_server, token, key)
fn ctx() -> Option<(String, String, String)> {
    let api_server = crate::get_api_server(
        Config::get_option("api-server"),
        Config::get_option("custom-rendezvous-server"),
    );
    if api_server.is_empty() {
        return None;
    }
    let token = LocalConfig::get_option("access_token");
    // 设备身份 key：本地保存的服务器公钥（与 api-server 的 rustdesk.key 一致）
    let key = {
        let k = Config::get_option("key");
        if k.is_empty() {
            hbb_common::config::RS_PUB_KEY.to_owned()
        } else {
            k
        }
    };
    Some((api_server, token, key))
}

fn enabled() -> bool {
    ctx().is_some()
}

/// 构造 HTTP 客户端：优先 rustls，TLS 类型未缓存时失败则降级 native-tls
/// （与 common.rs::check_custom_update 的策略一致，兼容自签名证书）
async fn client_for(url: &str) -> ResultType<reqwest::Client> {
    use hbb_common::tls::TlsType;
    let proxy_conf = Config::get_socks();
    let tls_url = crate::hbbs_http::get_url_for_tls(url, &proxy_conf);
    let tls_type = hbb_common::tls::get_cached_tls_type(&tls_url);
    let is_tls_not_cached = tls_type.is_none();
    let tls_type = tls_type.unwrap_or(TlsType::Rustls);
    // create_http_client_async 的第一个参数是 TlsType
    let client = crate::hbbs_http::create_http_client_async(tls_type, true);
    // 探测一下：未缓存 TLS 类型且直连失败时换 native-tls 重试
    if is_tls_not_cached {
        let probe = client.get(url).send().await;
        if probe.is_err() {
            let client2 = crate::hbbs_http::create_http_client_async(TlsType::NativeTls, true);
            if client2.get(url).send().await.is_ok() {
                return Ok(client2);
            }
        }
    }
    Ok(client)
}

/// 上报自己的打洞地址。`target` 非空表示"我想连 target"（被控端据此发现请求）。
pub async fn report(
    peer_id: &str,
    target: &str,
    addrs: Vec<(String, String, i32, i64)>,
) -> ResultType<()> {
    let (api_server, token, key) = ctx().ok_or_else(|| anyhow!("api server not configured"))?;
    if addrs.is_empty() {
        return Ok(());
    }
    let url = format!("{}{}", api_server, PUNCH_ADDR_PATH);
    let body = ReportBody {
        peer_id: peer_id.to_owned(),
        target: target.to_owned(),
        addrs: addrs
            .into_iter()
            .map(|(addr, addr_type, delta, sync_at_ms)| ReportAddr {
                addr,
                addr_type,
                delta,
                sync_at_ms,
            })
            .collect(),
    };
    let client = client_for(&url).await?;
    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", token))
        .header("X-Rustdesk-Key", key)
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow!("punch addr report failed: {}", e))?;
    if !resp.status().is_success() {
        return Err(anyhow!("punch addr report status {}", resp.status()));
    }
    Ok(())
}

/// 拉取某设备上报的打洞地址（控制端用：peer_id = 被控端 id）
pub async fn get_peer_addrs(peer_id: &str) -> Vec<PunchAddr> {
    let Some((api_server, token, key)) = ctx() else {
        return Vec::new();
    };
    let url = format!("{}{}", api_server, PUNCH_ADDR_PATH);
    let Ok(client) = client_for(&url).await else {
        return Vec::new();
    };
    let resp = client
        .get(&url)
        .query(&[("peer_id", peer_id)])
        .header("Authorization", format!("Bearer {}", token))
        .header("X-Rustdesk-Key", key)
        .send()
        .await;
    let Ok(resp) = resp else {
        return Vec::new();
    };
    let Ok(v) = resp.json::<serde_json::Value>().await else {
        return Vec::new();
    };
    parse_addr_list(v.get("addrs"))
}

/// 拉取"想连我的设备"列表（被控端用：peer_id = 自己）
pub async fn get_inbox(peer_id: &str) -> Vec<InboxEntry> {
    let Some((api_server, token, key)) = ctx() else {
        return Vec::new();
    };
    let url = format!("{}{}", api_server, PUNCH_INBOX_PATH);
    let Ok(client) = client_for(&url).await else {
        return Vec::new();
    };
    let resp = client
        .get(&url)
        .query(&[("peer_id", peer_id)])
        .header("Authorization", format!("Bearer {}", token))
        .header("X-Rustdesk-Key", key)
        .send()
        .await;
    let Ok(resp) = resp else {
        return Vec::new();
    };
    let Ok(v) = resp.json::<serde_json::Value>().await else {
        return Vec::new();
    };
    let mut out = Vec::new();
    if let Some(arr) = v.get("requests").and_then(|r| r.as_array()) {
        for item in arr {
            let Some(peer_id) = item.get("peer_id").and_then(|p| p.as_str()) else {
                continue;
            };
            let addrs = parse_addr_list(item.get("addrs"));
            if !addrs.is_empty() {
                out.push(InboxEntry {
                    peer_id: peer_id.to_owned(),
                    addrs,
                });
            }
        }
    }
    out
}

fn parse_addr_list(v: Option<&serde_json::Value>) -> Vec<PunchAddr> {
    let Some(arr) = v.and_then(|x| x.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in arr {
        if let Ok(a) = serde_json::from_value::<PeerAddrRaw>(item.clone()) {
            if a.addr.is_empty() {
                continue;
            }
            out.push(PunchAddr {
                addr: a.addr,
                addr_type: a.addr_type,
                delta: a.delta,
                sync_at_ms: a.sync_at_ms,
            });
        }
    }
    out
}

/// 是否配置了 api-server（未配置则整套流程跳过，走原中继逻辑）
pub fn available() -> bool {
    enabled()
}
