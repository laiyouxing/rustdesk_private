//! 通过 api-server 交换打洞地址（UDP 打洞前置）。
//!
//! 背景：原来双方通过已建立的中继连接交换公网地址（PunchPeerAddr），
//! 导致必须"先建中继 → 再打洞 → 再换流"，产生双连接与换流竞态。
//! 本模块把地址交换改走 api-server：设备周期上报自己的全部候选地址，
//! 控制端连接前即可拉取对端地址并立即打洞，成功则直接用直连建立会话。
//!
//! 为提高成功率，交换的不只是单个 STUN 地址，而是：
//!   - 多映射地址：主/备 STUN（对称 NAT 下映射不同）、UPnP 外部端口
//!   - LAN 网卡地址：双方同网段时直连，无需穿透
//!   - 公网 IPv6：双栈互通
//!   - TCP 同步打开端口：UDP 全封时的 fallback
//!   - last_success：上一会话成功地址（重连时 NAT 映射通常仍在）
//!   - 网络环境：NAT 类型、有无 UPnP、是否代理后（对端选策略）
//!   - NTP 对齐的约定起始时刻：两端同时发起
//!
//! 鉴权方式与 process_monitor 一致：已登录带 access_token（Bearer），
//! 未登录设备带 X-Rustdesk-Key（服务器共享 key）。

use hbb_common::{anyhow::anyhow, config::Config, config::LocalConfig, ResultType};
use serde::{Deserialize, Serialize};

const PUNCH_ADDR_PATH: &str = "/api/punch/addr";
const PUNCH_INBOX_PATH: &str = "/api/punch/addr/inbox";

// 地址来源与建议优先级（越大越先试）
pub const KIND_LAST_SUCCESS: &str = "last_success";
pub const KIND_STUN_PRIMARY: &str = "stun_primary";
pub const KIND_STUN_ALT: &str = "stun_alt";
pub const KIND_UPNP: &str = "upnp";
pub const KIND_LAN: &str = "lan";
pub const KIND_V6: &str = "v6";
pub const KIND_TCP: &str = "tcp";

pub const PRIO_LAST_SUCCESS: i32 = 100;
pub const PRIO_STUN_PRIMARY: i32 = 80;
pub const PRIO_STUN_ALT: i32 = 60;
pub const PRIO_UPNP: i32 = 50;
pub const PRIO_LAN: i32 = 40;
pub const PRIO_V6: i32 = 30;
pub const PRIO_TCP: i32 = 10;

/// 一个可打洞地址（addr 形如 "1.2.3.4:5678"）
#[derive(Debug, Clone)]
pub struct PunchAddr {
    pub addr: String,
    /// "udp" | "udp6" | "tcp"
    pub addr_type: String,
    /// 地址来源，见 KIND_* 常量
    pub kind: String,
    /// 建议优先级（越大越先试）
    pub priority: i32,
    /// 对称 NAT 端口增量（相对主映射；0 = 锥形 NAT，端口恒定）
    pub delta: i32,
    /// NTP 对齐的双方约定起始时刻（0 = 不使用）
    pub sync_at_ms: i64,
}

impl PunchAddr {
    pub fn new(addr: String, addr_type: &str, kind: &str, priority: i32, delta: i32) -> Self {
        Self {
            addr,
            addr_type: addr_type.to_owned(),
            kind: kind.to_owned(),
            priority,
            delta,
            sync_at_ms: 0,
        }
    }

    pub fn is_udp(&self) -> bool {
        self.addr_type != "tcp"
    }
}

/// 设备网络环境（对端据此选择打洞策略）
#[derive(Debug, Clone, Default)]
pub struct PunchMeta {
    /// "cone" | "symmetric" | "unknown"
    pub nat_type: String,
    /// 是否有 UPnP 外部映射
    pub upnp: bool,
    /// 是否经 SOCKS/WS 代理（打洞基本无意义）
    pub via_proxy: bool,
}

/// 对端上报的全部信息
#[derive(Debug, Clone, Default)]
pub struct PeerPunchInfo {
    pub addrs: Vec<PunchAddr>,
    pub meta: PunchMeta,
}

#[derive(Serialize)]
struct ReportAddr {
    addr: String,
    addr_type: String,
    kind: String,
    priority: i32,
    delta: i32,
    sync_at_ms: i64,
}

#[derive(Serialize)]
struct ReportMeta {
    nat_type: String,
    upnp: bool,
    via_proxy: bool,
}

#[derive(Serialize)]
struct ReportBody {
    peer_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    target: String,
    meta: ReportMeta,
    addrs: Vec<ReportAddr>,
}

#[derive(Deserialize)]
struct PeerAddrRaw {
    addr: String,
    #[serde(default)]
    addr_type: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    priority: i32,
    #[serde(default)]
    delta: i32,
    #[serde(default)]
    sync_at_ms: i64,
}

#[derive(Deserialize, Default)]
struct PeerMetaRaw {
    #[serde(default)]
    nat_type: String,
    #[serde(default)]
    upnp: bool,
    #[serde(default)]
    via_proxy: bool,
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
    let client = crate::hbbs_http::create_http_client_async(tls_type, true);
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

/// 上报自己的候选打洞地址与网络环境。
/// `target` 非空表示"我想连 target"（被控端据此发现请求）。
pub async fn report(
    peer_id: &str,
    target: &str,
    meta: &PunchMeta,
    addrs: Vec<PunchAddr>,
) -> ResultType<()> {
    let (api_server, token, key) = ctx().ok_or_else(|| anyhow!("api server not configured"))?;
    if addrs.is_empty() {
        return Ok(());
    }
    let url = format!("{}{}", api_server, PUNCH_ADDR_PATH);
    let body = ReportBody {
        peer_id: peer_id.to_owned(),
        target: target.to_owned(),
        meta: ReportMeta {
            nat_type: meta.nat_type.clone(),
            upnp: meta.upnp,
            via_proxy: meta.via_proxy,
        },
        addrs: addrs
            .into_iter()
            .map(|a| ReportAddr {
                addr: a.addr,
                addr_type: a.addr_type,
                kind: a.kind,
                priority: a.priority,
                delta: a.delta,
                sync_at_ms: a.sync_at_ms,
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

/// 拉取某设备上报的全部信息（控制端用：peer_id = 被控端 id）
pub async fn get_peer_info(peer_id: &str) -> PeerPunchInfo {
    let Some((api_server, token, key)) = ctx() else {
        return PeerPunchInfo::default();
    };
    let url = format!("{}{}", api_server, PUNCH_ADDR_PATH);
    let Ok(client) = client_for(&url).await else {
        return PeerPunchInfo::default();
    };
    let resp = client
        .get(&url)
        .query(&[("peer_id", peer_id)])
        .header("Authorization", format!("Bearer {}", token))
        .header("X-Rustdesk-Key", key)
        .send()
        .await;
    let Ok(resp) = resp else {
        return PeerPunchInfo::default();
    };
    let Ok(v) = resp.json::<serde_json::Value>().await else {
        return PeerPunchInfo::default();
    };
    let meta = v
        .get("meta")
        .and_then(|m| serde_json::from_value::<PeerMetaRaw>(m.clone()).ok())
        .unwrap_or_default();
    PeerPunchInfo {
        addrs: parse_addr_list(v.get("addrs")),
        meta: PunchMeta {
            nat_type: meta.nat_type,
            upnp: meta.upnp,
            via_proxy: meta.via_proxy,
        },
    }
}

/// inbox 里的一条连接请求
#[derive(Debug, Clone)]
pub struct InboxEntry {
    pub peer_id: String,
    pub addrs: Vec<PunchAddr>,
    pub meta: PunchMeta,
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
            if addrs.is_empty() {
                continue;
            }
            let meta = item
                .get("meta")
                .and_then(|m| serde_json::from_value::<PeerMetaRaw>(m.clone()).ok())
                .unwrap_or_default();
            out.push(InboxEntry {
                peer_id: peer_id.to_owned(),
                addrs,
                meta: PunchMeta {
                    nat_type: meta.nat_type,
                    upnp: meta.upnp,
                    via_proxy: meta.via_proxy,
                },
            });
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
                kind: a.kind,
                priority: a.priority,
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

/// 按优先级排序（稳定排序，同优先级保持服务端返回顺序），并做基本过滤：
/// 去掉无法解析/不可用的地址。UDP 类排在同级 TCP 之前。
pub fn sort_addrs(mut addrs: Vec<PunchAddr>) -> Vec<PunchAddr> {
    addrs.retain(|a| {
        a.addr
            .parse::<std::net::SocketAddr>()
            .map(|sa| !crate::common::is_unusable_punch_addr(&sa))
            .unwrap_or(false)
    });
    addrs.sort_by(|a, b| {
        // priority 降序；同级 UDP 优先于 TCP
        b.priority
            .cmp(&a.priority)
            .then(a.is_udp().cmp(&b.is_udp()).reverse())
    });
    addrs
}
