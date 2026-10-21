use hbb_common::{
    async_recursion::async_recursion,
    config::{Config, Socks5Server},
    log::{self, info},
    proxy::{Proxy, ProxyScheme},
    tls::{
        get_cached_tls_accept_invalid_cert, get_cached_tls_type, is_plain, upsert_tls_cache,
        TlsType,
    },
};
use reqwest::{blocking::Client as SyncClient, Client as AsyncClient};

macro_rules! configure_http_client {
    ($builder:expr, $tls_type:expr, $danger_accept_invalid_cert:expr, $Client: ty, $socks:expr, $degraded:expr) => {{
        // https://github.com/rustdesk/rustdesk/issues/11569
        // https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html#method.no_proxy
        let mut builder = $builder.no_proxy();

        match $tls_type {
            TlsType::Plain => {}
            TlsType::NativeTls => {
                builder = builder.use_native_tls();
                if $danger_accept_invalid_cert {
                    builder = builder.danger_accept_invalid_certs(true);
                }
            }
            TlsType::Rustls => {
                #[cfg(any(target_os = "android", target_os = "ios"))]
                match hbb_common::verifier::client_config($danger_accept_invalid_cert) {
                    Ok(client_config) => {
                        builder = builder.use_preconfigured_tls(client_config);
                    }
                    Err(e) => {
                        hbb_common::log::error!("Failed to get client config: {}", e);
                    }
                }
                #[cfg(not(any(target_os = "android", target_os = "ios")))]
                {
                    builder = builder.use_rustls_tls();
                    if $danger_accept_invalid_cert {
                        builder = builder.danger_accept_invalid_certs(true);
                    }
                }
            }
        }

        let client = if let Some(conf) = $socks {
            let proxy_result = Proxy::from_conf(conf, None);

            match proxy_result {
                Ok(proxy) => {
                    let proxy_setup = match &proxy.intercept {
                        ProxyScheme::Http { host, .. } => {
                            reqwest::Proxy::all(format!("http://{}", host))
                        }
                        ProxyScheme::Https { host, .. } => {
                            reqwest::Proxy::all(format!("https://{}", host))
                        }
                        ProxyScheme::Socks5 { addr, .. } => {
                            reqwest::Proxy::all(&format!("socks5://{}", addr))
                        }
                    };

                    match proxy_setup {
                        Ok(mut p) => {
                            if let Some(auth) = proxy.intercept.maybe_auth() {
                                if !auth.username().is_empty() && !auth.password().is_empty() {
                                    p = p.basic_auth(auth.username(), auth.password());
                                }
                            }
                            builder = builder.proxy(p);
                            builder.build().unwrap_or_else(|e| {
                                info!("Failed to create a proxied client: {}", e);
                                $degraded = true;
                                <$Client>::new()
                            })
                        }
                        Err(e) => {
                            info!("Failed to set up proxy: {}", e);
                            $degraded = true;
                            <$Client>::new()
                        }
                    }
                }
                Err(e) => {
                    info!("Failed to configure proxy: {}", e);
                    $degraded = true;
                    <$Client>::new()
                }
            }
        } else {
            builder.build().unwrap_or_else(|e| {
                info!("Failed to create a client: {}", e);
                $degraded = true;
                <$Client>::new()
            })
        };

        client
    }};
}

pub fn create_http_client(tls_type: TlsType, danger_accept_invalid_cert: bool) -> SyncClient {
    let danger = normalize_danger(tls_type, danger_accept_invalid_cert);
    let socks = Config::get_socks();
    let mut degraded = false;
    let client = configure_http_client!(
        SyncClient::builder(),
        tls_type,
        danger,
        SyncClient,
        &socks,
        degraded
    );
    if degraded {
        // 阻塞版 client 不缓存，仅在降级时提示：本次拿到的是无代理/默认 TLS 的
        // 降级 client（代理配置写错或证书库加载失败时会走到这里）。
        info!("sync http client degraded (proxy or tls setup failed)");
    }
    client
}

/// 进程级异步 HTTP client 缓存。
///
/// 为什么需要：reqwest 的连接池是**按 Client 实例**隔离的。每请求新建一个
/// Client，就等于每请求新开一条 TCP + 重做一次 TLS 握手，请求结束 Client 析构、
/// 连接关闭，本地留下 2 分钟 TIME_WAIT——内置的 keep-alive 连接池完全没用上。
/// 实测（本机 5 个 rustdesk 进程：tray + 若干 --cm + 服务）到 api-server
/// 的连接：ESTABLISHED 5 条（每进程 1 条，必要），TIME_WAIT 常驻 ~58 条，
/// 每分钟新建约 29 条（心跳 15s × 5 进程 + sysinfo_ver + 打洞上报 + inbox 轮询）。
/// 缓存后每进程只需 1 条长连接，TIME_WAIT 归零，且每个 API 请求少付 2 个 RTT。
///
/// key 必须包含：TLS 类型、是否接受无效证书、socks 代理配置。后两者在运行时
/// 可能变化（用户改代理/切 TLS），不进 key 会拿到旧配置的 client。
static ASYNC_CLIENT_CACHE: std::sync::Mutex<Option<std::collections::HashMap<String, AsyncClient>>> =
    std::sync::Mutex::new(None);

pub fn create_http_client_async(
    tls_type: TlsType,
    danger_accept_invalid_cert: bool,
) -> AsyncClient {
    let danger = normalize_danger(tls_type, danger_accept_invalid_cert);
    // 代理配置只读一次：缓存 key 与真正构造必须基于**同一份**配置。若各读一次，
    // 并发修改代理时会构造出"配置 B 的 client"却存到"key A"名下，之后粘性生效。
    let socks = Config::get_socks();
    let key = format!("{:?}|{}|{:?}", tls_type, danger, socks);

    // 快路径：命中缓存（同步段内只做查表/克隆，不跨 await）
    if let Ok(cache) = ASYNC_CLIENT_CACHE.lock() {
        if let Some(client) = cache.as_ref().and_then(|m| m.get(&key)) {
            return client.clone();
        }
    }
    // 锁外构造：首次构建要加载 rustls 根证书（Windows 上读系统证书库），
    // 持锁会让并发请求一起等。
    let mut degraded = false;
    let client = configure_http_client!(
        AsyncClient::builder(),
        tls_type,
        danger,
        AsyncClient,
        &socks,
        degraded
    );
    if degraded {
        // 降级 client（代理设置失败 / 构建失败回退的默认 client）不缓存：
        // 以前这种失败只影响单次请求，缓存下来会让整会话静默绕过代理。
        return client;
    }
    if let Ok(mut cache) = ASYNC_CLIENT_CACHE.lock() {
        let map = cache.get_or_insert_with(Default::default);
        // 并发下可能已被其他线程插入，保留先到者
        return map.entry(key).or_insert(client).clone();
    }
    // 锁被污染（panic 后）时退化为不缓存，保证功能不受影响
    client
}

/// Plain（http）下 `danger_accept_invalid_cert` 对客户端构造没有任何影响，
/// 归一化为 false：否则同一逻辑客户端会因调用方传值不同（心跳用缓存值、
/// 打洞上报硬编码 true）而缓存成多份、各占一条连接。
fn normalize_danger(tls_type: TlsType, danger_accept_invalid_cert: bool) -> bool {
    if matches!(tls_type, TlsType::Plain) {
        false
    } else {
        danger_accept_invalid_cert
    }
}

pub fn get_url_for_tls<'a>(url: &'a str, proxy_conf: &'a Option<Socks5Server>) -> &'a str {
    if is_plain(url) {
        if let Some(conf) = proxy_conf {
            if conf.proxy.starts_with("https://") {
                return &conf.proxy;
            }
        }
    }
    url
}

pub fn create_http_client_with_url(url: &str) -> SyncClient {
    let proxy_conf = Config::get_socks();
    let tls_url = get_url_for_tls(url, &proxy_conf);
    let tls_type = get_cached_tls_type(tls_url);
    let is_tls_type_cached = tls_type.is_some();
    let tls_type = tls_type.unwrap_or(TlsType::Rustls);
    let tls_danger_accept_invalid_cert = get_cached_tls_accept_invalid_cert(tls_url);
    create_http_client_with_url_(
        url,
        tls_url,
        tls_type,
        is_tls_type_cached,
        tls_danger_accept_invalid_cert,
        tls_danger_accept_invalid_cert,
    )
}

fn create_http_client_with_url_(
    url: &str,
    tls_url: &str,
    tls_type: TlsType,
    is_tls_type_cached: bool,
    danger_accept_invalid_cert: Option<bool>,
    original_danger_accept_invalid_cert: Option<bool>,
) -> SyncClient {
    let mut client = create_http_client(tls_type, danger_accept_invalid_cert.unwrap_or(false));
    if is_tls_type_cached && original_danger_accept_invalid_cert.is_some() {
        return client;
    }
    if let Err(e) = client.head(url).send() {
        if e.is_request() {
            match (tls_type, is_tls_type_cached, danger_accept_invalid_cert) {
                (TlsType::Rustls, _, None) => {
                    log::warn!(
                        "Failed to connect to server {} with rustls-tls: {:?}, trying accept invalid cert",
                        tls_url,
                        e
                    );
                    client = create_http_client_with_url_(
                        url,
                        tls_url,
                        tls_type,
                        is_tls_type_cached,
                        Some(true),
                        original_danger_accept_invalid_cert,
                    );
                }
                (TlsType::Rustls, false, Some(_)) => {
                    log::warn!(
                        "Failed to connect to server {} with rustls-tls: {:?}, trying native-tls",
                        tls_url,
                        e
                    );
                    client = create_http_client_with_url_(
                        url,
                        tls_url,
                        TlsType::NativeTls,
                        is_tls_type_cached,
                        original_danger_accept_invalid_cert,
                        original_danger_accept_invalid_cert,
                    );
                }
                (TlsType::NativeTls, _, None) => {
                    log::warn!(
                        "Failed to connect to server {} with native-tls: {:?}, trying accept invalid cert",
                        tls_url,
                        e
                    );
                    client = create_http_client_with_url_(
                        url,
                        tls_url,
                        tls_type,
                        is_tls_type_cached,
                        Some(true),
                        original_danger_accept_invalid_cert,
                    );
                }
                _ => {
                    log::error!(
                        "Failed to connect to server {} with {:?}, err: {:?}.",
                        tls_url,
                        tls_type,
                        e
                    );
                }
            }
        } else {
            log::warn!(
                "Failed to connect to server {} with {:?}, err: {}.",
                tls_url,
                tls_type,
                e
            );
        }
    } else {
        log::info!(
            "Successfully connected to server {} with {:?}",
            tls_url,
            tls_type
        );
        upsert_tls_cache(
            tls_url,
            tls_type,
            danger_accept_invalid_cert.unwrap_or(false),
        );
    }
    client
}

pub async fn create_http_client_async_with_url(url: &str) -> AsyncClient {
    let proxy_conf = Config::get_socks();
    let tls_url = get_url_for_tls(url, &proxy_conf);
    let tls_type = get_cached_tls_type(tls_url);
    let is_tls_type_cached = tls_type.is_some();
    let tls_type = tls_type.unwrap_or(TlsType::Rustls);
    let danger_accept_invalid_cert = get_cached_tls_accept_invalid_cert(tls_url);
    create_http_client_async_with_url_(
        url,
        tls_url,
        tls_type,
        is_tls_type_cached,
        danger_accept_invalid_cert,
        danger_accept_invalid_cert,
    )
    .await
}

#[async_recursion]
async fn create_http_client_async_with_url_(
    url: &str,
    tls_url: &str,
    tls_type: TlsType,
    is_tls_type_cached: bool,
    danger_accept_invalid_cert: Option<bool>,
    original_danger_accept_invalid_cert: Option<bool>,
) -> AsyncClient {
    let mut client =
        create_http_client_async(tls_type, danger_accept_invalid_cert.unwrap_or(false));
    if is_tls_type_cached && original_danger_accept_invalid_cert.is_some() {
        return client;
    }
    if let Err(e) = client.head(url).send().await {
        match (tls_type, is_tls_type_cached, danger_accept_invalid_cert) {
            (TlsType::Rustls, _, None) => {
                log::warn!(
                    "Failed to connect to server {} with rustls-tls: {:?}, trying accept invalid cert",
                    tls_url,
                    e
                );
                client = create_http_client_async_with_url_(
                    url,
                    tls_url,
                    tls_type,
                    is_tls_type_cached,
                    Some(true),
                    original_danger_accept_invalid_cert,
                )
                .await;
            }
            (TlsType::Rustls, false, Some(_)) => {
                log::warn!(
                    "Failed to connect to server {} with rustls-tls: {:?}, trying native-tls",
                    tls_url,
                    e
                );
                client = create_http_client_async_with_url_(
                    url,
                    tls_url,
                    TlsType::NativeTls,
                    is_tls_type_cached,
                    original_danger_accept_invalid_cert,
                    original_danger_accept_invalid_cert,
                )
                .await;
            }
            (TlsType::NativeTls, _, None) => {
                log::warn!(
                    "Failed to connect to server {} with native-tls: {:?}, trying accept invalid cert",
                    tls_url,
                    e
                );
                client = create_http_client_async_with_url_(
                    url,
                    tls_url,
                    tls_type,
                    is_tls_type_cached,
                    Some(true),
                    original_danger_accept_invalid_cert,
                )
                .await;
            }
            _ => {
                log::error!(
                    "Failed to connect to server {} with {:?}, err: {:?}.",
                    tls_url,
                    tls_type,
                    e
                );
            }
        }
    } else {
        log::info!(
            "Successfully connected to server {} with {:?}",
            tls_url,
            tls_type
        );
        upsert_tls_cache(
            tls_url,
            tls_type,
            danger_accept_invalid_cert.unwrap_or(false),
        );
    }
    client
}
