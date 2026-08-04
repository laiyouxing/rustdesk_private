use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use uuid::Uuid;

use hbb_common::{
    allow_err,
    anyhow::{self, bail},
    config::{
        self, keys::*, option2bool, use_ws, Config, CONNECT_TIMEOUT, REG_INTERVAL,
        RENDEZVOUS_PORT, RELAY_PORT,
    },
    futures::future::join_all,
    log,
    protobuf::Message as _,
    rendezvous_proto::*,
    sleep,
    socket_client::{self, connect_tcp, is_ipv4, new_udp_for},
    timeout,
    tokio::{self, select, sync::Mutex, time::interval},
    udp::FramedSocket,
    AddrMangle, IntoTargetAddr, ResultType, Stream, TargetAddr,
};

use crate::{
    check_port,
    server::{check_zombie, new as new_server, ServerPtr},
};

type Message = RendezvousMessage;

lazy_static::lazy_static! {
    static ref SOLVING_PK_MISMATCH: Mutex<String> = Default::default();
    // (encoded_socket_addr_bytes, decoded_addr, timestamp)
    // Using encoded bytes + decoded addr as key to prevent false dedup
    // when different peers happen to have the same decoded addr (extremely rare).
    static ref LAST_INTRANET_MSG: Mutex<(Vec<u8>, SocketAddr, Instant)> = Mutex::new((Vec::new(), SocketAddr::new([0; 4].into(), 0), Instant::now()));
    static ref LAST_RELAY_MSG: Mutex<(SocketAddr, Instant)> = Mutex::new((SocketAddr::new([0; 4].into(), 0), Instant::now()));
}
static SHOULD_EXIT: AtomicBool = AtomicBool::new(false);
static MANUAL_RESTARTED: AtomicBool = AtomicBool::new(false);
static SENT_REGISTER_PK: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
pub struct RendezvousMediator {
    addr: TargetAddr<'static>,
    host: String,
    host_prefix: String,
    keep_alive: i32,
}

impl RendezvousMediator {
    pub fn restart() {
        SHOULD_EXIT.store(true, Ordering::SeqCst);
        MANUAL_RESTARTED.store(true, Ordering::SeqCst);
        log::info!("server restart");
    }

    pub async fn start_all() {
        crate::test_nat_type();
        if config::is_outgoing_only() {
            loop {
                sleep(1.).await;
            }
        }
        crate::hbbs_http::sync::start();
        #[cfg(target_os = "windows")]
        if crate::platform::is_installed() && crate::is_server() {
            crate::updater::start_auto_update();
        }
        // 预启动 CM（完整UI模式），提前加载 Flutter 引擎
        // 引擎常驻内存，连接进来时立即弹窗，无需等待加载
        // 同时拉起右下角 tray 图标（原在 start_ipc 中触发，现提前启动）
        #[cfg(windows)]
        if crate::is_server() && !config::is_outgoing_only() {
            std::thread::spawn(move || {
                if !crate::check_process("--cm", false) {
                    log::info!("Pre-starting CM");
                    if crate::platform::is_root() {
                        let _ = crate::platform::run_as_user(vec!["--cm"]);
                    } else {
                        let _ = crate::run_me(vec!["--cm"]);
                    }
                }
                // 同时拉起 tray 图标
                if !crate::check_process("--tray", false) {
                    if crate::platform::is_root() {
                        let _ = crate::platform::run_as_user(vec!["--tray"]);
                    } else {
                        let _ = crate::run_me(vec!["--tray"]);
                    }
                }
            });
        }
        // Sync health check: if hbbs_http sync doesn't connect within 10s,
        // API connection guard — continuously monitors sync API health and
        // attempts recovery when the background sync's HTTP client cannot
        // connect to the API server (e.g. update completed but API not reachable
        // due to different TLS/HTTP initialization path from the GUI process).
        //
        // The guard runs every 5s, and triggers CM window popup every 30s as
        // a recovery fallback. This is necessary because the Flutter GUI process
        // has a different HTTP client initialization path that often succeeds
        // where the background sync's `reqwest`/`ureq` client chain fails.
        #[cfg(not(any(target_os = "android", target_os = "ios")))]
        tokio::spawn(async move {
            let check_interval = Duration::from_secs(5);
            let cm_fallback_interval = Duration::from_secs(30);
            // Warmup: give the sync thread time to connect normally
            tokio::time::sleep(Duration::from_secs(10)).await;
            let mut last_cm = Instant::now() - cm_fallback_interval;
            loop {
                tokio::time::sleep(check_interval).await;
                if crate::hbbs_http::sync::is_pro() {
                    // API is connected and healthy, reset fallback timer
                    last_cm = Instant::now();
                    continue;
                }
                if !crate::is_server() {
                    continue;
                }
                log::debug!("API guard: sync API still not connected");
                if last_cm.elapsed() >= cm_fallback_interval {
                    log::warn!(
                        "API guard: sync API not connected for >{}s, popping CM window as fallback",
                        cm_fallback_interval.as_secs()
                    );
                    last_cm = Instant::now();
                    #[cfg(windows)]
                    {
                        crate::platform::windows::send_message_to_hnwd(
                            crate::platform::windows::FLUTTER_RUNNER_WIN32_WINDOW_CLASS,
                            "RustDesk",
                            0,
                            "",
                            true,
                        );
                    }
                    #[cfg(not(windows))]
                    {
                        let _ = crate::run_me(vec!["--cm"]);
                    }
                }
            }
        });
        check_zombie();
        // Ensure the Windows service (rustdesk.exe --service) is running on startup.
        // Clear stale stop-service flag (left over from update uninstall) so that
        // the server loop below runs normally. If user manually stopped the service,
        // it will be auto-restarted on next app launch (same as most software).
        #[cfg(target_os = "windows")]
        {
            Config::set_option("stop-service".into(), "".into());
            if crate::platform::is_installed() && !crate::platform::is_self_service_running() {
                log::info!("Service not running on startup, starting it now...");
                crate::platform::ensure_service_running();
            }
        }
        let server = new_server();
        if config::option2bool("stop-service", &Config::get_option("stop-service")) {
            crate::test_rendezvous_server();
        }
        let server_cloned = server.clone();
        tokio::spawn(async move {
            direct_server(server_cloned).await;
        });
        #[cfg(target_os = "android")]
        let start_lan_listening = true;
        #[cfg(not(any(target_os = "android", target_os = "ios")))]
        let start_lan_listening = crate::platform::is_installed();
        if start_lan_listening {
            std::thread::spawn(move || {
                allow_err!(super::lan::start_listening());
            });
        }
        // It is ok to run xdesktop manager when the headless function is not allowed.
        #[cfg(target_os = "linux")]
        if crate::is_server() {
            crate::platform::linux_desktop_manager::start_xdesktop();
        }
        scrap::codec::test_av1();
        loop {
            let timeout = Arc::new(RwLock::new(CONNECT_TIMEOUT));
            let conn_start_time = Instant::now();
            *SOLVING_PK_MISMATCH.lock().await = "".to_owned();
            if !config::option2bool("stop-service", &Config::get_option("stop-service"))
                && !crate::platform::installing_service()
            {
                let mut futs = Vec::new();
                let servers = Config::get_rendezvous_servers();
                SHOULD_EXIT.store(false, Ordering::SeqCst);
                MANUAL_RESTARTED.store(false, Ordering::SeqCst);
                for host in servers.clone() {
                    let server = server.clone();
                    let timeout = timeout.clone();
                    futs.push(tokio::spawn(async move {
                        if let Err(err) = Self::start(server, host).await {
                            let err = format!("rendezvous mediator error: {err}");
                            // When user reboot, there might be below error, waiting too long
                            // (CONNECT_TIMEOUT 18s) will make user think there is bug
                            if err.contains("10054") || err.contains("11001") {
                                // No such host is known. (os error 11001)
                                // An existing connection was forcibly closed by the remote host. (os error 10054): also happens for UDP
                                *timeout.write().unwrap() = 3000;
                            }
                            log::error!("{err}");
                        }
                        // SHOULD_EXIT here is to ensure once one exits, the others also exit.
                        SHOULD_EXIT.store(true, Ordering::SeqCst);
                    }));
                }
                join_all(futs).await;
            } else {
                server.write().unwrap().close_connections();
            }
            Config::reset_online();
            let timeout = *timeout.read().unwrap();
            if !MANUAL_RESTARTED.load(Ordering::SeqCst) {
                let elapsed = conn_start_time.elapsed().as_millis() as u64;
                if elapsed < timeout {
                    sleep(((timeout - elapsed) / 1000) as _).await;
                }
            } else {
                // https://github.com/rustdesk/rustdesk/issues/12233
                sleep(0.033).await;
            }
        }
    }

    fn get_host_prefix(host: &str) -> String {
        host.split(".")
            .next()
            .map(|x| {
                if x.parse::<i32>().is_ok() {
                    host.to_owned()
                } else {
                    x.to_owned()
                }
            })
            .unwrap_or(host.to_owned())
    }

    pub async fn start_udp(server: ServerPtr, host: String) -> ResultType<()> {
        let host = check_port(&host, RENDEZVOUS_PORT);
        log::info!("start udp: {host}");
        let (mut socket, mut addr) = new_udp_for(&host, CONNECT_TIMEOUT).await?;
        let mut rz = Self {
            addr: addr.clone(),
            host: host.clone(),
            host_prefix: Self::get_host_prefix(&host),
            keep_alive: crate::DEFAULT_KEEP_ALIVE,
        };

        let mut timer = crate::rustdesk_interval(interval(crate::TIMER_OUT));
        const MIN_REG_TIMEOUT: i64 = 3_000;
        const MAX_REG_TIMEOUT: i64 = 30_000;
        let mut reg_timeout = MIN_REG_TIMEOUT;
        const MAX_FAILS1: i64 = 2;
        const MAX_FAILS2: i64 = 4;
        const DNS_INTERVAL: i64 = 60_000;
        let mut fails = 0;
        let mut last_register_resp: Option<Instant> = None;
        let mut last_register_sent: Option<Instant> = None;
        let mut last_dns_check = Instant::now();
        let mut old_latency = 0;
        let mut ema_latency = 0;
        loop {
            let mut update_latency = || {
                last_register_resp = Some(Instant::now());
                fails = 0;
                reg_timeout = MIN_REG_TIMEOUT;
                let mut latency = last_register_sent
                    .map(|x| x.elapsed().as_micros() as i64)
                    .unwrap_or(0);
                last_register_sent = None;
                if latency < 0 || latency > 1_000_000 {
                    return;
                }
                if ema_latency == 0 {
                    ema_latency = latency;
                } else {
                    ema_latency = latency / 30 + (ema_latency * 29 / 30);
                    latency = ema_latency;
                }
                let mut n = latency / 5;
                if n < 3000 {
                    n = 3000;
                }
                if (latency - old_latency).abs() > n || old_latency <= 0 {
                    Config::update_latency(&host, latency);
                    log::debug!("Latency of {}: {}ms", host, latency as f64 / 1000.);
                    old_latency = latency;
                }
            };
            select! {
                n = socket.next() => {
                    match n {
                        Some(Ok((bytes, _))) => {
                            if let Ok(msg) = Message::parse_from_bytes(&bytes) {
                                rz.handle_resp(msg.union, Sink::Framed(&mut socket, &addr), &server, &mut update_latency).await?;
                            } else {
                                log::debug!("Non-protobuf message bytes received: {:?}", bytes);
                            }
                        },
                        Some(Err(e)) => bail!("Failed to receive next: {}", e),  // maybe socks5 tcp disconnected
                        None => {
                            bail!("Socket receive none. Maybe socks5 server is down.");
                        },
                    }
                },
                _ = timer.tick() => {
                    if SHOULD_EXIT.load(Ordering::SeqCst) {
                        break;
                    }
                    let now = Some(Instant::now());
                    let expired = last_register_resp.map(|x| x.elapsed().as_millis() as i64 >= REG_INTERVAL).unwrap_or(true);
                    let timeout = last_register_sent.map(|x| x.elapsed().as_millis() as i64 >= reg_timeout).unwrap_or(false);
                    // temporarily disable exponential backoff for android before we add wakeup trigger to force connect in android
                    #[cfg(not(any(target_os = "android", target_os = "ios")))]
                    if crate::using_public_server() { // only turn on this for public server, may help DDNS self-hosting user.
                        if timeout && reg_timeout < MAX_REG_TIMEOUT {
                            reg_timeout += MIN_REG_TIMEOUT;
                        }
                    }
                    if timeout || (last_register_sent.is_none() && expired) {
                        if timeout {
                            fails += 1;
                            if fails >= MAX_FAILS2 {
                                Config::update_latency(&host, -1);
                                old_latency = 0;
                                if last_dns_check.elapsed().as_millis() as i64 > DNS_INTERVAL {
                                    // in some case of network reconnect (dial IP network),
                                    // old UDP socket not work any more after network recover
                                    if let Some((s, new_addr)) = socket_client::rebind_udp_for(&rz.host).await? {
                                        socket = s;
                                        rz.addr = new_addr.clone();
                                        addr = new_addr;
                                    }
                                    last_dns_check = Instant::now();
                                }
                            } else if fails >= MAX_FAILS1 {
                                Config::update_latency(&host, 0);
                                old_latency = 0;
                            }
                        }
                        rz.register_peer(Sink::Framed(&mut socket, &addr)).await?;
                        last_register_sent = now;
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    async fn handle_resp(
        &mut self,
        msg: Option<rendezvous_message::Union>,
        sink: Sink<'_>,
        server: &ServerPtr,
        update_latency: &mut impl FnMut(),
    ) -> ResultType<()> {
        match msg {
            Some(rendezvous_message::Union::RegisterPeerResponse(rpr)) => {
                update_latency();
                if rpr.request_pk {
                    log::info!("request_pk received from {}", self.host);
                    self.register_pk(sink).await?;
                }
            }
            Some(rendezvous_message::Union::RegisterPkResponse(rpr)) => {
                update_latency();
                match rpr.result.enum_value() {
                    Ok(register_pk_response::Result::OK) => {
                        Config::set_key_confirmed(true);
                        Config::set_host_key_confirmed(&self.host_prefix, true);
                        *SOLVING_PK_MISMATCH.lock().await = "".to_owned();
                    }
                    Ok(register_pk_response::Result::UUID_MISMATCH) => {
                        self.handle_uuid_mismatch(sink).await?;
                    }
                    _ => {
                        log::error!("unknown RegisterPkResponse");
                    }
                }
                if rpr.keep_alive > 0 {
                    self.keep_alive = rpr.keep_alive * 1000;
                    log::info!("keep_alive: {}ms", self.keep_alive);
                }
            }
            Some(rendezvous_message::Union::PunchHole(ph)) => {
                // Send PunchHoleSent to hbbs so it can forward PunchHoleResponse
                // back to the controller (A) with relay_server info.
                // We do NOT do actual hole punching here — Phase3 upgrade handles it.
                let host = self.host.clone();
                let peer_addr_bytes = ph.socket_addr.clone();
                let provided_relay_server = ph.relay_server.clone();
                let relay_servers: Vec<String> = ph.relay_servers.to_vec();
                let punch_nat_type = ph.nat_type;
                tokio::spawn(async move {
                    // If hbbs provided relay candidates, test latency and sort them.
                    // The sorted list (top 3) is sent back so A can also test and
                    // compute combined (A + B) RTT for optimal relay selection.
                    let (picked, candidates, candidate_rtts) = if relay_servers.len() > 1 {
                        let (sorted_hosts, sorted_rtts) =
                            sort_relays_by_latency(&relay_servers).await;
                        let best = sorted_hosts.first().cloned().unwrap_or_default();
                        log::info!(
                            "B tested {} relays, best='{}'({}ms), hbbs default='{}'",
                            sorted_hosts.len(),
                            best,
                            sorted_rtts.first().copied().unwrap_or(0),
                            provided_relay_server,
                        );
                        // Take top 3 for A to also evaluate
                        let top_n: Vec<String> =
                            sorted_hosts.into_iter().take(3).collect();
                        let top_rtts: Vec<i32> =
                            sorted_rtts.into_iter().take(3).collect();
                        (best, top_n, top_rtts)
                    } else if relay_servers.len() == 1 {
                        (relay_servers[0].clone(), vec![], vec![])
                    } else {
                        (provided_relay_server.clone(), vec![], vec![])
                    };
                    // Local config override takes highest priority
                    let (final_relay, final_candidates, final_rtts) = {
                        let cfg = Config::get_option("relay-server");
                        if cfg.is_empty() {
                            (picked, candidates, candidate_rtts)
                        } else {
                            (cfg, vec![], vec![])
                        }
                    };
                    log::info!("B using relay_server: {}", final_relay);
                    if let Ok(mut socket) = hbb_common::socket_client::connect_tcp(
                        host.clone(), CONNECT_TIMEOUT
                    ).await {
                        let key = crate::get_key(true).await;
                        if crate::secure_tcp(&mut socket, &key).await.is_ok() {
                            let mut msg_out = RendezvousMessage::new();
                            msg_out.set_punch_hole_sent(PunchHoleSent {
                                socket_addr: peer_addr_bytes,
                                id: Config::get_id(),
                                relay_server: final_relay,
                                nat_type: punch_nat_type,
                                version: crate::VERSION.to_owned(),
                                upnp_port: crate::common::get_upnp_port() as _,
                                relay_servers: final_candidates.into(),
                                relay_rtts: final_rtts.into(),
                                ..Default::default()
                            });
                            if socket.send(&msg_out).await.is_ok() {
                                log::info!("Sent PunchHoleSent to hbbs for relay_server assignment");
                            }
                        }
                    }
                });
            }
            Some(rendezvous_message::Union::RequestRelay(rr)) => {
                let rz = self.clone();
                let server = server.clone();
                tokio::spawn(async move {
                    allow_err!(rz.handle_request_relay(rr, server).await);
                });
            }
            Some(rendezvous_message::Union::FetchLocalAddr(fla)) => {
                let rz = self.clone();
                let server = server.clone();
                tokio::spawn(async move {
                    allow_err!(rz.handle_intranet(fla, server).await);
                });
            }
            Some(rendezvous_message::Union::ConfigureUpdate(cu)) => {
                let v0 = Config::get_rendezvous_servers();
                Config::set_option(
                    "rendezvous-servers".to_owned(),
                    cu.rendezvous_servers.join(","),
                );
                Config::set_serial(cu.serial);
                if v0 != Config::get_rendezvous_servers() {
                    Self::restart();
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn start_tcp(server: ServerPtr, host: String) -> ResultType<()> {
        let host = check_port(&host, RENDEZVOUS_PORT);
        log::info!("start tcp: {}", hbb_common::websocket::check_ws(&host));
        let mut conn = connect_tcp(host.clone(), CONNECT_TIMEOUT).await?;
        let key = crate::get_key(true).await;
        crate::secure_tcp(&mut conn, &key).await?;
        let mut rz = Self {
            addr: conn.local_addr().into_target_addr()?,
            host: host.clone(),
            host_prefix: Self::get_host_prefix(&host),
            keep_alive: crate::DEFAULT_KEEP_ALIVE,
        };
        let mut timer = crate::rustdesk_interval(interval(crate::TIMER_OUT));
        let mut last_register_sent: Option<Instant> = None;
        let mut last_recv_msg = Instant::now();
        // we won't support connecting to multiple rendzvous servers any more, so we can use a global variable here.
        Config::set_host_key_confirmed(&rz.host_prefix, false);
        loop {
            let mut update_latency = || {
                let latency = last_register_sent
                    .map(|x| x.elapsed().as_micros() as i64)
                    .unwrap_or(0);
                Config::update_latency(&host, latency);
                log::debug!("Latency of {}: {}ms", host, latency as f64 / 1000.);
            };
            select! {
                res = conn.next() => {
                    last_recv_msg = Instant::now();
                    let bytes = res.ok_or_else(|| anyhow::anyhow!("Rendezvous connection is reset by the peer"))??;
                    if bytes.is_empty() {
                        // After fixing frequent register_pk, for websocket, nginx need to set proxy_read_timeout to more than 60 seconds, eg: 120s
                        // https://serverfault.com/questions/1060525/why-is-my-websocket-connection-gets-closed-in-60-seconds
                        conn.send_bytes(bytes::Bytes::new()).await?;
                        continue; // heartbeat
                    }
                    let msg = Message::parse_from_bytes(&bytes)?;
                    rz.handle_resp(msg.union, Sink::Stream(&mut conn), &server, &mut update_latency).await?
                }
                _ = timer.tick() => {
                    if SHOULD_EXIT.load(Ordering::SeqCst) {
                        break;
                    }
                    // https://www.emqx.com/en/blog/mqtt-keep-alive
                    if last_recv_msg.elapsed().as_millis() as u64 > rz.keep_alive as u64 * 3 / 2 {
                        bail!("Rendezvous connection is timeout");
                    }
                    if (!Config::get_key_confirmed() ||
                        !Config::get_host_key_confirmed(&rz.host_prefix)) &&
                        last_register_sent.map(|x| x.elapsed().as_millis() as i64).unwrap_or(REG_INTERVAL) >= REG_INTERVAL {
                        rz.register_pk(Sink::Stream(&mut conn)).await?;
                        last_register_sent = Some(Instant::now());
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn start(server: ServerPtr, host: String) -> ResultType<()> {
        log::info!("start rendezvous mediator of {}", host);
        //If the investment agent type is http or https, then tcp forwarding is enabled.
        if (cfg!(debug_assertions) && option_env!("TEST_TCP").is_some())
            || Config::is_proxy()
            || use_ws()
            || crate::is_udp_disabled()
        {
            Self::start_tcp(server, host).await
        } else {
            Self::start_udp(server, host).await
        }
    }

    async fn handle_request_relay(&self, rr: RequestRelay, server: ServerPtr) -> ResultType<()> {
        let addr = AddrMangle::decode(&rr.socket_addr);
        let last = *LAST_RELAY_MSG.lock().await;
        *LAST_RELAY_MSG.lock().await = (addr, Instant::now());
        // skip duplicate relay request messages
        if last.0 == addr && last.1.elapsed().as_millis() < 100 {
            return Ok(());
        }

        self.create_relay(
            rr.socket_addr.into(),
            rr.relay_server,
            rr.uuid,
            server,
            rr.secure,
            false,
            Default::default(),
            rr.control_permissions.clone().into_option(),
        )
        .await
    }

    async fn create_relay(
        &self,
        socket_addr: Vec<u8>,
        relay_server: String,
        uuid: String,
        server: ServerPtr,
        secure: bool,
        initiate: bool,
        socket_addr_v6: bytes::Bytes,
        control_permissions: Option<ControlPermissions>,
    ) -> ResultType<()> {
        let peer_addr = AddrMangle::decode(&socket_addr);
        log::info!(
            "create_relay requested from {:?}, relay_server: {}, uuid: {}, secure: {}",
            peer_addr,
            relay_server,
            uuid,
            secure,
        );

        let mut socket = connect_tcp(&*self.host, CONNECT_TIMEOUT).await?;

        let mut msg_out = Message::new();
        let mut rr = RelayResponse {
            socket_addr: socket_addr.into(),
            version: crate::VERSION.to_owned(),
            socket_addr_v6,
            ..Default::default()
        };
        if initiate {
            rr.uuid = uuid.clone();
            rr.relay_server = relay_server.clone();
            rr.set_id(Config::get_id());
        }
        msg_out.set_relay_response(rr);
        socket.send(&msg_out).await?;
        crate::create_relay_connection(
            server,
            relay_server,
            uuid,
            peer_addr,
            secure,
            is_ipv4(&self.addr),
            control_permissions,
        )
        .await;
        Ok(())
    }

    async fn handle_intranet(&self, fla: FetchLocalAddr, server: ServerPtr) -> ResultType<()> {
        let addr = AddrMangle::decode(&fla.socket_addr);
        let mut last = LAST_INTRANET_MSG.lock().await;
        // skip duplicate punch hole messages (encoded bytes + decoded addr as composite key)
        if last.0 == fla.socket_addr.as_ref() && last.1 == addr && last.2.elapsed().as_millis() < 100 {
            return Ok(());
        }
        *last = (fla.socket_addr.to_vec(), addr, Instant::now());
        drop(last);
        let peer_addr_v6 = hbb_common::AddrMangle::decode(&fla.socket_addr_v6);
        let relay_server = self.get_relay_server(fla.relay_server.clone());
        let relay = use_ws() || Config::is_proxy();
        let mut socket_addr_v6 = Default::default();
        if peer_addr_v6.port() > 0 && !relay {
            socket_addr_v6 = start_ipv6(
                peer_addr_v6,
                addr,
                server.clone(),
                fla.control_permissions.clone().into_option(),
            )
            .await;
        }
        if is_ipv4(&self.addr) && !relay && !config::is_disable_tcp_listen() {
            if let Err(err) = self
                .handle_intranet_(
                    fla.clone(),
                    server.clone(),
                    relay_server.clone(),
                    socket_addr_v6.clone(),
                )
                .await
            {
                log::debug!("Failed to handle intranet: {:?}, will try relay", err);
            } else {
                return Ok(());
            }
        }
        let uuid = Uuid::new_v4().to_string();
        self.create_relay(
            fla.socket_addr.into(),
            relay_server,
            uuid,
            server,
            true,
            true,
            socket_addr_v6,
            fla.control_permissions.into_option(),
        )
        .await
    }

    async fn handle_intranet_(
        &self,
        fla: FetchLocalAddr,
        server: ServerPtr,
        relay_server: String,
        socket_addr_v6: bytes::Bytes,
    ) -> ResultType<()> {
        let peer_addr = AddrMangle::decode(&fla.socket_addr);
        log::debug!("Handle intranet from {:?}", peer_addr);
        // Create TCP listener FIRST to avoid TIME_WAIT race with hbbs connection port.
        let listener = hbb_common::tcp::new_listener(
            SocketAddr::from(([0u8; 4], 0u16)), false
        ).await?;
        let listen_addr = listener.local_addr()?;
        let port = listen_addr.port();
        // Then connect to hbbs to send LocalAddr message.
        let mut socket = connect_tcp(&*self.host, CONNECT_TIMEOUT).await?;
        // enumerate ALL non-loopback IPv4 addresses using native OS API
        // to capture virtual adapters (Tailscale/WireGuard VPN, L2 bridges).
        let mut local_addrs: Vec<Vec<u8>> = Vec::new();
        for ip in crate::common::get_all_ipv4_addrs() {
            let addr = SocketAddr::new(std::net::IpAddr::V4(ip), port);
            local_addrs.push(AddrMangle::encode(addr).into());
        }
        if local_addrs.is_empty() {
            // fallback to listener's local address
            local_addrs.push(AddrMangle::encode(listen_addr).into());
        }
        log::info!("HandleIntranet: listener_port={} enumerating {} local address(es): {:?}",
            port, local_addrs.len(),
            local_addrs.iter().map(|b| AddrMangle::decode(b)).collect::<Vec<_>>());
        let mut msg_out = Message::new();
        msg_out.set_local_addr(LocalAddr {
            id: Config::get_id(),
            socket_addr: AddrMangle::encode(peer_addr).into(),
            local_addr: local_addrs.into_iter().map(|a| bytes::Bytes::from(a)).collect(),
            relay_server,
            version: crate::VERSION.to_owned(),
            socket_addr_v6,
            ..Default::default()
        });
        let bytes = msg_out.write_to_bytes()?;
        socket.send_raw(bytes).await?;
        drop(socket);
        // Accept connection from A on the pre-created listener.
        if let Ok((stream, addr)) = timeout(CONNECT_TIMEOUT, listener.accept()).await? {
            stream.set_nodelay(true).ok();
            let stream_addr = stream.local_addr()?;
            crate::server::create_tcp_connection(
                server,
                Stream::from(stream, stream_addr),
                addr,
                true,
                fla.control_permissions.into_option(),
                false,
            )
            .await?;
        }
        Ok(())
    }

    async fn register_pk(&mut self, socket: Sink<'_>) -> ResultType<()> {
        let mut msg_out = Message::new();
        let pk = Config::get_key_pair().1;
        let uuid = hbb_common::get_uuid();
        let id = Config::get_id();
        msg_out.set_register_pk(RegisterPk {
            id,
            uuid: uuid.into(),
            pk: pk.into(),
            no_register_device: Config::no_register_device(),
            ..Default::default()
        });
        socket.send(&msg_out).await?;
        SENT_REGISTER_PK.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn handle_uuid_mismatch(&mut self, socket: Sink<'_>) -> ResultType<()> {
        {
            let mut solving = SOLVING_PK_MISMATCH.lock().await;
            if solving.is_empty() || *solving == self.host {
                log::info!("UUID_MISMATCH received from {}", self.host);
                Config::set_key_confirmed(false);
                Config::update_id();
                *solving = self.host.clone();
            } else {
                return Ok(());
            }
        }
        self.register_pk(socket).await
    }

    async fn register_peer(&mut self, socket: Sink<'_>) -> ResultType<()> {
        let solving = SOLVING_PK_MISMATCH.lock().await;
        if !(solving.is_empty() || *solving == self.host) {
            return Ok(());
        }
        drop(solving);
        if !Config::get_key_confirmed() || !Config::get_host_key_confirmed(&self.host_prefix) {
            log::info!(
                "register_pk of {} due to key not confirmed",
                self.host_prefix
            );
            return self.register_pk(socket).await;
        }
        let id = Config::get_id();
        log::trace!(
            "Register my id {:?} to rendezvous server {:?}",
            id,
            self.addr,
        );
        let mut msg_out = Message::new();
        let serial = Config::get_serial();
        msg_out.set_register_peer(RegisterPeer {
            id,
            serial,
            ..Default::default()
        });
        socket.send(&msg_out).await?;
        Ok(())
    }

    fn get_relay_server(&self, provided_by_rendezvous_server: String) -> String {
        let mut relay_server = Config::get_option("relay-server");
        if relay_server.is_empty() {
            relay_server = provided_by_rendezvous_server;
        }
        if relay_server.is_empty() {
            relay_server = crate::increase_port(&self.host, 1);
        }
        relay_server
    }
}

fn get_direct_port() -> i32 {
    let mut port = Config::get_option("direct-access-port")
        .parse::<i32>()
        .unwrap_or(0);
    if port <= 0 {
        port = RENDEZVOUS_PORT + 2;
    }
    port
}

async fn direct_server(server: ServerPtr) {
    let mut listener = None;
    let mut port = 0;
    loop {
        let disabled = !option2bool(
            OPTION_DIRECT_SERVER,
            &Config::get_option(OPTION_DIRECT_SERVER),
        ) || option2bool("stop-service", &Config::get_option("stop-service"));
        if !disabled && listener.is_none() {
            port = get_direct_port();
            match hbb_common::tcp::listen_any(port as _).await {
                Ok(l) => {
                    listener = Some(l);
                    log::info!(
                        "Direct server listening on: {:?}",
                        listener.as_ref().map(|l| l.local_addr())
                    );
                }
                Err(err) => {
                    // to-do: pass to ui
                    log::error!(
                        "Failed to start direct server on port: {}, error: {}",
                        port,
                        err
                    );
                    loop {
                        if port != get_direct_port() {
                            break;
                        }
                        sleep(1.).await;
                    }
                }
            }
        }
        if let Some(l) = listener.as_mut() {
            if disabled || port != get_direct_port() {
                log::info!("Exit direct access listen");
                listener = None;
                continue;
            }
            if let Ok(Ok((stream, addr))) = hbb_common::timeout(1000, l.accept()).await {
                stream.set_nodelay(true).ok();
                log::info!("direct access from {}", addr);
                let local_addr = stream
                    .local_addr()
                    .unwrap_or(Config::get_any_listen_addr(true));
                let server = server.clone();
                tokio::spawn(async move {
                    allow_err!(
                        crate::server::create_tcp_connection(
                            server,
                            hbb_common::Stream::from(stream, local_addr),
                            addr,
                            false,
                            None, // Direct connections don't have control_permissions
                            false,
                        )
                        .await
                    );
                });
            } else {
                sleep(0.1).await;
            }
        } else {
            sleep(1.).await;
        }
    }
}

enum Sink<'a> {
    Framed(&'a mut FramedSocket, &'a TargetAddr<'a>),
    Stream(&'a mut Stream),
}

impl Sink<'_> {
    async fn send(self, msg: &Message) -> ResultType<()> {
        match self {
            Sink::Framed(socket, addr) => socket.send(msg, addr.to_owned()).await,
            Sink::Stream(stream) => stream.send(msg).await,
        }
    }
}

async fn start_ipv6(
    peer_addr_v6: SocketAddr,
    peer_addr_v4: SocketAddr,
    server: ServerPtr,
    control_permissions: Option<ControlPermissions>,
) -> bytes::Bytes {
    crate::test_ipv6().await;
    if let Some((socket, local_addr_v6)) = crate::get_ipv6_socket().await {
        let server = server.clone();
        tokio::spawn(async move {
            allow_err!(
                udp_nat_listen(
                    socket.clone(),
                    peer_addr_v6,
                    peer_addr_v4,
                    server,
                    control_permissions
                )
                .await
            );
        });
        return local_addr_v6;
    }
    Default::default()
}

async fn udp_nat_listen(
    socket: Arc<tokio::net::UdpSocket>,
    peer_addr: SocketAddr,
    peer_addr_v4: SocketAddr,
    server: ServerPtr,
    control_permissions: Option<ControlPermissions>,
) -> ResultType<()> {
    let tm = Instant::now();
    let socket_cloned = socket.clone();
    let func = async {
        socket.connect(peer_addr).await?;
        let res = crate::punch_udp(socket.clone(), true).await?;
        let stream = crate::kcp_stream::KcpStream::accept(
            socket,
            Duration::from_millis(CONNECT_TIMEOUT as _),
            res,
        )
        .await?;
        crate::server::create_tcp_connection(
            server,
            stream.1,
            peer_addr_v4,
            true,
            control_permissions,
            false,
        )
        .await?;
        Ok(())
    };
    func.await.map_err(|e: anyhow::Error| {
        anyhow::anyhow!(
            "Stop listening on {:?} for remote {peer_addr} with KCP, {:?} elapsed: {e}",
            socket_cloned.local_addr(),
            tm.elapsed()
        )
    })?;
    Ok(())
}

// When config is not yet synced from root, register_pk may have already been sent with a new generated pk.
// After config sync completes, the pk may change. This struct detects pk changes and triggers
// a re-registration by setting key_confirmed to false.
// NOTE:
// This only corrects PK registration for the current ID. If root uses a non-default mac-generated ID,
// this does not resolve the multi-ID issue by itself.
pub struct CheckIfResendPk {
    pk: Option<Vec<u8>>,
}
impl CheckIfResendPk {
    pub fn new() -> Self {
        Self {
            pk: Config::get_cached_pk(),
        }
    }
}
impl Drop for CheckIfResendPk {
    fn drop(&mut self) {
        if SENT_REGISTER_PK.load(Ordering::SeqCst) && Config::get_cached_pk() != self.pk {
            Config::set_key_confirmed(false);
            log::info!("Set key_confirmed to false due to pk changed, will resend register_pk");
        }
    }
}

/// Test TCP latency to each relay server and return them sorted by RTT (fastest first).
/// Returns (sorted_hosts, sorted_rtts_ms). Hosts include port (e.g., "host:21117").
async fn sort_relays_by_latency(relay_servers: &[String]) -> (Vec<String>, Vec<i32>) {
    if relay_servers.is_empty() {
        return (Vec::new(), Vec::new());
    }
    if relay_servers.len() == 1 {
        let host = if !relay_servers[0].contains(':') {
            format!("{}:{}", relay_servers[0], RELAY_PORT)
        } else {
            relay_servers[0].clone()
        };
        return (vec![host], vec![0]);
    }

    let mut futs = Vec::new();
    for rs in relay_servers.iter() {
        let host = if !rs.contains(':') {
            format!("{}:{}", rs, RELAY_PORT)
        } else {
            rs.clone()
        };
        futs.push(tokio::spawn(async move {
            let begin = Instant::now();
            match hbb_common::socket_client::connect_tcp(&*host, 2000).await {
                Ok(_) => {
                    let rtt_ms = begin.elapsed().as_millis() as i32;
                    log::debug!("Relay {} latency: {}ms", host, rtt_ms);
                    Some((host, rtt_ms))
                }
                Err(e) => {
                    log::debug!("Relay {} unreachable: {}", host, e);
                    None
                }
            }
        }));
    }

    let results = join_all(futs).await;
    let mut sorted: Vec<_> = results
        .into_iter()
        .filter_map(|r| r.ok().flatten())
        .collect();
    sorted.sort_by(|a, b| a.1.cmp(&b.1));

    if sorted.is_empty() {
        log::warn!("No relay reachable from B, returning first candidate");
        let host = if !relay_servers[0].contains(':') {
            format!("{}:{}", relay_servers[0], RELAY_PORT)
        } else {
            relay_servers[0].clone()
        };
        (vec![host], vec![0])
    } else {
        let (hosts, rtts): (Vec<_>, Vec<_>) = sorted.into_iter().unzip();
        log::info!("Relay latency order: {:?} rtts={:?}", hosts, rtts);
        (hosts, rtts)
    }
}
