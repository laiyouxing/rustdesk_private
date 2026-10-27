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
    protobuf::{Enum, Message as _},
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
                // 回显**本机（host）自己的** NAT 类型：hbbs 会把它写进给控制端的
                // PunchHoleResponse.nat_type，控制端据此做 NAT 错峰判定（宽容端先打）。
                // 原先回显 ph.nat_type（控制端发来的值），控制端收到的其实是自己的
                // 类型，错峰条件恒不成立（offset 恒 0），功能失效。
                let punch_nat_type = NatType::from_i32(Config::get_nat_type())
                    .unwrap_or(NatType::UNKNOWN_NAT);
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
    // ===== 中继前打洞（被控端）=====
    // 发现控制端请求后立即主动打洞（主机侧 prefer_connect=false，即 accept
    // 控制端发起的流），成功则直接交给 ConnInner 接管——连接从一开始就是
    // 直连会话。
    //
    // 上报策略（事件触发，不做定时轮询，省带宽）：
    //   1) 启动后上报一次——保证"打开软件即就绪"；
    //   2) 本地 IP 集合变化（切换 WiFi/有线/VPN）时上报——循环内纯本地检测，
    //      零带宽，同时作废映射缓存强制重探测；
    //   3) 每次 api-server 心跳成功后附带上报（sync.rs 挂点，节流见
    //      PUNCH_REPORT_THROTTLE），掉线重连后网络恢复时心跳成功即自动覆盖；
    //   4) 控制端连接时必带 target 上报（punch_direct_via_api，不节流）。
    // inbox（"谁想连我"）每 3s 拉一次：这是被控端发现连接请求的唯一途径，
    // 3s 内可发现；控制端等对端地址上限 10s，足以覆盖。
    let mut reported_startup = false;
    let mut last_inbox = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(60))
        .unwrap_or_else(std::time::Instant::now);
    // 映射看门狗：定时轻量探测映射、仅变化时上报（覆盖路由器 NAT 老化等
    // 本地 IP 不变的静默变化）。整个进程一个实例，随 mediator 生命周期。
    tokio::spawn(crate::common::punch_mapped_watchdog());
    // 本地 IP 集合（变化检测基线；纯本地枚举，无网络开销）
    let mut last_local_ips = local_ip_set();
    // 已发起过打洞的请求 -> (请求指纹, 发起时间)。
    // 去重必须按"请求"而不是"请求方"：inbox 的 TTL 只有 60s，控制端断开重连
    // 时会生成新请求，若按 peer_id 屏蔽 120s，重连在 60~120s 内会被静默忽略，
    // 控制端白等到超时才回退中继。指纹 = 请求刷新时间(updated_at) + 地址集合
    // + 最大 sync_at，详见 inbox_request_key；同一请求被轮询到多次才去重。
    let mut handled: std::collections::HashMap<String, (String, std::time::Instant)> =
        std::collections::HashMap::new();
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

        // ---- 中继前打洞：事件触发上报 + inbox（不依赖 direct-server 监听是否可用）----
        if crate::hbbs_http::punch_exchange::available() {
            // 1) 启动后上报一次（打开即就绪）
            if !reported_startup {
                reported_startup = true;
                tokio::spawn(crate::common::punch_report_own_addrs());
            }
            // 2) 本地 IP 集合变化（网络切换）→ 立即上报（绕过节流）
            let cur_ips = local_ip_set();
            if cur_ips != last_local_ips {
                log::info!(
                    "pre-relay punch(host): local IP set changed ({:?} -> {:?}), refreshing punch address",
                    last_local_ips,
                    cur_ips
                );
                last_local_ips = cur_ips;
                crate::common::invalidate_punch_mapped_cache();
                // 走 report_own_addrs_inner 绕开 20s 节流：缓存刚被作废，
                // 若刚好在节流窗口内，punch_report_own_addrs 会直接 return，
                // 新地址要再等最多 20s 才上报，期间对端拿旧映射打不进来。
                tokio::spawn(crate::common::report_own_addrs_inner());
            }
            // 3) inbox：每 3s 检查一次连接请求
            if last_inbox.elapsed() >= std::time::Duration::from_secs(3) {
                last_inbox = std::time::Instant::now();
                let entries =
                    crate::hbbs_http::punch_exchange::get_inbox(&Config::get_id()).await;
                for e in entries {
                    // 请求指纹：地址集合 + 最大 sync_at。同一请求的重复轮询去重，
                    // 新请求（重连/换网络）指纹不同，立即重新打洞。
                    let req_key = inbox_request_key(&e);
                    if let Some((last_key, t)) = handled.get(&e.peer_id) {
                        if *last_key == req_key
                            && t.elapsed() < std::time::Duration::from_secs(120)
                        {
                            continue;
                        }
                    }
                    handled.insert(e.peer_id.clone(), (req_key, std::time::Instant::now()));
                    log::info!(
                        "pre-relay punch(host): connection request from {} via api-server, punching {} addr(s)",
                        e.peer_id,
                        e.addrs.len()
                    );
                    let server = server.clone();
                    tokio::spawn(punch_inbound_once(server, e.peer_id, e.addrs));
                }
                handled.retain(|_, (_, t)| t.elapsed() < std::time::Duration::from_secs(300));
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

/// 本机 IPv4 地址集合（排序去重，用于网络切换检测）。纯本地枚举，无网络开销。
fn local_ip_set() -> Vec<String> {
    let mut v: Vec<String> = crate::common::get_all_ipv4_addrs()
        .iter()
        .map(|ip| ip.to_string())
        .collect();
    v.sort();
    v.dedup();
    v
}

/// inbox 请求指纹：同一控制端重新连接会生成新请求，靠它区分"重复轮询同
/// 一个请求"和"这是一次新的连接请求"。
/// 组成：api-server 的请求刷新时间（updated_at，秒级，最可靠）+ 地址集合 +
/// 最大 sync_at。updated_at 在旧版 api-server 上为 0，此时退化为后两项——
/// 注意 sync_at 对齐到 8s 边界，同一边界内的两次连接指纹会相同（见下）。
fn inbox_request_key(e: &crate::hbbs_http::punch_exchange::InboxEntry) -> String {
    let mut addrs: Vec<&str> = e.addrs.iter().map(|a| a.addr.as_str()).collect();
    addrs.sort();
    let sync_at = e.addrs.iter().map(|a| a.sync_at_ms).max().unwrap_or(0);
    format!("{}|{}|{}", e.updated_at, addrs.join(","), sync_at)
}

/// 被控端：对一台控制端的公网地址主动打洞（中继前打洞的一次性任务）。
/// 成功则把直连流交给 `create_tcp_connection`（ConnInner 接管，等 Login）。
async fn punch_inbound_once(
    server: ServerPtr,
    peer_id: String,
    addrs: Vec<crate::hbbs_http::punch_exchange::PunchAddr>,
) {
    // 预算从函数入口算：下面的 STUN 探测（ensure_punch_mapped）可能花几秒，
    // 也要计入，否则打洞窗口比预期短、或整体耗时超出预算。
    let budget = Duration::from_secs(45);
    let started = std::time::Instant::now();
    let total_deadline = started + budget;

    // 按对端建议优先级排序，并过滤不可用地址
    let peer_addrs = crate::hbbs_http::punch_exchange::sort_addrs(addrs);
    let mut targets: Vec<std::net::SocketAddr> = Vec::new();
    for a in &peer_addrs {
        if !a.is_udp() {
            continue;
        }
        let Ok(sa) = a.addr.parse::<std::net::SocketAddr>() else {
            continue;
        };
        if crate::common::is_unusable_punch_addr(&sa) {
            continue;
        }
        if !targets.contains(&sa) {
            targets.push(sa);
        }
        // 对端（疑似）对称 NAT：用 delta 预测其映射邻居端口
        if a.delta != 0 {
            let p = sa.port() as i32 + a.delta;
            if (1..=65535).contains(&p) {
                let mut t = sa;
                t.set_port(p as u16);
                if !targets.contains(&t) {
                    targets.push(t);
                }
            }
        }
    }
    if targets.is_empty() {
        return;
    }
    log::info!(
        "pre-relay punch(host): punching {} for {} target(s)",
        peer_id,
        targets.len()
    );

    // 打洞 socket：用进程级共享 socket（启动时已创建 + STUN 缓存），
    // 保证 NAT 映射与上报的一致，也省去现场探测。
    let socket = match crate::common::ensure_shared_punch_socket().await {
        Ok(s) => s,
        Err(e) => {
            log::info!("pre-relay punch(host): bind failed: {:?}", e);
            return;
        }
    };
    // 记录本机主映射（成功后存为 last_success）；缓存热则零探测。
    // 注意顺序：先探测、后申领共享 socket 使用权——否则 ensure_punch_mapped
    // 会因"socket 忙"而跳过探测（只拿到可能已过期的旧缓存）。
    let my_primary = crate::common::ensure_punch_mapped()
        .await
        .map(|(a, _)| a);
    // 共享 socket 使用凭证：持有到函数返回（含整个打洞过程），期间看门狗/
    // 其它 STUN 探测会跳过，不会 recv_from 抢走我们的报文。
    let _shared_use = crate::common::shared_socket_acquire();

    // 同步：与控制端约定起始时刻取较大值（取对端上报的 sync_at）。
    // 约束：sync 等待不得吃掉打洞窗口——最晚等到"预算死线前 10s"，
    // 否则被控端可能等到控制端早已放弃（控制端总预算 30s）。
    const HOST_PUNCH_MIN_WINDOW: Duration = Duration::from_secs(10);
    let peer_sync_at = peer_addrs.iter().map(|a| a.sync_at_ms).max().unwrap_or(0);
    let ntp_offset = crate::common::ntp_offset_ms().await;
    let own_sync = crate::common::punch_sync_boundary(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64 + ntp_offset.unwrap_or(0))
            .unwrap_or(0),
    );
    let target_sync = own_sync.max(peer_sync_at);
    if target_sync > 0 {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let wait = target_sync - now_ms;
        if wait > 0 && wait <= 30_000 {
            let now = std::time::Instant::now();
            let wanted = now + Duration::from_millis(wait as u64);
            // 最晚等到"死线前保留最小打洞窗口"的时刻
            // （Instant 没有 saturating_sub，用 checked_sub + 兜底）
            let latest = total_deadline
                .checked_sub(HOST_PUNCH_MIN_WINDOW)
                .unwrap_or(total_deadline);
            let until = wanted.min(latest);
            let actual_ms = until.saturating_duration_since(now).as_millis() as u64;
            if actual_ms > 0 {
                log::info!(
                    "pre-relay punch(host): waiting {}ms until sync_at {} (clipped to keep {}s punch window)",
                    actual_ms,
                    target_sync,
                    HOST_PUNCH_MIN_WINDOW.as_secs()
                );
                hbb_common::tokio::time::sleep(Duration::from_millis(actual_ms)).await;
            }
        }
    }

    // 多地址轮转打洞（主机侧 prefer_connect=false，accept 控制端发起的流）。
    // 每个目标最多 5s，预算 45s 内按优先级轮换。
    for target in targets {
        if std::time::Instant::now() >= total_deadline {
            break;
        }
        let per_target = Duration::from_millis(5_000);
        let target_deadline = (std::time::Instant::now() + per_target).min(total_deadline);
        while std::time::Instant::now() < target_deadline {
            let sock = socket.clone();
            // 未 connect 模式：共享 socket 要同时服务多个控制端（一对多远程），
            // 不能 connect 锁定单一目标；用 send_to 探测可达性
            if sock.send_to(&[], target).await.is_err() {
                break;
            }
            let remain_ms = total_deadline
                .saturating_duration_since(std::time::Instant::now())
                .as_millis() as u64;
            let round_ms = crate::common::PUNCH_ROUND_MS.min(remain_ms.max(500));
            if let Some((kcp, stream)) =
                crate::common::punch_target_sustained(&sock, target, round_ms, false, Some(target)).await
            {
                log::info!(
                    "pre-relay punch(host): KCP punch to {} succeeded after {:?}, handing over to connection",
                    target,
                    started.elapsed()
                );
                if let Some(mine) = my_primary {
                    crate::common::save_punch_last_success(&mine);
                }
                let server = server.clone();
                // KcpStream 必须活到会话结束：它的 Drop 会关掉 stop_sender，
                // kcp_io 泵随之退出，直连会话立刻无法收发包（原来这里绑成
                // `_kcp`，函数返回即 drop，打洞成功也用不了）。
                // create_tcp_connection 内部 await Connection::start，直到
                // 会话结束才返回，所以把 kcp 一起 move 进同一个任务即可。
                tokio::spawn(async move {
                    allow_err!(
                        crate::server::create_tcp_connection(
                            server,
                            stream,
                            target,
                            true,
                            None,
                            false,
                        )
                        .await
                    );
                    drop(kcp);
                });
                return;
            }
        }
    }
    log::info!(
        "pre-relay punch(host): punch to {} failed after {:?} (will rely on normal relay/accept path)",
        peer_id,
        started.elapsed()
    );
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
        // KcpStream 必须活到会话结束：它的 Drop 会关掉 stop_sender，kcp_io
        // 泵随之退出，这条 LAN 直连会立刻不通（表现为连上就卡住）。
        // create_tcp_connection 内部 await 到会话结束，所以一起留着即可。
        let kcp_keepalive = stream.0;
        crate::server::create_tcp_connection(
            server,
            stream.1,
            peer_addr_v4,
            true,
            control_permissions,
            false,
        )
        .await?;
        drop(kcp_keepalive);
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
