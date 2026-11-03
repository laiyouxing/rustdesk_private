use std::sync::{Arc, Mutex, RwLock};

use crate::client::*;
use hbb_common::{
    allow_err, bail,
    config::READ_TIMEOUT,
    futures::{FutureExt, SinkExt, StreamExt},
    log,
    message_proto::*,
    protobuf::Message as _,
    rendezvous_proto::ConnType,
    tcp, timeout,
    tokio::{self, net::TcpStream, sync::mpsc},
    tokio_util::codec::{BytesCodec, Framed},
    ResultType, Stream,
};

/// 隧道当前状态，供界面展示（用户需要知道现在到底走直连还是中继）。
/// 用 Mutex 静态量而不是走配置，避免频繁写配置文件。
struct TunnelStatus {
    /// 当前活跃的转发连接数（每来一个本地 TCP 连接算一条）。
    active: usize,
    /// 最近一次连接是否直连（false = 走中继）。
    direct: bool,
    /// 最近一次连接是否应用层加密。
    secure: bool,
    local_port: i32,
    remote_host: String,
    remote_port: i32,
    /// 累计转发字节。
    bytes: u64,
    /// 最近一次失败原因（空 = 没失败过）。
    last_error: String,
}

static TUNNEL_STATUS: Mutex<TunnelStatus> = Mutex::new(TunnelStatus {
    active: 0,
    direct: false,
    secure: false,
    local_port: 0,
    remote_host: String::new(),
    remote_port: 0,
    bytes: 0,
    last_error: String::new(),
});

fn status_set_connected(direct: bool, secure: bool, local_port: i32, remote_host: &str, remote_port: i32) {
    if let Ok(mut s) = TUNNEL_STATUS.lock() {
        s.active = s.active.saturating_add(1);
        s.direct = direct;
        s.secure = secure;
        s.local_port = local_port;
        s.remote_host = remote_host.to_owned();
        s.remote_port = remote_port;
        s.last_error.clear();
    }
}

fn status_set_closed(nbytes: u64) {
    if let Ok(mut s) = TUNNEL_STATUS.lock() {
        s.active = s.active.saturating_sub(1);
        s.bytes = s.bytes.saturating_add(nbytes);
    }
}

fn status_set_error(err: &str) {
    if let Ok(mut s) = TUNNEL_STATUS.lock() {
        s.last_error = err.to_owned();
    }
}

/// 供界面读取的隧道状态（JSON）。
/// 通过已有的 `main_get_common("port-forward-status")` 通道下发，不需要新增 FFI 绑定。
pub fn status_json() -> String {
    let s = match TUNNEL_STATUS.lock() {
        Ok(s) => s,
        Err(_) => return String::new(),
    };
    // 先取出拷贝，避免 json! 宏对 MutexGuard 内部字段做移动。
    let active = s.active;
    let direct = s.direct;
    let secure = s.secure;
    let local_port = s.local_port;
    let remote_host = s.remote_host.clone();
    let remote_port = s.remote_port;
    let bytes = s.bytes;
    let last_error = s.last_error.clone();
    drop(s);
    serde_json::json!({
        "active": active,
        "direct": direct,
        "secure": secure,
        "local_port": local_port,
        "remote_host": remote_host,
        "remote_port": remote_port,
        "bytes": bytes,
        "last_error": last_error,
    })
    .to_string()
}

fn run_rdp(port: u16) {
    std::process::Command::new("cmdkey")
        .arg("/delete:localhost")
        .output()
        .ok();
    let username = std::env::var("rdp_username").unwrap_or_default();
    let password = std::env::var("rdp_password").unwrap_or_default();
    if !username.is_empty() || !password.is_empty() {
        let mut args = vec!["/generic:localhost".to_owned()];
        if !username.is_empty() {
            args.push(format!("/user:{}", username));
        }
        if !password.is_empty() {
            args.push(format!("/pass:{}", password));
        }
        println!("{:?}", args);
        std::process::Command::new("cmdkey")
            .args(&args)
            .output()
            .ok();
    }
    std::process::Command::new("mstsc")
        .arg(format!("/v:localhost:{}", port))
        .spawn()
        .ok();
}

pub async fn listen(
    id: String,
    password: String,
    port: i32,
    interface: impl Interface,
    ui_receiver: mpsc::UnboundedReceiver<Data>,
    key: &str,
    token: &str,
    lc: Arc<RwLock<LoginConfigHandler>>,
    remote_host: String,
    remote_port: i32,
) -> ResultType<()> {
    let listener = tcp::new_listener(format!("127.0.0.1:{}", port), true).await?;
    let addr = listener.local_addr()?;
    log::info!("listening on port {:?}", addr);
    let is_rdp = port == 0;
    // 用户可见反馈：原实现只有一行 log，界面上完全看不出隧道有没有起来。
    // RDP（port==0）会自己拉起 mstsc，不需要额外弹窗。
    if !is_rdp {
        interface.msgbox(
            "success",
            "Port Forward",
            &format!(
                "本地 {} 已开始监听\n{} -> {}:{}",
                addr, id, remote_host, remote_port
            ),
            "",
        );
    }
    if is_rdp {
        run_rdp(addr.port());
    }
    let mut ui_receiver = ui_receiver;
    // 注意：循环里的 `addr` 是被 accept 的连接方地址（临时端口），
    // 状态里要显示的是隧道自己的本地监听端口，所以先存下来。
    let listen_port = addr.port() as i32;
    loop {
        tokio::select! {
            Ok((forward, addr)) = listener.accept() => {
                log::info!("new connection from {:?}", addr);
                lc.write().unwrap().port_forward = (remote_host.clone(), remote_port);
                let id = id.clone();
                let password = password.clone();
                let mut forward = Framed::new(forward, BytesCodec::new());
                match connect_and_login(&id, &password, &mut ui_receiver, interface.clone(), &mut forward, key, token, is_rdp).await {
                    Ok(Some((stream, direct, secure))) => {
                        let interface = interface.clone();
                        let target = format!("{}:{}", remote_host, remote_port);
                        status_set_connected(direct, secure, listen_port, &remote_host, remote_port);
                        tokio::spawn(async move {
                            let began = std::time::Instant::now();
                            match run_forward(forward, stream).await {
                                Ok(nbytes) => {
                                    status_set_closed(nbytes);
                                    let elapsed = began.elapsed();
                                    log::info!(
                                        "connection from {:?} closed ({} bytes, {:?})",
                                        addr,
                                        nbytes,
                                        elapsed
                                    );
                                    // 秒断且零字节：几乎一定是目标端口没开/被拒绝。
                                    // 原实现这里只写 log，界面上毫无反馈，用户以为隧道没生效。
                                    if nbytes == 0 && elapsed.as_secs() < 2 {
                                        status_set_error("目标端口没有服务在监听，或被防火墙/对端拒绝");
                                        interface.msgbox(
                                            "error",
                                            "Port Forward",
                                            &format!(
                                                "{} 立即断开了连接（0 字节）。\n目标端口可能没有服务在监听，或被防火墙/对端拒绝。",
                                                target
                                            ),
                                            "",
                                        );
                                    }
                                }
                                Err(err) => {
                                    status_set_closed(0);
                                    status_set_error(&err.to_string());
                                    interface.msgbox("error", "Error", &err.to_string(), "");
                                    log::info!("connection from {:?} closed with error", addr);
                                }
                            }
                        });
                    }
                    Err(err) => {
                        status_set_error(&err.to_string());
                        interface.on_establish_connection_error(err.to_string());
                    }
                    _ => {}
                }
            }
            d = ui_receiver.recv() => {
                match d {
                    Some(Data::Close) => {
                        break;
                    }
                    Some(Data::NewRDP) => {
                        println!("receive run_rdp from ui_receiver");
                        run_rdp(addr.port());
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

async fn connect_and_login(
    id: &str,
    password: &str,
    ui_receiver: &mut mpsc::UnboundedReceiver<Data>,
    interface: impl Interface,
    forward: &mut Framed<TcpStream, BytesCodec>,
    key: &str,
    token: &str,
    is_rdp: bool,
) -> ResultType<Option<(Stream, bool, bool)>> {
    let conn_type = if is_rdp {
        ConnType::RDP
    } else {
        ConnType::PORT_FORWARD
    };
    let ((mut stream, direct, _pk, _kcp, _stream_type), (feedback, rendezvous_server, ..)) =
        Client::start(id, key, token, conn_type, interface.clone()).await?;
    interface.update_direct(Some(direct));
    // 界面需要展示链路类型（直连 / 中继）和是否加密。
    // 注意 set_raw() 会清掉应用层加密密钥，所以要在切裸流之前取。
    let secure = stream.is_secured();
    // 复用已有的 connection_ready 事件通道，让窗口能显示「直连/中继」。
    interface.report_connection_type(secure, direct);
    let mut buffer = Vec::new();
    let mut received = false;

    let _keep_it = hc_connection(feedback, rendezvous_server, token).await;

    loop {
        tokio::select! {
            res = timeout(READ_TIMEOUT, stream.next()) => match res {
                Err(_) => {
                    bail!("Timeout");
                }
                Ok(Some(Ok(bytes))) => {
                    if !received {
                        received = true;
                        interface.update_received(true);
                    }
                    let msg_in = Message::parse_from_bytes(&bytes)?;
                    match msg_in.union {
                        Some(message::Union::Hash(hash)) => {
                            interface.handle_hash(password, hash, &mut stream).await;
                        }
                        Some(message::Union::LoginResponse(lr)) => match lr.union {
                            Some(login_response::Union::Error(err)) => {
                                if !interface.handle_login_error(&err) {
                                    return Ok(None);
                                }
                            }
                            Some(login_response::Union::PeerInfo(pi)) => {
                                interface.handle_peer_info(pi);
                                break;
                            }
                            _ => {}
                        }
                        Some(message::Union::TestDelay(t)) => {
                            interface.handle_test_delay(t, &mut stream).await;
                        }
                        _ => {}
                    }
                }
                Ok(Some(Err(err))) => {
                    bail!("Connection closed: {}", err);
                }
                _ => {
                    bail!("Reset by the peer");
                }
            },
            d = ui_receiver.recv() => {
                match d {
                    Some(Data::Login((os_username, os_password, password, remember))) => {
                        interface.handle_login_from_ui(os_username, os_password, password, remember, &mut stream).await;
                    }
                    Some(Data::Message(msg)) => {
                        allow_err!(stream.send(&msg).await);
                    }
                    _ => {}
                }
            },
            res = forward.next() => {
                if let Some(Ok(bytes)) = res {
                    buffer.extend(bytes);
                } else {
                    return Ok(None);
                }
            },
        }
    }
    stream.set_raw();
    if !buffer.is_empty() {
        allow_err!(stream.send_bytes(buffer.into()).await);
    }
    Ok(Some((stream, direct, secure)))
}

/// 攒批上限：单次转发最多合并这么大的数据，避免大流量时占用过多内存。
/// 达到上限就立刻发出，不等待。
pub(crate) const FORWARD_BATCH_MAX: usize = 64 * 1024;

/// 隧道数据转发循环（控制端）。
///
/// 原来每个 TCP 分片都立刻 `send` 一次：网页/SQL 这类一问一答的协议会产生
/// 大量小包，每个小包都要单独走一遍「加密 + 写套接字 + 对端解密」，在
/// 中继链路上尤其吃亏。这里改为**攒批**：一次最多合并到 [`FORWARD_BATCH_MAX`]。
///
/// 关键点：攒批只在「数据已经就绪」时进行（`now_or_never` 非阻塞地再取一块），
/// **不引入任何人为延迟**，所以交互式协议的往返时延不会变差。
async fn run_forward(
    forward: Framed<TcpStream, BytesCodec>,
    stream: Stream,
) -> ResultType<u64> {
    log::info!("new port forwarding connection started");
    let mut forward = forward;
    let mut stream = stream;
    let mut nbytes: u64 = 0;
    loop {
        tokio::select! {
            res = forward.next() => {
                match res {
                    Some(Ok(bytes)) => {
                        let mut batch = bytes;
                        // 非阻塞地把已就绪的后续分片一并取走。
                        loop {
                            if batch.len() >= FORWARD_BATCH_MAX {
                                break;
                            }
                            match forward.next().now_or_never() {
                                Some(Some(Ok(more))) => batch.extend_from_slice(&more),
                                _ => break,
                            }
                        }
                        nbytes += batch.len() as u64;
                        if let Err(err) = stream.send_bytes(batch.freeze()).await {
                            log::debug!("forward -> stream failed: {err}");
                            break;
                        }
                    }
                    _ => break,
                }
            },
            res = stream.next() => {
                match res {
                    Some(Ok(bytes)) => {
                        let mut batch = bytes;
                        loop {
                            if batch.len() >= FORWARD_BATCH_MAX {
                                break;
                            }
                            match stream.next().now_or_never() {
                                Some(Some(Ok(more))) => batch.extend_from_slice(&more),
                                _ => break,
                            }
                        }
                        nbytes += batch.len() as u64;
                        if let Err(err) = forward.send(batch.freeze()).await {
                            log::debug!("stream -> forward failed: {err}");
                            break;
                        }
                    }
                    _ => break,
                }
            },
        }
    }
    Ok(nbytes)
}
