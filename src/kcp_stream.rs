use hbb_common::{
    anyhow,
    bytes::{Bytes, BytesMut},
    bytes_codec::BytesCodec,
    config, log,
    tcp::{DynTcpStream, FramedStream},
    tokio::{self, net::UdpSocket, sync::mpsc, sync::oneshot},
    tokio_util, ResultType, Stream,
};
use kcp_sys::{
    endpoint::KcpEndpoint,
    ffi_safe::KcpConfig,
    packet_def::{KcpPacket, KcpPacketHeader},
    stream,
};
use std::{net::SocketAddr, sync::Arc};

/// 进程级累计收到的 KCP 数据报数（所有 kcp_io 收包循环共享）。
/// 仅用于诊断：直连"握手成功但对端零数据"时，用它的增量区分
/// 「对端根本没发/包到不了」与「收到了但应用层没数据」。
static KCP_RX_TOTAL: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// 读取累计收到的 KCP 数据报数（诊断用）。
pub fn kcp_rx_total() -> usize {
    KCP_RX_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

pub struct KcpStream {
    _endpoint: KcpEndpoint,
    stop_sender: Option<oneshot::Sender<()>>,
    /// 共享打洞 socket 使用凭证（仅未 connect 模式持有）。
    /// 随 KcpStream 生命周期生效：会话存活期间看门狗/其它 STUN 探测会跳过，
    /// 不会 recv_from 抢走本会话的报文。
    _shared_use: Option<crate::common::SharedSocketUse>,
}

impl KcpStream {
    fn create_framed(stream: stream::KcpStream, local_addr: Option<SocketAddr>) -> Stream {
        Stream::Tcp(FramedStream(
            tokio_util::codec::Framed::new(DynTcpStream(Box::new(stream)), BytesCodec::new()),
            local_addr.unwrap_or(config::Config::get_any_listen_addr(true)),
            None,
            0,
        ))
    }

    pub async fn accept(
        udp_socket: Arc<UdpSocket>,
        timeout: std::time::Duration,
        init_packet: Option<BytesMut>,
    ) -> ResultType<(Self, Stream)> {
        let mut endpoint = KcpEndpoint::new();
        // Community/common KCP tuning for a low-latency P2P control channel:
        // nodelay mode (no congestion control), 10ms internal update, 2x fast
        // resend; MTU 1400; enlarged send/recv windows.
        // Uses KcpConfigFactory as KcpEndpoint does not expose set_nodelay/set_mtu/set_wndsize.
        endpoint.set_kcp_config_factory(Box::new(|conv| KcpConfig {
            conv,
            mtu: Some(1400),
            sndwnd: Some(128),
            rcvwnd: Some(128),
            nodelay: Some(1),
            interval: Some(10),
            resend: Some(2),
            nc: Some(1),
        }));
        endpoint.run().await;

        let (input, output) = (
            endpoint.input_sender(),
            endpoint
                .output_receiver()
                .ok_or_else(|| anyhow::anyhow!("Failed to get output receiver"))?,
        );
        let (stop_sender, stop_receiver) = oneshot::channel();
        if let Some(packet) = init_packet {
            if packet.len() >= std::mem::size_of::<KcpPacketHeader>() {
                input.send(packet.into()).await?;
            }
        }
        Self::kcp_io(udp_socket.clone(), input, output, stop_receiver, None).await;

        let conn_id = tokio::time::timeout(timeout, endpoint.accept()).await??;
        if let Some(stream) = stream::KcpStream::new(&endpoint, conn_id) {
            Ok((
                Self {
                    _endpoint: endpoint,
                    stop_sender: Some(stop_sender),
                    _shared_use: None,
                },
                Self::create_framed(stream, udp_socket.local_addr().ok()),
            ))
        } else {
            Err(anyhow::anyhow!("Failed to create KcpStream"))
        }
    }

    pub async fn connect(
        udp_socket: Arc<UdpSocket>,
        timeout: std::time::Duration,
    ) -> ResultType<(Self, Stream)> {
        let mut endpoint = KcpEndpoint::new();
        // Community/common KCP tuning for a low-latency P2P control channel:
        // nodelay mode (no congestion control), 10ms internal update, 2x fast
        // resend; MTU 1400; enlarged send/recv windows.
        // Uses KcpConfigFactory as KcpEndpoint does not expose set_nodelay/set_mtu/set_wndsize.
        endpoint.set_kcp_config_factory(Box::new(|conv| KcpConfig {
            conv,
            mtu: Some(1400),
            sndwnd: Some(128),
            rcvwnd: Some(128),
            nodelay: Some(1),
            interval: Some(10),
            resend: Some(2),
            nc: Some(1),
        }));
        endpoint.run().await;

        let (input, output) = (
            endpoint.input_sender(),
            endpoint
                .output_receiver()
                .ok_or_else(|| anyhow::anyhow!("Failed to get output receiver"))?,
        );
        let (stop_sender, stop_receiver) = oneshot::channel();
        Self::kcp_io(udp_socket.clone(), input, output, stop_receiver, None).await;

        let conn_id = endpoint.connect(timeout, 0, 0, Bytes::new()).await?;
        if let Some(stream) = stream::KcpStream::new(&endpoint, conn_id) {
            Ok((
                Self {
                    _endpoint: endpoint,
                    stop_sender: Some(stop_sender),
                    _shared_use: None,
                },
                Self::create_framed(stream, udp_socket.local_addr().ok()),
            ))
        } else {
            Err(anyhow::anyhow!("Failed to create KcpStream"))
        }
    }

    /// Race KCP connect vs accept on a SINGLE endpoint (and a single kcp_io
    /// pump). Racing separate connect/accept endpoints on the same UDP socket
    /// makes them fight over recv_from, so handshake packets are ~50% likely
    /// to be eaten by the wrong endpoint.
    ///
    /// `prefer_connect`: the connector side prefers outbound connect, the
    /// host side prefers accept. **Only the preferred direction's result is
    /// used** — the connector always takes the stream it connected, the host
    /// always takes the stream it accepted, so both sides land on the *same*
    /// KCP stream (the one the connector initiated). Falling back to the
    /// non-preferred direction would pair the two sides on different streams
    /// (different conv), which never carry traffic: the classic
    /// "punch succeeded but the remote is uncontrollable" symptom.
    /// A failed preferred direction simply fails this round; the next round
    /// retries (the empty-packet burst keeps opening the NAT hole).
    pub async fn race(
        udp_socket: Arc<UdpSocket>,
        timeout: std::time::Duration,
        prefer_connect: bool,
        // Some(addr) = 未 connect 模式：KCP 包用 send_to 发往 addr。用于
        // 中继前打洞的进程级共享 socket——同一 socket 可同时与多个对端
        // 握手/收发（recv_from 收所有源，KCP 按 conv 分发），这对"一对多"
        // 远程（多个控制端同时连同一被控端）是必需的。None = connect 模式
        // （调用方已把 socket connect 到目标，旧 Phase3 路径使用）。
        peer_addr: Option<std::net::SocketAddr>,
    ) -> ResultType<(Self, Stream)> {
        let mut endpoint = KcpEndpoint::new();
        // Same KCP tuning as connect()/accept() above.
        endpoint.set_kcp_config_factory(Box::new(|conv| KcpConfig {
            conv,
            mtu: Some(1400),
            sndwnd: Some(128),
            rcvwnd: Some(128),
            nodelay: Some(1),
            interval: Some(10),
            resend: Some(2),
            nc: Some(1),
        }));
        endpoint.run().await;

        let (input, output) = (
            endpoint.input_sender(),
            endpoint
                .output_receiver()
                .ok_or_else(|| anyhow::anyhow!("Failed to get output receiver"))?,
        );
        let (stop_sender, stop_receiver) = oneshot::channel();
        Self::kcp_io(udp_socket.clone(), input, output, stop_receiver, peer_addr).await;

        // 关键：两端**只驱动自己会用到的那一个 future**，绝不能同时驱动
        // connect + accept。原因（kcp-sys endpoint.rs:593-595）：
        //   状态机每次进入 Established 都会 `new_conn_sender.try_send(conv)`，
        //   **不区分入站连接还是本端主动 connect**。
        // 而本端 connect 只需 1 个 RTT 就 Established（收到 SYN-ACK），入站连接
        // 要 1.5 个 RTT（收到 ACK+DATA）。所以主机端若同时驱动 connect，
        // `accept()` 会先把**自己 connect 的 conv** 返回给它——两端就此各用各的
        // 流，KCP 层面永不相通，但两边 endpoint 的 ping/pong 照跑：表现为
        // "打洞成功、泵没死、应用层零数据"（本机实测 100% 复现）。
        // 打洞本身由 punch_target_sustained 的空包 burst 完成，KCP SYN 对开洞
        // 没有额外贡献，去掉对端那个用不到的 future 无副作用。
        let conn_id = if prefer_connect {
            // 控制端：只发 connect（主机端 accept 的正是它）
            log::debug!("KCP race (connector side): connect only");
            endpoint.connect(timeout, 0, 0, Bytes::new()).await?
        } else {
            // 主机端：只 accept（控制端 connect 的正是它）
            log::debug!("KCP race (host side): accept only");
            match tokio::time::timeout(timeout, endpoint.accept()).await {
                Ok(r) => r?,
                Err(_) => return Err(anyhow::anyhow!("KCP accept timeout")),
            }
        };
        // 关键诊断：记录**最终选用**的连接号（Conv）。两端日志里这一行的 conv
        // 必须相同；如果控制端与主机端打出的 conv 不一样，就说明两端各用了
        // 自己的流（配对错误），表现正是"换流后画面卡死、无法操作"。
        log::info!(
            "KCP stream selected: side={} conn={:?} (prefer_connect={}, peer_addr={:?})",
            if prefer_connect { "connector(connect)" } else { "host(accept)" },
            conn_id,
            prefer_connect,
            peer_addr
        );
        // 未 connect 模式（peer_addr 为 Some）使用的是进程级共享打洞 socket：
        // 立即申领使用凭证并随本 KcpStream 存活，期间看门狗/其它 STUN 探测
        // 会跳过，不会 recv_from 抢走本会话的报文。
        let shared_use = if peer_addr.is_some() {
            Some(crate::common::shared_socket_acquire())
        } else {
            None
        };
        if let Some(stream) = stream::KcpStream::new(&endpoint, conn_id) {
            Ok((
                Self {
                    _endpoint: endpoint,
                    stop_sender: Some(stop_sender),
                    _shared_use: shared_use,
                },
                Self::create_framed(stream, udp_socket.local_addr().ok()),
            ))
        } else {
            Err(anyhow::anyhow!("Failed to create KcpStream"))
        }
    }

    async fn kcp_io(
        udp_socket: Arc<UdpSocket>,
        input: mpsc::Sender<KcpPacket>,
        mut output: mpsc::Receiver<KcpPacket>,
        mut stop_receiver: oneshot::Receiver<()>,
        // Some(addr) = 未 connect 模式：用 send_to 发往指定对端。这样同一个
        // socket 可同时服务多个对端（recv_from 收所有源，KCP 按 conv 分发），
        // 也不必先 connect——connect 会锁定唯一目标，导致第二个对端的包被
        // 发去错误地址（一对多远程时 inbound 打洞互相覆盖的根因）。
        // None = connect 模式：沿用 udp.send() 发往已 connect 的目标。
        peer_addr: Option<std::net::SocketAddr>,
    ) {
        let udp = udp_socket.clone();
        tokio::spawn(async move {
            let mut buf = vec![0; 1500];
            loop {
                tokio::select! {
                    _ = &mut stop_receiver => {
                        log::debug!("KCP io loop received stop signal");
                        break;
                    }
                    Some(data) = output.recv() => {
                        let send_res = match peer_addr {
                            Some(a) => udp.send_to(&data.inner(), a).await.map(|_| ()),
                            None => udp.send(&data.inner()).await.map(|_| ()),
                        };
                        if let Err(e) = send_res {
                            // 提到 info：KCP 泵死掉后会话就再也收发不了任何东西，
                            // 而日志里此前只有 debug，定位不到（表现为"连上了但
                            // 画面卡住、无任何提示"）。
                            log::warn!("KCP send error (pump will stop): {:?}", e);
                            break;
                        }
                    }
                    result = udp.recv_from(&mut buf) => {
                        match result {
                            Ok((size, _)) => {
                                if size < std::mem::size_of::<KcpPacketHeader>() {
                                    continue;
                                }
                                // 全局计数：用于判断换流后"收方向是否真的一个包都没有"。
                                // 直连假成功时这是单侧可判定的关键指标——rx 不增长说明
                                // 对端根本没在发（或包到不了），而不是应用层/解密问题。
                                KCP_RX_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                input
                                    .send(BytesMut::from(&buf[..size]).into())
                                    .await.ok();
                            }
                            Err(e) => {
                                // Windows 上 UDP 收到 ICMP 不可达会报 ConnectionReset，
                                // 打洞期间很常见，忽略继续收而不是停掉 KCP 泵
                                if e.kind() == std::io::ErrorKind::ConnectionReset {
                                    continue;
                                }
                                log::warn!("KCP recv_from error (pump will stop): {:?}", e);
                                break;
                            }
                        }
                    }
                    else => {
                        log::debug!("KCP endpoint input closed");
                        break;
                    }
                }
            }
        });
    }
}

impl Drop for KcpStream {
    fn drop(&mut self) {
        if let Some(sender) = self.stop_sender.take() {
            let _ = sender.send(());
        }
    }
}
