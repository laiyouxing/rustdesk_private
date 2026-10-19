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

pub struct KcpStream {
    _endpoint: KcpEndpoint,
    stop_sender: Option<oneshot::Sender<()>>,
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

        // Drive connect and accept concurrently on the same endpoint (both &self).
        // Both directions must run to completion even though only one result is
        // used: connect sends the packets that open the peer's NAT hole, and
        // accept keeps the endpoint receiving. `join!` waits for both (each is
        // bounded by `timeout`), which also matches the previous timing.
        let conn_id = {
            let connect_fut = Box::pin(endpoint.connect(timeout, 0, 0, Bytes::new()));
            let accept_fut = Box::pin(async {
                match tokio::time::timeout(timeout, endpoint.accept()).await {
                    Ok(r) => r,
                    Err(_) => Err(kcp_sys::error::Error::ConnectTimeout),
                }
            });
            let (conn_res, accept_res) = tokio::join!(connect_fut, accept_fut);
            if prefer_connect {
                // 控制端：只用自己 connect 的那条流（主机端 accept 的正是它）。
                // 失败绝不 fallback 到 accept 的流——主机会用它自己 accept 到的
                // 流（conv 不同），两条流之间不通，换流后即"无法操作"。
                log::debug!(
                    "KCP race (connector side): connect={:?}, accept(discarded)={:?}",
                    conn_res.as_ref().map(|_| "ok"),
                    accept_res.as_ref().map(|_| "ok")
                );
                conn_res?
            } else {
                // 主机端：只用自己 accept 到的那条流（控制端 connect 的正是它）。
                log::debug!(
                    "KCP race (host side): accept={:?}, connect(discarded)={:?}",
                    accept_res.as_ref().map(|_| "ok"),
                    conn_res.as_ref().map(|_| "ok")
                );
                accept_res?
            }
        };
        if let Some(stream) = stream::KcpStream::new(&endpoint, conn_id) {
            Ok((
                Self {
                    _endpoint: endpoint,
                    stop_sender: Some(stop_sender),
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
                            None => udp.send(&data.inner()).await,
                        };
                        if let Err(e) = send_res {
                            log::debug!("KCP send error: {:?}", e);
                            break;
                        }
                    }
                    result = udp.recv_from(&mut buf) => {
                        match result {
                            Ok((size, _)) => {
                                if size < std::mem::size_of::<KcpPacketHeader>() {
                                    continue;
                                }
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
                                log::debug!("KCP recv_from error: {:?}", e);
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
