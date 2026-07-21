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
        Self::kcp_io(udp_socket.clone(), input, output, stop_receiver).await;

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
        Self::kcp_io(udp_socket.clone(), input, output, stop_receiver).await;

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
    /// host side prefers accept. If the non-preferred direction finishes
    /// first with Ok, its result is kept as a fallback while we keep waiting
    /// for the preferred direction until it completes: a preferred Ok wins,
    /// otherwise the saved non-preferred Ok is used. Both Err -> Err.
    pub async fn race(
        udp_socket: Arc<UdpSocket>,
        timeout: std::time::Duration,
        prefer_connect: bool,
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
        Self::kcp_io(udp_socket.clone(), input, output, stop_receiver).await;

        // Drive connect and accept concurrently on the same endpoint (both &self).
        let conn_id = {
            let mut connect_fut = Box::pin(endpoint.connect(timeout, 0, 0, Bytes::new()));
            let mut accept_fut = Box::pin(async {
                match tokio::time::timeout(timeout, endpoint.accept()).await {
                    Ok(r) => r,
                    Err(_) => Err(kcp_sys::error::Error::ConnectTimeout),
                }
            });
            let mut conn_res = None;
            let mut accept_res = None;
            tokio::select! {
                res = &mut connect_fut => conn_res = Some(res),
                res = &mut accept_fut => accept_res = Some(res),
            }
            // 非优先方向先完成时保存结果，继续等优先方向完成再决定采用哪条路
            if conn_res.is_none() {
                conn_res = Some(connect_fut.await);
            }
            if accept_res.is_none() {
                accept_res = Some(accept_fut.await);
            }
            let conn_res = conn_res.unwrap();
            let accept_res = accept_res.unwrap();
            if prefer_connect {
                match conn_res {
                    Ok(id) => id,
                    Err(e) => match accept_res {
                        Ok(id) => id,
                        Err(_) => return Err(e.into()),
                    },
                }
            } else {
                match accept_res {
                    Ok(id) => id,
                    Err(e) => match conn_res {
                        Ok(id) => id,
                        Err(_) => return Err(e.into()),
                    },
                }
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
                        if let Err(e) = udp.send(&data.inner()).await {
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
