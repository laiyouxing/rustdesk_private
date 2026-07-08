use hbb_common::{bail, log, tokio, ResultType};
use igd_next::aio::tokio::search_gateway;
use igd_next::PortMappingProtocol;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

/// UPnP port mapping result
pub struct UpnpMapping {
    pub external_port: u16,
    pub local_port: u16,
}

/// Try to add a UPnP port mapping for the given local port.
/// The caller must hold a reservation socket on `local_port` to prevent
/// TOCTOU race (the port should already be bound by a reservation socket).
/// Runs synchronously at startup (best-effort, failure is silently ignored).
pub fn try_add_port_mapping_with_port(local_port: u16) -> Option<UpnpMapping> {
    match add_mapping(local_port) {
        Ok(external_port) => {
            log::info!("UPnP: mapped external port {} → local {}", external_port, local_port);
            Some(UpnpMapping {
                external_port,
                local_port,
            })
        }
        Err(e) => {
            log::info!("UPnP: not available ({})", e);
            None
        }
    }
}

fn add_mapping(local_port: u16) -> ResultType<u16> {
    let rt = tokio::runtime::Runtime::new()?;
    let gateway = rt.block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            search_gateway(Default::default()),
        )
        .await
    });

    let gateway = match gateway {
        Ok(Ok(g)) => g,
        _ => bail!("no UPnP gateway found"),
    };

    let local_addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), local_port);

    // Try the exact port first, then fallback to any available port
    let ports = [local_port, 0];
    for &ext_port in &ports {
        let result = rt.block_on(async {
            gateway
                .add_any_port(
                    PortMappingProtocol::TCP,
                    SocketAddr::V4(local_addr),
                    0, // lease duration: 0 = permanent
                    "RustDesk Direct Access",
                )
                .await
        });
        match result {
            Ok(mapped_port) => return Ok(mapped_port),
            Err(e) => {
                if ext_port == 0 {
                    bail!("UPnP add port mapping failed: {}", e);
                }
            }
        }
    }
    bail!("UPnP add port mapping failed");
}

/// Remove UPnP port mapping at shutdown.
pub fn try_remove_mapping(external_port: u16) {
    let rt = match tokio::runtime::Runtime::new() {
        Ok(r) => r,
        Err(_) => return,
    };
    let result = rt.block_on(async {
        match search_gateway(Default::default()).await {
            Ok(gateway) => {
                gateway
                    .remove_port(PortMappingProtocol::TCP, external_port)
                    .await
                    .ok();
                true
            }
            Err(_) => false,
        }
    });
    if !result {
        log::warn!("UPnP: failed to remove mapping for port {}", external_port);
    }
}
