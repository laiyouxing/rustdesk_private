use std::{
    collections::HashMap,
    future::Future,
    net::{SocketAddr, ToSocketAddrs},
    sync::{Arc, Mutex, RwLock},
    task::Poll,
};

use serde_json::{json, Map, Value};

#[cfg(not(target_os = "ios"))]
use hbb_common::whoami;
use hbb_common::{
    anyhow::{anyhow, Context},
    async_recursion::async_recursion,
    bail, base64,
    bytes::Bytes,
    config::{
        self, keys, use_ws, Config, LocalConfig, CONNECT_TIMEOUT, READ_TIMEOUT, RENDEZVOUS_PORT,
    },
    futures::future::join_all,
    futures_util::future::poll_fn,
    get_version_number, log,
    message_proto::*,
    protobuf::{Enum, Message as _},
    rendezvous_proto::*,
    socket_client,
    sodiumoxide::crypto::{box_, secretbox, sign},
    timeout,
    tls::{get_cached_tls_accept_invalid_cert, get_cached_tls_type, upsert_tls_cache, TlsType},
    tokio::{
        self,
        net::UdpSocket,
        sync::mpsc,
        time::{Duration, Instant, Interval},
    },
    ResultType, Stream,
};

use crate::{
    hbbs_http::{create_http_client_async, get_url_for_tls},
    ui_interface::{get_api_server as ui_get_api_server, get_option, set_option},
};

#[derive(Debug, Eq, PartialEq)]
pub enum GrabState {
    Ready,
    Run,
    Wait,
    Exit,
}

pub type NotifyMessageBox = fn(String, String, String, String) -> dyn Future<Output = ()>;

// the executable name of the portable version
pub const PORTABLE_APPNAME_RUNTIME_ENV_KEY: &str = "RUSTDESK_APPNAME";

pub const PLATFORM_WINDOWS: &str = "Windows";
pub const PLATFORM_LINUX: &str = "Linux";
pub const PLATFORM_MACOS: &str = "Mac OS";
pub const PLATFORM_ANDROID: &str = "Android";

// STUN-detected public (mapped) address, populated on startup by test_nat_type_
// and also during relay_upgrade_task for the latest NAT mapping.
pub static PUBLIC_ADDR: std::sync::Mutex<String> = std::sync::Mutex::new(String::new());

/// Custom build identifier used to verify that the remote peer is also running
/// this custom fork (not stock RustDesk).  Set in PunchHoleRequest.custom_tag.
pub const CUSTOM_TAG: &str = "rustdesk-custom";

/// Phase3 punch failure retry counter.
/// Up to 3 retries are allowed with exponential backoff (via auto-reconnect).
static PHASE3_RETRY_COUNT: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);
// 最近一次失败时刻：用于"失败计数随时间衰减"，避免偶发误判（对端首帧慢、
// 手动接受久）把打洞长期禁掉。
static PHASE3_LAST_FAIL_AT: std::sync::Mutex<Option<std::time::Instant>> =
    std::sync::Mutex::new(None);
// 只允许一次尝试：本机实测连续 6 次"KCP 握手成功但零数据"，每次要付
// 2.5s 打洞 + 5s 看门狗 ≈ 7.5s 画面卡死。3 次意味着每次重连白卡 22s。
// 直连要么一次就通，要么这个环境下根本不通——一次失败即熔断，5 分钟后
// 再给一次机会（PHASE3_RETRY_WINDOW 衰减）。
const PHASE3_MAX_RETRIES: u32 = 1;
// 失败计数窗口：距上次失败超过该时长则重新从 1 开始计。
// 5 分钟 ≈ 用户换一个对端/网络环境的时间尺度，兼顾"连续失败快速熔断"与
// "不要把一次误判固化成永久降级"。
const PHASE3_RETRY_WINDOW: std::time::Duration = std::time::Duration::from_secs(300);

/// Record that Phase3 succeeded, clearing the retry counter.
/// On subsequent reconnections, Phase3 will be attempted again.
pub fn record_phase3_success() {
    PHASE3_RETRY_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
    if let Ok(mut g) = PHASE3_LAST_FAIL_AT.lock() {
        *g = None;
    }
}

/// Record that Phase3 failed. Increments retry counter; after
/// PHASE3_MAX_RETRIES failures within PHASE3_RETRY_WINDOW, Phase3 is skipped
/// until user actively disconnects (reset), Phase3 succeeds, or the window
/// elapses (decay).
pub fn record_phase3_failure() {
    let now = std::time::Instant::now();
    let expired = if let Ok(mut g) = PHASE3_LAST_FAIL_AT.lock() {
        let expired = matches!(*g, Some(last) if now.duration_since(last) >= PHASE3_RETRY_WINDOW);
        *g = Some(now);
        expired
    } else {
        false
    };
    if expired {
        // 上一次失败已超出窗口：本轮算第 1 次失败，重新起算
        PHASE3_RETRY_COUNT.store(1, std::sync::atomic::Ordering::SeqCst);
    } else {
        PHASE3_RETRY_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Returns true if Phase3 should be skipped.
/// Allows up to PHASE3_MAX_RETRIES attempts within PHASE3_RETRY_WINDOW;
/// auto-reconnect exponential backoff provides natural spacing between
/// retries (1s→1.5s→2.3s→…).
pub fn should_skip_phase3() -> bool {
    if PHASE3_RETRY_COUNT.load(std::sync::atomic::Ordering::SeqCst) >= PHASE3_MAX_RETRIES {
        // 计数已达上限，但若距上次失败已超过窗口，视为衰减、放行
        let expired = if let Ok(g) = PHASE3_LAST_FAIL_AT.lock() {
            matches!(*g, Some(last) if last.elapsed() >= PHASE3_RETRY_WINDOW)
        } else {
            false
        };
        if expired {
            PHASE3_RETRY_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
            if let Ok(mut g) = PHASE3_LAST_FAIL_AT.lock() {
                *g = None;
            }
            return false;
        }
        return true;
    }
    false
}

/// Reset Phase3 state so the next connection will try punching again.
/// Called when the user explicitly closes the connection.
pub fn reset_phase3_state() {
    PHASE3_RETRY_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
    if let Ok(mut g) = PHASE3_LAST_FAIL_AT.lock() {
        *g = None;
    }
    // 用户主动断开说明网络环境可能已变，v6 熔断一并复位，下次重连重新尝试
    V6_RETRY_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
    if let Ok(mut g) = V6_LAST_FAIL_AT.lock() {
        *g = None;
    }
}

// ===================== IPv6 直连快速通道熔断 =====================
// 为什么必须与 Phase3 熔断**完全独立**：v6 只是并发抢跑的附加通道，v4 才是
// 主路径。若共用计数器，v6 失败会把 Phase3/v4 一并禁掉——那是实打实的成功率
// 损失。这里只用它决定"这次连接还要不要 spawn v6 任务"。
//
// 与 Phase3 的区别：v6 并发执行，失败不占用户额外时间（不像 Phase3 串行
// 占预算），所以门槛可以比 Phase3（1 次）宽松一点，给偶发丢包一次机会；
// 但 v6 长期不通的环境（对端无 v6 / 路由不可达 / 防火墙丢 ICMPv6）每次
// 连接都会白起一个任务跑到预算耗尽，因此仍需熔断。
static V6_RETRY_COUNT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
static V6_LAST_FAIL_AT: std::sync::Mutex<Option<std::time::Instant>> =
    std::sync::Mutex::new(None);
/// v6 连续失败多少次后熔断。2 次：一次可能是握手期偶发丢包，两次基本可以
/// 判定这个环境下 v6 路径不通。
const V6_MAX_RETRIES: u32 = 2;
/// 熔断窗口：超过该时长视为环境可能已变（换网络、对端开启 v6），放行重试。
const V6_RETRY_WINDOW: std::time::Duration = std::time::Duration::from_secs(300);

/// v6 打洞成功：清零计数，下次连接继续尝试 v6。
pub fn record_v6_success() {
    V6_RETRY_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
    if let Ok(mut g) = V6_LAST_FAIL_AT.lock() {
        *g = None;
    }
}

/// v6 打洞失败（有 v6 地址但没打通）。窗口外的旧失败不累计。
pub fn record_v6_failure() {
    let now = std::time::Instant::now();
    let expired = if let Ok(mut g) = V6_LAST_FAIL_AT.lock() {
        let expired = matches!(*g, Some(last) if now.duration_since(last) >= V6_RETRY_WINDOW);
        *g = Some(now);
        expired
    } else {
        false
    };
    if expired {
        V6_RETRY_COUNT.store(1, std::sync::atomic::Ordering::SeqCst);
    } else {
        V6_RETRY_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// 是否跳过本次 v6 尝试（熔断已跳闸且未过窗口）。
///
/// 注意：**只影响 v6**，v4 打洞永远照常进行。
pub fn should_skip_v6() -> bool {
    if V6_RETRY_COUNT.load(std::sync::atomic::Ordering::SeqCst) >= V6_MAX_RETRIES {
        let expired = if let Ok(g) = V6_LAST_FAIL_AT.lock() {
            matches!(*g, Some(last) if last.elapsed() >= V6_RETRY_WINDOW)
        } else {
            false
        };
        if expired {
            V6_RETRY_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
            if let Ok(mut g) = V6_LAST_FAIL_AT.lock() {
                *g = None;
            }
            return false;
        }
        return true;
    }
    false
}

pub const TIMER_OUT: Duration = Duration::from_secs(1);
pub const DEFAULT_KEEP_ALIVE: i32 = 60_000;

const MIN_VER_MULTI_UI_SESSION: &str = "1.2.4";

pub mod input {
    pub const MOUSE_TYPE_MOVE: i32 = 0;
    pub const MOUSE_TYPE_DOWN: i32 = 1;
    pub const MOUSE_TYPE_UP: i32 = 2;
    pub const MOUSE_TYPE_WHEEL: i32 = 3;
    pub const MOUSE_TYPE_TRACKPAD: i32 = 4;
    /// Relative mouse movement type for gaming/3D applications.
    /// This type sends delta (dx, dy) values instead of absolute coordinates.
    /// NOTE: This is only supported by the Flutter client. The Sciter client (deprecated)
    /// does not support relative mouse mode due to:
    /// 1. Fixed send_mouse() function signature that doesn't allow type differentiation
    /// 2. Lack of pointer lock API in Sciter/TIS
    /// 3. No OS cursor control (hide/show/clip) FFI bindings in Sciter UI
    pub const MOUSE_TYPE_MOVE_RELATIVE: i32 = 5;

    /// Mask to extract the mouse event type from the mask field.
    /// The lower 3 bits contain the event type (MOUSE_TYPE_*), giving a valid range of 0-7.
    /// Currently defined types use values 0-5; values 6 and 7 are reserved for future use.
    pub const MOUSE_TYPE_MASK: i32 = 0x7;

    pub const MOUSE_BUTTON_LEFT: i32 = 0x01;
    pub const MOUSE_BUTTON_RIGHT: i32 = 0x02;
    pub const MOUSE_BUTTON_WHEEL: i32 = 0x04;
    pub const MOUSE_BUTTON_BACK: i32 = 0x08;
    pub const MOUSE_BUTTON_FORWARD: i32 = 0x10;
}

lazy_static::lazy_static! {
    pub static ref SOFTWARE_UPDATE_URL: Arc<Mutex<String>> = Default::default();
    pub static ref SOFTWARE_UPDATE_VERSION: Arc<Mutex<String>> = Default::default();
    pub static ref DEVICE_ID: Arc<Mutex<String>> = Default::default();
    pub static ref DEVICE_NAME: Arc<Mutex<String>> = Default::default();
    static ref PUBLIC_IPV6_ADDR: Arc<Mutex<(Option<SocketAddr>, Option<Instant>)>> = Default::default();
}

lazy_static::lazy_static! {
    // Is server process, with "--server" args
    static ref IS_SERVER: bool = std::env::args().nth(1) == Some("--server".to_owned());
    // Is server logic running. The server code can invoked to run by the main process if --server is not running.
    static ref SERVER_RUNNING: Arc<RwLock<bool>> = Default::default();
    static ref IS_MAIN: bool = std::env::args().nth(1).map_or(true, |arg| !arg.starts_with("--"));
    static ref IS_CM: bool = std::env::args().nth(1) == Some("--cm".to_owned()) || std::env::args().nth(1) == Some("--cm-no-ui".to_owned());
}

pub struct SimpleCallOnReturn {
    pub b: bool,
    pub f: Box<dyn Fn() + Send + 'static>,
}

impl Drop for SimpleCallOnReturn {
    fn drop(&mut self) {
        if self.b {
            (self.f)();
        }
    }
}

/// UPnP mapping state (set once at startup)
static UPNP_MAPPING: std::sync::Mutex<Option<crate::upnp::UpnpMapping>> =
    std::sync::Mutex::new(None);

/// Local port reserved for the UPnP mapping (set at startup, read by the renew task).
pub static UPNP_LOCAL_PORT: std::sync::Mutex<Option<u16>> = std::sync::Mutex::new(None);

/// Reservation socket that keeps the UPnP local port occupied until Phase3 uses it.
/// Prevents TOCTOU race: port is reserved between UPnP discovery and socket creation.
static UPNP_RESERVATION: std::sync::Mutex<Option<std::net::UdpSocket>> =
    std::sync::Mutex::new(None);

/// Get the UPnP-mapped external port (0 if not available).
pub fn get_upnp_port() -> u16 {
    UPNP_MAPPING.lock().ok().and_then(|g| g.as_ref().map(|m| m.external_port)).unwrap_or(0)
}

/// Get the local port that was bound for UPnP (0 if not available).
/// Use this as `punch_port` so UDP/TCP sockets use the UPnP-mapped local port.
pub fn get_upnp_local_port() -> u16 {
    UPNP_MAPPING.lock().ok().and_then(|g| g.as_ref().map(|m| m.local_port)).unwrap_or(0)
}

/// Release the UPnP reservation socket so the port can be used by Phase3.
pub fn release_upnp_reservation() {
    if let Ok(mut guard) = UPNP_RESERVATION.lock() {
        guard.take();
    }
}

pub fn global_init() -> bool {
    #[cfg(target_os = "linux")]
    {
        if !crate::platform::linux::is_x11() {
            crate::server::wayland::init();
        }
    }
    // Initialize UPnP if enabled (non-Android/non-iOS)
    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    {
        let opt = hbb_common::config::LocalConfig::get_option(
            hbb_common::config::keys::OPTION_ENABLE_UPNP);
        if hbb_common::config::option2bool(hbb_common::config::keys::OPTION_ENABLE_UPNP, &opt) {
            // Create a reservation socket to hold the port until Phase3 needs it
            if let Ok(reservation) = std::net::UdpSocket::bind(
                std::net::SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 0))
            {
                let local_port = reservation.local_addr().map(|a| a.port()).unwrap_or(0);
                if local_port > 0 {
                    if let Ok(mut rguard) = UPNP_RESERVATION.lock() {
                        *rguard = Some(reservation);
                    }
                    *UPNP_LOCAL_PORT.lock().unwrap() = Some(local_port);
                    if let Ok(mut guard) = UPNP_MAPPING.lock() {
                        *guard = crate::upnp::try_add_port_mapping_with_port(local_port);
                    }
                    start_upnp_renew_task();
                }
            }
        }
    }
    true
}

pub fn global_clean() {
    // Remove UPnP mapping on shutdown
    if let Ok(guard) = UPNP_MAPPING.lock() {
        if let Some(ref mapping) = *guard {
            crate::upnp::try_remove_mapping(mapping.external_port);
        }
    }
}

/// Background task: keep the UPnP mapping alive only while a remote desktop
/// connection is active. Renews the lease (and re-probes LAN IP) when in use;
/// lets the mapping expire when idle; re-maps on LAN IP change.
fn start_upnp_renew_task() {
    std::thread::spawn(|| {
        const RENEW_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1800); // 30 min, < 1h lease
        let mut last_lan: Option<std::net::Ipv4Addr> = None;
        loop {
            std::thread::sleep(RENEW_INTERVAL);
            let local_port = match *UPNP_LOCAL_PORT.lock().unwrap() {
                Some(p) => p,
                None => break,
            };
            let active = crate::server::active_remote_conn_count();
            let lan = crate::upnp::local_ipv4();
            if last_lan.is_none() {
                // 首探仅记录当前 LAN IP，不误判为"IP 变化"导致空闲期误续期
                last_lan = lan;
                continue;
            }
            let should_remap = match (active > 0, lan != last_lan) {
                (true, _) => true,       // in use -> renew (also re-probes LAN IP)
                (false, true) => true,   // idle but LAN IP changed -> re-map so next connect works
                (false, false) => false, // idle and IP stable -> let lease expire
            };
            last_lan = lan;
            if !should_remap {
                log::debug!("UPnP: idle, skipping renew (mapping will expire naturally)");
                continue;
            }
            match crate::upnp::try_add_port_mapping_with_port(local_port) {
                Some(m) => {
                    let external_port = m.external_port;
                    *UPNP_MAPPING.lock().unwrap() = Some(m);
                    log::info!("UPnP: renewed external port {} -> local {}", external_port, local_port);
                }
                None => log::warn!("UPnP: renew failed (will retry next cycle)"),
            }
        }
    });
}

#[inline]
pub fn set_server_running(b: bool) {
    *SERVER_RUNNING.write().unwrap() = b;
}

#[inline]
pub fn is_support_multi_ui_session(ver: &str) -> bool {
    is_support_multi_ui_session_num(hbb_common::get_version_number(ver))
}

#[inline]
pub fn is_support_multi_ui_session_num(ver: i64) -> bool {
    ver >= hbb_common::get_version_number(MIN_VER_MULTI_UI_SESSION)
}

#[inline]
#[cfg(feature = "unix-file-copy-paste")]
pub fn is_support_file_copy_paste(ver: &str) -> bool {
    is_support_file_copy_paste_num(hbb_common::get_version_number(ver))
}

#[inline]
#[cfg(feature = "unix-file-copy-paste")]
pub fn is_support_file_copy_paste_num(ver: i64) -> bool {
    ver >= hbb_common::get_version_number("1.3.8")
}

pub fn is_support_remote_print(ver: &str) -> bool {
    hbb_common::get_version_number(ver) >= hbb_common::get_version_number("1.3.9")
}

pub fn is_support_file_paste_if_macos(ver: &str) -> bool {
    hbb_common::get_version_number(ver) >= hbb_common::get_version_number("1.3.9")
}

#[inline]
pub fn is_support_screenshot(ver: &str) -> bool {
    is_support_multi_ui_session_num(hbb_common::get_version_number(ver))
}

#[inline]
pub fn is_support_screenshot_num(ver: i64) -> bool {
    ver >= hbb_common::get_version_number("1.4.0")
}

#[inline]
pub fn is_support_file_transfer_resume(ver: &str) -> bool {
    is_support_file_transfer_resume_num(hbb_common::get_version_number(ver))
}

#[inline]
pub fn is_support_file_transfer_resume_num(ver: i64) -> bool {
    ver >= hbb_common::get_version_number("1.4.2")
}

/// Minimum server version required for relative mouse mode support.
/// This constant must mirror Flutter's `kMinVersionForRelativeMouseMode` in `consts.dart`.
const MIN_VERSION_RELATIVE_MOUSE_MODE: &str = "1.4.5";

#[inline]
pub fn is_support_relative_mouse_mode(ver: &str) -> bool {
    is_support_relative_mouse_mode_num(hbb_common::get_version_number(ver))
}

#[inline]
pub fn is_support_relative_mouse_mode_num(ver: i64) -> bool {
    ver >= hbb_common::get_version_number(MIN_VERSION_RELATIVE_MOUSE_MODE)
}

// is server process, with "--server" args
#[inline]
pub fn is_server() -> bool {
    *IS_SERVER
}

#[inline]
pub fn need_fs_cm_send_files() -> bool {
    #[cfg(windows)]
    {
        is_server()
    }
    #[cfg(not(windows))]
    {
        false
    }
}

#[inline]
pub fn is_main() -> bool {
    *IS_MAIN
}

#[inline]
pub fn is_cm() -> bool {
    *IS_CM
}

// Is server logic running.
#[inline]
pub fn is_server_running() -> bool {
    *SERVER_RUNNING.read().unwrap()
}

#[inline]
pub fn valid_for_numlock(evt: &KeyEvent) -> bool {
    if let Some(key_event::Union::ControlKey(ck)) = evt.union {
        let v = ck.value();
        (v >= ControlKey::Numpad0.value() && v <= ControlKey::Numpad9.value())
            || v == ControlKey::Decimal.value()
    } else {
        false
    }
}

/// Set sound input device.
pub fn set_sound_input(device: String) {
    let prior_device = get_option("audio-input".to_owned());
    if prior_device != device {
        log::info!("switch to audio input device {}", device);
        std::thread::spawn(move || {
            set_option("audio-input".to_owned(), device);
        });
    } else {
        log::info!("audio input is already set to {}", device);
    }
}

/// Get system's default sound input device name.
#[inline]
#[cfg(not(any(target_os = "android", target_os = "ios")))]
pub fn get_default_sound_input() -> Option<String> {
    #[cfg(not(target_os = "linux"))]
    {
        use cpal::traits::{DeviceTrait, HostTrait};
        let host = cpal::default_host();
        let dev = host.default_input_device();
        return if let Some(dev) = dev {
            match dev.name() {
                Ok(name) => Some(name),
                Err(_) => None,
            }
        } else {
            None
        };
    }
    #[cfg(target_os = "linux")]
    {
        let input = crate::platform::linux::get_default_pa_source();
        return if let Some(input) = input {
            Some(input.1)
        } else {
            None
        };
    }
}

#[inline]
#[cfg(any(target_os = "android", target_os = "ios"))]
pub fn get_default_sound_input() -> Option<String> {
    None
}

#[cfg(feature = "use_rubato")]
pub fn resample_channels(
    data: &[f32],
    sample_rate0: u32,
    sample_rate: u32,
    channels: u16,
) -> Vec<f32> {
    use rubato::{
        InterpolationParameters, InterpolationType, Resampler, SincFixedIn, WindowFunction,
    };
    let params = InterpolationParameters {
        sinc_len: 256,
        f_cutoff: 0.95,
        interpolation: InterpolationType::Nearest,
        oversampling_factor: 160,
        window: WindowFunction::BlackmanHarris2,
    };
    let mut resampler = SincFixedIn::<f64>::new(
        sample_rate as f64 / sample_rate0 as f64,
        params,
        data.len() / (channels as usize),
        channels as _,
    );
    let mut waves_in = Vec::new();
    if channels == 2 {
        waves_in.push(
            data.iter()
                .step_by(2)
                .map(|x| *x as f64)
                .collect::<Vec<_>>(),
        );
        waves_in.push(
            data.iter()
                .skip(1)
                .step_by(2)
                .map(|x| *x as f64)
                .collect::<Vec<_>>(),
        );
    } else {
        waves_in.push(data.iter().map(|x| *x as f64).collect::<Vec<_>>());
    }
    if let Ok(x) = resampler.process(&waves_in) {
        if x.is_empty() {
            Vec::new()
        } else if x.len() == 2 {
            x[0].chunks(1)
                .zip(x[1].chunks(1))
                .flat_map(|(a, b)| a.into_iter().chain(b))
                .map(|x| *x as f32)
                .collect()
        } else {
            x[0].iter().map(|x| *x as f32).collect()
        }
    } else {
        Vec::new()
    }
}

#[cfg(feature = "use_dasp")]
pub fn audio_resample(
    data: &[f32],
    sample_rate0: u32,
    sample_rate: u32,
    channels: u16,
) -> Vec<f32> {
    use dasp::{interpolate::linear::Linear, signal, Signal};
    let n = data.len() / (channels as usize);
    let n = n * sample_rate as usize / sample_rate0 as usize;
    if channels == 2 {
        let mut source = signal::from_interleaved_samples_iter::<_, [_; 2]>(data.iter().cloned());
        let a = source.next();
        let b = source.next();
        let interp = Linear::new(a, b);
        let mut data = Vec::with_capacity(n << 1);
        for x in source
            .from_hz_to_hz(interp, sample_rate0 as _, sample_rate as _)
            .take(n)
        {
            data.push(x[0]);
            data.push(x[1]);
        }
        data
    } else {
        let mut source = signal::from_iter(data.iter().cloned());
        let a = source.next();
        let b = source.next();
        let interp = Linear::new(a, b);
        source
            .from_hz_to_hz(interp, sample_rate0 as _, sample_rate as _)
            .take(n)
            .collect()
    }
}

#[cfg(feature = "use_samplerate")]
pub fn audio_resample(
    data: &[f32],
    sample_rate0: u32,
    sample_rate: u32,
    channels: u16,
) -> Vec<f32> {
    use samplerate::{convert, ConverterType};
    convert(
        sample_rate0 as _,
        sample_rate as _,
        channels as _,
        ConverterType::SincBestQuality,
        data,
    )
    .unwrap_or_default()
}

pub fn audio_rechannel(
    input: Vec<f32>,
    in_hz: u32,
    out_hz: u32,
    in_chan: u16,
    output_chan: u16,
) -> Vec<f32> {
    if in_chan == output_chan {
        return input;
    }
    let mut input = input;
    input.truncate(input.len() / in_chan as usize * in_chan as usize);
    match (in_chan, output_chan) {
        (1, 2) => audio_rechannel_1_2(&input, in_hz, out_hz),
        (1, 3) => audio_rechannel_1_3(&input, in_hz, out_hz),
        (1, 4) => audio_rechannel_1_4(&input, in_hz, out_hz),
        (1, 5) => audio_rechannel_1_5(&input, in_hz, out_hz),
        (1, 6) => audio_rechannel_1_6(&input, in_hz, out_hz),
        (1, 7) => audio_rechannel_1_7(&input, in_hz, out_hz),
        (1, 8) => audio_rechannel_1_8(&input, in_hz, out_hz),
        (2, 1) => audio_rechannel_2_1(&input, in_hz, out_hz),
        (2, 3) => audio_rechannel_2_3(&input, in_hz, out_hz),
        (2, 4) => audio_rechannel_2_4(&input, in_hz, out_hz),
        (2, 5) => audio_rechannel_2_5(&input, in_hz, out_hz),
        (2, 6) => audio_rechannel_2_6(&input, in_hz, out_hz),
        (2, 7) => audio_rechannel_2_7(&input, in_hz, out_hz),
        (2, 8) => audio_rechannel_2_8(&input, in_hz, out_hz),
        (3, 1) => audio_rechannel_3_1(&input, in_hz, out_hz),
        (3, 2) => audio_rechannel_3_2(&input, in_hz, out_hz),
        (3, 4) => audio_rechannel_3_4(&input, in_hz, out_hz),
        (3, 5) => audio_rechannel_3_5(&input, in_hz, out_hz),
        (3, 6) => audio_rechannel_3_6(&input, in_hz, out_hz),
        (3, 7) => audio_rechannel_3_7(&input, in_hz, out_hz),
        (3, 8) => audio_rechannel_3_8(&input, in_hz, out_hz),
        (4, 1) => audio_rechannel_4_1(&input, in_hz, out_hz),
        (4, 2) => audio_rechannel_4_2(&input, in_hz, out_hz),
        (4, 3) => audio_rechannel_4_3(&input, in_hz, out_hz),
        (4, 5) => audio_rechannel_4_5(&input, in_hz, out_hz),
        (4, 6) => audio_rechannel_4_6(&input, in_hz, out_hz),
        (4, 7) => audio_rechannel_4_7(&input, in_hz, out_hz),
        (4, 8) => audio_rechannel_4_8(&input, in_hz, out_hz),
        (5, 1) => audio_rechannel_5_1(&input, in_hz, out_hz),
        (5, 2) => audio_rechannel_5_2(&input, in_hz, out_hz),
        (5, 3) => audio_rechannel_5_3(&input, in_hz, out_hz),
        (5, 4) => audio_rechannel_5_4(&input, in_hz, out_hz),
        (5, 6) => audio_rechannel_5_6(&input, in_hz, out_hz),
        (5, 7) => audio_rechannel_5_7(&input, in_hz, out_hz),
        (5, 8) => audio_rechannel_5_8(&input, in_hz, out_hz),
        (6, 1) => audio_rechannel_6_1(&input, in_hz, out_hz),
        (6, 2) => audio_rechannel_6_2(&input, in_hz, out_hz),
        (6, 3) => audio_rechannel_6_3(&input, in_hz, out_hz),
        (6, 4) => audio_rechannel_6_4(&input, in_hz, out_hz),
        (6, 5) => audio_rechannel_6_5(&input, in_hz, out_hz),
        (6, 7) => audio_rechannel_6_7(&input, in_hz, out_hz),
        (6, 8) => audio_rechannel_6_8(&input, in_hz, out_hz),
        (7, 1) => audio_rechannel_7_1(&input, in_hz, out_hz),
        (7, 2) => audio_rechannel_7_2(&input, in_hz, out_hz),
        (7, 3) => audio_rechannel_7_3(&input, in_hz, out_hz),
        (7, 4) => audio_rechannel_7_4(&input, in_hz, out_hz),
        (7, 5) => audio_rechannel_7_5(&input, in_hz, out_hz),
        (7, 6) => audio_rechannel_7_6(&input, in_hz, out_hz),
        (7, 8) => audio_rechannel_7_8(&input, in_hz, out_hz),
        (8, 1) => audio_rechannel_8_1(&input, in_hz, out_hz),
        (8, 2) => audio_rechannel_8_2(&input, in_hz, out_hz),
        (8, 3) => audio_rechannel_8_3(&input, in_hz, out_hz),
        (8, 4) => audio_rechannel_8_4(&input, in_hz, out_hz),
        (8, 5) => audio_rechannel_8_5(&input, in_hz, out_hz),
        (8, 6) => audio_rechannel_8_6(&input, in_hz, out_hz),
        (8, 7) => audio_rechannel_8_7(&input, in_hz, out_hz),
        _ => input,
    }
}

macro_rules! audio_rechannel {
    ($name:ident, $in_channels:expr, $out_channels:expr) => {
        fn $name(input: &[f32], in_hz: u32, out_hz: u32) -> Vec<f32> {
            use fon::{chan::Ch32, Audio, Frame};
            let mut in_audio =
                Audio::<Ch32, $in_channels>::with_silence(in_hz, input.len() / $in_channels);
            for (x, y) in input.chunks_exact($in_channels).zip(in_audio.iter_mut()) {
                let mut f = Frame::<Ch32, $in_channels>::default();
                let mut i = 0;
                for c in f.channels_mut() {
                    *c = x[i].into();
                    i += 1;
                }
                *y = f;
            }
            Audio::<Ch32, $out_channels>::with_audio(out_hz, &in_audio)
                .as_f32_slice()
                .to_owned()
        }
    };
}

audio_rechannel!(audio_rechannel_1_2, 1, 2);
audio_rechannel!(audio_rechannel_1_3, 1, 3);
audio_rechannel!(audio_rechannel_1_4, 1, 4);
audio_rechannel!(audio_rechannel_1_5, 1, 5);
audio_rechannel!(audio_rechannel_1_6, 1, 6);
audio_rechannel!(audio_rechannel_1_7, 1, 7);
audio_rechannel!(audio_rechannel_1_8, 1, 8);
audio_rechannel!(audio_rechannel_2_1, 2, 1);
audio_rechannel!(audio_rechannel_2_3, 2, 3);
audio_rechannel!(audio_rechannel_2_4, 2, 4);
audio_rechannel!(audio_rechannel_2_5, 2, 5);
audio_rechannel!(audio_rechannel_2_6, 2, 6);
audio_rechannel!(audio_rechannel_2_7, 2, 7);
audio_rechannel!(audio_rechannel_2_8, 2, 8);
audio_rechannel!(audio_rechannel_3_1, 3, 1);
audio_rechannel!(audio_rechannel_3_2, 3, 2);
audio_rechannel!(audio_rechannel_3_4, 3, 4);
audio_rechannel!(audio_rechannel_3_5, 3, 5);
audio_rechannel!(audio_rechannel_3_6, 3, 6);
audio_rechannel!(audio_rechannel_3_7, 3, 7);
audio_rechannel!(audio_rechannel_3_8, 3, 8);
audio_rechannel!(audio_rechannel_4_1, 4, 1);
audio_rechannel!(audio_rechannel_4_2, 4, 2);
audio_rechannel!(audio_rechannel_4_3, 4, 3);
audio_rechannel!(audio_rechannel_4_5, 4, 5);
audio_rechannel!(audio_rechannel_4_6, 4, 6);
audio_rechannel!(audio_rechannel_4_7, 4, 7);
audio_rechannel!(audio_rechannel_4_8, 4, 8);
audio_rechannel!(audio_rechannel_5_1, 5, 1);
audio_rechannel!(audio_rechannel_5_2, 5, 2);
audio_rechannel!(audio_rechannel_5_3, 5, 3);
audio_rechannel!(audio_rechannel_5_4, 5, 4);
audio_rechannel!(audio_rechannel_5_6, 5, 6);
audio_rechannel!(audio_rechannel_5_7, 5, 7);
audio_rechannel!(audio_rechannel_5_8, 5, 8);
audio_rechannel!(audio_rechannel_6_1, 6, 1);
audio_rechannel!(audio_rechannel_6_2, 6, 2);
audio_rechannel!(audio_rechannel_6_3, 6, 3);
audio_rechannel!(audio_rechannel_6_4, 6, 4);
audio_rechannel!(audio_rechannel_6_5, 6, 5);
audio_rechannel!(audio_rechannel_6_7, 6, 7);
audio_rechannel!(audio_rechannel_6_8, 6, 8);
audio_rechannel!(audio_rechannel_7_1, 7, 1);
audio_rechannel!(audio_rechannel_7_2, 7, 2);
audio_rechannel!(audio_rechannel_7_3, 7, 3);
audio_rechannel!(audio_rechannel_7_4, 7, 4);
audio_rechannel!(audio_rechannel_7_5, 7, 5);
audio_rechannel!(audio_rechannel_7_6, 7, 6);
audio_rechannel!(audio_rechannel_7_8, 7, 8);
audio_rechannel!(audio_rechannel_8_1, 8, 1);
audio_rechannel!(audio_rechannel_8_2, 8, 2);
audio_rechannel!(audio_rechannel_8_3, 8, 3);
audio_rechannel!(audio_rechannel_8_4, 8, 4);
audio_rechannel!(audio_rechannel_8_5, 8, 5);
audio_rechannel!(audio_rechannel_8_6, 8, 6);
audio_rechannel!(audio_rechannel_8_7, 8, 7);

pub struct CheckTestNatType {
    is_direct: bool,
}

impl CheckTestNatType {
    pub fn new() -> Self {
        Self {
            is_direct: Config::get_socks().is_none() && !config::use_ws(),
        }
    }
}

impl Drop for CheckTestNatType {
    fn drop(&mut self) {
        let is_direct = Config::get_socks().is_none() && !config::use_ws();
        if self.is_direct != is_direct {
            test_nat_type();
        }
    }
}

pub fn test_nat_type() {
    test_ipv6_sync();
    use std::sync::atomic::{AtomicBool, Ordering};
    std::thread::spawn(move || {
        static IS_RUNNING: AtomicBool = AtomicBool::new(false);
        if IS_RUNNING.load(Ordering::SeqCst) {
            return;
        }
        IS_RUNNING.store(true, Ordering::SeqCst);

        #[cfg(not(any(target_os = "android", target_os = "ios")))]
        crate::ipc::get_socks_ws();
        let is_direct = Config::get_socks().is_none() && !config::use_ws();
        if !is_direct {
            Config::set_nat_type(NatType::SYMMETRIC as _);
            IS_RUNNING.store(false, Ordering::SeqCst);
            return;
        }

        let mut i = 0;
        loop {
            match test_nat_type_() {
                Ok(true) => break,
                Err(err) => {
                    log::error!("test nat: {}", err);
                }
                _ => {}
            }
            if Config::get_nat_type() != 0 {
                break;
            }
            i = i * 2 + 1;
            if i > 300 {
                i = 300;
            }
            std::thread::sleep(std::time::Duration::from_secs(i));
        }

        IS_RUNNING.store(false, Ordering::SeqCst);
    });
}

#[tokio::main(flavor = "current_thread")]
async fn test_nat_type_() -> ResultType<bool> {
    log::info!("Testing nat ...");
    let start = std::time::Instant::now();

    // Discover public address via STUN at the very start, before any TCP
    // connection attempt.  This ensures the UI can always display the public
    // IP even when the rendezvous server is temporarily unreachable.
    if let Ok((addr, _srv)) = {
        let servers = get_stun_servers_v4();
        if let Some(first) = servers.first() {
            stun_ipv4_test(first).await
        } else {
            stun_ipv4_test(STUNS_V4_DEFAULT[0]).await
        }
    } {
        if let Ok(mut public) = PUBLIC_ADDR.lock() {
            *public = addr.to_string();
        }
    }

    // Prefer UDP STUN-based NAT detection (reflects actual UDP hole-punch
    // behavior). Many NATs treat TCP and UDP differently — a device may be
    // "cone" for TCP but "symmetric" for UDP. The TCP-only detection below
    // (querying two hbbs ports) is kept as a fallback.
    //
    // Only use STUN to confirm SYMMETRIC (high confidence). For ASYMMETRIC /
    // inconclusive results, fall through to TCP-based detection to avoid
    // misclassifying due to STUN server unavailability.
    if let Ok(true) = detect_symmetric_nat().await {
        Config::set_nat_type(NatType::SYMMETRIC as _);
        log::info!("Tested nat type: SYMMETRIC (UDP STUN) in {:?}", start.elapsed());
        return Ok(true);
    }

    // Fallback: TCP-based NAT detection via hbbs (two different server ports).
    // This is less reliable for UDP hole-punching but works when UDP is blocked.
    let server1 = Config::get_rendezvous_server();
    let server2 = crate::increase_port(&server1, -1);
    let mut msg_out = RendezvousMessage::new();
    let serial = Config::get_serial();
    msg_out.set_test_nat_request(TestNatRequest {
        serial,
        ..Default::default()
    });
    let mut port1 = 0;
    let mut port2 = 0;
    let mut local_addr = None;
    for i in 0..2 {
        let server = if i == 0 { &*server1 } else { &*server2 };
        let mut socket =
            socket_client::connect_tcp_local(server, local_addr, CONNECT_TIMEOUT).await?;
        if i == 0 {
            // reuse the local addr is required for nat test
            local_addr = Some(socket.local_addr());
            Config::set_option(
                "local-ip-addr".to_owned(),
                socket.local_addr().ip().to_string(),
            );
        }
        socket.send(&msg_out).await?;
        if let Some(msg_in) = get_next_nonkeyexchange_msg(&mut socket, None).await {
            if let Some(rendezvous_message::Union::TestNatResponse(tnr)) = msg_in.union {
                log::debug!("Got nat response from {}: port={}", server, tnr.port);
                if i == 0 {
                    port1 = tnr.port;
                } else {
                    port2 = tnr.port;
                }
                if let Some(cu) = tnr.cu.as_ref() {
                    Config::set_option(
                        "rendezvous-servers".to_owned(),
                        cu.rendezvous_servers.join(","),
                    );
                    Config::set_serial(cu.serial);
                }
            }
        } else {
            break;
        }
    }
    let ok = port1 > 0 && port2 > 0;
    if ok {
        let t = if port1 == port2 {
            NatType::ASYMMETRIC
        } else {
            NatType::SYMMETRIC
        };
        Config::set_nat_type(t as _);
        log::info!("Tested nat type: {:?} in {:?}", t, start.elapsed());
    }
    Ok(ok)
}

pub async fn get_rendezvous_server(ms_timeout: u64) -> (String, Vec<String>, bool) {
    #[cfg(any(target_os = "android", target_os = "ios"))]
    let (mut a, mut b) = get_rendezvous_server_(ms_timeout);
    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    let (mut a, mut b) = get_rendezvous_server_(ms_timeout).await;
    #[cfg(windows)]
    if let Ok(lic) = crate::platform::get_license_from_exe_name() {
        if !lic.host.is_empty() {
            a = lic.host;
        }
    }
    let mut b: Vec<String> = b
        .drain(..)
        .map(|x| socket_client::check_port(x, config::RENDEZVOUS_PORT))
        .collect();
    let c = if b.contains(&a) {
        b = b.drain(..).filter(|x| x != &a).collect();
        true
    } else {
        a = b.pop().unwrap_or(a);
        false
    };
    (a, b, c)
}

#[inline]
#[cfg(any(target_os = "android", target_os = "ios"))]
fn get_rendezvous_server_(_ms_timeout: u64) -> (String, Vec<String>) {
    (
        Config::get_rendezvous_server(),
        Config::get_rendezvous_servers(),
    )
}

#[inline]
#[cfg(not(any(target_os = "android", target_os = "ios")))]
async fn get_rendezvous_server_(ms_timeout: u64) -> (String, Vec<String>) {
    crate::ipc::get_rendezvous_server(ms_timeout).await
}

#[inline]
#[cfg(any(target_os = "android", target_os = "ios"))]
pub async fn get_nat_type(_ms_timeout: u64) -> i32 {
    Config::get_nat_type()
}

#[inline]
#[cfg(not(any(target_os = "android", target_os = "ios")))]
pub async fn get_nat_type(ms_timeout: u64) -> i32 {
    crate::ipc::get_nat_type(ms_timeout).await
}

// used for client to test which server is faster in case stop-servic=Y
#[tokio::main(flavor = "current_thread")]
async fn test_rendezvous_server_() {
    let servers = Config::get_rendezvous_servers();
    if servers.len() <= 1 {
        return;
    }
    let mut futs = Vec::new();
    for host in servers {
        futs.push(tokio::spawn(async move {
            let tm = std::time::Instant::now();
            if socket_client::connect_tcp(
                crate::check_port(&host, RENDEZVOUS_PORT),
                CONNECT_TIMEOUT,
            )
            .await
            .is_ok()
            {
                let elapsed = tm.elapsed().as_micros();
                Config::update_latency(&host, elapsed as _);
            } else {
                Config::update_latency(&host, -1);
            }
        }));
    }
    join_all(futs).await;
    Config::reset_online();
}

// #[cfg(any(target_os = "android", target_os = "ios", feature = "cli"))]
pub fn test_rendezvous_server() {
    std::thread::spawn(test_rendezvous_server_);
}

pub fn refresh_rendezvous_server() {
    #[cfg(any(target_os = "android", target_os = "ios", feature = "cli"))]
    test_rendezvous_server();
    #[cfg(not(any(target_os = "android", target_os = "ios", feature = "cli")))]
    std::thread::spawn(|| {
        if crate::ipc::test_rendezvous_server().is_err() {
            test_rendezvous_server();
        }
    });
}

pub fn run_me<T: AsRef<std::ffi::OsStr>>(args: Vec<T>) -> std::io::Result<std::process::Child> {
    #[cfg(target_os = "linux")]
    if let Ok(appdir) = std::env::var("APPDIR") {
        let appimage_cmd = std::path::Path::new(&appdir).join("AppRun");
        if appimage_cmd.exists() {
            log::info!("path: {:?}", appimage_cmd);
            return std::process::Command::new(appimage_cmd).args(&args).spawn();
        }
    }
    let cmd = std::env::current_exe()?;
    let mut cmd = std::process::Command::new(cmd);
    #[cfg(windows)]
    let mut force_foreground = false;
    #[cfg(windows)]
    {
        let arg_strs = args
            .iter()
            .map(|x| x.as_ref().to_string_lossy())
            .collect::<Vec<_>>();
        if arg_strs == vec!["--install"] || arg_strs == &["--noinstall"] {
            cmd.env(crate::platform::SET_FOREGROUND_WINDOW, "1");
            force_foreground = true;
        }
    }
    let result = cmd.args(&args).spawn();
    match result.as_ref() {
        Ok(_child) =>
        {
            #[cfg(windows)]
            if force_foreground {
                unsafe { winapi::um::winuser::AllowSetForegroundWindow(_child.id() as u32) };
            }
        }
        Err(err) => log::error!("run_me: {err:?}"),
    }
    result
}

#[inline]
pub fn username() -> String {
    // fix bug of whoami
    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    return whoami::username().trim_end_matches('\0').to_owned();
    #[cfg(any(target_os = "android", target_os = "ios"))]
    return DEVICE_NAME.lock().unwrap().clone();
}

// Exactly the implementation of "whoami::hostname()".
// This wrapper is to suppress warnings.
#[inline(always)]
#[cfg(not(target_os = "ios"))]
pub fn whoami_hostname() -> String {
    let mut hostname = whoami::fallible::hostname().unwrap_or_else(|_| "localhost".to_string());
    hostname.make_ascii_lowercase();
    hostname
}

#[inline]
pub fn hostname() -> String {
    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    {
        #[allow(unused_mut)]
        let mut name = whoami_hostname();
        // some time, there is .local, some time not, so remove it for osx
        #[cfg(target_os = "macos")]
        if name.ends_with(".local") {
            name = name.trim_end_matches(".local").to_owned();
        }
        name
    }
    #[cfg(any(target_os = "android", target_os = "ios"))]
    return DEVICE_NAME.lock().unwrap().clone();
}

#[inline]
pub fn get_sysinfo() -> serde_json::Value {
    use hbb_common::sysinfo::System;
    let mut system = System::new();
    system.refresh_memory();
    system.refresh_cpu();
    let memory = system.total_memory();
    let memory = (memory as f64 / 1024. / 1024. / 1024. * 100.).round() / 100.;
    let cpus = system.cpus();
    let cpu_name = cpus.first().map(|x| x.brand()).unwrap_or_default();
    let cpu_name = cpu_name.trim_end();
    let cpu_freq = cpus.first().map(|x| x.frequency()).unwrap_or_default();
    let cpu_freq = (cpu_freq as f64 / 1024. * 100.).round() / 100.;
    let cpu = if cpu_freq > 0. {
        format!("{}, {}GHz, ", cpu_name, cpu_freq)
    } else {
        "".to_owned() // android
    };
    let num_cpus = num_cpus::get();
    let num_pcpus = num_cpus::get_physical();
    let mut os = system.distribution_id();
    os = format!("{} / {}", os, system.long_os_version().unwrap_or_default());
    #[cfg(windows)]
    {
        os = format!("{os} - {}", system.os_version().unwrap_or_default());
    }
    let hostname = hostname(); // sys.hostname() return localhost on android in my test
    #[cfg(any(target_os = "android", target_os = "ios"))]
    let out;
    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    let mut out;
    out = json!({
        "cpu": format!("{cpu}{num_cpus}/{num_pcpus} cores"),
        "memory": format!("{memory}GB"),
        "os": os,
        "hostname": hostname,
    });
    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    {
        let username = crate::platform::get_active_username();
        if !username.is_empty() && (!cfg!(windows) || username != "SYSTEM") {
            out["username"] = json!(username);
        }
    }
    out
}

#[inline]
pub fn check_port<T: std::string::ToString>(host: T, port: i32) -> String {
    hbb_common::socket_client::check_port(host, port)
}

#[inline]
pub fn increase_port<T: std::string::ToString>(host: T, offset: i32) -> String {
    hbb_common::socket_client::increase_port(host, offset)
}

pub const POSTFIX_SERVICE: &'static str = "_service";

#[inline]
pub fn is_control_key(evt: &KeyEvent, key: &ControlKey) -> bool {
    if let Some(key_event::Union::ControlKey(ck)) = evt.union {
        ck.value() == key.value()
    } else {
        false
    }
}

#[inline]
pub fn is_modifier(evt: &KeyEvent) -> bool {
    if let Some(key_event::Union::ControlKey(ck)) = evt.union {
        let v = ck.value();
        v == ControlKey::Alt.value()
            || v == ControlKey::Shift.value()
            || v == ControlKey::Control.value()
            || v == ControlKey::Meta.value()
            || v == ControlKey::RAlt.value()
            || v == ControlKey::RShift.value()
            || v == ControlKey::RControl.value()
            || v == ControlKey::RWin.value()
    } else {
        false
    }
}

pub fn check_software_update() {
    // Custom build: check API server for new version
    // Note: we skip the is_custom_client check here because even the standard
    // RustDesk build should be able to check for updates from the custom API server
    std::thread::spawn(|| {
        if let Err(e) = check_custom_update() {
            log::error!("Custom update check failed: {}", e);
        }
    });
}

#[tokio::main(flavor = "current_thread")]
pub async fn check_custom_update() -> hbb_common::ResultType<()> {
    let api_server = get_api_server(
        Config::get_option("api-server"),
        Config::get_option("custom-rendezvous-server"),
    );
    if api_server.is_empty() {
        return Ok(());
    }
    let platform = if cfg!(target_os = "windows") {
        "windows"
    } else if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "android") {
        "android"
    } else {
        "ubuntu"
    };
    let url = format!("{}/api/version/latest?platform={}", api_server, platform);
    let proxy_conf = Config::get_socks();
    let tls_url = get_url_for_tls(&url, &proxy_conf);
    let tls_type = get_cached_tls_type(tls_url);
    let is_tls_not_cached = tls_type.is_none();
    let tls_type = tls_type.unwrap_or(TlsType::Rustls);
    // api-server 为自签名证书（IP:端口，证书 CN 不匹配），更新检查固定接受无效证书。
    // 原先取 TLS 缓存值：若缓存被其他请求写为 false（create_http_client 成功后的
    // upsert_tls_cache(_, _, unwrap_or(false))），会按 false 校验证书 → 自签名证书
    // 校验失败；且此路径没有心跳 post_request_ 那样的 danger=true 重试，一失败就
    // 直接返回错误，表现为"更新提示证书校验失败"。
    // 固定 true 与 is_custom_client 的下载路径（create_http_client(_, true)）一致，
    // 代价仅可能与 danger=false 的连接各占一条缓存，功能优先。
    let danger = true;
    let client = create_http_client_async(tls_type, danger);
    let resp = match client.get(&url).send().await {
        Ok(r) => r,
        Err(_) if is_tls_not_cached => {
            let client = create_http_client_async(TlsType::NativeTls, danger);
            client.get(&url).send().await?
        }
        Err(e) => return Err(e.into()),
    };
    let bytes = resp.bytes().await?;
    let json: serde_json::Value = serde_json::from_slice(&bytes)?;
    let latest_version = json["data"]["version"].as_str().unwrap_or("").to_string();
    let download_url = json["data"]["url"].as_str().unwrap_or("").to_string();
    let force_update = json["data"]["force_update"].as_bool().unwrap_or(false);
    if latest_version.is_empty() || download_url.is_empty() {
        return Ok(());
    }
    if get_version_number(&latest_version) > get_version_number(crate::VERSION) {
        if force_update {
            // Force update: auto-download and install without user prompt
            log::info!("Force update to version {} from {}", latest_version, download_url);
            #[cfg(not(any(target_os = "android", target_os = "ios")))]
            if let Some(file_path) = crate::updater::get_download_file_from_url(&download_url) {
                // 下载前清理历史更新下载残留（保留本次文件名对应的缓存文件）
                crate::updater::cleanup_old_update_files(Some(&file_path));
                // Download using the same HTTP client
                if let Ok(resp) = client.get(&download_url).send().await {
                    if let Ok(bytes) = resp.bytes().await {
                        if std::fs::write(&file_path, &bytes).is_ok() {
                            log::info!("Force update: downloaded to {:?}", file_path);
                            if let Some(path_str) = file_path.to_str() {
                                // Try to install via service IPC first (no UAC prompt on Windows).
                                // Fallback to direct update if IPC fails.
                                if let Ok(mut stream) = crate::ipc::connect(2000, "").await {
                                    let _ = stream.send(&crate::ipc::Data::InstallUpdate(path_str.to_owned())).await;
                                    log::info!("Force update: sent install command to service");
                                } else {
                                    log::info!("Force update: service IPC failed, running directly");
                                    let _ = crate::platform::update_to(path_str);
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // Normal update: notify Flutter to show dialog
            #[cfg(feature = "flutter")]
            {
                let mut m = std::collections::HashMap::new();
                m.insert("name", "check_software_update_finish");
                m.insert("url", &download_url);
                if let Ok(data) = serde_json::to_string(&m) {
                    let _ = crate::flutter::push_global_event(crate::flutter::APP_TYPE_MAIN, data);
                }
            }
        }
        *SOFTWARE_UPDATE_URL.lock().unwrap() = download_url;
        *SOFTWARE_UPDATE_VERSION.lock().unwrap() = latest_version;
    } else {
        *SOFTWARE_UPDATE_URL.lock().unwrap() = "".to_string();
        *SOFTWARE_UPDATE_VERSION.lock().unwrap() = "".to_string();
        // 无新版本时也要发一次结束事件：设置页的"检查更新"按钮需要确定的
        // 结束信号，否则用户点了按钮只能靠超时才知道结果。
        #[cfg(feature = "flutter")]
        {
            let mut m = std::collections::HashMap::new();
            m.insert("name", "check_software_update_finish");
            m.insert("url", "");
            if let Ok(data) = serde_json::to_string(&m) {
                let _ = crate::flutter::push_global_event(crate::flutter::APP_TYPE_MAIN, data);
            }
        }
    }
    Ok(())
}

// No need to check `danger_accept_invalid_cert` for now.
// Because the url is always `https://api.rustdesk.com/version/latest`.
#[tokio::main(flavor = "current_thread")]
pub async fn do_check_software_update() -> hbb_common::ResultType<()> {
    let (request, url) =
        hbb_common::version_check_request(hbb_common::VER_TYPE_RUSTDESK_CLIENT.to_string());
    let proxy_conf = Config::get_socks();
    let tls_url = get_url_for_tls(&url, &proxy_conf);
    let tls_type = get_cached_tls_type(tls_url);
    let is_tls_not_cached = tls_type.is_none();
    let tls_type = tls_type.unwrap_or(TlsType::Rustls);
    let client = create_http_client_async(tls_type, false);
    let latest_release_response = match client.post(&url).json(&request).send().await {
        Ok(resp) => {
            upsert_tls_cache(tls_url, tls_type, false);
            resp
        }
        Err(err) => {
            if is_tls_not_cached && err.is_request() {
                let tls_type = TlsType::NativeTls;
                let client = create_http_client_async(tls_type, false);
                let resp = client.post(&url).json(&request).send().await?;
                upsert_tls_cache(tls_url, tls_type, false);
                resp
            } else {
                return Err(err.into());
            }
        }
    };
    let bytes = latest_release_response.bytes().await?;
    let resp: hbb_common::VersionCheckResponse = serde_json::from_slice(&bytes)?;
    let response_url = resp.url;
    let latest_release_version = response_url.rsplit('/').next().unwrap_or_default();

    if get_version_number(&latest_release_version) > get_version_number(crate::VERSION) {
        #[cfg(feature = "flutter")]
        {
            let mut m = HashMap::new();
            m.insert("name", "check_software_update_finish");
            m.insert("url", &response_url);
            if let Ok(data) = serde_json::to_string(&m) {
                let _ = crate::flutter::push_global_event(crate::flutter::APP_TYPE_MAIN, data);
            }
        }
        *SOFTWARE_UPDATE_URL.lock().unwrap() = response_url;
    } else {
        *SOFTWARE_UPDATE_URL.lock().unwrap() = "".to_string();
    }
    Ok(())
}

#[inline]
pub fn get_app_name() -> String {
    hbb_common::config::APP_NAME.read().unwrap().clone()
}

#[inline]
pub fn is_rustdesk() -> bool {
    hbb_common::config::APP_NAME.read().unwrap().eq("RustDesk")
}

#[inline]
pub fn get_uri_prefix() -> String {
    format!("{}://", get_app_name().to_lowercase())
}

#[cfg(target_os = "macos")]
pub fn get_full_name() -> String {
    format!(
        "{}.{}",
        hbb_common::config::ORG.read().unwrap(),
        hbb_common::config::APP_NAME.read().unwrap(),
    )
}

pub fn is_setup(name: &str) -> bool {
    name.to_lowercase().ends_with("install.exe")
}

pub fn get_custom_rendezvous_server(custom: String) -> String {
    #[cfg(windows)]
    if let Ok(lic) = crate::platform::windows::get_license_from_exe_name() {
        if !lic.host.is_empty() {
            return lic.host.clone();
        }
    }
    if !custom.is_empty() {
        return custom;
    }
    if !config::PROD_RENDEZVOUS_SERVER.read().unwrap().is_empty() {
        return config::PROD_RENDEZVOUS_SERVER.read().unwrap().clone();
    }
    "".to_owned()
}

#[inline]
pub fn get_api_server(api: String, custom: String) -> String {
    if Config::no_register_device() {
        return "".to_owned();
    }
    let mut res = get_api_server_(api, custom);
    if res.ends_with('/') {
        res.pop();
    }
    if res.starts_with("https")
        && res.ends_with(":21114")
        && get_builtin_option(keys::OPTION_ALLOW_HTTPS_21114) != "Y"
    {
        return res.replace(":21114", "");
    }
    res
}

fn get_api_server_(api: String, custom: String) -> String {
    #[cfg(windows)]
    if let Ok(lic) = crate::platform::windows::get_license_from_exe_name() {
        if !lic.api.is_empty() {
            return lic.api.clone();
        }
    }
    if !api.is_empty() {
        return api.to_owned();
    }
    let s0 = get_custom_rendezvous_server(custom);
    if !s0.is_empty() {
        let s = crate::increase_port(&s0, -2);
        if s == s0 {
            return format!("http://{}:{}", s, config::RENDEZVOUS_PORT - 2);
        } else {
            return format!("http://{}", s);
        }
    }
    config::API_SERVER_DEFAULT.to_owned()
}

#[inline]
pub fn is_public(url: &str) -> bool {
    let url = url.to_ascii_lowercase();
    url.contains("rustdesk.com/") || url.ends_with("rustdesk.com")
}

pub fn get_udp_punch_enabled() -> bool {
    config::option2bool(
        keys::OPTION_ENABLE_UDP_PUNCH,
        &get_local_option(keys::OPTION_ENABLE_UDP_PUNCH),
    )
}

pub fn get_ipv6_punch_enabled() -> bool {
    config::option2bool(
        keys::OPTION_ENABLE_IPV6_PUNCH,
        &get_local_option(keys::OPTION_ENABLE_IPV6_PUNCH),
    )
}

pub fn get_local_option(key: &str) -> String {
    let v = LocalConfig::get_option(key);
    if key == keys::OPTION_ENABLE_UDP_PUNCH || key == keys::OPTION_ENABLE_IPV6_PUNCH {
        if v.is_empty() {
            return "Y".to_owned(); // Enable by default for both public and custom servers
        }
    }
    v
}

pub fn get_audit_server(api: String, custom: String, typ: String) -> String {
    let url = get_api_server(api, custom);
    if url.is_empty() || is_public(&url) {
        return "".to_owned();
    }
    format!("{}/api/audit/{}", url, typ)
}

/// Check if we should use raw TCP proxy for API calls.
/// Returns true if USE_RAW_TCP_FOR_API builtin option is "Y", WebSocket is off,
/// and the target URL belongs to the configured non-public API host.
#[inline]
fn should_use_raw_tcp_for_api(url: &str) -> bool {
    get_builtin_option(keys::OPTION_USE_RAW_TCP_FOR_API) == "Y"
        && !use_ws()
        && is_tcp_proxy_api_target(url)
}

/// Check if we can attempt raw TCP proxy fallback for this target URL.
#[inline]
fn can_fallback_to_raw_tcp(url: &str) -> bool {
    !use_ws() && is_tcp_proxy_api_target(url)
}

#[inline]
fn should_use_tcp_proxy_for_api_url(url: &str, api_url: &str) -> bool {
    if api_url.is_empty() || is_public(api_url) {
        return false;
    }

    let target_host = url::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(|host| host.to_ascii_lowercase()));
    let api_host = url::Url::parse(api_url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(|host| host.to_ascii_lowercase()));

    matches!((target_host, api_host), (Some(target), Some(api)) if target == api)
}

#[inline]
fn is_tcp_proxy_api_target(url: &str) -> bool {
    should_use_tcp_proxy_for_api_url(url, &ui_get_api_server())
}

fn tcp_proxy_log_target(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .map(|parsed| {
            let mut redacted = format!("{}://", parsed.scheme());
            let Some(host) = parsed.host() else {
                return "<invalid-url>".to_owned();
            };
            redacted.push_str(&host.to_string());
            if let Some(port) = parsed.port() {
                redacted.push(':');
                redacted.push_str(&port.to_string());
            }
            redacted.push_str(parsed.path());
            redacted
        })
        .unwrap_or_else(|| "<invalid-url>".to_owned())
}

#[inline]
fn get_tcp_proxy_addr() -> String {
    check_port(Config::get_rendezvous_server(), RENDEZVOUS_PORT)
}

/// Send an HTTP request via the rendezvous server's TCP proxy using protobuf.
/// Connects with `connect_tcp` + `secure_tcp`, sends `HttpProxyRequest`,
/// receives `HttpProxyResponse`.
///
/// The entire operation (connect + handshake + send + receive) is wrapped in
/// an overall timeout of `CONNECT_TIMEOUT + READ_TIMEOUT` so that a stall at
/// any stage cannot block the caller indefinitely.
async fn tcp_proxy_request(
    method: &str,
    url: &str,
    body: &[u8],
    headers: Vec<HeaderEntry>,
) -> ResultType<HttpProxyResponse> {
    let tcp_addr = get_tcp_proxy_addr();
    if tcp_addr.is_empty() {
        bail!("No rendezvous server configured for TCP proxy");
    }

    let parsed = url::Url::parse(url)?;
    let path = if let Some(query) = parsed.query() {
        format!("{}?{}", parsed.path(), query)
    } else {
        parsed.path().to_string()
    };

    log::debug!(
        "Sending {} {} via TCP proxy to {}",
        method,
        parsed.path(),
        tcp_addr
    );

    let overall_timeout = CONNECT_TIMEOUT + READ_TIMEOUT;
    timeout(overall_timeout, async {
        let mut conn = socket_client::connect_tcp(&*tcp_addr, CONNECT_TIMEOUT).await?;
        let key = crate::get_key(true).await;
        secure_tcp_silent(&mut conn, &key).await?;

        let mut req = HttpProxyRequest::new();
        req.method = method.to_uppercase();
        req.path = path;
        req.headers = headers.into();
        req.body = Bytes::from(body.to_vec());

        let mut msg_out = RendezvousMessage::new();
        msg_out.set_http_proxy_request(req);
        conn.send(&msg_out).await?;

        match conn.next().await {
            Some(Ok(bytes)) => {
                let msg_in = RendezvousMessage::parse_from_bytes(&bytes)?;
                match msg_in.union {
                    Some(rendezvous_message::Union::HttpProxyResponse(resp)) => Ok(resp),
                    _ => bail!("Unexpected response from TCP proxy"),
                }
            }
            Some(Err(e)) => bail!("TCP proxy read error: {}", e),
            None => bail!("TCP proxy connection closed without response"),
        }
    })
    .await?
}

/// Build HeaderEntry list from "Key: Value" style header string (used by post_request).
/// If the caller supplies a Content-Type header it overrides the default `application/json`.
fn parse_simple_header(header: &str) -> Vec<HeaderEntry> {
    let mut entries = Vec::new();
    let mut has_content_type = false;
    if !header.is_empty() {
        let tmp: Vec<&str> = header.splitn(2, ": ").collect();
        if tmp.len() == 2 {
            if tmp[0].eq_ignore_ascii_case("Content-Type") {
                has_content_type = true;
            }
            entries.push(HeaderEntry {
                name: tmp[0].into(),
                value: tmp[1].into(),
                ..Default::default()
            });
        }
    }
    if !has_content_type {
        entries.insert(
            0,
            HeaderEntry {
                name: "Content-Type".into(),
                value: "application/json".into(),
                ..Default::default()
            },
        );
    }
    entries
}

/// POST request via TCP proxy.
async fn post_request_via_tcp_proxy(url: &str, body: &str, header: &str) -> ResultType<String> {
    let headers = parse_simple_header(header);
    let resp = tcp_proxy_request("POST", url, body.as_bytes(), headers).await?;
    if !resp.error.is_empty() {
        bail!("TCP proxy error: {}", resp.error);
    }
    Ok(String::from_utf8_lossy(&resp.body).to_string())
}

fn http_proxy_response_to_json(resp: HttpProxyResponse) -> ResultType<String> {
    if !resp.error.is_empty() {
        bail!("TCP proxy error: {}", resp.error);
    }

    let mut response_headers = Map::new();
    for entry in resp.headers.iter() {
        response_headers.insert(entry.name.to_lowercase(), json!(entry.value));
    }

    let mut result = Map::new();
    result.insert("status_code".to_string(), json!(resp.status));
    result.insert("headers".to_string(), Value::Object(response_headers));
    result.insert(
        "body".to_string(),
        json!(String::from_utf8_lossy(&resp.body)),
    );

    serde_json::to_string(&result).map_err(|e| anyhow!("Failed to serialize response: {}", e))
}

fn parse_json_header_entries(header: &str) -> ResultType<Vec<HeaderEntry>> {
    let v: Value = serde_json::from_str(header)?;
    if let Value::Object(obj) = v {
        Ok(obj
            .iter()
            .map(|(key, value)| HeaderEntry {
                name: key.clone(),
                value: value.as_str().unwrap_or_default().into(),
                ..Default::default()
            })
            .collect())
    } else {
        Err(anyhow!("HTTP header information parsing failed!"))
    }
}

/// Returns (status_code, body_text). Separating status so the wrapper can decide on fallback.
async fn post_request_http(url: &str, body: &str, header: &str) -> ResultType<(u16, String)> {
    let proxy_conf = Config::get_socks();
    let tls_url = get_url_for_tls(url, &proxy_conf);
    let tls_type = get_cached_tls_type(tls_url);
    let danger_accept_invalid_cert = get_cached_tls_accept_invalid_cert(tls_url);
    let response = post_request_(
        url,
        tls_url,
        body.to_owned(),
        header,
        tls_type,
        danger_accept_invalid_cert,
        danger_accept_invalid_cert,
    )
    .await?;
    let status = response.status().as_u16();
    let text = response.text().await?;
    Ok((status, text))
}

/// Try `http_fn` first; on connection failure or 5xx, fall back to `tcp_fn`
/// if the URL is eligible. 4xx responses are returned as-is.
async fn with_tcp_proxy_fallback<HttpFut, TcpFut>(
    url: &str,
    method: &str,
    http_fn: HttpFut,
    tcp_fn: TcpFut,
) -> ResultType<String>
where
    HttpFut: Future<Output = ResultType<(u16, String)>>,
    TcpFut: Future<Output = ResultType<String>>,
{
    if should_use_raw_tcp_for_api(url) {
        return tcp_fn.await;
    }

    let http_result = http_fn.await;
    let should_fallback = match &http_result {
        Err(_) => true,
        Ok((status, _)) => *status >= 500,
    };

    if should_fallback && can_fallback_to_raw_tcp(url) {
        log::warn!(
            "HTTP {} to {} failed or 5xx (result: {:?}), trying TCP proxy fallback",
            method,
            tcp_proxy_log_target(url),
            http_result
                .as_ref()
                .map(|(s, _)| *s)
                .map_err(|e| e.to_string()),
        );
        match tcp_fn.await {
            Ok(resp) => return Ok(resp),
            Err(tcp_err) => {
                log::warn!("TCP proxy fallback also failed: {:?}", tcp_err);
            }
        }
    }

    http_result.map(|(_status, text)| text)
}

/// POST request with raw TCP proxy support.
/// - If `USE_RAW_TCP_FOR_API` is "Y" and WS is off, goes directly through TCP proxy.
/// - Otherwise tries HTTP first; on connection failure or 5xx status,
///   falls back to TCP proxy if WS is off.
/// - 4xx responses are returned as-is (server is reachable, business logic error).
/// - If fallback also fails, returns the original HTTP result (text or error).
pub async fn post_request(url: String, body: String, header: &str) -> ResultType<String> {
    with_tcp_proxy_fallback(
        &url,
        "POST",
        post_request_http(&url, &body, header),
        post_request_via_tcp_proxy(&url, &body, header),
    )
    .await
}

#[async_recursion]
async fn post_request_(
    url: &str,
    tls_url: &str,
    body: String,
    header: &str,
    tls_type: Option<TlsType>,
    danger_accept_invalid_cert: Option<bool>,
    original_danger_accept_invalid_cert: Option<bool>,
) -> ResultType<reqwest::Response> {
    let mut req = create_http_client_async(
        tls_type.unwrap_or(TlsType::Rustls),
        danger_accept_invalid_cert.unwrap_or(false),
    )
    .post(url);
    if !header.is_empty() {
        let tmp: Vec<&str> = header.split(": ").collect();
        if tmp.len() == 2 {
            req = req.header(tmp[0], tmp[1]);
        }
    }
    req = req.header("Content-Type", "application/json");
    let to = std::time::Duration::from_secs(12);
    if tls_type.is_some() && danger_accept_invalid_cert.is_some() {
        // This branch is used to reduce a `clone()` when both `tls_type` and
        // `danger_accept_invalid_cert` are cached.
        match req.body(body.clone()).timeout(to).send().await {
            Ok(resp) => {
                upsert_tls_cache(
                    tls_url,
                    tls_type.unwrap_or(TlsType::Rustls),
                    danger_accept_invalid_cert.unwrap_or(false),
                );
                Ok(resp)
            }
            Err(e) => Err(anyhow!("{:?}", e)),
        }
    } else {
        match req.body(body.clone()).timeout(to).send().await {
            Ok(resp) => {
                upsert_tls_cache(
                    tls_url,
                    tls_type.unwrap_or(TlsType::Rustls),
                    danger_accept_invalid_cert.unwrap_or(false),
                );
                Ok(resp)
            }
            Err(e) => {
                if (tls_type.is_none() || danger_accept_invalid_cert.is_none()) && e.is_request() {
                    if danger_accept_invalid_cert.is_none() {
                        log::warn!(
                            "HTTP request failed: {:?}, try again, danger accept invalid cert",
                            e
                        );
                        post_request_(
                            url,
                            tls_url,
                            body,
                            header,
                            tls_type,
                            Some(true),
                            original_danger_accept_invalid_cert,
                        )
                        .await
                    } else {
                        log::warn!("HTTP request failed: {:?}, try again with native-tls", e);
                        post_request_(
                            url,
                            tls_url,
                            body,
                            header,
                            Some(TlsType::NativeTls),
                            original_danger_accept_invalid_cert,
                            original_danger_accept_invalid_cert,
                        )
                        .await
                    }
                } else {
                    Err(anyhow!("{:?}", e))
                }
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
pub async fn post_request_sync(url: String, body: String, header: &str) -> ResultType<String> {
    post_request(url, body, header).await
}

#[async_recursion]
async fn get_http_response_async(
    url: &str,
    tls_url: &str,
    method: &str,
    body: Option<String>,
    header: &str,
    tls_type: Option<TlsType>,
    danger_accept_invalid_cert: Option<bool>,
    original_danger_accept_invalid_cert: Option<bool>,
) -> ResultType<reqwest::Response> {
    let http_client = create_http_client_async(
        tls_type.unwrap_or(TlsType::Rustls),
        danger_accept_invalid_cert.unwrap_or(false),
    );
    let normalized_method = method.to_ascii_lowercase();
    let mut http_client = match normalized_method.as_str() {
        "get" => http_client.get(url),
        "post" => http_client.post(url),
        "put" => http_client.put(url),
        "delete" => http_client.delete(url),
        _ => return Err(anyhow!("The HTTP request method is not supported!")),
    };
    for entry in parse_json_header_entries(header)? {
        http_client = http_client.header(entry.name, entry.value);
    }

    if tls_type.is_some() && danger_accept_invalid_cert.is_some() {
        if let Some(b) = body {
            http_client = http_client.body(b);
        }
        match http_client
            .timeout(std::time::Duration::from_secs(12))
            .send()
            .await
        {
            Ok(resp) => {
                upsert_tls_cache(
                    tls_url,
                    tls_type.unwrap_or(TlsType::Rustls),
                    danger_accept_invalid_cert.unwrap_or(false),
                );
                Ok(resp)
            }
            Err(e) => Err(anyhow!("{:?}", e)),
        }
    } else {
        if let Some(b) = body.clone() {
            http_client = http_client.body(b);
        }

        match http_client
            .timeout(std::time::Duration::from_secs(12))
            .send()
            .await
        {
            Ok(resp) => {
                upsert_tls_cache(
                    tls_url,
                    tls_type.unwrap_or(TlsType::Rustls),
                    danger_accept_invalid_cert.unwrap_or(false),
                );
                Ok(resp)
            }
            Err(e) => {
                if (tls_type.is_none() || danger_accept_invalid_cert.is_none()) && e.is_request() {
                    if danger_accept_invalid_cert.is_none() {
                        log::warn!(
                            "HTTP request failed: {:?}, try again, danger accept invalid cert",
                            e
                        );
                        get_http_response_async(
                            url,
                            tls_url,
                            method,
                            body,
                            header,
                            tls_type,
                            Some(true),
                            original_danger_accept_invalid_cert,
                        )
                        .await
                    } else {
                        log::warn!("HTTP request failed: {:?}, try again with native-tls", e);
                        get_http_response_async(
                            url,
                            tls_url,
                            method,
                            body,
                            header,
                            Some(TlsType::NativeTls),
                            original_danger_accept_invalid_cert,
                            original_danger_accept_invalid_cert,
                        )
                        .await
                    }
                } else {
                    Err(anyhow!("{:?}", e))
                }
            }
        }
    }
}

/// Returns (status_code, json_string) so the caller can inspect the status
/// without re-parsing the serialized JSON.
async fn http_request_http(
    url: &str,
    method: &str,
    body: Option<String>,
    header: &str,
) -> ResultType<(u16, String)> {
    let proxy_conf = Config::get_socks();
    let tls_url = get_url_for_tls(url, &proxy_conf);
    let tls_type = get_cached_tls_type(tls_url);
    let danger_accept_invalid_cert = get_cached_tls_accept_invalid_cert(tls_url);
    let response = get_http_response_async(
        url,
        tls_url,
        method,
        body,
        header,
        tls_type,
        danger_accept_invalid_cert,
        danger_accept_invalid_cert,
    )
    .await?;
    // Serialize response headers
    let mut response_headers = Map::new();
    for (key, value) in response.headers() {
        response_headers.insert(key.to_string(), json!(value.to_str().unwrap_or("")));
    }

    let status_code = response.status().as_u16();
    let response_body = response.text().await?;

    // Construct the JSON object
    let mut result = Map::new();
    result.insert("status_code".to_string(), json!(status_code));
    result.insert("headers".to_string(), Value::Object(response_headers));
    result.insert("body".to_string(), json!(response_body));

    // Convert map to JSON string
    let json_str = serde_json::to_string(&result)
        .map_err(|e| anyhow!("Failed to serialize response: {}", e))?;
    Ok((status_code, json_str))
}

/// HTTP request with raw TCP proxy support.
#[tokio::main(flavor = "current_thread")]
pub async fn http_request_sync(
    url: String,
    method: String,
    body: Option<String>,
    header: String,
) -> ResultType<String> {
    with_tcp_proxy_fallback(
        &url,
        &method,
        http_request_http(&url, &method, body.clone(), &header),
        http_request_via_tcp_proxy(&url, &method, body.as_deref(), &header),
    )
    .await
}

/// General HTTP request via TCP proxy. Header is a JSON string (used by http_request_sync).
/// Returns a JSON string with status_code, headers, body (same format as http_request_sync).
async fn http_request_via_tcp_proxy(
    url: &str,
    method: &str,
    body: Option<&str>,
    header: &str,
) -> ResultType<String> {
    let headers = parse_json_header_entries(header)?;
    let body_bytes = body.unwrap_or("").as_bytes();

    let resp = tcp_proxy_request(method, url, body_bytes, headers).await?;
    http_proxy_response_to_json(resp)
}

#[inline]
pub fn make_privacy_mode_msg_with_details(
    state: back_notification::PrivacyModeState,
    details: String,
    impl_key: String,
) -> Message {
    let mut misc = Misc::new();
    let mut back_notification = BackNotification {
        details,
        impl_key,
        ..Default::default()
    };
    back_notification.set_privacy_mode_state(state);
    misc.set_back_notification(back_notification);
    let mut msg_out = Message::new();
    msg_out.set_misc(misc);
    msg_out
}

#[inline]
pub fn make_privacy_mode_msg(
    state: back_notification::PrivacyModeState,
    impl_key: String,
) -> Message {
    make_privacy_mode_msg_with_details(state, "".to_owned(), impl_key)
}

pub fn is_keyboard_mode_supported(
    keyboard_mode: &KeyboardMode,
    version_number: i64,
    peer_platform: &str,
) -> bool {
    match keyboard_mode {
        KeyboardMode::Legacy => true,
        KeyboardMode::Map => {
            if peer_platform.to_lowercase() == crate::PLATFORM_ANDROID.to_lowercase() {
                false
            } else {
                version_number >= hbb_common::get_version_number("1.2.0")
            }
        }
        KeyboardMode::Translate => version_number >= hbb_common::get_version_number("1.2.0"),
        KeyboardMode::Auto => version_number >= hbb_common::get_version_number("1.2.0"),
    }
}

pub fn get_supported_keyboard_modes(version: i64, peer_platform: &str) -> Vec<KeyboardMode> {
    KeyboardMode::iter()
        .filter(|&mode| is_keyboard_mode_supported(mode, version, peer_platform))
        .map(|&mode| mode)
        .collect::<Vec<_>>()
}

pub fn make_fd_to_json(id: i32, path: String, entries: &Vec<FileEntry>) -> String {
    let fd_json = _make_fd_to_json(id, path, entries);
    serde_json::to_string(&fd_json).unwrap_or("".into())
}

pub fn _make_fd_to_json(id: i32, path: String, entries: &Vec<FileEntry>) -> Map<String, Value> {
    let mut fd_json = serde_json::Map::new();
    fd_json.insert("id".into(), json!(id));
    fd_json.insert("path".into(), json!(path));

    let mut entries_out = vec![];
    for entry in entries {
        let mut entry_map = serde_json::Map::new();
        entry_map.insert("entry_type".into(), json!(entry.entry_type.value()));
        entry_map.insert("name".into(), json!(entry.name));
        entry_map.insert("size".into(), json!(entry.size));
        entry_map.insert("modified_time".into(), json!(entry.modified_time));
        entries_out.push(entry_map);
    }
    fd_json.insert("entries".into(), json!(entries_out));
    fd_json
}

pub fn make_vec_fd_to_json(fds: &[FileDirectory]) -> String {
    let mut fd_jsons = vec![];

    for fd in fds.iter() {
        let fd_json = _make_fd_to_json(fd.id, fd.path.clone(), &fd.entries);
        fd_jsons.push(fd_json);
    }

    serde_json::to_string(&fd_jsons).unwrap_or("".into())
}

pub fn make_empty_dirs_response_to_json(res: &ReadEmptyDirsResponse) -> String {
    let mut map: Map<String, Value> = serde_json::Map::new();
    map.insert("path".into(), json!(res.path));

    let mut fd_jsons = vec![];

    for fd in res.empty_dirs.iter() {
        let fd_json = _make_fd_to_json(fd.id, fd.path.clone(), &fd.entries);
        fd_jsons.push(fd_json);
    }
    map.insert("empty_dirs".into(), fd_jsons.into());

    serde_json::to_string(&map).unwrap_or("".into())
}

/// The function to handle the url scheme sent by the system.
///
/// 1. Try to send the url scheme from ipc.
/// 2. If failed to send the url scheme, we open a new main window to handle this url scheme.
pub fn handle_url_scheme(url: String) {
    #[cfg(not(target_os = "ios"))]
    if let Err(err) = crate::ipc::send_url_scheme(url.clone()) {
        log::debug!("Send the url to the existing flutter process failed, {}. Let's open a new program to handle this.", err);
        let _ = crate::run_me(vec![url]);
    }
}

#[inline]
pub fn encode64<T: AsRef<[u8]>>(input: T) -> String {
    #[allow(deprecated)]
    base64::encode(input)
}

#[inline]
pub fn decode64<T: AsRef<[u8]>>(input: T) -> Result<Vec<u8>, base64::DecodeError> {
    #[allow(deprecated)]
    base64::decode(input)
}

pub async fn get_key(sync: bool) -> String {
    #[cfg(windows)]
    if let Ok(lic) = crate::platform::windows::get_license_from_exe_name() {
        if !lic.key.is_empty() {
            return lic.key;
        }
    }
    #[cfg(target_os = "ios")]
    let mut key = Config::get_option("key");
    #[cfg(not(target_os = "ios"))]
    let mut key = if sync {
        Config::get_option("key")
    } else {
        let mut options = crate::ipc::get_options_async().await;
        options.remove("key").unwrap_or_default()
    };
    if key.is_empty() {
        key = config::RS_PUB_KEY.to_owned();
    }
    key
}

pub fn pk_to_fingerprint(pk: Vec<u8>) -> String {
    let s: String = pk.iter().map(|u| format!("{:02x}", u)).collect();
    s.chars()
        .enumerate()
        .map(|(i, c)| {
            if i > 0 && i % 4 == 0 {
                format!(" {}", c)
            } else {
                format!("{}", c)
            }
        })
        .collect()
}

#[inline]
pub async fn get_next_nonkeyexchange_msg(
    conn: &mut Stream,
    timeout: Option<u64>,
) -> Option<RendezvousMessage> {
    let timeout = timeout.unwrap_or(READ_TIMEOUT);
    for _ in 0..2 {
        if let Some(Ok(bytes)) = conn.next_timeout(timeout).await {
            if let Ok(msg_in) = RendezvousMessage::parse_from_bytes(&bytes) {
                match &msg_in.union {
                    Some(rendezvous_message::Union::KeyExchange(_)) => {
                        continue;
                    }
                    _ => {
                        return Some(msg_in);
                    }
                }
            }
        }
        break;
    }
    None
}

#[cfg(all(target_os = "windows", not(target_pointer_width = "64")))]
pub fn check_process(arg: &str, same_session_id: bool) -> bool {
    let mut path = std::env::current_exe().unwrap_or_default();
    if let Ok(linked) = path.read_link() {
        path = linked;
    }
    let Some(filename) = path.file_name() else {
        return false;
    };
    let filename = filename.to_string_lossy().to_string();
    match crate::platform::windows::get_pids_with_first_arg_check_session(
        &filename,
        arg,
        same_session_id,
    ) {
        Ok(pids) => {
            let self_pid = hbb_common::sysinfo::Pid::from_u32(std::process::id());
            pids.into_iter().filter(|pid| *pid != self_pid).count() > 0
        }
        Err(e) => {
            log::error!("Failed to check process with arg: \"{}\", {}", arg, e);
            false
        }
    }
}

#[allow(unused_mut)]
#[cfg(not(all(target_os = "windows", not(target_pointer_width = "64"))))]
#[cfg(not(any(target_os = "android", target_os = "ios")))]
pub fn check_process(arg: &str, mut same_uid: bool) -> bool {
    #[cfg(target_os = "macos")]
    if !crate::platform::is_root() && !same_uid {
        log::warn!("Can not get other process's command line arguments on macos without root");
        same_uid = true;
    }
    use hbb_common::sysinfo::System;
    let mut sys = System::new();
    sys.refresh_processes();
    let mut path = std::env::current_exe().unwrap_or_default();
    if let Ok(linked) = path.read_link() {
        path = linked;
    }
    let path = path.to_string_lossy().to_lowercase();
    let my_uid = sys
        .process((std::process::id() as usize).into())
        .map(|x| x.user_id())
        .unwrap_or_default();
    for (_, p) in sys.processes().iter() {
        let mut cur_path = p.exe().to_path_buf();
        if let Ok(linked) = cur_path.read_link() {
            cur_path = linked;
        }
        if cur_path.to_string_lossy().to_lowercase() != path {
            continue;
        }
        if p.pid().to_string() == std::process::id().to_string() {
            continue;
        }
        if same_uid && p.user_id() != my_uid {
            continue;
        }
        // on mac, p.cmd() get "/Applications/RustDesk.app/Contents/MacOS/RustDesk", "XPC_SERVICE_NAME=com.carriez.RustDesk_server"
        let parg = if p.cmd().len() <= 1 { "" } else { &p.cmd()[1] };
        if arg.is_empty() {
            if !parg.starts_with("--") {
                return true;
            }
        } else if arg == parg {
            return true;
        }
    }
    false
}

async fn secure_tcp_impl(conn: &mut Stream, key: &str, log_on_success: bool) -> ResultType<()> {
    // Skip additional encryption when using WebSocket connections (wss://)
    // as WebSocket Secure (wss://) already provides transport layer encryption.
    // This doesn't affect the end-to-end encryption between clients,
    // it only avoids redundant encryption between client and server.
    if use_ws() {
        return Ok(());
    }
    let rs_pk = get_rs_pk(key);
    let Some(rs_pk) = rs_pk else {
        bail!("Handshake failed: invalid public key from rendezvous server");
    };
    match timeout(READ_TIMEOUT, conn.next()).await? {
        Some(Ok(bytes)) => {
            if let Ok(msg_in) = RendezvousMessage::parse_from_bytes(&bytes) {
                match msg_in.union {
                    Some(rendezvous_message::Union::KeyExchange(ex)) => {
                        if ex.keys.len() != 1 {
                            bail!("Handshake failed: invalid key exchange message");
                        }
                        let their_pk_b = sign::verify(&ex.keys[0], &rs_pk)
                            .map_err(|_| anyhow!("Signature mismatch in key exchange"))?;
                        let (asymmetric_value, symmetric_value, key) = create_symmetric_key_msg(
                            get_pk(&their_pk_b)
                                .context("Wrong their public length in key exchange")?,
                        );
                        let mut msg_out = RendezvousMessage::new();
                        msg_out.set_key_exchange(KeyExchange {
                            keys: vec![asymmetric_value, symmetric_value],
                            ..Default::default()
                        });
                        timeout(CONNECT_TIMEOUT, conn.send(&msg_out)).await??;
                        conn.set_key(key);
                        if log_on_success {
                            log::info!("Connection secured");
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    Ok(())
}

pub async fn secure_tcp(conn: &mut Stream, key: &str) -> ResultType<()> {
    secure_tcp_impl(conn, key, true).await
}

async fn secure_tcp_silent(conn: &mut Stream, key: &str) -> ResultType<()> {
    secure_tcp_impl(conn, key, false).await
}

#[inline]
fn get_pk(pk: &[u8]) -> Option<[u8; 32]> {
    if pk.len() == 32 {
        let mut tmp = [0u8; 32];
        tmp[..].copy_from_slice(&pk);
        Some(tmp)
    } else {
        None
    }
}

#[inline]
pub fn get_rs_pk(str_base64: &str) -> Option<sign::PublicKey> {
    if let Ok(pk) = crate::decode64(str_base64) {
        get_pk(&pk).map(|x| sign::PublicKey(x))
    } else {
        None
    }
}

pub fn decode_id_pk(signed: &[u8], key: &sign::PublicKey) -> ResultType<(String, [u8; 32])> {
    let res = IdPk::parse_from_bytes(
        &sign::verify(signed, key).map_err(|_| anyhow!("Signature mismatch"))?,
    )?;
    if let Some(pk) = get_pk(&res.pk) {
        Ok((res.id, pk))
    } else {
        bail!("Wrong their public length");
    }
}

pub fn create_symmetric_key_msg(their_pk_b: [u8; 32]) -> (Bytes, Bytes, secretbox::Key) {
    let their_pk_b = box_::PublicKey(their_pk_b);
    let (our_pk_b, out_sk_b) = box_::gen_keypair();
    let key = secretbox::gen_key();
    let nonce = box_::Nonce([0u8; box_::NONCEBYTES]);
    let sealed_key = box_::seal(&key.0, &nonce, &their_pk_b, &out_sk_b);
    (Vec::from(our_pk_b.0).into(), sealed_key.into(), key)
}

#[inline]
pub fn using_public_server() -> bool {
    crate::get_custom_rendezvous_server(get_option("custom-rendezvous-server")).is_empty()
}

pub struct ThrottledInterval {
    interval: Interval,
    next_tick: Instant,
    min_interval: Duration,
}

impl ThrottledInterval {
    pub fn new(i: Interval) -> ThrottledInterval {
        let period = i.period();
        ThrottledInterval {
            interval: i,
            next_tick: Instant::now(),
            min_interval: Duration::from_secs_f64(period.as_secs_f64() * 0.9),
        }
    }

    pub async fn tick(&mut self) -> Instant {
        let instant = poll_fn(|cx| self.poll_tick(cx));
        instant.await
    }

    pub fn poll_tick(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Instant> {
        match self.interval.poll_tick(cx) {
            Poll::Ready(instant) => {
                let now = Instant::now();
                if self.next_tick <= now {
                    self.next_tick = now + self.min_interval;
                    Poll::Ready(instant)
                } else {
                    // This call is required since tokio 1.27
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub type RustDeskInterval = ThrottledInterval;

#[inline]
pub fn rustdesk_interval(i: Interval) -> ThrottledInterval {
    ThrottledInterval::new(i)
}

pub fn load_custom_client() {
    #[cfg(debug_assertions)]
    if let Ok(data) = std::fs::read_to_string("./custom.txt") {
        read_custom_client(data.trim());
        return;
    }
    let Some(path) = std::env::current_exe().map_or(None, |x| x.parent().map(|x| x.to_path_buf()))
    else {
        return;
    };
    #[cfg(target_os = "macos")]
    let path = path.join("../Resources");
    let path = path.join("custom.txt");
    if path.is_file() {
        let Ok(data) = std::fs::read_to_string(&path) else {
            log::error!("Failed to read custom client config");
            return;
        };
        read_custom_client(&data.trim());
    }
}

fn read_custom_client_advanced_settings(
    settings: serde_json::Value,
    map_display_settings: &HashMap<String, &&str>,
    map_local_settings: &HashMap<String, &&str>,
    map_settings: &HashMap<String, &&str>,
    map_buildin_settings: &HashMap<String, &&str>,
    is_override: bool,
) {
    let mut display_settings = if is_override {
        config::OVERWRITE_DISPLAY_SETTINGS.write().unwrap()
    } else {
        config::DEFAULT_DISPLAY_SETTINGS.write().unwrap()
    };
    let mut local_settings = if is_override {
        config::OVERWRITE_LOCAL_SETTINGS.write().unwrap()
    } else {
        config::DEFAULT_LOCAL_SETTINGS.write().unwrap()
    };
    let mut server_settings = if is_override {
        config::OVERWRITE_SETTINGS.write().unwrap()
    } else {
        config::DEFAULT_SETTINGS.write().unwrap()
    };
    let mut buildin_settings = config::BUILTIN_SETTINGS.write().unwrap();

    if let Some(settings) = settings.as_object() {
        for (k, v) in settings {
            let Some(v) = v.as_str() else {
                continue;
            };
            if let Some(k2) = map_display_settings.get(k) {
                display_settings.insert(k2.to_string(), v.to_owned());
            } else if let Some(k2) = map_local_settings.get(k) {
                local_settings.insert(k2.to_string(), v.to_owned());
            } else if let Some(k2) = map_settings.get(k) {
                server_settings.insert(k2.to_string(), v.to_owned());
            } else if let Some(k2) = map_buildin_settings.get(k) {
                buildin_settings.insert(k2.to_string(), v.to_owned());
            } else {
                let k2 = k.replace("_", "-");
                let k = k2.replace("-", "_");
                // display
                display_settings.insert(k.clone(), v.to_owned());
                display_settings.insert(k2.clone(), v.to_owned());
                // local
                local_settings.insert(k.clone(), v.to_owned());
                local_settings.insert(k2.clone(), v.to_owned());
                // server
                server_settings.insert(k.clone(), v.to_owned());
                server_settings.insert(k2.clone(), v.to_owned());
                // buildin
                buildin_settings.insert(k.clone(), v.to_owned());
                buildin_settings.insert(k2.clone(), v.to_owned());
            }
        }
    }
}

#[inline]
#[cfg(target_os = "macos")]
pub fn get_dst_align_rgba() -> usize {
    // https://developer.apple.com/forums/thread/712709
    // Memory alignment should be multiple of 64.
    if crate::ui_interface::use_texture_render() {
        64
    } else {
        1
    }
}

#[inline]
#[cfg(not(target_os = "macos"))]
pub fn get_dst_align_rgba() -> usize {
    1
}

pub fn read_custom_client(config: &str) {
    let Ok(data) = decode64(config) else {
        log::error!("Failed to decode custom client config");
        return;
    };
    const KEY: &str = "5Qbwsde3unUcJBtrx9ZkvUmwFNoExHzpryHuPUdqlWM=";
    let Some(pk) = get_rs_pk(KEY) else {
        log::error!("Failed to parse public key of custom client");
        return;
    };
    let Ok(data) = sign::verify(&data, &pk) else {
        log::error!("Failed to dec custom client config");
        return;
    };
    let Ok(mut data) =
        serde_json::from_slice::<std::collections::HashMap<String, serde_json::Value>>(&data)
    else {
        log::error!("Failed to parse custom client config");
        return;
    };

    if let Some(app_name) = data.remove("app-name") {
        if let Some(app_name) = app_name.as_str() {
            *config::APP_NAME.write().unwrap() = app_name.to_owned();
        }
    }

    let mut map_display_settings = HashMap::new();
    for s in keys::KEYS_DISPLAY_SETTINGS {
        map_display_settings.insert(s.replace("_", "-"), s);
    }
    let mut map_local_settings = HashMap::new();
    for s in keys::KEYS_LOCAL_SETTINGS {
        map_local_settings.insert(s.replace("_", "-"), s);
    }
    let mut map_settings = HashMap::new();
    for s in keys::KEYS_SETTINGS {
        map_settings.insert(s.replace("_", "-"), s);
    }
    let mut buildin_settings = HashMap::new();
    for s in keys::KEYS_BUILDIN_SETTINGS {
        buildin_settings.insert(s.replace("_", "-"), s);
    }
    if let Some(default_settings) = data.remove("default-settings") {
        read_custom_client_advanced_settings(
            default_settings,
            &map_display_settings,
            &map_local_settings,
            &map_settings,
            &buildin_settings,
            false,
        );
    }
    if let Some(overwrite_settings) = data.remove("override-settings") {
        read_custom_client_advanced_settings(
            overwrite_settings,
            &map_display_settings,
            &map_local_settings,
            &map_settings,
            &buildin_settings,
            true,
        );
    }
    for (k, v) in data {
        if let Some(v) = v.as_str() {
            config::HARD_SETTINGS
                .write()
                .unwrap()
                .insert(k, v.to_owned());
        };
    }
}

#[inline]
pub fn is_empty_uni_link(arg: &str) -> bool {
    let prefix = crate::get_uri_prefix();
    if !arg.starts_with(&prefix) {
        return false;
    }
    arg[prefix.len()..].chars().all(|c| c == '/')
}

pub fn get_hwid() -> Bytes {
    use hbb_common::sha2::{Digest, Sha256};

    let uuid = hbb_common::get_uuid();
    let mut hasher = Sha256::new();
    hasher.update(&uuid);
    Bytes::from(hasher.finalize().to_vec())
}

#[inline]
pub fn get_builtin_option(key: &str) -> String {
    config::BUILTIN_SETTINGS
        .read()
        .unwrap()
        .get(key)
        .cloned()
        .unwrap_or_default()
}

#[inline]
pub fn is_custom_client() -> bool {
    get_app_name() != "RustDesk"
}

pub fn verify_login(_raw: &str, _id: &str) -> bool {
    true
    /*
    if is_custom_client() {
        return true;
    }
    #[cfg(debug_assertions)]
    return true;
    let Ok(pk) = crate::decode64("IycjQd4TmWvjjLnYd796Rd+XkK+KG+7GU1Ia7u4+vSw=") else {
        return false;
    };
    let Some(key) = get_pk(&pk).map(|x| sign::PublicKey(x)) else {
        return false;
    };
    let Ok(v) = crate::decode64(raw) else {
        return false;
    };
    let raw = sign::verify(&v, &key).unwrap_or_default();
    let v_str = std::str::from_utf8(&raw)
        .unwrap_or_default()
        .split(":")
        .next()
        .unwrap_or_default();
    v_str == id
    */
}

#[inline]
pub fn is_udp_disabled() -> bool {
    Config::get_option(keys::OPTION_DISABLE_UDP) == "Y"
}

// this crate https://github.com/yoshd/stun-client supports nat type
async fn stun_ipv6_test(stun_server: &str) -> ResultType<(SocketAddr, String)> {
    use stunclient::StunClient;
    let local_addr = SocketAddr::from(([0u16; 8], 0)); // [::]:0

    let socket = UdpSocket::bind(&local_addr).await?;
    let Some(stun_addr) = stun_server
        .to_socket_addrs()?
        .filter(|x| x.is_ipv6())
        .next()
    else {
        bail!(
            "Failed to resolve STUN ipv6 server address: {}",
            stun_server
        );
    };
    let client = StunClient::new(stun_addr);
    let addr = client.query_external_address_async(&socket).await?;
    Ok(if addr.ip().is_ipv6() {
        (addr, stun_server.to_owned())
    } else {
        bail!("STUN server returned non-IPv6 address: {}", addr)
    })
}

async fn stun_ipv4_test(stun_server: &str) -> ResultType<(SocketAddr, String)> {
    use stunclient::StunClient;
    let local_addr = SocketAddr::from(([0u8; 4], 0));

    let socket = UdpSocket::bind(&local_addr).await?;
    let Some(stun_addr) = stun_server
        .to_socket_addrs()?
        .filter(|x| x.is_ipv4())
        .next()
    else {
        bail!(
            "Failed to resolve STUN ipv4 server address: {}",
            stun_server
        );
    };
    let client = StunClient::new(stun_addr);
    let addr = client.query_external_address_async(&socket).await?;
    Ok(if addr.ip().is_ipv4() {
        (addr, stun_server.to_owned())
    } else {
        bail!("STUN server returned non-IPv6 address: {}", addr)
    })
}

static STUNS_V4_DEFAULT: [&str; 8] = [
    "stun.qq.com:3478",
    "stun.miwifi.com:3478",
    "stun.l.google.com:19302",
    "stun.cloudflare.com:3478",
    "stun.nextcloud.com:3478",
    "stun.voipstunt.com:3478",
    "stun.hot-chilli.net:3478",
    "stun.fitauto.ru:3478",
];

// ===================== Phase3 打洞统一时序参数 =====================
// 这些常量被控制端(relay_upgrade_task)与主机端(relay_phase3_punch_to_peer)共用。
// 两端必须"步调一致"：UDP 打洞要求双方几乎同时向对方发包才能各自打开 NAT 映射，
// 此前两端每 target 耗时不同(4.1s vs 3.2s)、偏移序列不同、起始时刻不同，导致
// 双方几乎不可能同时打中正确的地址对，成功率≈0。
// ---------------------------------------------------------------------
/// 单个 target 的 KCP 尝试时长（两端一致）
pub const PUNCH_ROUND_MS: u64 = 8_000;
/// 精确地址（offset 0）持续打洞总时长：足够长以容忍两端数十秒的起始时间差
pub const PUNCH_EXACT_MS: u64 = 60_000;
/// 空包发送间隔（维持 NAT 映射不老化）
pub const PUNCH_BURST_INTERVAL_MS: u64 = 200;
/// 统一偏移序列：砍掉原来的 ±50 盲扫（101 个 target × 4s 远超时序预算，
/// 使一端永远走不到精确地址）。保留"精确 + 小幅漂移"。
pub const PUNCH_OFFSETS: &[i32] = &[0, 1, -1, 2, -2, 3, -3, 5, -5];

/// NAT 类型错峰：宽容端（ASYMMETRIC，映射端口固定）提前发起打洞的毫秒数。
/// 原理：宽容端先打 → 严格端（SYMMETRIC）的 NAT 记录其固定端口，回包可被
/// Cone NAT 放行；反之严格端先打，其源端口是对当前目标临时分配的（≠STUN
/// 端口），对端 Cone/PortRestricted 会因端口不匹配丢弃 → 失败。
/// 提前量须小于 PUNCH_EXACT_MS，保证两端打洞窗口仍重叠。
pub const PUNCH_INITIATOR_LEAD_MS: u64 = 2_000;
/// NAT 类型错峰：严格端（SYMMETRIC）延迟配合的毫秒数，让对端（宽容端）先打。
pub const PUNCH_FOLLOWER_DELAY_MS: u64 = 4_000;

/// NTP 服务器（用于双方时钟对齐的兜底同步）
static NTP_SERVERS: [&str; 4] = [
    "pool.ntp.org:123",
    "time.windows.com:123",
    "ntp.aliyun.com:123",
    "time.cloudflare.com:123",
];

/// 查询 NTP 服务器，返回 (NTP 时间 - 本机时间) 的毫秒偏移。
/// 用于两端把"约定的发起时刻"对齐到同一绝对时间轴上。
/// 任一服务器失败则尝试下一个；全部失败返回 None（调用方退化为仅靠本机时钟）。
pub async fn ntp_offset_ms() -> Option<i64> {
    for server in NTP_SERVERS.iter() {
        if let Some(off) = ntp_query_once(server).await {
            return Some(off);
        }
    }
    None
}

/// 返回 NTP 校准后的当前 Unix 毫秒时间；NTP 不可用时退化为本机系统时间。
pub async fn ntp_now_ms() -> i64 {
    let local = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    match ntp_offset_ms().await {
        Some(off) => local + off,
        None => local,
    }
}

/// 单个 NTP 服务器的 SNTP 查询（RFC 4330 最小实现）
async fn ntp_query_once(server: &str) -> Option<i64> {
    let socket = UdpSocket::bind("0.0.0.0:0").await.ok()?;
    let server_addr = server.to_socket_addrs().ok()?.find(|a| a.is_ipv4())?;
    socket.connect(server_addr).await.ok()?;

    // NTP v4 client request: LI=0(无警告), VN=4, Mode=3(client)
    let mut req = [0u8; 48];
    req[0] = 0x23; // 0b00_100_011
    let t0 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?;
    socket.send(&req).await.ok()?;

    let mut buf = [0u8; 48];
    // 单台超时 1s：NTP 只是打洞兜底（双方时钟对齐），不应阻塞打洞启动太久
    let (n, _) = hbb_common::tokio::time::timeout(Duration::from_secs(1), socket.recv_from(&mut buf))
        .await
        .ok()?
        .ok()?;
    if n < 48 {
        return None;
    }
    let t3 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?;

    // Transmit Timestamp (服务器发出时刻) 位于报文第 40..48 字节
    let mut secs = 0u32;
    for (i, &b) in buf[40..44].iter().enumerate() {
        secs |= (b as u32) << (24 - 8 * i);
    }
    let mut frac = 0u32;
    for (i, &b) in buf[44..48].iter().enumerate() {
        frac |= (b as u32) << (24 - 8 * i);
    }
    let ntp_secs = secs as u64;
    if ntp_secs == 0 {
        return None;
    }
    // NTP 纪元 1900-01-01，Unix 纪元 1970-01-01，相差 2208988800 秒
    const NTP_TO_UNIX_SECS: u64 = 2_208_988_800;
    if ntp_secs < NTP_TO_UNIX_SECS {
        return None;
    }
    let server_unix_ms = (ntp_secs - NTP_TO_UNIX_SECS) as i64 * 1000
        + ((frac as u64 * 1000) >> 32) as i64;

    // 单向延迟按 RTT/2 估算（t0 发出、t3 收到）
    let t0_ms = t0.as_millis() as i64;
    let t3_ms = t3.as_millis() as i64;
    let rtt = (t3_ms - t0_ms).max(0);
    Some(server_unix_ms + rtt / 2 - t3_ms)
}

/// 把 NTP 校准时刻对齐到下一个"轮边界"（PUNCH_ROUND_MS 的整数倍）。
/// 两端各自调用都会得到同一个绝对时刻，从而在相同时刻同时发起打洞。
pub fn punch_sync_boundary(now_ntp_ms: i64) -> i64 {
    let r = PUNCH_ROUND_MS as i64;
    ((now_ntp_ms / r) + 1) * r
}

static STUNS_V6: [&str; 8] = [
    "stun.qq.com:3478",
    "stun.miwifi.com:3478",
    "stun.l.google.com:19302",
    "stun.cloudflare.com:3478",
    "stun.nextcloud.com:3478",
    "stun.voipstunt.com:3478",
    "stun.hot-chilli.net:3478",
    "stun.fitauto.ru:3478",
];

/// Returns the list of STUN servers to use for IPv4.
/// Checks `custom-stun-server` config option first (comma-separated),
/// falls back to built-in defaults if empty.
pub(crate) fn get_stun_servers_v4() -> Vec<String> {
    let custom = Config::get_option("custom-stun-server");
    if custom.is_empty() {
        return STUNS_V4_DEFAULT.iter().map(|s| s.to_string()).collect();
    }
    custom
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

pub async fn test_nat_ipv4() -> ResultType<(SocketAddr, String)> {
    use hbb_common::futures::future::{select_ok, FutureExt};
    let servers = get_stun_servers_v4();
    let tests = servers
        .iter()
        .map(|stun| stun_ipv4_test(stun).boxed())
        .collect::<Vec<_>>();

    match select_ok(tests).await {
        Ok(res) => {
            return Ok(res.0);
        }
        Err(e) => {
            bail!(
                "Failed to get public IPv4 address via public STUN servers: {}",
                e
            );
        }
    };
}

async fn test_bind_ipv6() -> ResultType<SocketAddr> {
    let local_addr = SocketAddr::from(([0u16; 8], 0)); // [::]:0
    let socket = UdpSocket::bind(local_addr).await?;
    let addr = STUNS_V6[0]
        .to_socket_addrs()?
        .filter(|x| x.is_ipv6())
        .next()
        .ok_or_else(|| {
            anyhow!(
                "Failed to resolve STUN ipv6 server address: {}",
                STUNS_V6[0]
            )
        })?;
    socket.connect(addr).await?;
    Ok(socket.local_addr()?)
}

pub async fn test_ipv6() -> Option<tokio::task::JoinHandle<()>> {
    if PUBLIC_IPV6_ADDR
        .lock()
        .unwrap()
        .1
        .map(|x| x.elapsed().as_secs() < 60)
        .unwrap_or(false)
    {
        return None;
    }
    PUBLIC_IPV6_ADDR.lock().unwrap().1 = Some(Instant::now());

    match test_bind_ipv6().await {
        Ok(mut addr) => {
            if let std::net::IpAddr::V6(ip) = addr.ip() {
                if !ip.is_loopback()
                    && !ip.is_unspecified()
                    && !ip.is_multicast()
                    && (ip.segments()[0] & 0xe000) == 0x2000
                {
                    addr.set_port(0);
                    PUBLIC_IPV6_ADDR.lock().unwrap().0 = Some(addr);
                    log::debug!("Found public IPv6 address locally: {}", addr);
                }
            }
        }
        Err(e) => {
            log::warn!("Failed to bind IPv6 socket: {}", e);
        }
    }
    // Interestingly, on my macOS, sometimes my ipv6 works, sometimes not (test with ping6 or https://test-ipv6.com/).
    // I checked ifconfig, could not see any difference. Both secure ipv6 and temporary ipv6 are there.
    // So we can not rely on the local ipv6 address queries with if_addrs.
    // above test_bind_ipv6 is safer, because it can fail in this case.
    /*
    std::thread::spawn(|| {
        if let Ok(ifaces) = if_addrs::get_if_addrs() {
            for iface in ifaces {
                if let if_addrs::IfAddr::V6(v6) = iface.addr {
                    let ip = v6.ip;
                    if !ip.is_loopback()
                        && !ip.is_unspecified()
                        && !ip.is_multicast()
                        && !ip.is_unique_local()
                        && !ip.is_unicast_link_local()
                        && (ip.segments()[0] & 0xe000) == 0x2000
                    {
                        // only use the first one, on mac, the first one is the stable
                        // one, the last one is the temporary one. The middle ones are deperecated.
                        *PUBLIC_IPV6_ADDR.lock().unwrap() =
                            Some((SocketAddr::from((ip, 0)), Instant::now()));
                        log::debug!("Found public IPv6 address locally: {}", ip);
                        break;
                    }
                }
            }
        }
    });
    */

    Some(tokio::spawn(async {
        use hbb_common::futures::future::{select_ok, FutureExt};
        let tests = STUNS_V6
            .iter()
            .map(|&stun| stun_ipv6_test(stun).boxed())
            .collect::<Vec<_>>();

        match select_ok(tests).await {
            Ok(res) => {
                let mut addr = res.0 .0;
                addr.set_port(0); // Set port to 0 to avoid conflicts
                PUBLIC_IPV6_ADDR.lock().unwrap().0 = Some(addr);
                log::debug!(
                    "Found public IPv6 address via STUN server {}: {}",
                    res.0 .1,
                    addr
                );
            }
            Err(e) => {
                log::error!("Failed to get public IPv6 address: {}", e);
            }
        };
    }))
}

pub async fn punch_udp(
    socket: Arc<UdpSocket>,
    listen: bool,
) -> ResultType<Option<bytes::BytesMut>> {
    let mut retry_interval = Duration::from_millis(10);
    const MAX_INTERVAL: Duration = Duration::from_millis(200);
    const MAX_TIME: Duration = Duration::from_secs(30);
    let mut packets_sent = 0;
    socket.send(&[]).await.ok();
    packets_sent += 1;
    let mut last_send_time = Instant::now();
    let tm = Instant::now();
    let mut data = [0u8; 1500];

    loop {
        tokio::select! {
            _ = hbb_common::sleep(retry_interval.as_secs_f32()) => {
                if tm.elapsed() > MAX_TIME {
                    bail!("UDP punch is timed out, stop sending packets after {:?} packets", packets_sent);
                }
                let elapsed = last_send_time.elapsed();

                if elapsed >= retry_interval {
                    socket.send(&[]).await.ok();
                    packets_sent += 1;

                    // Exponentially increase interval to reduce network pressure
                    retry_interval = std::cmp::min(
                        Duration::from_millis((retry_interval.as_millis() as f64 * 1.3) as u64),
                        MAX_INTERVAL
                    );
                    last_send_time = Instant::now();
                }
            }
            res = socket.recv(&mut data) => match res {
                Err(e) => bail!("UDP punch failed, {packets_sent} packets sent: {e}"),
                Ok(n) => {
                    // log::debug!("UDP punch succeeded after sending {} packets after {:?}", packets_sent, tm.elapsed());
                    if listen {
                        if n == 0 {
                            continue;
                        }
                        return Ok(Some(bytes::BytesMut::from(&data[..n])));
                    }
                    return Ok(None);
                }
            }
        }
    }
}

/// After relay is established, keep trying UDP punching in background (Tailscale-style).
/// Tries all known addresses of the peer (local + public).
/// Query STUN server using an existing socket to discover the NAT-mapped public address.
/// Serial over the hardcoded STUN server list; tries each one with timeouts.
/// #4: Validates transaction ID in response.
/// Multi-STUN concurrent query using an existing socket.
/// Queries all 3 STUN servers in parallel and takes the first 2 replies.
/// If they agree on the mapped address, uses it (high confidence).
/// Otherwise falls back to the first reply (better than nothing).
/// Query STUN servers concurrently on a shared (unconnected) socket.
///
/// Sends a binding request to every server up front, then dispatches replies
/// by transaction ID in a single recv loop. The previous implementation
/// awaited each server's future in turn, so one dead server burned its full
/// 2s timeout before the next server was even contacted.
pub async fn stun_query_with_socket(socket: &UdpSocket) -> ResultType<(SocketAddr, String)> {
    use hbb_common::rand::{self, Rng};

    const TOTAL_TIMEOUT: Duration = Duration::from_secs(6);

    // 1. Fire a binding request at every STUN server (best effort per server).
    let mut tx_map: std::collections::HashMap<[u8; 12], String> = std::collections::HashMap::new();
    for stun in get_stun_servers_v4().iter() {
        let stun_addr = match stun.to_socket_addrs() {
            Ok(mut addrs) => match addrs.find(|x| x.is_ipv4()) {
                Some(a) => a,
                None => continue,
            },
            Err(_) => continue,
        };
        let mut req = vec![0u8; 20];
        req[0..2].copy_from_slice(&0x0001u16.to_be_bytes());
        req[4..8].copy_from_slice(&0x2112A442u32.to_be_bytes());
        let mut tx_id = [0u8; 12];
        rand::thread_rng().fill(&mut tx_id);
        req[8..20].copy_from_slice(&tx_id);
        if socket.send_to(&req, stun_addr).await.is_err() {
            continue;
        }
        tx_map.insert(tx_id, stun.clone());
    }
    if tx_map.is_empty() {
        bail!("Failed to send STUN requests to any server");
    }

    // 2. Single recv loop; dispatch each reply by its transaction ID so
    //    interleaved responses from different servers are matched correctly.
    let deadline = std::time::Instant::now() + TOTAL_TIMEOUT;
    let mut results: Vec<(SocketAddr, String)> = Vec::new();
    let mut buf = vec![0u8; 4096];
    while results.len() < 2 {
        let now = std::time::Instant::now();
        if now >= deadline {
            break;
        }
        let (n, _) = match hbb_common::tokio::time::timeout(
            deadline - now,
            socket.recv_from(&mut buf),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                // Windows 上 UDP 收到 ICMP 不可达会报 ConnectionReset，忽略继续收
                if e.kind() == std::io::ErrorKind::ConnectionReset {
                    continue;
                }
                break;
            }
            Err(_) => break,
        };
        if n < 20 {
            continue;
        }
        if u16::from_be_bytes([buf[0], buf[1]]) != 0x0101 {
            continue;
        }
        let tx_id: [u8; 12] = match buf[8..20].try_into() {
            Ok(t) => t,
            Err(_) => continue,
        };
        let Some(stun) = tx_map.get(&tx_id) else {
            continue; // 非本次请求发出的响应（其他探测流量/旧请求）
        };
        let mut pos = 20;
        while pos + 4 <= n {
            let attr_type = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
            let attr_len = u16::from_be_bytes([buf[pos + 2], buf[pos + 3]]) as usize;
            if pos + 4 + attr_len > n {
                break;
            }
            if attr_type == 0x0020 && attr_len >= 8 {
                let xor_port = u16::from_be_bytes([buf[pos + 6], buf[pos + 7]]);
                let port = xor_port ^ 0x2112;
                let mut ip_bytes = [0u8; 4];
                for i in 0..4 {
                    ip_bytes[i] = buf[pos + 8 + i] ^ buf[4 + i];
                }
                let ip =
                    std::net::Ipv4Addr::new(ip_bytes[0], ip_bytes[1], ip_bytes[2], ip_bytes[3]);
                results.push((SocketAddr::from((ip, port)), stun.clone()));
                break;
            }
            pos += 4 + attr_len;
            if attr_len % 4 != 0 {
                pos += 4 - (attr_len % 4);
            }
        }
    }
    if results.is_empty() {
        bail!("All STUN servers failed");
    }
    // 两台以上一致即高置信；不一致时信任第一台（与旧实现一致）
    Ok(results.remove(0))
}

/// Query a single specific STUN server (not race all servers).
/// Used for multi-STUN port prediction where all queries must target
/// the same server to get consistent delta on Symmetric NAT.
pub async fn stun_query_single_server(
    socket: &UdpSocket,
    stun: &str,
) -> ResultType<(SocketAddr, String)> {
    use hbb_common::rand::{self, Rng};

    let stun_addr = stun
        .to_socket_addrs()?
        .filter(|x| x.is_ipv4())
        .next()
        .ok_or_else(|| anyhow!("Failed to resolve STUN server: {}", stun))?;

    let mut req = vec![0u8; 20];
    req[0..2].copy_from_slice(&0x0001u16.to_be_bytes());
    req[4..8].copy_from_slice(&0x2112A442u32.to_be_bytes());
    let mut tx_id = [0u8; 12];
    rand::thread_rng().fill(&mut tx_id);
    req[8..20].copy_from_slice(&tx_id);

    socket.send_to(&req, stun_addr).await?;

    let mut buf = vec![0u8; 4096];
    let (n, _) = tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut buf))
        .await
        .map_err(|_| anyhow!("STUN recv timeout from {}", stun))??;
    if n < 20 {
        bail!("STUN response too short");
    }
    let resp_type = u16::from_be_bytes([buf[0], buf[1]]);
    if resp_type != 0x0101 {
        bail!("Not a STUN Binding Response");
    }
    if &buf[8..20] != &tx_id {
        bail!("STUN transaction ID mismatch from {}", stun);
    }
    let mut pos = 20;
    while pos + 4 <= n {
        let attr_type = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
        let attr_len = u16::from_be_bytes([buf[pos + 2], buf[pos + 3]]) as usize;
        if pos + 4 + attr_len > n {
            break;
        }
        if attr_type == 0x0020 && attr_len >= 8 {
            let xor_port = u16::from_be_bytes([buf[pos + 6], buf[pos + 7]]);
            let port = xor_port ^ 0x2112;
            let mut ip_bytes = [0u8; 4];
            for i in 0..4 {
                ip_bytes[i] = buf[pos + 8 + i] ^ buf[4 + i];
            }
            let ip = std::net::Ipv4Addr::new(
                ip_bytes[0], ip_bytes[1], ip_bytes[2], ip_bytes[3],
            );
            return Ok((SocketAddr::from((ip, port)), stun.to_string()));
        }
        pos += 4 + attr_len;
        if attr_len % 4 != 0 {
            pos += 4 - (attr_len % 4);
        }
    }
    bail!("No XOR-MAPPED-ADDRESS found in STUN response from {}", stun)
}

/// Accept loop for TCP simultaneous open: only accept a connection whose
/// source IP matches `ip`; discard others and keep accepting. Returns None
/// on accept error. Meant to be wrapped in an outer timeout by the caller.
async fn accept_from_ip(
    listener: &tokio::net::TcpListener,
    ip: std::net::IpAddr,
) -> Option<tokio::net::TcpStream> {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                if addr.ip() == ip {
                    return Some(stream);
                }
                log::debug!("Phase3 TCP: discard connection from unexpected {}", addr);
            }
            Err(_) => return None,
        }
    }
}






/// 地址类型（与 message.proto PunchPeerAddr.addr_type 一致）
pub const PUNCH_ADDR_TYPE_UDP: u32 = 0; // UDP/STUN（KCP 打洞目标）
pub const PUNCH_ADDR_TYPE_TCP: u32 = 1; // TCP listener（TCP simultaneous open）
pub const PUNCH_ADDR_TYPE_IPV6: u32 = 2; // IPv6

/// Phase3 地址 + 同步信息。
/// 两端通过 relay 交换：addr 是打洞目标，ready/delta/sync_at_ms 用于
/// 让双方在同一时刻、按同一节奏发起打洞（此前两端各自盲扫，窗口互相错过）。
/// `addr_type` 显式标记地址用途，取代"同 IP 不同端口=TCP"启发式分类。
#[derive(Clone, Copy, Debug)]
pub struct PunchAddrInfo {
    pub addr: std::net::SocketAddr,
    /// 本端 STUN 已完成、已就绪
    pub ready: bool,
    /// 本端对称 NAT delta（端口预测步长）
    pub delta: i32,
    /// 约定的打洞起始绝对时刻（NTP 校准后的 Unix 毫秒）
    pub sync_at_ms: i64,
    /// 地址类型：PUNCH_ADDR_TYPE_UDP / TCP / IPV6
    pub addr_type: u32,
}

impl PunchAddrInfo {
    pub fn new(addr: std::net::SocketAddr) -> Self {
        Self {
            addr,
            ready: false,
            delta: 0,
            sync_at_ms: 0,
            addr_type: PUNCH_ADDR_TYPE_UDP,
        }
    }
    pub fn new_tcp(addr: std::net::SocketAddr) -> Self {
        Self {
            addr,
            ready: false,
            delta: 0,
            sync_at_ms: 0,
            addr_type: PUNCH_ADDR_TYPE_TCP,
        }
    }
    pub fn is_tcp(&self) -> bool {
        self.addr_type == PUNCH_ADDR_TYPE_TCP
    }
}

/// 判断一个地址能否作为打洞目标（两端共用同一标准）：
/// - `0.0.0.0` / 端口 0：不可用（0.0.0.0 connect 等价于连本机，
///   可能把本机服务误当成对端；端口 0 表示"未提供实际端口"，如未修正的 IPv6 地址）
/// - IPv6 链路本地（fe80::/10）与唯一本地（fc00::/7）：公网不可路由
pub fn is_unusable_punch_addr(addr: &std::net::SocketAddr) -> bool {
    if addr.ip().is_unspecified() || addr.port() == 0 {
        return true;
    }
    if let std::net::IpAddr::V6(v6) = addr.ip() {
        if (v6.segments()[0] & 0xffc0) == 0xfe80 || (v6.segments()[0] & 0xfe00) == 0xfc00 {
            return true;
        }
    }
    false
}

fn now_ms_local() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 创建一个打洞 UDP socket：优先绑 UPnP 本地端口（保持 NAT 映射稳定），
/// 失败则回退随机端口。IPv4 only（v6 打洞由旧 Phase3 路径负责）。
pub async fn create_punch_socket_v4() -> ResultType<Arc<tokio::net::UdpSocket>> {
    let upnp_local = crate::common::get_upnp_local_port();
    let bind_addr = if upnp_local > 0 {
        SocketAddr::from(([0u8; 4], upnp_local))
    } else {
        SocketAddr::from(([0u8; 4], 0u16))
    };
    match UdpSocket::bind(bind_addr).await {
        Ok(s) => Ok(Arc::new(s)),
        Err(_) if upnp_local > 0 => Ok(Arc::new(UdpSocket::bind("0.0.0.0:0").await?)),
        Err(e) => Err(e.into()),
    }
}

/// 探测本 socket 的公网映射地址并返回 (地址, 对称delta)。
/// delta=0 表示锥形 NAT（端口恒定）；对称 NAT 时换一台 STUN 服务器复测，
/// 端口差即 delta，对端用它预测我们的映射端口。
///
/// 全程使用未 connect 模式（send_to/recv_from，靠 txid 匹配响应）：connect
/// 会锁定对端，之后既无法再向其它服务器探测，打洞前也必须先切目标。
pub async fn stun_probe_with_delta(
    socket: &tokio::net::UdpSocket,
) -> ResultType<(SocketAddr, i32)> {
    let (addr, _) = stun_query_with_socket(socket).await?;
    let servers = get_stun_servers_v4();
    let alt = match servers.get(1).or_else(|| servers.first()) {
        Some(s) => s,
        None => return Ok((addr, 0)),
    };
    if let Ok((alt_res, _)) = stun_query_single_server(socket, alt).await {
        if alt_res != addr {
            return Ok((addr, alt_res.port() as i32 - addr.port() as i32));
        }
    }
    Ok((addr, 0))
}

// ===== 进程级共享打洞 socket + STUN 映射缓存 =====
// 目的："软件打开即就绪"——启动后由 direct_server 循环提前完成 STUN 探测
// 并周期上报到 api-server；开始远程时直接复用缓存的映射地址，跳过现场
// 1-4s 的 STUN 与重复上报，立刻进入打洞。
//
// 单实例约束：打洞 socket 必须唯一（同端口双 socket 会互相抢收入站包），
// 因此启动上报 / 连接前打洞 / 被控端 inbound 打洞共用这一个 socket。
// UDP socket 可随时 connect 切换到新目标，无需"断开"。
//
// STUN 探测（send_to）不能在 connect 状态进行，故：
//   - 缓存热（<PUNCH_MAPPED_TTL）时任何路径都不再探测；
//   - 缓存过期且 socket 已 connect（打洞中）时跳过刷新，沿用旧缓存。
//
// 注意：一个 socket 上会有多个读循环（每个打洞/每条直连会话一个 kcp_io），
// 再加不定期的 STUN 探测，多个 recv_from 会互相抢包（收到对方的报文后按
// txid/conv 丢弃）。因此：
//   - 所有 STUN 探测用 PUNCH_PROBE_LOCK 单飞（并发只探测一次，其余等待复用）；
//   - 共享 socket 使用凭证（SharedSocketUse）由打洞函数与已建立的直连会话
//     （KcpStream）持有，看门狗在"有人使用"时跳过本轮探测——它是唯一长期
//     周期性的探测源，跳过它就不会反复干扰活动会话；
//   - 连接前/心跳触发的探测是偶发的，且 KCP 可靠重传，抢走一两个报文代价
//     可忽略，因此不做跳过（否则一对多远程时第二个控制端会拿不到本地映射）。

static SHARED_PUNCH_SOCKET: std::sync::Mutex<Option<Arc<tokio::net::UdpSocket>>> =
    std::sync::Mutex::new(None);
/// 进程级共享 IPv6 打洞 socket（双栈直连用）。
///
/// 必须单实例且端口固定：上报给对端的 v6 地址端口就是本 socket 的本地端口，
/// 对端据此打过来——若上报端口与实际收包端口不一致，v6 直连必然单向死亡
/// （收到 0 个包）。对应的 v4 教训见 get_cached_ipv6_addr 的端口 0 问题。
static SHARED_PUNCH_SOCKET_V6: std::sync::Mutex<Option<Arc<tokio::net::UdpSocket>>> =
    std::sync::Mutex::new(None);
// v6 打洞串行锁：共享 v6 socket 只有一个 recv_from 投递点，多个 v6 打洞
// 并发会互相吞掉对方的 KCP 握手包（v4 侧靠 SHARED_SOCKET_USERS 让 STUN
// 探测避让，v6 侧没有等价机制，故用锁串行化）。
// 注：用普通注释而非 ///，因为 rustdoc 不为宏调用生成文档。
lazy_static::lazy_static! {
    static ref V6_PUNCH_LOCK: hbb_common::tokio::sync::Mutex<()> =
        hbb_common::tokio::sync::Mutex::new(());
}

// (映射地址, 对称 delta, 探测时间)
static SHARED_PUNCH_MAPPED: std::sync::Mutex<Option<(SocketAddr, i32, std::time::Instant)>> =
    std::sync::Mutex::new(None);
// 上次上报时间（附带上报在此窗口内跳过，避免反复打 API）
static LAST_PUNCH_REPORT_AT: std::sync::Mutex<Option<std::time::Instant>> =
    std::sync::Mutex::new(None);
// 正在使用共享打洞 socket 的读循环数量（kcp_io / STUN 探测）
static SHARED_SOCKET_USERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

// 缓存 TTL 取 2x 看门狗间隔：看门狗每 60s 刷新一次，缓存永远不会过期，
// 连接路径因此永远零现场探测。注意它只是"要不要重新探测"的门槛，与
// api-server 侧的 PunchAddrTTL(60s) 无关——地址实际新鲜度由看门狗保证。
const PUNCH_MAPPED_TTL: std::time::Duration = std::time::Duration::from_secs(120);
// 附带上报节流。连接时带 target 的上报不走此节流（必须让对端发现请求）。
// 心跳（sync.rs，15s 一次）与本地 IP 变化都会触发附带上报；api-server 侧
// 打洞地址 TTL 是 60s（punch_exchange.go: PunchAddrTTL），20s 节流留 3x 余量。
const PUNCH_REPORT_THROTTLE: std::time::Duration = std::time::Duration::from_secs(20);
// 映射看门狗探测间隔（< PUNCH_MAPPED_TTL，缓存因此永热）。
const PUNCH_WATCHDOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

// STUN 探测单飞锁：保证同一时刻只有一个探测在跑（多路并发只探测一次）。
lazy_static::lazy_static! {
    static ref PUNCH_PROBE_LOCK: hbb_common::tokio::sync::Mutex<()> =
        hbb_common::tokio::sync::Mutex::new(());
}

/// 共享 socket 使用凭证：持有期间共享 socket 被视为"忙"（看门狗跳过探测）。
/// Drop 时释放计数。
pub struct SharedSocketUse(());

impl Drop for SharedSocketUse {
    fn drop(&mut self) {
        SHARED_SOCKET_USERS.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// 当前是否有人在用共享打洞 socket（打洞/直连会话/STUN 探测）。
pub fn shared_socket_in_use() -> bool {
    SHARED_SOCKET_USERS.load(std::sync::atomic::Ordering::SeqCst) > 0
}

/// 申领共享 socket 使用权（持有期间看门狗/其它 STUN 探测会跳过）。
pub fn shared_socket_acquire() -> SharedSocketUse {
    SHARED_SOCKET_USERS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    SharedSocketUse(())
}

/// 获取进程级共享打洞 socket（若已创建）。
pub fn shared_punch_socket() -> Option<Arc<tokio::net::UdpSocket>> {
    SHARED_PUNCH_SOCKET.lock().ok().and_then(|g| g.clone())
}

/// 获取（必要时创建）进程级共享打洞 socket。
pub async fn ensure_shared_punch_socket() -> ResultType<Arc<tokio::net::UdpSocket>> {
    if let Some(s) = shared_punch_socket() {
        return Ok(s);
    }
    let s = create_punch_socket_v4().await?;
    let mut g = SHARED_PUNCH_SOCKET
        .lock()
        .map_err(|_| anyhow!("shared punch socket lock poisoned"))?;
    Ok(g.get_or_insert(s).clone())
}

/// 获取（必要时创建）进程级共享 IPv6 打洞 socket。
///
/// 双栈直连的前提：上报给对端的端口 == 本 socket 实际收包端口。因此 v6
/// 地址采集与打洞必须共用这一个 socket（同 v4 共享 socket 的设计）。
pub async fn ensure_shared_punch_socket_v6() -> ResultType<Arc<tokio::net::UdpSocket>> {
    if let Ok(g) = SHARED_PUNCH_SOCKET_V6.lock() {
        if let Some(s) = g.as_ref() {
            return Ok(s.clone());
        }
    }
    // 先 bind 再取锁：不在持锁状态下 await（std::sync::Mutex 不可跨 await）
    let s = Arc::new(UdpSocket::bind(SocketAddr::from(([0u16; 8], 0u16))).await?);
    let mut g = SHARED_PUNCH_SOCKET_V6
        .lock()
        .map_err(|_| anyhow!("shared punch socket v6 lock poisoned"))?;
    Ok(g.get_or_insert(s).clone())
}

/// 读取缓存的 STUN 映射（TTL 内有效）。
pub fn cached_punch_mapped() -> Option<(SocketAddr, i32)> {
    let g = SHARED_PUNCH_MAPPED.lock().ok()?;
    g.as_ref().and_then(|(a, d, t)| {
        if t.elapsed() < PUNCH_MAPPED_TTL {
            Some((*a, *d))
        } else {
            None
        }
    })
}

/// 读取缓存的映射（不看 TTL）：仅供"是否变化"的比较使用。
/// 直接用 cached_punch_mapped() 判断变化是错的——探测间隔 >= TTL 时缓存必然
/// 已过期，每次都会被判成"变化"而重复 HTTP 上报。
fn cached_punch_mapped_any_ttl() -> Option<(SocketAddr, i32)> {
    let g = SHARED_PUNCH_MAPPED.lock().ok()?;
    g.as_ref().map(|(a, d, _)| (*a, *d))
}

/// 作废映射缓存（网络切换时调用）：下次 ensure_punch_mapped 会重新 STUN
/// 探测，避免沿用旧网络的映射地址。
pub fn invalidate_punch_mapped_cache() {
    if let Ok(mut g) = SHARED_PUNCH_MAPPED.lock() {
        if g.take().is_some() {
            log::info!("punch: mapped addr cache invalidated, will re-probe");
        }
    }
}

/// 确保共享 socket 的映射已知：缓存热直接返回；否则探测一次并缓存。
/// 探测用单飞锁串行化：看门狗 / 心跳附带上报 / 连接前打洞可能同时调用，
/// 若各自发 STUN 会互相 recv_from 抢走对方的响应，双双超时。
///
/// 这里**不**因"共享 socket 正被使用"而跳过探测：一对多远程时第二个控制端
/// 连接就发生在已有直连会话期间，跳过会让它拿不到本地映射而无法打洞。
/// 探测期间可能零星抢走活动会话的一两个报文，但 KCP 本身可靠重传，代价可忽略；
/// 真正需要避免的是"长时间周期性探测"（看门狗），那个才在忙时跳过。
pub async fn ensure_punch_mapped() -> Option<(SocketAddr, i32)> {
    if let Some(c) = cached_punch_mapped() {
        return Some(c);
    }
    let sock = ensure_shared_punch_socket().await.ok()?;
    if sock.peer_addr().is_ok() {
        // socket 已 connect（旧 Phase3 路径）：不能 send_to 探测
        return None;
    }
    // 单飞：等锁期间别人可能已经探测并写好缓存
    let _guard = PUNCH_PROBE_LOCK.lock().await;
    if let Some(c) = cached_punch_mapped() {
        return Some(c);
    }
    match stun_probe_with_delta(&sock).await {
        Ok((addr, delta)) => {
            if let Ok(mut g) = SHARED_PUNCH_MAPPED.lock() {
                *g = Some((addr, delta, std::time::Instant::now()));
            }
            log::info!("punch: shared socket mapped addr {} (delta {})", addr, delta);
            Some((addr, delta))
        }
        Err(e) => {
            log::debug!("ensure_punch_mapped: STUN failed: {:?}", e);
            None
        }
    }
}

/// 上报节流：`PUNCH_REPORT_THROTTLE` 内已上报过返回 true（调用方跳过本次上报）。
pub fn punch_report_throttled() -> bool {
    let now = std::time::Instant::now();
    if let Ok(mut g) = LAST_PUNCH_REPORT_AT.lock() {
        if let Some(last) = *g {
            if now.duration_since(last) < PUNCH_REPORT_THROTTLE {
                return true;
            }
        }
        *g = Some(now);
    }
    false
}

/// 记录一次上报（变化上报绕过节流时也调用，避免与附带上报叠加）。
fn mark_punch_reported() {
    if let Ok(mut g) = LAST_PUNCH_REPORT_AT.lock() {
        *g = Some(std::time::Instant::now());
    }
}

/// 映射看门狗：定时轻量探测共享 socket 的公网映射，**仅在变化时** HTTP 上报。
///
/// 为什么需要它（纯事件触发的盲区）：路由器 NAT 表老化、CGNAPT 重新分配、
/// WAN 口重拨等情况下，**公网映射会变而本地网卡 IP 不变**——direct_server
/// 里的 IP 变化检测看不到，心跳挂点又要等周期 + 缓存 TTL，最长 90-120s 才
/// 恢复正确上报。这期间被控端 inbox 打洞用的是旧映射，控制端打不进来。
///
/// 成本设计（"最优自动更新时间"的关键）：探测 = 每台 STUN 服务器 1 个 20 字节
/// UDP 请求，可忽略；上报 = HTTP+TLS，贵。因此：
///   - 每 `PUNCH_WATCHDOG_INTERVAL` 探测一次（小于缓存 TTL，缓存永热，
///     连接时永远零探测）；
///   - 映射未变 → **零 HTTP 请求**，只刷新缓存时间戳；
///   - 映射变化 → 立即上报（绕过节流，对端必须尽快知道）；
///   - 共享 socket 正被使用（打洞中 / 已建立直连会话 / 另一路探测）时
///     **跳过本轮**，绝不 recv_from 抢走别人的报文。
pub async fn punch_mapped_watchdog() {
    // 启动后先让一次启动上报走完，避免与它抢着探测
    hbb_common::tokio::time::sleep(Duration::from_secs(5)).await;
    loop {
        hbb_common::tokio::time::sleep(PUNCH_WATCHDOG_INTERVAL).await;
        if !crate::hbbs_http::punch_exchange::available() {
            continue;
        }
        // 有人在打洞/直连会话中：不能探测（会抢包），下一轮再来
        if shared_socket_in_use() {
            continue;
        }
        let sock = match ensure_shared_punch_socket().await {
            Ok(s) => s,
            Err(_) => continue,
        };
        // 旧 Phase3 路径可能把共享 socket connect 到目标，此时不能 send_to 探测
        if sock.peer_addr().is_ok() {
            continue;
        }
        // 与其它探测（连接前 STUN、附带上报）串行：多个 recv_from 同时跑会
        // 互相抢走对方的 STUN 响应，双双超时。
        // 注意锁的作用域：report_own_addrs_inner 内部还会走 ensure_punch_mapped
        // 再取一次这把锁，tokio::Mutex 不可重入，必须在这里就释放。
        let probe_res = {
            let _probe = PUNCH_PROBE_LOCK.lock().await;
            stun_probe_with_delta(&sock).await
        };
        let Ok((addr, delta)) = probe_res else {
            continue;
        };
        // 与缓存比较时不能走 cached_punch_mapped()：它带 TTL 过滤，一旦探测
        // 间隔 >= TTL，缓存必然已过期，每轮都会被误判成"变化"而重复上报。
        // 直接读底层缓存比较最稳（探测间隔就是 refresh 间隔，不存在这个陷阱）。
        let changed = match cached_punch_mapped_any_ttl() {
            Some((a, d)) => a != addr || d != delta,
            None => true,
        };
        // 无论是否变化都刷新缓存时间戳（保证连接时缓存是热的）
        if let Ok(mut g) = SHARED_PUNCH_MAPPED.lock() {
            *g = Some((addr, delta, std::time::Instant::now()));
        }
        if !changed {
            continue; // 未变化：零 HTTP 请求
        }
        log::info!(
            "punch watchdog: mapped addr changed to {} (delta {}), reporting",
            addr,
            delta
        );
        report_own_addrs_inner().await;
    }
}

/// 实际执行一次上报（绕过节流；映射变化/网络切换时必须让对端尽快知道）。
pub async fn report_own_addrs_inner() -> bool {
    let known_mapped = ensure_punch_mapped().await;
    let Some((primary, _)) = known_mapped else {
        return false;
    };
    // known_mapped 已知时不需要 socket：传 None 会白白再 bind 一个 UDP socket
    // （还会重复抢 UPnP 本地端口）。socket 仅用于"现场探测 STUN"。
    let (addrs, meta) = collect_punch_addrs(None, Some((primary, 0))).await;
    if addrs.is_empty() {
        return false;
    }
    let ntp_offset = ntp_offset_ms().await;
    let sync_at_ms = punch_sync_boundary(now_ms_local() + ntp_offset.unwrap_or(0));
    let addrs: Vec<_> = addrs
        .into_iter()
        .map(|mut a| {
            a.sync_at_ms = sync_at_ms;
            a
        })
        .collect();
    let count = addrs.len();
    match crate::hbbs_http::punch_exchange::report(&Config::get_id(), "", &meta, addrs).await {
        Ok(_) => {
            // 只在成功后才记账：失败也要允许尽快重试，否则一次抖动就会让
            // 打洞地址在 TTL 内没人刷新。
            mark_punch_reported();
            log::debug!(
                "punch report: reported {} candidate addr(s), nat_type={}",
                count,
                meta.nat_type
            );
            true
        }
        Err(e) => {
            log::debug!("punch report failed: {:?}", e);
            false
        }
    }
}

/// direct-access-port（被控端 TCP 直连监听端口；LAN 与 TCP fallback 都用它）
fn get_direct_access_port() -> u16 {
    let p = Config::get_option("direct-access-port")
        .parse::<i32>()
        .unwrap_or(0);
    if p > 0 {
        p as u16
    } else {
        (hbb_common::config::RENDEZVOUS_PORT + 2) as u16
    }
}

/// 收集本机的全部候选打洞地址与网络环境（尽可能多的信息交换，提高成功率）。
///
/// 地址按优先级：
/// - `last_success`  上一会话成功用过的映射（重连时 NAT 映射通常仍在，命中率最高）
/// - `stun_primary`  主 STUN 服务器看到的映射
/// - `stun_alt`      备 STUN 服务器看到的映射（对称 NAT 下与主不同，附带 delta）
/// - `lan`           本机各网卡内网地址 + direct-access-port（双方同网段时直连）
/// - `tcp`           主映射 IP + direct-access-port（UDP 全封时的 TCP 同步打开）
///
/// `known_mapped` 传入已知映射（启动时已探测并缓存）时**跳过 STUN**——这就是
/// “打开软件即就绪”的关键：连接时不再现场花 1-4s 探测。为 None 时才探测。
/// `socket` 传入时复用它做 STUN（保证探测到的映射与后续打洞同一个 socket）。
pub async fn collect_punch_addrs(
    socket: Option<&Arc<tokio::net::UdpSocket>>,
    known_mapped: Option<(SocketAddr, i32)>,
) -> (
    Vec<crate::hbbs_http::punch_exchange::PunchAddr>,
    crate::hbbs_http::punch_exchange::PunchMeta,
) {
    use crate::hbbs_http::punch_exchange::{
        PunchAddr, PunchMeta, KIND_LAN, KIND_LAST_SUCCESS, KIND_STUN_ALT, KIND_STUN_PRIMARY,
        KIND_TCP, KIND_V6, PRIO_LAN, PRIO_LAST_SUCCESS, PRIO_STUN_ALT, PRIO_STUN_PRIMARY,
        PRIO_TCP, PRIO_V6,
    };

    let mut addrs: Vec<PunchAddr> = Vec::new();
    let mut primary_ip: Option<std::net::IpAddr> = None;

    // 1. 上一会话成功的映射地址（本机 NAT 映射通常仍在，命中率最高）
    let last_ok = LocalConfig::get_option("punch-last-success");
    if !last_ok.is_empty() {
        if let Ok(sa) = last_ok.parse::<SocketAddr>() {
            if !is_unusable_punch_addr(&sa) {
                addrs.push(PunchAddr::new(
                    last_ok,
                    "udp",
                    KIND_LAST_SUCCESS,
                    PRIO_LAST_SUCCESS,
                    0,
                ));
            }
        }
    }

    // 2. 主/备 STUN 映射。
    //    known_mapped 有值（启动时已探测缓存）→ 直接用，跳过现场 STUN，
    //    也就不需要 socket——避免每次上报都白 bind 一个 UDP socket（还会
    //    重复抢一次 UPnP 本地端口）；
    //    否则现场探测（同一 socket，保证与后续打洞的 NAT 映射一致）。
    let owned_socket;
    let sock: Option<&Arc<tokio::net::UdpSocket>> = match socket {
        Some(s) if known_mapped.is_none() => Some(s),
        None if known_mapped.is_none() => match create_punch_socket_v4().await {
            Ok(s) => {
                owned_socket = s;
                Some(&owned_socket)
            }
            Err(_) => None,
        },
        _ => None,
    };
    match known_mapped {
        Some((primary, _delta)) => {
            primary_ip = Some(primary.ip());
            addrs.push(PunchAddr::new(
                primary.to_string(),
                "udp",
                KIND_STUN_PRIMARY,
                PRIO_STUN_PRIMARY,
                0,
            ));
        }
        None => {
            if let Some(sock) = sock {
                if let Ok((primary, _)) = stun_query_with_socket(sock).await {
                    primary_ip = Some(primary.ip());
                    addrs.push(PunchAddr::new(
                        primary.to_string(),
                        "udp",
                        KIND_STUN_PRIMARY,
                        PRIO_STUN_PRIMARY,
                        0,
                    ));
                    // 备 STUN：对称 NAT 下不同目的地址的映射不同，对端可一并尝试
                    let servers = get_stun_servers_v4();
                    let alt_srv = servers.get(1).or_else(|| servers.first());
                    if let Some(alt_srv) = alt_srv {
                        if let Ok((alt_res, _)) = stun_query_single_server(sock, alt_srv).await {
                            if alt_res != primary {
                                let delta = alt_res.port() as i32 - primary.port() as i32;
                                addrs.push(PunchAddr::new(
                                    alt_res.to_string(),
                                    "udp",
                                    KIND_STUN_ALT,
                                    PRIO_STUN_ALT,
                                    delta,
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    // 3. LAN 网卡地址 + direct-access-port（双方同网段时根本不用穿透）
    let direct_port = get_direct_access_port();
    for ip in crate::common::get_all_ipv4_addrs() {
        addrs.push(PunchAddr::new(
            format!("{}:{}", ip, direct_port),
            "tcp",
            KIND_LAN,
            PRIO_LAN,
            0,
        ));
    }

    // 4. TCP 同步打开候选：主映射 IP + direct-access-port（UDP 全封时用）
    if let Some(ip) = primary_ip {
        addrs.push(PunchAddr::new(
            format!("{}:{}", ip, direct_port),
            "tcp",
            KIND_TCP,
            PRIO_TCP,
            0,
        ));
    }

    // 4.5 公网 IPv6（双栈直连）：双方都有全球单播 v6 时，v6 路径没有 NAT，
    //     成功率与延迟都优于 v4 打洞，对端会优先尝试（见 punch_ipv6_direct）。
    //
    // 端口必须取**共享 v6 打洞 socket 的实际本地端口**：test_ipv6() 缓存的地址
    // 端口是 0，而 is_unusable_punch_addr() 会把端口 0 判为不可打洞直接丢弃
    // （Phase3 曾经因此让 IPv6 打洞单向死亡）。v6 无 NAT66，本地端口即对端
    // 该打的目标端口。
    //
    // **绝不能在关键路径上等待探测**：test_ipv6 -> test_bind_ipv6 里的
    // `STUNS_V6[0].to_socket_addrs()` 是同步阻塞的 getaddrinfo，直接跑在 async
    // 任务里；纯 v4 环境下缓存永远为空，每次上报都会触发一次 DNS（Windows
    // resolver 超时常见 5-10s），会把连接预算吃掉一大块。因此这里只 kick 一次
    // 后台探测、绝不等它，本轮没有缓存就不上报 v6，下一轮（心跳附带上报 /
    // 看门狗）缓存就绪后自然带上。
    if crate::get_ipv6_punch_enabled() {
        if get_cached_ipv6_addr().is_none() {
            // 后台探测：不 await、不阻塞上报路径
            hbb_common::tokio::spawn(async {
                let _ = crate::test_ipv6().await;
            });
        } else if let Some(v6_addr) = get_cached_ipv6_addr() {
            match ensure_shared_punch_socket_v6().await {
                Ok(sock) => {
                    if let Ok(local) = sock.local_addr() {
                        if local.port() > 0 && !v6_addr.ip().is_unspecified() {
                            addrs.push(PunchAddr::new(
                                SocketAddr::new(v6_addr.ip(), local.port()).to_string(),
                                "udp6",
                                KIND_V6,
                                PRIO_V6,
                                0,
                            ));
                        }
                    }
                }
                Err(e) => log::debug!("collect_punch_addrs: no IPv6 punch socket: {:?}", e),
            }
        }
    }

    // 5. 网络环境 meta：对端据此选策略
    let nat_type = match Config::get_nat_type() {
        2 => "symmetric",
        1 => "cone",
        _ => "unknown",
    };
    let meta = PunchMeta {
        nat_type: nat_type.to_owned(),
        upnp: get_upnp_port() > 0,
        via_proxy: Config::get_socks().is_some() || hbb_common::config::use_ws(),
    };

    (addrs, meta)
}

/// 记录上一会话成功的本机映射地址（下次上报为 last_success，优先被打）
pub fn save_punch_last_success(addr: &std::net::SocketAddr) {
    LocalConfig::set_option("punch-last-success".to_owned(), addr.to_string());
}

/// 控制端：通过 api-server 交换地址，在**建立中继之前**完成 UDP 打洞。
///
/// 流程：收集本机全部候选地址 → 上报（target=对端）→ 拉取对端地址与环境 →
/// 双方约定时刻同时发起 → 多地址轮转打洞（含对称 NAT 端口预测）。
/// 成功返回 (KcpStream, Stream, 对端地址)；失败返回 Err，调用方回退中继流程。
///
/// 这样连接建立时就直接是直连会话：不存在"先中继再换流"，也就没有双连接
/// 与换流竞态（切换后无法操作的根因）。
pub async fn punch_direct_via_api(
    peer_id: &str,
    total_budget: std::time::Duration,
) -> ResultType<(crate::kcp_stream::KcpStream, Stream, std::net::SocketAddr)> {
    // 1. 打洞 socket：用进程级共享 socket（启动时已创建并完成 STUN，
    //    映射已缓存），保证 NAT 映射一致且无需重新探测。
    let socket_v4 = ensure_shared_punch_socket().await?;

    // 总死线**从函数入口算起**：STUN、report、等对端地址、sync 等待与打洞
    // 全部计入预算，保证任何情况下打洞窗口都不为零。调用方按 `total_budget`
    // 控制用户等待时长，超支会让"回退中继"迟迟不发生（原来死线是在
    // STUN + report 之后才创建的，最坏多花 8s 以上）。
    let total_deadline = std::time::Instant::now() + total_budget;

    // 2. 收集本机全部候选地址 + 网络环境。
    //    缓存热时直接复用启动时探测的映射，跳过现场 1-4s STUN；
    //    缓存过期或缺失时才探测（ensure_punch_mapped 内部处理）。
    //    顺序：先探测/收集，再申领共享 socket 使用权——提前申领会让我们
    //    自己的探测被"socket 忙"判断挡掉。
    let known_mapped = ensure_punch_mapped().await;
    let (my_addrs, my_meta) = collect_punch_addrs(Some(&socket_v4), known_mapped).await;
    if my_addrs.is_empty() {
        bail!("punch_direct_via_api: no local punch address");
    }
    // 共享 socket 使用凭证：从这里一直持有到函数返回（含整个打洞过程），
    // 期间看门狗/其它 STUN 探测会跳过，不会 recv_from 抢走我们的报文。
    let _shared_use = shared_socket_acquire();
    let my_primary = my_addrs
        .iter()
        .find(|a| a.kind == crate::hbbs_http::punch_exchange::KIND_STUN_PRIMARY)
        .and_then(|a| a.addr.parse::<SocketAddr>().ok());
    log::info!(
        "punch_direct_via_api: collected {} local candidate addr(s), nat_type={}",
        my_addrs.len(),
        my_meta.nat_type
    );

    // 3. 上报自己的地址（target=对端，让被控端发现连接请求）
    let ntp_offset = ntp_offset_ms().await;
    let sync_at_ms = punch_sync_boundary(now_ms_local() + ntp_offset.unwrap_or(0));
    let addrs: Vec<_> = my_addrs
        .into_iter()
        .map(|mut a| {
            a.sync_at_ms = sync_at_ms;
            a
        })
        .collect();
    if let Err(e) =
        crate::hbbs_http::punch_exchange::report(&Config::get_id(), peer_id, &my_meta, addrs)
            .await
    {
        log::info!("punch_direct_via_api: report failed: {:?}", e);
        return Err(e);
    }

    // 4. 拉取对端地址与环境（对端可能还没上报，轮询等待）。
    // 等待上限 10s：正好覆盖被控端一个上报周期；超过则多半是对端没配
    // api-server（旧版），早退回退中继，不白等——等太久会把打洞窗口挤没。
    let addr_wait_cap = Duration::from_secs(10);
    let deadline = (std::time::Instant::now() + addr_wait_cap).min(total_deadline);
    let mut peer_info = crate::hbbs_http::punch_exchange::PeerPunchInfo::default();
    while std::time::Instant::now() < deadline {
        peer_info = crate::hbbs_http::punch_exchange::get_peer_info(peer_id).await;
        if !peer_info.addrs.is_empty() {
            break;
        }
        hbb_common::tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let peer_addrs = crate::hbbs_http::punch_exchange::sort_addrs(peer_info.addrs.clone());
    if peer_addrs.is_empty() {
        bail!("punch_direct_via_api: no peer address from api-server");
    }
    let peer_symmetric = peer_info.meta.nat_type == "symmetric";
    log::info!(
        "punch_direct_via_api: got {} peer candidate addr(s), peer nat_type={} (symmetric={})",
        peer_addrs.len(),
        peer_info.meta.nat_type,
        peer_symmetric
    );

    // 5. 双方约定起始时刻取较大值，两端才会在同一绝对时刻同时发起。
    // 关键约束：等待必须为打洞留出至少 PUNCH_MIN_WINDOW 的窗口——
    // 否则"等地址 10s + sync 对齐 10s"会把 30s 预算吃光，打洞只剩 0-3s，
    // 多地址轮转形同虚设。因此 sync 等待最多等到"死线前 10s"。
    const PUNCH_MIN_WINDOW: Duration = Duration::from_secs(10);
    // NAT 类型错峰（与 relay_upgrade_task 同一规则）：宽容端（ASYMMETRIC）先打。
    // 对端类型取自 api-server 交换的 meta.nat_type（比 PunchHoleResponse 回显可靠）。
    let my_nat_type = crate::get_nat_type(100).await;
    let mut sync_offset_ms: i64 = 0;
    if my_nat_type == NatType::ASYMMETRIC as i32 && peer_symmetric {
        sync_offset_ms = -(PUNCH_INITIATOR_LEAD_MS as i64);
    } else if my_nat_type == NatType::SYMMETRIC as i32
        && peer_info.meta.nat_type == "cone"
    {
        sync_offset_ms = PUNCH_FOLLOWER_DELAY_MS as i64;
    }
    if sync_offset_ms != 0 {
        log::info!(
            "punch_direct_via_api: NAT 错峰生效：本端 nat={} / 对端 {}，offset={}ms（宽容端先打）",
            my_nat_type,
            peer_info.meta.nat_type,
            sync_offset_ms
        );
    }
    let peer_sync_at = peer_addrs.iter().map(|a| a.sync_at_ms).max().unwrap_or(0);
    let target_sync = sync_at_ms.max(peer_sync_at);
    if target_sync > 0 {
        let wait = target_sync + sync_offset_ms - now_ms_local();
        if wait > 0 && wait <= 30_000 {
            let now = std::time::Instant::now();
            let wanted = now + Duration::from_millis(wait as u64);
            // 最晚等到"死线前保留最小打洞窗口"的时刻
            // （Instant 没有 saturating_sub，用 checked_sub + 兜底）
            let latest = total_deadline
                .checked_sub(PUNCH_MIN_WINDOW)
                .unwrap_or(total_deadline);
            let until = wanted.min(latest);
            let actual_ms = until.saturating_duration_since(now).as_millis() as u64;
            if actual_ms > 0 {
                log::info!(
                    "punch_direct_via_api: waiting {}ms until sync_at {} (own={}, peer={}, clipped to keep {}s punch window)",
                    actual_ms,
                    target_sync,
                    sync_at_ms,
                    peer_sync_at,
                    PUNCH_MIN_WINDOW.as_secs()
                );
                hbb_common::tokio::time::sleep(Duration::from_millis(actual_ms)).await;
            }
        }
    }

    // 6. 组装打洞目标列表（按对端建议优先级）：
    //    - UDP 类地址全部纳入（**IPv6 除外**：v4 共享 socket 无法 send_to v6 目标，
    //      若混进来会占掉前面 idx<2 的 8s 高优先槽位而必然失败，反而压低 v4 成功率；
    //      v6 走下面独立的 IPv6 快速通道）
    let mut targets: Vec<SocketAddr> = Vec::new();
    // 对端公网 IPv6（取第一个可用）——用于 v6 直连快速通道
    let mut peer_v6: Option<SocketAddr> = None;
    for a in &peer_addrs {
        if !a.is_udp() {
            continue;
        }
        let Ok(sa) = a.addr.parse::<SocketAddr>() else {
            continue;
        };
        if is_unusable_punch_addr(&sa) {
            continue;
        }
        if sa.is_ipv6() {
            if peer_v6.is_none() {
                peer_v6 = Some(sa);
            }
            continue;
        }
        if !targets.contains(&sa) {
            targets.push(sa);
        }
        if peer_symmetric && a.delta != 0 {
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
    // 对称 NAT 下对前几个主要地址补充 ±1/±2 的小幅漂移（映射可能逐目的微增）
    if peer_symmetric {
        let base: Vec<SocketAddr> = targets.iter().take(3).cloned().collect();
        for b in base {
            for off in [-2i32, -1, 1, 2] {
                let p = b.port() as i32 + off;
                if (1..=65535).contains(&p) {
                    let mut t = b;
                    t.set_port(p as u16);
                    if !targets.contains(&t) {
                        targets.push(t);
                    }
                }
            }
        }
    }
    if targets.is_empty() && peer_v6.is_none() {
        bail!("punch_direct_via_api: no usable peer UDP target");
    }
    log::info!(
        "punch_direct_via_api: {} UDP target(s) to try: {:?}, peer IPv6: {:?}",
        targets.len(),
        targets,
        peer_v6
    );

    // 6.5 IPv6 直连快速通道：与下面的 v4 轮转**并发**进行，绝不串行占用预算。
    //
    // 为什么必须并发而不是"先跑 v6 再跑 v4"：串行时 v6 的耗时是从同一个
    // total_deadline 里切走的，v4 的每目标 8s/3s 槽位会少打 1~2 个
    // （对称 NAT 的 delta / 正负漂移地址首当其冲），等于变相降低 v4 成功率。
    // 并发则完全不碰 v4 的墙钟：v6 只是抢跑，先握手成功就用它，没成功就
    // 静默退出，v4 轮转的时序、顺序、预算与改动前逐字一致。
    //
    // v6 无 NAT、无需 sync 对齐与端口预测，因此可以和 v4 同时起跑；
    // 控制端这里 connect（主机端对应 accept）。
    // 熔断已跳闸时不 spawn：省掉一次注定失败的后台任务（只跳过 v6，不影响 v4）。
    let mut v6_task = peer_v6.filter(|_| !should_skip_v6()).map(|v6| {
        let shared_use = shared_socket_acquire();
        hbb_common::tokio::spawn(async move {
            // 凭证随任务存活：v6 打洞期间不让看门狗探测抢包
            let _hold = shared_use;
            // 预算与 v4 总预算同源：v4 都放弃时 v6 也没有意义了
            let budget = total_deadline
                .saturating_duration_since(std::time::Instant::now())
                .max(Duration::from_millis(800));
            punch_ipv6_direct(v6, true, budget).await
        })
    });

    // 7. 多地址轮转打洞：预算内轮换。时间向高优先级倾斜——列表前两个
    //    （last_success / stun_primary，命中率最高）各 8s，其后每个 3s
    //    （覆盖 stun_alt 与对称 NAT 预测地址）。total_deadline 统一截断。
    let started = std::time::Instant::now();
    for (idx, target) in targets.iter().enumerate() {
        let target = *target;
        if std::time::Instant::now() >= total_deadline {
            break;
        }
        let per_target = if idx < 2 {
            Duration::from_secs(8)
        } else {
            Duration::from_secs(3)
        };
        let target_deadline = (std::time::Instant::now() + per_target).min(total_deadline);
        while std::time::Instant::now() < target_deadline {
            // v4 每轮之间顺带看一眼 v6 是否已经抢先成功：成功就没有继续
            // 打 v4 的必要（只省时间，不影响 v4 自身的任何参数）。
            // take() 后未完成则放回，避免在循环里反复借用。
            if let Some(task) = v6_task.take() {
                if task.is_finished() {
                    match task.await {
                        Ok(Ok((kcp, stream, addr))) => {
                            log::info!(
                                "[NAT穿透] 中继前 IPv6 直连成功({}), 耗时={:?}",
                                addr,
                                started.elapsed()
                            );
                            if let Some(mine) = my_primary {
                                save_punch_last_success(&mine);
                            }
                            return Ok((kcp, stream, addr));
                        }
                        // v6 失败/取消是常态，v4 继续
                        _ => log::debug!("punch_direct_via_api: IPv6 fast path ended without link"),
                    }
                } else {
                    v6_task = Some(task);
                }
            }
            let sock = socket_v4.clone();
            // 未 connect 模式：不 connect（共享 socket 要同时服务多个对端），
            // 用一次 send_to 探测目标可达性，失败即换下一个目标
            if sock.send_to(&[], target).await.is_err() {
                break;
            }
            let remain_ms = total_deadline
                .saturating_duration_since(std::time::Instant::now())
                .as_millis() as u64;
            let round_ms = PUNCH_ROUND_MS.min(remain_ms.max(500));
            if let Some((kcp, stream)) =
                punch_target_sustained(&sock, target, round_ms, true, Some(target)).await
            {
                // 优先确认 v6 是否也已成功：v6 无 NAT、无端口映射老化问题，
                // 且主机端同样会优先采纳 v6。若此处直接返回 v4，而主机端此刻
                // 恰好采纳了 v6，两端就会各用一条链路（都能握手但互不相通），
                // 本可直连的会话退化为中继。因此 v6 已就绪时让位给 v6。
                if let Some(task) = v6_task.take() {
                    if task.is_finished() {
                        if let Ok(Ok((v6_kcp, v6_stream, v6_addr))) = task.await {
                            log::info!(
                                "[NAT穿透] 中继前 IPv6 直连成功({}), 耗时={:?}（v4 {} 同时成功，优先 v6）",
                                v6_addr,
                                started.elapsed(),
                                target
                            );
                            if let Some(mine) = my_primary {
                                save_punch_last_success(&mine);
                            }
                            return Ok((v6_kcp, v6_stream, v6_addr));
                        }
                    } else {
                        // v6 还没结果：abort 掉，v4 已经拿到可用链路。
                        // JoinHandle 的 drop 不会中止 tokio 任务，不 abort 的话
                        // 它会继续向对端 v6 发包、继续持有 v6 socket 的 kcp_io
                        // 读循环直到自己超时；快速重连会累积多个游离任务同时
                        // recv_from 同一个共享 v6 socket，互相吞包。
                        task.abort();
                    }
                }
                log::info!(
                    "[NAT穿透] 中继前打洞成功({}), 耗时={:?}",
                    target,
                    started.elapsed()
                );
                // 记录本机映射为 last_success，下次重连优先被打
                if let Some(mine) = my_primary {
                    save_punch_last_success(&mine);
                }
                return Ok((kcp, stream, target));
            }
        }
    }
    // v4 轮转结束仍未成功：给 v6 一个收尾机会（它可能刚好还在握手中）。
    // 只等剩余预算，不多占任何时间。
    if let Some(task) = v6_task {
        if let Ok(Ok((kcp, stream, addr))) = task.await {
            log::info!(
                "[NAT穿透] 中继前 IPv6 直连成功({}, v4 轮转结束后收尾)",
                addr
            );
            if let Some(mine) = my_primary {
                save_punch_last_success(&mine);
            }
            return Ok((kcp, stream, addr));
        }
    }
    bail!("punch_direct_via_api: punch failed after {:?}", started.elapsed())
}

/// 被控端：上报自己的全部候选打洞地址与网络环境。控制端据此拉取。
/// 返回是否上报成功（api-server 未配置/探测失败均返回 false）。
pub async fn punch_report_own_addrs() -> bool {
    if !crate::hbbs_http::punch_exchange::available() {
        return false;
    }
    // 附带上报节流（见 PUNCH_REPORT_THROTTLE）内刚上报过则跳过（启动/IP 变化/
    // 心跳附带都会调用，避免反复打 API）。
    // 注意：映射看门狗检测到变化、以及 IP 变化都走 report_own_addrs_inner，
    // 绕开此节流——那两种情况下必须让对端尽快拿到新地址。
    if punch_report_throttled() {
        return true;
    }
    report_own_addrs_inner().await
}

/// 在单个 target 上"持续打洞"：整个时长内并行维持空包（防止 NAT 映射老化），
/// 同时 KCP 在该时长内反复握手。返回 Some 表示打洞成功。
///
/// 这是修复成功率≈0 的关键：改成"长时间持续打同一个精确地址"后，即使两端
/// 起始时刻相差数十秒，窗口仍有大量重叠，双方能真正同时向对方发包。
///
/// `peer_addr` 为 Some 时使用未 connect 模式（KCP 包 send_to 到该地址，
/// 空包亦然）——进程级共享 socket 由此可同时服务多个对端（一对多远程）；
/// 为 None 时沿用 connect 模式（调用方已 connect，旧 Phase3 路径）。
pub async fn punch_target_sustained(
    socket: &Arc<UdpSocket>,
    target: std::net::SocketAddr,
    duration_ms: u64,
    prefer_connect: bool,
    peer_addr: Option<std::net::SocketAddr>,
) -> Option<(crate::kcp_stream::KcpStream, Stream)> {
    // 后台持续发空包维持 NAT 映射（打洞成败取决于双方是否都在持续发包）
    let sock = socket.clone();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_flag = stop.clone();
    let burst = hbb_common::tokio::spawn(async move {
        while !stop_flag.load(std::sync::atomic::Ordering::SeqCst) {
            let send_res = match peer_addr {
                Some(a) => sock.send_to(&[], a).await.map(|_| ()),
                None => sock.send(&[]).await.map(|_| ()),
            };
            if send_res.is_err() {
                break;
            }
            hbb_common::tokio::time::sleep(Duration::from_millis(PUNCH_BURST_INTERVAL_MS)).await;
        }
    });

    let res = crate::kcp_stream::KcpStream::race(
        socket.clone(),
        Duration::from_millis(duration_ms),
        prefer_connect,
        peer_addr,
    )
    .await;

    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = burst.await;
    match res {
        Ok(v) => Some(v),
        Err(_) => None,
    }
}

/// IPv6 直连快速通道（中继前）：双方都是全球单播 IPv6 时，v6 路径无 NAT，
/// 一次打洞即可直连，成功率和延迟都优于 v4 打洞。
///
/// **不占用、不影响** v4 打洞的预算与时序：
///   - 独立 socket（共享 v6 socket）、独立 KCP endpoint；
///   - 调用方用 `tokio::spawn` 让它与 v4 轮转**并发**跑，只把 `total_deadline`
///     的剩余时间当作自己的死线，**不从 v4 的预算里切走任何时间**；
///   - 失败即返回 Err，调用方静默忽略，v4 轮转照原样继续。
///
/// 返回 (KcpStream, Stream, 对端地址)。
///
/// `prefer_connect`：控制端 connect、主机端 accept，与 v4 打洞同一约定。
/// 双方都只有 v6 地址时不存在 NAT 映射差异，"同时发起"即可，无需 delta/错峰。
pub async fn punch_ipv6_direct(
    peer_v6: SocketAddr,
    prefer_connect: bool,
    budget: Duration,
) -> ResultType<(crate::kcp_stream::KcpStream, Stream, SocketAddr)> {
    if !peer_v6.is_ipv6() {
        bail!("punch_ipv6_direct: not an IPv6 target");
    }
    if is_unusable_punch_addr(&peer_v6) {
        bail!("punch_ipv6_direct: unusable IPv6 target {}", peer_v6);
    }
    if !crate::get_ipv6_punch_enabled() {
        bail!("punch_ipv6_direct: IPv6 punch disabled");
    }
    // 熔断：本环境下 v6 连续不通就别再起任务（只跳过 v6，v4 照常）
    if should_skip_v6() {
        bail!("punch_ipv6_direct: IPv6 punch suspended by circuit breaker");
    }
    let sock = ensure_shared_punch_socket_v6().await?;

    // 单轮时长取小值：v6 无 NAT，握手窗口只需覆盖一个 RTT 级往返；
    // 预算内可重试多轮，避免偶发丢包就判失败。
    // （同时也是"排队后预算是否还够打一轮"的门槛，见下）
    const V6_ROUND_MS: u64 = 3_000;

    // 死线在拿锁**之前**就算好：排队等锁耗掉的时间也算进本次预算，
    // 避免后到的控制端靠排队把 v6 打洞时间无限延长（v4 早就结束了）。
    let started = std::time::Instant::now();
    let deadline = started + budget;

    // v6 打洞互斥：共享 v6 socket 只有一个 recv_from 投递点，多个 v6 打洞
    // 并发会互相抢包（v4 侧靠 SHARED_SOCKET_USERS 挡住 STUN 探测，v6 侧
    // 没有等价机制）。这里用一把异步锁串行化：一对多远程时后到的控制端
    // 排队等前一个 v6 打洞结束，而不是两边一起把对方的 SYN 吞掉。
    let _v6_guard = V6_PUNCH_LOCK.lock().await;

    // 排队期间预算已被吃掉：直接放弃，且**不计入熔断**——这是调度排队
    // 造成的，不是"这个环境下 v6 不通"，记进去会错误地把 v6 长期禁掉。
    //
    // 门槛取 V6_ROUND_MS（3s）而不是 500ms：剩余不足一轮时长时打出去也
    // 必然等不到握手完成，若只判 500ms，排队后剩 1~3s 的情况仍会跑一轮
    // 必败打洞并被记成一次失败——一对多远程下连续两次这样的排队就能把
    // 本来可用的 v6 熔断 5 分钟。
    let remain_ms = deadline
        .saturating_duration_since(std::time::Instant::now())
        .as_millis() as u64;
    if remain_ms < V6_ROUND_MS {
        bail!("punch_ipv6_direct: no budget left after waiting for the v6 punch lock");
    }

    // 每轮都要求剩余时长够打满一轮：不足一轮时打出去也等不到握手完成，
    // 白耗预算还容易被误判成"v6 不通"。
    while std::time::Instant::now() < deadline {
        let remain_ms = deadline
            .saturating_duration_since(std::time::Instant::now())
            .as_millis() as u64;
        if remain_ms < V6_ROUND_MS {
            break;
        }
        let round_ms = V6_ROUND_MS.min(remain_ms);
        // 未 connect 模式（peer_addr=Some）：共享 v6 socket 可同时服务多个对端
        if let Some((kcp, stream)) =
            punch_target_sustained(&sock, peer_v6, round_ms, prefer_connect, Some(peer_v6)).await
        {
            log::info!(
                "[NAT穿透] IPv6 直连成功({}), 耗时={:?}",
                peer_v6,
                started.elapsed()
            );
            record_v6_success();
            return Ok((kcp, stream, peer_v6));
        }
    }
    // 有 v6 地址但没打通：计入熔断。注意这里只影响后续 v6 尝试。
    record_v6_failure();
    bail!(
        "punch_ipv6_direct: no IPv6 direct link to {} within {:?}",
        peer_v6,
        started.elapsed()
    )
}

/// Returns true if punch succeeded, false otherwise.
///
/// Merged with Phase 3: after STUN, sends our address out through
/// `phase3_out_tx` so io_loop can forward it to the peer via relay.
/// Receives peer's Phase 3 addresses through `phase3_peer_rx` and adds
/// them to the target list, so both sides punch through the same socket.
/// `cancel` 用于重连/断连时取消本任务：各打洞循环开头检查，已取消则立即退出。
pub async fn relay_upgrade_task(
    peer_addrs: Vec<SocketAddr>,
    notify: Arc<hbb_common::tokio::sync::Notify>,
    direct_stream: Arc<hbb_common::tokio::sync::Mutex<Option<Stream>>>,
    kcp_handle: Arc<std::sync::Mutex<Option<crate::kcp_stream::KcpStream>>>,
    punch_port: u16,
    phase3_out_tx: mpsc::Sender<PunchAddrInfo>,
    phase3_peer_rx: Arc<std::sync::Mutex<Vec<PunchAddrInfo>>>,
    phase3_tcp_rx: Arc<std::sync::Mutex<Vec<std::net::SocketAddr>>>,
    cancel: hbb_common::tokio_util::sync::CancellationToken,
    // 对端 NAT 类型（NatType as i32）：用于 NAT 错峰——宽容端（ASYMMETRIC）先打。
    peer_nat_type: i32,
    // 穿透阶段流水线：各阶段 send 一行描述，io_loop 转发到 UI 磁贴，
    // 让用户看到全流程（开始→STUN→等地址→等同步→阶段A→阶段B→阶段C→结果），
    // 便于判断卡在哪一步。std mpsc 无阻塞，任务在独立线程运行。
    stage_tx: std::sync::mpsc::Sender<String>,
) -> bool {
    // 先等 relay 稳定再启动任何网络操作，避免与中继抢 tokio 运行时资源
    hbb_common::tokio::time::sleep(Duration::from_secs(3)).await;

    // #2: if we haven't determined NAT type yet, try STUN-based detection.
    if Config::get_nat_type() == 0 {
        match detect_symmetric_nat().await {
            Ok(true) => {
                log::info!("relay_upgrade_task: detected SYMMETRIC NAT");
                Config::set_nat_type(NatType::SYMMETRIC as _);
            }
            Ok(false) => {
                Config::set_nat_type(NatType::ASYMMETRIC as _);
            }
            Err(e) => {
                // 检测失败不写配置（保持未知，下次连接重测），按"未知"继续尝试打洞
                log::info!(
                    "relay_upgrade_task: NAT type detection inconclusive ({:?}), keep unknown and continue punching",
                    e
                );
            }
        }
    }

    // 对称 NAT 分两种处理：
    // - 经代理/WS 连接时打洞无意义，直接放弃走中继；
    // - 否则进入"低强度模式"：只打精确地址 + 对端 delta 预测端口（约 60s 预算），
    //   跳过阶段 B 盲扫与 TCP fallback。轻度对称 NAT（端口随目的地址线性递增，
    //   如部分 4G 热点/企业网关）有机会打通；严格对称的也不会比现在更糟
    //   （3 次失败熔断仍然生效，不会反复白打）。
    let symmetric_mode = Config::get_nat_type() == NatType::SYMMETRIC as i32;
    let via_proxy = Config::get_socks().is_some() || config::use_ws();
    if symmetric_mode && via_proxy {
        log::info!(
            "relay_upgrade_task: SYMMETRIC NAT via proxy/ws, skipping punch, staying on relay"
        );
        return false;
    }
    if symmetric_mode {
        log::info!(
            "relay_upgrade_task: SYMMETRIC NAT, low-intensity punch mode (exact + predicted port only)"
        );
    }
    // KCP 握手角色按 NAT 类型分配（修复 对称↔非对称 打不通的核心）：
    // - 对称端必须做 KCP connect 的发起方：它的 NAT 为对端目标单独分配映射端口
    //   （P_new），主动发包才能打通；它的 STUN 端口（P_stun）对入站无效。
    // - 非对称（锥形）端做 accept 方：NAT 允许任意源进入固定端口，且能收到
    //   对称端从 P_new 发来的包并正确回包。
    // 本端非对称且对端对称时，本端 accept（等对端主动），否则保持默认 connect。
    // host 端用对称规则（symmetric_mode → connect），两端互补。
    let prefer_connect = !(Config::get_nat_type() == NatType::ASYMMETRIC as i32
        && peer_nat_type == NatType::SYMMETRIC as i32);
    if !prefer_connect {
        log::info!(
            "relay_upgrade_task: local ASYMMETRIC vs peer SYMMETRIC, switching to KCP accept (peer initiates)"
        );
    }
    let nat_label = |n: i32| match n {
        1 => "非对称(锥形)",
        2 => "对称",
        _ => "未知",
    };
    let _ = stage_tx.send(format!(
        "开始穿透：本端NAT={} 对端NAT={}，角色={}",
        nat_label(Config::get_nat_type()),
        nat_label(peer_nat_type),
        if prefer_connect { "主动发起" } else { "等待对端发起" },
    ));

    // "以时间换资源"：中继已建立，不急。拉长总预算到 3 分钟，每个目标间隔 1s，
    // 空包只发 2-3 个，让 Phase3 几乎不影响中继操作。
    const TOTAL_BUDGET: Duration = Duration::from_secs(180);
    let started = std::time::Instant::now();

    // Create TCP listener for TCP simultaneous open.
    // Use UPnP local port if available for better NAT traversal.
    let upnp_local = crate::common::get_upnp_local_port();
    let tcp_listener = if upnp_local > 0 {
        tokio::net::TcpListener::bind(format!("0.0.0.0:{}", upnp_local)).await.ok()
    } else {
        tokio::net::TcpListener::bind("0.0.0.0:0").await.ok()
    };
    let tcp_listener_port = tcp_listener.as_ref()
        .and_then(|l| l.local_addr().ok())
        .map(|a| a.port());
    if tcp_listener.is_some() {
        log::info!("RelayUpgrade: created TCP listener for simultaneous open");
    }

    // Create persistent sockets for all rounds.
    let socket_v4 = {
        let bind_addr = if punch_port > 0 {
            SocketAddr::from(([0u8; 4], punch_port))
        } else {
            SocketAddr::from(([0u8; 4], 0))
        };
        match UdpSocket::bind(bind_addr).await {
            Ok(s) => Some(Arc::new(s)),
            Err(_) if punch_port > 0 => UdpSocket::bind("0.0.0.0:0").await.ok().map(Arc::new),
            Err(_) => None,
        }
    };
    let socket_v4 = match socket_v4 {
        Some(s) => s,
        None => {
            log::info!("RelayUpgrade: failed to create IPv4 socket, giving up");
            return false;
        }
    };
    let socket = socket_v4.clone();

    // 注：Phase3 不做 IPv6 打洞。v6 直连统一走中继前打洞
    // （punch_direct_via_api / punch_inbound_once），那里两端共用同一个
    // 共享 v6 socket、上报端口与实际收包端口一致、时序也对齐。
    // Phase3 此前的那套 v6（控制端 socket_v6 + 广播 PUNCH_ADDR_TYPE_IPV6、
    // 主机端阶段 B 用 punch_socket_v6 打）从来打不通，反而白烧 v4 预算：
    //   - 控制端 io_loop 会丢弃所有对端 v6 地址，v6 只有单向在发包；
    //   - 广播端口取的是 Phase3 临时 socket 的端口，与打洞用的 socket 不同；
    //   - 阶段 B 跳过 offset 0，而 v6 无 NAT、只有 offset 0 是有效端口，
    //     于是每个 v6 目标要白打 8 偏移 x 8s = 64s。
    // 因此这里不再创建 v6 socket、也不再广播 v6 地址。

    // Fresh hbbs TestNatRequest using OUR OWN socket. This replaces the
    // old approach of relying on the pre-connection test socket (which goes
    // stale on auto-reconnect). Each Phase3 invocation gets its own fresh
    // address discovery, accurate for this specific socket, right now.
    let mut targets = peer_addrs.clone();
    // 对端精确地址（由 PunchPeerAddr ready 信号提供）与对端 delta
    let mut peer_exact: Option<std::net::SocketAddr> = None;
    let mut peer_delta: i32 = 0;
    // 对端约定的打洞起始时刻（NTP 对齐）：等待时与自己的 sync_at 取较大值，
    // 两端才会在同一个绝对时刻同时发起（此前对端 sync_at 只进日志、不参与等待）
    let mut peer_sync_at_ms: i64 = 0;
    let mut our_addr: Option<std::net::SocketAddr> = None;
    let mut stun_ports: Vec<u16> = Vec::new();
    let mut selected_server: Option<String> = None;

    // Send TestNatRequest to hbbs using Phase3's own socket, get the
    // real NAT mapping for THIS socket (not a stale one from minutes ago).
    let hbbs_host = Config::get_rendezvous_server();
    if !hbbs_host.is_empty() {
        if let Some(hbbs_addr) = hbbs_host
            .to_socket_addrs()
            .ok()
            .and_then(|mut i| i.find(|a| a.is_ipv4()))
        {
            let mut msg = RendezvousMessage::new();
            msg.set_test_nat_request(TestNatRequest::default());
            if let Ok(data) = msg.write_to_bytes() {
                socket.send_to(&data, hbbs_addr).await.ok();
                let mut buf = vec![0u8; 1024];
                if let Ok(Ok((n, _))) = hbb_common::tokio::time::timeout(
                    Duration::from_secs(3),
                    socket.recv_from(&mut buf),
                )
                .await
                {
                    if let Ok(msg_in) = RendezvousMessage::parse_from_bytes(&buf[..n]) {
                        if let Some(rendezvous_message::Union::TestNatResponse(resp)) = msg_in.union {
                            let port = resp.port as u16;
                            // Extract IP from response (new server) or from sender addr
                            let ip = if !resp.ip.is_empty() {
                                String::from_utf8(resp.ip.to_vec()).unwrap_or_default()
                            } else {
                                hbbs_addr.ip().to_string()
                            };
                            if let Ok(addr) = format!("{}:{}", ip, port).parse::<std::net::SocketAddr>() {
                                our_addr = Some(addr);
                                stun_ports.push(addr.port());
                                log::info!("[NAT穿透] Phase3 hbbs-inline: {} (skip STUN)", addr);
                            }
                        }
                    }
                }
            }
        }
    }

    // Only run STUN if hbbs didn't already provide the address
    if our_addr.is_none() {
        for i in 0..5 {
            // P1: retry up to 2 times with exponential backoff (200ms, 400ms)
            let mut retries: u32 = 2;
            loop {
                let result = if let Some(ref server) = selected_server {
                    // Queries 2-5: prefer locked server for consistent delta on Symmetric NAT
                    stun_query_single_server(&socket, server).await
                } else {
                    // Query 1: race all servers, pick the best
                    stun_query_with_socket(&socket).await
                };
                match result {
                    Ok((addr, srv)) => {
                        if our_addr.is_none() {
                            selected_server = Some(srv.clone());
                            our_addr = Some(addr);
                            log::info!(
                                "RelayUpgrade STUN #{}: mapped {} (port {}, server {})",
                                i + 1,
                                addr,
                                addr.port(),
                                srv
                            );
                            if let Ok(mut public) = PUBLIC_ADDR.lock() {
                                *public = addr.to_string();
                            }
                        } else {
                            log::info!(
                                "RelayUpgrade STUN #{}: port {} via {}",
                                i + 1,
                                addr.port(),
                                srv
                            );
                        }
                        stun_ports.push(addr.port());
                        break; // success, exit retry loop
                    }
                    Err(e) => {
                        if retries > 0 {
                            let delay = Duration::from_millis(200 * (3 - retries) as u64);
                            log::info!(
                                "RelayUpgrade STUN #{} retrying in {:?} ({}/2): {:?}",
                                i + 1,
                                delay,
                                3 - retries,
                                e
                            );
                            retries -= 1;
                            hbb_common::tokio::time::sleep(delay).await;
                            // P0: if locked server exhausted retries, try other servers one by one.
                            // Cannot use stun_query_with_socket (concurrent send_to) because
                            // the socket may be connected (Windows WSAEISCONN on send_to mismatch).
                            if retries == 0 && selected_server.is_some() {
                                log::info!("RelayUpgrade STUN #{} locked server unresponsive, trying other servers one by one", i + 1);
                                let alt_servers = get_stun_servers_v4();
                                let mut fallback_ok = false;
                                for alt_srv in alt_servers.iter() {
                                    if selected_server.as_ref().map_or(false, |s| s == alt_srv) {
                                        continue; // skip the already-failed locked server
                                    }
                                    match stun_query_single_server(&socket, alt_srv).await {
                                        Ok((fb_addr, fb_srv)) => {
                                            log::info!("RelayUpgrade STUN #{} fallback OK via {} (port {})",
                                            i + 1, fb_srv, fb_addr.port());
                                            selected_server = Some(fb_srv);
                                            stun_ports.push(fb_addr.port());
                                            fallback_ok = true;
                                            break;
                                        }
                                        Err(e) => {
                                            log::info!(
                                                "RelayUpgrade STUN #{} fallback vs {} failed: {:?}",
                                                i + 1,
                                                alt_srv,
                                                e
                                            );
                                        }
                                    }
                                }
                                if !fallback_ok {
                                    log::info!(
                                        "RelayUpgrade STUN #{} all fallback servers failed",
                                        i + 1
                                    );
                                }
                            }
                            continue;
                        }
                        // All retries exhausted, no locked server to fall back from
                        if our_addr.is_none() {
                            log::info!(
                                "RelayUpgrade STUN #{} failed after retries: {:?}",
                                i + 1,
                                e
                            );
                        } else {
                            log::info!(
                                "RelayUpgrade STUN #{} failed, stopping prediction: {:?}",
                                i + 1,
                                e
                            );
                        }
                        break;
                    }
                }
            }
            // If very first query completely failed (no our_addr yet), exit early
            if our_addr.is_none() && selected_server.is_none() {
                break;
            }
            hbb_common::tokio::time::sleep(Duration::from_millis(50)).await;
        }
    } // end if our_addr.is_none() (skip STUN when hbbs already provided addr)

    // Phase 3: send our public address to peer through relay.
    // We delay this until AFTER delta measurement so we can include
    // the predicted port for symmetric NAT (BUGFIX: on symmetric NAT,
    // the STUN-discovered port differs from the port the peer must punch to).
    // TCP listener is sent immediately (it doesn't need delta).
    //
    // 注意：这里**不**广播 IPv6 地址。Phase3 的 v6 打洞已整体移除（见上方
    // 创建 socket 处的说明）——它从来打不通，只会让主机端白烧 v4 预算。
    // v6 直连统一走中继前打洞。
    // If hbbs-discovered address differs from STUN (symmetric NAT), send it
    // first as the primary target so the peer starts punching immediately.
    // Send TCP listener address for TCP simultaneous open fallback.
    if let Some(tcp_port) = tcp_listener_port {
        let tcp_addr = std::net::SocketAddr::new(
            our_addr.map(|a| a.ip()).unwrap_or(std::net::Ipv4Addr::UNSPECIFIED.into()),
            tcp_port,
        );
        let _ = phase3_out_tx.try_send(PunchAddrInfo::new_tcp(tcp_addr));
        log::info!("Phase3: sent TCP listener address: {}", tcp_addr);
    }

    // Measure our symmetric NAT delta by querying different STUN servers.
    // On symmetric NAT, the mapped port changes for each different destination.
    // P2: Try multiple alternative servers and take the consensus delta
    // (most common non-zero value) instead of relying on a single server.
    // Cone NAT（已缓存 ASYMMETRIC）端口恒定，delta 必为 0，跳过测量
    // 以缩小两端打洞起始时刻差。
    let our_delta: i32 = if Config::get_nat_type() != NatType::ASYMMETRIC as i32
        && stun_ports.len() >= 2
        && our_addr.is_some()
    {
        let base_port = stun_ports[0] as i32;
        let alt_servers = get_stun_servers_v4();
        let mut deltas: Vec<i32> = Vec::new();
        // Try up to 4 different servers, collect all non-zero deltas
        for alt in alt_servers.iter() {
            // Skip the primary server (delta to self is always 0)
            if selected_server.as_ref().map_or(false, |s| s == alt) {
                continue;
            }
            if let Some(alt_addr) = alt.to_socket_addrs().ok()
                .and_then(|mut i| i.find(|a| a.is_ipv4()))
            {
                // 用 send_to 而非 connect：connect 会让 socket 之后只接收
                // connected 对端的包（内核过滤），打洞阶段对端从其它映射端口
                // （对称 NAT 的 P_new）回来的 KCP 包会被全部丢弃。
                socket.send_to(&[], alt_addr).await.ok();
                match stun_query_single_server(&socket, alt).await {
                    Ok((alt_result, _)) => {
                        let delta = alt_result.port() as i32 - base_port;
                        log::info!("RelayUpgrade: delta vs {}: {} (port {} vs {})",
                            alt, delta, base_port, alt_result.port());
                        if delta != 0 {
                            deltas.push(delta);
                        }
                    }
                    Err(e) => {
                        log::info!("RelayUpgrade: delta vs {} failed: {:?}", alt, e);
                    }
                }
                hbb_common::tokio::time::sleep(Duration::from_millis(30)).await;
            }
            // Collect up to 4 deltas for consensus
            if deltas.len() >= 4 {
                break;
            }
        }
        // P2 consensus: pick the most frequent non-zero delta
        if deltas.is_empty() {
            log::info!("RelayUpgrade: no symmetric delta (all alt STUN failed or returned 0)");
            0
        } else if deltas.len() == 1 {
            log::info!("RelayUpgrade: symmetric delta = {} (only 1 server worked)", deltas[0]);
            deltas[0]
        } else {
            // Find most common delta value
            let mut freq: std::collections::HashMap<i32, usize> = std::collections::HashMap::new();
            for &d in &deltas {
                *freq.entry(d).or_insert(0) += 1;
            }
            let best = freq.into_iter().max_by_key(|&(_, count)| count).unwrap();
            log::info!("RelayUpgrade: symmetric delta = {} (consensus from {} servers, freq {})",
                best.0, deltas.len(), best.1);
            best.0
        }
    } else {
        0
    };
    // NTP 时钟校准 + 计算双方约定的打洞起始时刻（同一绝对时间轴的轮边界）。
    // 两端各自算出同一个 sync_at，从而在相同时刻同时发起，解决"窗口错位"。
    // 保存 NTP 偏移供后续等待时用（避免每次重新查询 NTP）。
    let ntp_offset_ms = ntp_offset_ms().await;
    let now_ms = || -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64 + ntp_offset_ms.unwrap_or(0))
            .unwrap_or(0)
    };
    let mut sync_at_ms: i64 = 0;
    if our_addr.is_some() {
        sync_at_ms = punch_sync_boundary(now_ms());
        log::info!("Phase3: NTP offset={:?}, now={}, sync_at={}",
            ntp_offset_ms, now_ms(), sync_at_ms);
    }

    // BUGFIX: Send our address NOW (after delta measurement) so we can include
    // the predicted port for symmetric NAT. On cone NAT both ports are the same.
    // On symmetric NAT the peer needs to try: STUN_port (for our us→STUN mapping)
    // AND STUN_port+our_delta (for our us→peer mapping).
    if let Some(addr) = our_addr {
        // ready=true 告知对端：本端 STUN 已完成，可以开始同步打洞
        let _ = phase3_out_tx.try_send(PunchAddrInfo {
            addr,
            ready: true,
            delta: our_delta,
            sync_at_ms,
            addr_type: PUNCH_ADDR_TYPE_UDP,
        });
        log::info!("Phase3: sent our address (ready, delta={}, sync_at={}) to relay loop: {}",
            our_delta, sync_at_ms, addr);
        if our_delta != 0 {
            // i32 域计算后检查 1..=65535，越界则跳过，避免 u16 回绕
            let predicted_port = addr.port() as i32 + our_delta;
            if (1..=65535).contains(&predicted_port) && predicted_port != addr.port() as i32 {
                let predicted_addr = std::net::SocketAddr::new(addr.ip(), predicted_port as u16);
                let _ = phase3_out_tx.try_send(PunchAddrInfo::new(predicted_addr));
                log::info!("Phase3: sent predicted our address for symmetric NAT: {}", predicted_addr);
            }
        }
    }

    // TCP simultaneous open 目标列表：每轮从 phase3_tcp_rx drain 累积，限量保新
    let mut tcp_targets: Vec<std::net::SocketAddr> = Vec::new();
    // 始终没收到对端地址的计时：超过 45s 就早失败，别空转满 180s
    // （否则 UI 的穿透状态会一直显示"正在穿透"）
    let mut no_peer_addr_since: Option<std::time::Instant> = None;
    for _round in 0..6 {
        if cancel.is_cancelled() {
            log::info!("[NAT穿透] Phase3 已取消（重连/断连）, 退出打洞");
            return false;
        }
        if started.elapsed() >= TOTAL_BUDGET {
            log::info!(
                "[NAT穿透] 穿透总预算({}s)超时, 放弃",
                TOTAL_BUDGET.as_secs()
            );
            return false;
        }
        // 对称低强度模式总预算 60s：只打精确 + 预测端口，不做盲扫/TCP
        if symmetric_mode && started.elapsed() >= Duration::from_secs(60) {
            log::info!("[NAT穿透] 对称低强度模式预算(60s)超时, 放弃");
            return false;
        }

        // 收集对端地址与同步信息（PunchAddrInfo）。ready=true 的是对端主 STUN
        // 地址，作为"精确地址"；delta 用于对称 NAT 端口预测。
        if let Ok(mut infos) = phase3_peer_rx.lock() {
            for info in infos.drain(..) {
                if is_unusable_punch_addr(&info.addr) {
                    continue;
                }
                if !targets.contains(&info.addr) {
                    targets.push(info.addr);
                    log::info!("Phase3: added peer address {} to targets", info.addr);
                }
                // 优先取 ready 的 IPv4 主地址作为精确地址
                // （IPv6 目标不选作精确地址：精确地址持续打洞只走 v4 socket）
                if info.ready && peer_exact.is_none() && !info.addr.is_ipv6() {
                    peer_exact = Some(info.addr);
                    peer_delta = info.delta;
                    peer_sync_at_ms = info.sync_at_ms;
                    log::info!(
                        "Phase3: peer exact address {} (ready, peer_delta={}, sync_at={})",
                        info.addr,
                        info.delta,
                        info.sync_at_ms
                    );
                    let _ = stage_tx.send(format!(
                        "已获取对端公网地址 {}（端口预测步长 {}），等待同步起始时刻",
                        info.addr, info.delta
                    ));
                }
            }
        }

        let exact = match peer_exact {
            Some(a) => a,
            None => {
                // 还没收到对端地址（对端 STUN 任务没跑/被会话门控挡下/旧版对端），
                // 短暂等待后重试；但超过 45s 仍拿不到就早失败，让 UI 及时得到终态
                let since = *no_peer_addr_since.get_or_insert_with(std::time::Instant::now);
                if since.elapsed() >= Duration::from_secs(45) {
                    log::info!(
                        "[NAT穿透] 45s 未收到对端地址, 放弃本轮 (relay 已可用, 不影响使用)"
                    );
                    return false;
                }
                hbb_common::tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        // 同步：等到"双方约定起始时刻的较大值"再开始打洞。
        // 只等自己的 sync_at 没有意义——对端发来的 sync_at 也必须参与，
        // 两端才会在同一个绝对时刻同时发起（修复前对端 sync_at 只进日志）。
        let target_sync = sync_at_ms.max(peer_sync_at_ms);
        // NAT 类型错峰：让更宽容的一端（ASYMMETRIC，映射端口固定）先发起打洞。
        // 宽容端先打 → 严格端（SYMMETRIC）的 NAT 记录其固定端口，回包才能被 Cone
        // NAT 放行；反之严格端先打，其源端口是对当前目标临时分配的（≠STUN 端口），
        // 对端 Cone/PortRestricted 会因端口不匹配丢弃 → 打洞失败。
        // host 端（relay_phase3_punch_to_peer）保持按 sync 同时打，本端单方面偏移
        // 即可实现错峰；同类型/未知 offset=0，保持现有同步同时打不变。
        // 本端类型与 _start_inner 广播时同源（桌面端走 ipc service 探测，失败返回 0
        // 即不错峰，安全降级），保证"广播什么就用什么判定"，避免两来源不一致导致错配。
        let my_nat_type = crate::get_nat_type(100).await;
        let mut sync_offset_ms: i64 = 0;
        if my_nat_type == NatType::ASYMMETRIC as i32
            && peer_nat_type == NatType::SYMMETRIC as i32
        {
            sync_offset_ms = -(PUNCH_INITIATOR_LEAD_MS as i64);
        } else if my_nat_type == NatType::SYMMETRIC as i32
            && peer_nat_type == NatType::ASYMMETRIC as i32
        {
            sync_offset_ms = PUNCH_FOLLOWER_DELAY_MS as i64;
        }
        // 错峰只影响第 1 轮的起始时刻（后续轮 target_sync 已过期），日志仅首轮打印
        if _round == 0 && sync_offset_ms != 0 {
            log::info!(
                "[NAT穿透] NAT 错峰生效：本端 nat={} / 对端 nat={}，起始 offset={}ms（宽容端先打）",
                my_nat_type,
                peer_nat_type,
                sync_offset_ms
            );
            let _ = stage_tx.send(format!(
                "等待同步起始时刻：NAT错峰 offset={}ms（{}端先打）",
                sync_offset_ms,
                if sync_offset_ms < 0 { "宽容" } else { "严格" }
            ));
        }
        if target_sync > 0 {
            let wait = target_sync + sync_offset_ms - now_ms();
            if wait > 0 && wait <= 30_000 {
                log::info!(
                    "Phase3: waiting {}ms until sync_at {} (own={}, peer={}, offset={}) to start punching",
                    wait,
                    target_sync,
                    sync_at_ms,
                    peer_sync_at_ms,
                    sync_offset_ms
                );
                hbb_common::tokio::time::sleep(Duration::from_millis(wait as u64)).await;
            }
            // wait <= 0（已过约定时刻，或提前量超过剩余等待）：立即开始，实现"先打"
        }

        // ===== 阶段 A：在精确地址上"持续打洞" =====
        // 两端使用同一时长与同一轮长(PUNCH_ROUND_MS)，
        // 只要两端的活跃窗口有重叠就能同时向对方发包打开 NAT 映射。
        // 这是修复"窗口错位"的核心：此前每 target 只打 3~4s 就换，
        // 两端步调不同步导致几乎不可能撞上。
        // 对称低强度模式：除精确地址外同时打"对端预测端口" exact+peer_delta
        // （对端每条新目的地址的映射端口按 delta 递增），时长缩短为 30s。
        let mut primary_targets: Vec<std::net::SocketAddr> = vec![exact];
        if symmetric_mode && peer_delta != 0 {
            let p = exact.port() as i32 + peer_delta;
            if (1..=65535).contains(&p) {
                let mut predicted = exact;
                predicted.set_port(p as u16);
                log::info!("[NAT穿透] 对称低强度模式：同时打对端预测端口 {}", predicted);
                primary_targets.push(predicted);
            }
        }
        let exact_ms = if symmetric_mode {
            30_000
        } else {
            PUNCH_EXACT_MS
        };
        let exact_deadline = std::time::Instant::now() + Duration::from_millis(exact_ms);
        let _ = stage_tx.send(format!(
            "阶段A：精确地址 {} 持续打洞（{}s，{}）",
            exact,
            exact_ms / 1000,
            if symmetric_mode { "对称低强度模式" } else { "标准模式" }
        ));
        while std::time::Instant::now() < exact_deadline {
            if cancel.is_cancelled() {
                return false;
            }
            if started.elapsed() >= TOTAL_BUDGET {
                break;
            }
            for target in primary_targets.iter() {
                if cancel.is_cancelled() {
                    return false;
                }
                if started.elapsed() >= TOTAL_BUDGET {
                    break;
                }
                // 未 connect 模式（peer_addr=Some）：KCP 包 send_to 目标地址，
                // 同时 recv_from 接收所有源的包（按 conv 分发）。connect 模式会让
                // 内核只投递 connected 对端的包——对端是对称 NAT 时其回包来自
                // 另一个映射端口（P_new≠P_stun），全部被丢弃，必然打不通。
                let sock = socket_v4.clone();
                let remain_ms = TOTAL_BUDGET.saturating_sub(started.elapsed()).as_millis() as u64;
                let round_ms = PUNCH_ROUND_MS.min(remain_ms.max(500));
                if let Some((kcp, stream)) =
                    punch_target_sustained(&sock, *target, round_ms, prefer_connect, Some(*target))
                        .await
                {
                    if let Ok(mut h) = kcp_handle.lock() {
                        *h = Some(kcp);
                    }
                    let mut guard = direct_stream.lock().await;
                    *guard = Some(stream);
                    notify.notify_one();
                    let kind = if *target == exact {
                        "exact"
                    } else {
                        "predicted"
                    };
                    log::info!(
                        "[NAT穿透] Phase3 KCP 打洞成功({} {}), 耗时={:?}",
                        kind,
                        target,
                        started.elapsed()
                    );
                    return true;
                }
            }
        }

        // ===== 阶段 B：统一偏移序列扫描 =====
        // 砍掉原来的 ±50 盲扫：101 个 target × 4s ≈ 414s 远超 180s 预算，
        // 一端永远走不到真正有效的地址。这里只保留"精确 + 小幅漂移"，
        // 对称 NAT 额外加入对端 delta 预测。
        // 预算重排（修复"TCP fallback 永远跑不到"）：
        // - 目标按"精确地址优先"排序，每个 offset 最多打 2 个 base
        //   （原实现 9 偏移 × N 目标 × 8s ≈ 300s+，第 2 轮和 TCP 永远没预算）；
        // - 阶段 B 封顶 40s，给后面的 TCP fallback 留出时间。
        // 对称低强度模式跳过阶段 B（预测端口已在阶段 A 打过）。
        if !symmetric_mode {
            let mut ordered: Vec<std::net::SocketAddr> = Vec::with_capacity(targets.len() + 1);
            ordered.push(exact);
            for t in targets.iter() {
                if *t != exact && !ordered.contains(t) {
                    ordered.push(*t);
                }
            }
            let mut scan_offsets: Vec<i32> = PUNCH_OFFSETS.to_vec();
            if peer_delta != 0 && !scan_offsets.contains(&peer_delta) {
                scan_offsets.insert(1, peer_delta);
            }
            let stage_b_deadline = std::time::Instant::now() + Duration::from_secs(40);
            let _ = stage_tx.send("阶段B：端口偏移扫描（精确地址 + 小幅漂移）".to_owned());
            'offsets: for &offset in scan_offsets.iter() {
                if offset == 0 {
                    continue; // 精确地址已在阶段 A 充分尝试
                }
                for base in ordered.iter().take(2) {
                    if cancel.is_cancelled() {
                        return false;
                    }
                    if started.elapsed() >= TOTAL_BUDGET {
                        break 'offsets;
                    }
                    if std::time::Instant::now() >= stage_b_deadline {
                        break 'offsets;
                    }
                    // i32 域计算端口，越界跳过，避免 u16 回绕
                    let p = base.port() as i32 + offset;
                    if !(1..=65535).contains(&p) {
                        continue;
                    }
                    let mut target = *base;
                    target.set_port(p as u16);
                    // Phase3 只做 v4 打洞：v6 目标不在阶段 B 扫描。v6 无 NAT，
                    // 只有 offset 0 有效，而这里恰好跳过 offset 0 —— 对 v6 做
                    // 偏移扫描是 100% 必败的纯浪费，会挤占 v4 的扫描预算。
                    // v6 直连统一走中继前打洞（punch_ipv6_direct）。
                    if target.is_ipv6() {
                        continue;
                    }
                    let sock = socket_v4.clone();
                    // 未 connect 模式：send_to 发包 + 收所有源的包（见阶段 A 注释）
                    let remain_ms =
                        TOTAL_BUDGET.saturating_sub(started.elapsed()).as_millis() as u64;
                    let round_ms = PUNCH_ROUND_MS.min(remain_ms.max(500));
                    if let Some((kcp, stream)) =
                        punch_target_sustained(&sock, target, round_ms, prefer_connect, Some(target))
                            .await
                    {
                        if let Ok(mut h) = kcp_handle.lock() {
                            *h = Some(kcp);
                        }
                        let mut guard = direct_stream.lock().await;
                        *guard = Some(stream);
                        notify.notify_one();
                        log::info!(
                            "[NAT穿透] Phase3 KCP 打洞成功(offset {} -> {}), 耗时={:?}",
                            offset,
                            target,
                            started.elapsed()
                        );
                        return true;
                    }
                }
            }
        }

        // If KCP failed this round, try TCP simultaneous open on the peer's
        // dedicated TCP listener port (exchanged via PunchPeerAddr). Race an
        // outbound connect against a source-validated accept on our listener;
        // both sides bounded to 3s so each target costs at most ~3s.
        // 封顶 30s：7 偏移 × 3s × N 目标最坏 168s，不封顶会把第 2 轮的
        // UDP 打洞预算全部吃光。对称低强度模式跳过（TCP 映射同样每目的
        // 地址变化，预算只给 60s，全部留给 UDP 预测打洞）——tcp_listener_ref
        // 取 None 实现整段跳过。
        let tcp_listener_ref = if symmetric_mode {
            None
        } else {
            tcp_listener.as_ref()
        };
        if let Some(listener) = tcp_listener_ref {
            // drain 本轮到达的 TCP 目标，本地累积去重，只保留最新 8 条，
            // 避免无限增长和陈旧 target（原来只 clone 不 drain）
            if let Ok(mut addrs) = phase3_tcp_rx.lock() {
                for addr in addrs.drain(..) {
                    if !tcp_targets.contains(&addr) {
                        tcp_targets.push(addr);
                    }
                }
                if tcp_targets.len() > 8 {
                    let excess = tcp_targets.len() - 8;
                    tcp_targets.drain(..excess);
                }
            }
            if !tcp_targets.is_empty() {
                let _ = stage_tx.send(format!(
                    "阶段C：TCP 同步打开（{} 个 TCP 目标）",
                    tcp_targets.len()
                ));
            }
            let tcp_deadline = std::time::Instant::now() + Duration::from_secs(30);
            for &tcp_target in &tcp_targets {
                if cancel.is_cancelled() {
                    return false;
                }
                if started.elapsed() >= TOTAL_BUDGET {
                    break;
                }
                if std::time::Instant::now() >= tcp_deadline {
                    break;
                }
                if tcp_target.is_ipv6() {
                    continue;
                }
                let ports = [0i32, 1, -1, 2, -2, 5, -5];
                for &port_offset in &ports {
                    if started.elapsed() >= TOTAL_BUDGET {
                        break;
                    }
                    if std::time::Instant::now() >= tcp_deadline {
                        break;
                    }
                    // i32 域计算端口，越界跳过，避免 u16 回绕
                    let p = tcp_target.port() as i32 + port_offset;
                    if !(1..=65535).contains(&p) { continue; }
                    let mut target = tcp_target;
                    target.set_port(p as u16);
                    let tcp_res: Option<tokio::net::TcpStream> = tokio::select! {
                        // Accept 只接受来自目标 IP 的连接，其余丢弃继续等直到 3s 超时
                        res = tokio::time::timeout(Duration::from_secs(3),
                            accept_from_ip(listener, target.ip())) => {
                            match res {
                                Ok(Some(stream)) => {
                                    log::info!("RelayUpgrade TCP: accept from {}", target);
                                    Some(stream)
                                }
                                _ => None,
                            }
                        }
                        // Connect to peer via raw TCP
                        res = tokio::time::timeout(Duration::from_secs(3),
                            tokio::net::TcpStream::connect(target)) => {
                            match res {
                                Ok(Ok(stream)) => {
                                    stream.set_nodelay(true).ok();
                                    log::info!("RelayUpgrade TCP: connect to {} succeeded!", target);
                                    Some(stream)
                                }
                                _ => None,
                            }
                        }
                    };
                    if let Some(stream) = tcp_res {
                        stream.set_nodelay(true).ok();
                        let mut guard = direct_stream.lock().await;
                        *guard = Some(Stream::from(stream, target));
                        notify.notify_one();
                        log::info!("[NAT穿透] Phase3 TCP 打洞成功: {}, 耗时={:?}",
                            target, started.elapsed());
                        return true;
                    }
                }
            }
        }

        // 轮间休息 2s，让中继操作不被 Phase3 干扰
        hbb_common::tokio::time::sleep(Duration::from_millis(2000)).await;
        // Also check for new Phase 3 addresses during gap
        if let Ok(mut infos) = phase3_peer_rx.lock() {
            for info in infos.drain(..) {
                if is_unusable_punch_addr(&info.addr) {
                    continue;
                }
                if !targets.contains(&info.addr) {
                    targets.push(info.addr);
                    log::info!("Phase3: added peer address {} to targets (during gap)", info.addr);
                }
                if info.ready && peer_exact.is_none() && !info.addr.is_ipv6() {
                    peer_exact = Some(info.addr);
                    peer_delta = info.delta;
                    peer_sync_at_ms = info.sync_at_ms;
                    log::info!("Phase3: peer exact address {} (during gap)", info.addr);
                }
            }
        }
    }
    log::info!("[NAT穿透] 穿透未成功, 耗时={:?}", started.elapsed());
    let _ = stage_tx.send(format!(
        "穿透失败：各阶段均已尝试（耗时 {}s），持续使用中继",
        started.elapsed().as_secs()
    ));
    false
}


/// Host-side Phase 3 punch: single task per connection. on_message queues
/// the connector's public addresses (received via PunchPeerAddr on the relay)
/// into `targets`; each round drains newly queued addresses, then tries every
/// known target with an empty-packet burst + KCP connect/accept race on a
/// single endpoint (host prefers accept), falling back to TCP simultaneous
/// open (connect raced against a source-validated accept on `tcp_listener`).
/// Exits when the punch succeeds, `upgraded` is set, or the 180s budget
/// (aligned with the connector's TOTAL_BUDGET) is exhausted.
///
/// `punch_socket` is an optional persistent UDP socket created before the
/// STUN query. When provided, STUN discovery and hole punching share the
/// same socket, so the NAT mapping is consistent. When `None`, a new socket
/// is created internally (fallback for edge cases).
///
/// Phase3 只做 IPv4 打洞（IPv6 直连统一走中继前打洞，见 punch_ipv6_direct）。
pub async fn relay_phase3_punch_to_peer(
    targets: Arc<hbb_common::tokio::sync::Mutex<Vec<PunchAddrInfo>>>,
    kcp_handle: Arc<std::sync::Mutex<Option<crate::kcp_stream::KcpStream>>>,
    punch_socket: Option<Arc<tokio::net::UdpSocket>>,
    tcp_listener: Option<Arc<tokio::net::TcpListener>>,
    upgraded: Arc<std::sync::atomic::AtomicBool>,
    stun_done: Arc<std::sync::atomic::AtomicBool>,
) -> ResultType<Stream> {
    // Use the persistent punch socket if available, otherwise create a new one.
    let socket = if let Some(s) = punch_socket {
        s
    } else {
        let upnp_local = crate::common::get_upnp_local_port();
        let bind_addr = if upnp_local > 0 {
            SocketAddr::from(([0u8; 4], upnp_local))
        } else {
            SocketAddr::from(([0u8; 4], 0u16))
        };
        Arc::new(UdpSocket::bind(bind_addr).await?)
    };

    // 对称 NAT：与控制端 relay_upgrade_task 的同名逻辑保持一致（两端必须同时进退）。
    // - 经代理/WS 连接时打洞无意义，直接放弃走中继；
    // - 否则进入"低强度模式"：只打精确地址 + 对端 delta 预测端口（约 60s 预算），
    //   跳过阶段 B 盲扫与 TCP fallback。
    let symmetric_mode = Config::get_nat_type() == NatType::SYMMETRIC as i32;
    let via_proxy = Config::get_socks().is_some() || config::use_ws();
    if symmetric_mode && via_proxy {
        bail!("Phase3(Host) NAT type = SYMMETRIC via proxy/ws, skipping punch");
    }
    if symmetric_mode {
        log::info!(
            "Phase3(Host): SYMMETRIC NAT, low-intensity punch mode (exact + predicted port only)"
        );
    }
    // KCP 握手角色：本机是对称 NAT 时做 connect 发起方。
    // 对称 NAT 的 STUN 端口（P_stun）对入站无效——对端发来的包会被丢弃，
    // 只有本端主动发包（NAT 为对端目标分配映射 P_new）才能打通单向，
    // 锥形对端收到后回包到 P_new 即可建立双向。与控制端规则互补：
    // 控制端在"本端非对称且对端对称"时切 accept，等本端主动。
    let prefer_connect = symmetric_mode;
    if prefer_connect {
        log::info!("Phase3(Host): SYMMETRIC NAT, switching to KCP connect (initiate punch)");
    }

    // 等待 STUN 任务结束再操作共享 socket：socket.connect(target) 会过滤掉
    // 来自其他地址的包（STUN 响应），kcp_io 的 recv_from 也会抢走 STUN 包。
    // 最多等 5s（STUN 查询本身有 3s 超时），避免打洞任务被永久阻塞。
    for _ in 0..50 {
        if stun_done.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
        hbb_common::tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 与控制端 TOTAL_BUDGET(180s) 对齐：主机任务死了控制端单方面不可能成功
    const MAX_TIME: Duration = Duration::from_secs(180);
    let started = std::time::Instant::now();

    // NTP 时钟校准 + 双方约定的打洞起始时刻（与控制端计算方式一致）
    let ntp_offset_ms = crate::common::ntp_offset_ms().await;
    let host_now_ms = || -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64 + ntp_offset_ms.unwrap_or(0))
            .unwrap_or(0)
    };
    let sync_at_ms: i64 = punch_sync_boundary(host_now_ms());
    log::info!("Phase3(Host): NTP offset={:?}, now={}, sync_at={}",
        ntp_offset_ms, host_now_ms(), sync_at_ms);

    // 对端精确地址（由 PunchPeerAddr ready 信号提供）与对端 delta
    let mut peer_exact: Option<std::net::SocketAddr> = None;
    let mut peer_delta: i32 = 0;
    // 对端约定的打洞起始时刻（NTP 对齐）：等待时与自己的 sync_at 取较大值，
    // 两端才会在同一个绝对时刻同时发起（此前对端 sync_at 只进日志）
    let mut peer_sync_at_ms: i64 = 0;
    let mut round_targets: Vec<PunchAddrInfo> = Vec::new();

    loop {
        if upgraded.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("Phase3(Host) stop punch: already upgraded");
        }
        if started.elapsed() >= MAX_TIME {
            bail!("Phase3(Host) punch timed out after {:?}", started.elapsed());
        }
        // 对称低强度模式总预算 60s：只打精确 + 预测端口，不做盲扫/TCP
        if symmetric_mode && started.elapsed() >= Duration::from_secs(60) {
            bail!("Phase3(Host) symmetric low-intensity budget (60s) exhausted");
        }

        // Drain newly arrived peer addresses from the queue.
        {
            let mut q = targets.lock().await;
            for info in q.drain(..) {
                if is_unusable_punch_addr(&info.addr) {
                    continue;
                }
                if !round_targets.iter().any(|t| t.addr == info.addr) {
                    round_targets.push(info);
                    log::info!("Phase3(Host): added peer address {} (type={}) to targets",
                        info.addr, info.addr_type);
                }
                // 优先取 ready 的 UDP 主地址作为精确地址（TCP 地址不能作为 UDP 打洞目标；
                // IPv6 地址只在阶段 B 用 v6 socket 尝试，不作精确地址）
                if info.ready && info.addr_type == PUNCH_ADDR_TYPE_UDP && peer_exact.is_none() {
                    peer_exact = Some(info.addr);
                    peer_delta = info.delta;
                    peer_sync_at_ms = info.sync_at_ms;
                    log::info!(
                        "Phase3(Host): peer exact address {} (ready, peer_delta={}, sync_at={})",
                        info.addr,
                        info.delta,
                        info.sync_at_ms
                    );
                }
            }
        }

        let exact = match peer_exact {
            Some(a) => a,
            None => {
                // 队列暂空，500ms 后看是否有新地址
                hbb_common::tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        // 同步：等到"双方约定起始时刻的较大值"再开始打洞。
        // 只等自己的 sync_at 没有意义——对端发来的 sync_at 也必须参与，
        // 两端才会在同一个绝对时刻同时发起（修复前对端 sync_at 只进日志）。
        let target_sync = sync_at_ms.max(peer_sync_at_ms);
        if target_sync > 0 {
            let wait = target_sync - host_now_ms();
            if wait > 0 && wait <= 30_000 {
                log::info!("Phase3(Host): waiting {}ms until sync_at {} (own={}, peer={}) to start punching",
                    wait, target_sync, sync_at_ms, peer_sync_at_ms);
                hbb_common::tokio::time::sleep(Duration::from_millis(wait as u64)).await;
            }
        }

        // ===== 阶段 A：在精确地址上持续打洞（与控制端同一时长/轮长）=====
        // 主机端优先 accept（prefer_connect=false）。
        // 对称低强度模式：除精确地址外同时打"对端预测端口" exact+peer_delta
        // （对端每条新目的地址的映射端口按 delta 递增），时长缩短为 30s。
        let mut primary_targets: Vec<std::net::SocketAddr> = vec![exact];
        if symmetric_mode && peer_delta != 0 {
            let p = exact.port() as i32 + peer_delta;
            if (1..=65535).contains(&p) {
                let mut predicted = exact;
                predicted.set_port(p as u16);
                log::info!(
                    "Phase3(Host): symmetric low-intensity mode, also punch predicted port {}",
                    predicted
                );
                primary_targets.push(predicted);
            }
        }
        let exact_ms = if symmetric_mode {
            30_000
        } else {
            PUNCH_EXACT_MS
        };
        let exact_deadline = std::time::Instant::now() + Duration::from_millis(exact_ms);
        while std::time::Instant::now() < exact_deadline {
            if upgraded.load(std::sync::atomic::Ordering::SeqCst) {
                bail!("Phase3(Host) stop punch: already upgraded");
            }
            if started.elapsed() >= MAX_TIME {
                bail!("Phase3(Host) punch timed out");
            }
            for target in primary_targets.iter() {
                // 未 connect 模式（peer_addr=Some）：不能 connect——connected
                // socket 的 recv 只投递 connected 对端的包，对端（对称 NAT 时
                // 从 P_new、锥形时也可能非 STUN 端口）的 KCP 包会被内核全部
                // 丢弃；共享 socket 也不允许 connect（多对端场景）。
                let remain_ms = MAX_TIME.saturating_sub(started.elapsed()).as_millis() as u64;
                let round_ms = PUNCH_ROUND_MS.min(remain_ms.max(500));
                if let Some((kcp, stream)) =
                    punch_target_sustained(&socket, *target, round_ms, prefer_connect, Some(*target))
                        .await
                {
                    if let Ok(mut h) = kcp_handle.lock() {
                        *h = Some(kcp);
                    }
                    let kind = if *target == exact {
                        "exact"
                    } else {
                        "predicted"
                    };
                    log::info!(
                        "Phase3(Host) KCP punch to {} {} succeeded after {:?}",
                        kind,
                        target,
                        started.elapsed()
                    );
                    return Ok(stream);
                }
            }
        }

        // ===== 阶段 B：统一偏移序列扫描（对端 delta 预测 + 小幅漂移）=====
        // 对称低强度模式下 scan_offsets 为空，整段跳过（预测端口已在阶段 A 打过）
        let scan_offsets: Vec<i32> = if symmetric_mode {
            Vec::new()
        } else {
            let mut v = PUNCH_OFFSETS.to_vec();
            if peer_delta != 0 && !v.contains(&peer_delta) {
                v.insert(1, peer_delta);
            }
            v
        };
        'offsets: for &offset in scan_offsets.iter() {
            if offset == 0 {
                continue; // 精确地址已在阶段 A 充分尝试
            }
            for base in round_targets.iter() {
                // TCP listener 地址不是 UDP 打洞目标，跳过（留给 TCP fallback）
                if base.is_tcp() {
                    continue;
                }
                // IPv6 目标不在 Phase3 打洞：v6 无 NAT，只有 offset 0 是有效端口，
                // 而阶段 B 恰好跳过 offset 0（精确地址已在阶段 A 打过），因此这里
                // 对 v6 目标做偏移扫描是 100% 必败的纯浪费（8 偏移 x 8s = 64s），
                // 会直接摊薄 v4 的偏移扫描预算。v6 直连统一走中继前打洞。
                if base.addr.is_ipv6() {
                    continue;
                }
                if upgraded.load(std::sync::atomic::Ordering::SeqCst) {
                    bail!("Phase3(Host) stop punch: already upgraded");
                }
                if started.elapsed() >= MAX_TIME {
                    break 'offsets;
                }
                // i32 域计算端口，越界跳过，避免 u16 回绕
                let new_port = base.addr.port() as i32 + offset;
                if !(1..=65535).contains(&new_port) { continue; }
                let mut target = base.addr;
                target.set_port(new_port as u16);

                // 未 connect 模式：send_to 发包 + 收所有源的包（见阶段 A 注释）
                let sock = socket.clone();
                let remain_ms = MAX_TIME.saturating_sub(started.elapsed()).as_millis() as u64;
                let round_ms = PUNCH_ROUND_MS.min(remain_ms.max(500));
                if let Some((kcp, stream)) =
                    punch_target_sustained(&sock, target, round_ms, prefer_connect, Some(target)).await
                {
                    if let Ok(mut h) = kcp_handle.lock() {
                        *h = Some(kcp);
                    }
                    log::info!("Phase3(Host) KCP punch to {} (offset {}) succeeded after {:?}",
                        target, offset, started.elapsed());
                    return Ok(stream);
                }
            }
        }

        // After KCP rounds, try TCP simultaneous open on remaining budget.
        // 只对对端 TCP listener 类型的目标做（对 UDP 地址 connect 必然超时，
        // 纯烧预算：4 目标 × 11 偏移 × 3s ≈ 132s），并封顶 30s。
        // 对称低强度模式整段跳过。
        if !symmetric_mode {
            const TCP_SCAN_MAX: i32 = 20;
            let tcp_offsets: &[i32] =
                &[0, 1, -1, 2, -2, 5, -5, 10, -10, TCP_SCAN_MAX, -TCP_SCAN_MAX];
            let tcp_deadline = std::time::Instant::now() + Duration::from_secs(30);
            for base in &round_targets {
                if !base.is_tcp() {
                    continue;
                }
                for &offset in tcp_offsets {
                    if started.elapsed() >= MAX_TIME {
                        break;
                    }
                    if std::time::Instant::now() >= tcp_deadline {
                        break;
                    }
                    // i32 域计算端口，越界跳过，避免 u16 回绕
                    let new_port = base.addr.port() as i32 + offset;
                    if !(1..=65535).contains(&new_port) { continue; }
                    let mut tcp_target = base.addr;
                    tcp_target.set_port(new_port as u16);

                    let result: Option<tokio::net::TcpStream> = if let Some(ref listener) = tcp_listener {
                        tokio::select! {
                            // Accept 只接受来自目标 IP 的连接，其余丢弃继续等直到 3s 超时
                            res = tokio::time::timeout(Duration::from_secs(3),
                                accept_from_ip(listener, tcp_target.ip())) => {
                                match res {
                                    Ok(Some(stream)) => {
                                        log::info!("Phase3(Host) TCP: accept from {}", tcp_target);
                                        Some(stream)
                                    }
                                    _ => None,
                                }
                            }
                            res = tokio::time::timeout(Duration::from_secs(3),
                                tokio::net::TcpStream::connect(tcp_target)) => {
                                match res {
                                    Ok(Ok(stream)) => {
                                        stream.set_nodelay(true).ok();
                                        log::info!("Phase3(Host) TCP: connect to {} succeeded!", tcp_target);
                                        Some(stream)
                                    }
                                    _ => None,
                                }
                            }
                        }
                    } else {
                        // 无 listener 时只做 connect
                        match tokio::time::timeout(
                            Duration::from_secs(3),
                            tokio::net::TcpStream::connect(tcp_target),
                        )
                        .await
                        {
                            Ok(Ok(stream)) => {
                                stream.set_nodelay(true).ok();
                                log::info!("Phase3(Host) TCP: connect to {} succeeded!", tcp_target);
                                Some(stream)
                            }
                            _ => None,
                        }
                    };
                    if let Some(stream) = result {
                        log::info!("Phase3(Host) TCP simultaneous open succeeded to {} after {:?}!",
                            tcp_target, started.elapsed());
                        return Ok(Stream::from(stream, tcp_target));
                    }
                }
            }
        }

        // 每轮结束若队列为空 sleep 500ms 等新地址（已有目标会继续重试到预算耗尽）
        let queue_empty = targets.lock().await.is_empty();
        if queue_empty {
            hbb_common::tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

/// Detect NAT type by sending two STUN binding requests from the same socket
/// to the same STUN server, but to two different destination ports (or two servers).
/// If the mapped port differs -> Symmetric (NAT4).
/// If the mapped port is the same -> Cone (NAT1-3).
/// #2 enhancement: when only one STUN server is reachable, send two requests
/// to the same server and compare the mapped ports. The server may NAT the
/// response from different source ports, which can still differentiate symmetric.
///
/// 失败时返回 Err（不确定），而不是误判为 Cone：调用处只在 Ok(...) 有明确
/// 结论时才写 Config::set_nat_type，Err 保持未知以便下次连接重测。
pub async fn detect_symmetric_nat() -> ResultType<bool> {

    // Create a single socket for both queries so we can compare port mappings.
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let socket = Arc::new(socket);

    // Resolve the first STUN server.
    let servers_v4 = get_stun_servers_v4();
    let stun_str = match servers_v4.first() {
        Some(s) => s.clone(),
        None => bail!("no STUN server available"),
    };
    let base_addr: SocketAddr = match stun_str.to_socket_addrs()?.find(|x| x.is_ipv4()) {
        Some(a) => a,
        None => bail!("STUN server {} has no IPv4 address", stun_str),
    };

    // Helper: send a STUN binding request and return the XOR-mapped port.
    async fn query(socket: &UdpSocket, target: SocketAddr) -> ResultType<u16> {
        use hbb_common::rand::{self, Rng};
        let mut req = vec![0u8; 20];
        req[0..2].copy_from_slice(&0x0001u16.to_be_bytes());
        req[4..8].copy_from_slice(&0x2112A442u32.to_be_bytes());
        let mut tx_id = [0u8; 12];
        rand::thread_rng().fill(&mut tx_id);
        req[8..20].copy_from_slice(&tx_id);

        socket.send_to(&req, target).await?;
        let mut buf = vec![0u8; 4096];
        let (n, _) = tokio::time::timeout(
            Duration::from_secs(3),
            socket.recv_from(&mut buf),
        )
        .await
        .map_err(|_| anyhow!("STUN timeout"))??;
        if n < 20 {
            bail!("too short");
        }
        if u16::from_be_bytes([buf[0], buf[1]]) != 0x0101 {
            bail!("not binding response");
        }
        if &buf[8..20] != &tx_id {
            bail!("tx id mismatch");
        }
        let mut pos = 20;
        while pos + 4 <= n {
            let attr_type = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
            let attr_len = u16::from_be_bytes([buf[pos + 2], buf[pos + 3]]) as usize;
            if pos + 4 + attr_len > n {
                break;
            }
            if attr_type == 0x0020 && attr_len >= 8 {
                let xor_port = u16::from_be_bytes([buf[pos + 6], buf[pos + 7]]);
                return Ok(xor_port ^ 0x2112);
            }
            pos += 4 + attr_len;
            if attr_len % 4 != 0 {
                pos += 4 - (attr_len % 4);
            }
        }
        bail!("no XOR-MAPPED-ADDRESS")
    }

    // First request to STUN server
    let p1 = match query(&socket, base_addr).await {
        Ok(p) => p,
        Err(e) => {
            // 失败 = 不确定，返回 Err 而不是误判 Cone
            log::warn!("detect_symmetric_nat: first query failed: {:?}", e);
            return Err(e);
        }
    };

    // Second request to same STUN server (or different port if possible).
    // Most NATs will reuse the same mapping for the same destination, so
    // both requests will produce the same port even for symmetric NATs.
    // To force a different mapping on symmetric NATs, we need a DIFFERENT
    // destination. Try a second STUN server, then fallback to a fake port.
    let mut target2 = base_addr;
    let servers_v4 = get_stun_servers_v4();
    if servers_v4.len() >= 2 {
        if let Some(s2) = servers_v4.get(1) {
            if let Some(a2) = s2.to_socket_addrs()?.find(|x| x.is_ipv4()) {
                target2 = a2;
            }
        }
    } else {
        // Only one STUN server available - send a second request to the
        // same server but on a different UDP port. Many servers listen on
        // multiple ports (e.g. 3478 and 3479); if not, fall back to a
        // different fake port to at least get a different source mapping
        // for true symmetric NATs.
        target2.set_port(target2.port().wrapping_add(1));
    }

    let p2 = match query(&socket, target2).await {
        Ok(p) => p,
        Err(e) => {
            // 第二次查询失败无法下结论，返回 Err（保持未知，下次重测）
            log::warn!("detect_symmetric_nat: second query failed: {:?}", e);
            return Err(e);
        }
    };

    Ok(p1 != p2)
}

fn test_ipv6_sync() {
    #[tokio::main(flavor = "current_thread")]
    async fn func() {
        if let Some(job) = test_ipv6().await {
            job.await.ok();
        }
    }
    std::thread::spawn(func);
}

pub async fn get_ipv6_socket() -> Option<(Arc<UdpSocket>, bytes::Bytes)> {
    let Some(addr) = PUBLIC_IPV6_ADDR.lock().unwrap().0 else {
        return None;
    };

    match UdpSocket::bind(addr).await {
        Err(err) => {
            log::warn!("Failed to create UDP socket for IPv6: {err}");
        }
        Ok(socket) => {
            if let Ok(local_addr_v6) = socket.local_addr() {
                return Some((
                    Arc::new(socket),
                    hbb_common::AddrMangle::encode(local_addr_v6).into(),
                ));
            }
        }
    }
    None
}

/// Returns the cached public IPv6 address without creating a new socket.
/// The address was previously discovered by test_ipv6() (via local binding or STUN).
pub fn get_cached_ipv6_addr() -> Option<SocketAddr> {
    PUBLIC_IPV6_ADDR.lock().unwrap().0
}

// The color is the same to `str2color()` in flutter.
pub fn str2color(s: &str, alpha: u8) -> u32 {
    let bytes = s.as_bytes();
    // dart code `160 << 16 + 114 << 8 + 91` results `0`.
    let mut hash: u32 = 0;
    for &byte in bytes {
        let code = byte as u32;
        hash = code.wrapping_add((hash << 5).wrapping_sub(hash));
    }

    hash = hash % 16777216;
    let rgb = hash & 0xFF7FFF;

    (alpha as u32) << 24 | rgb
}

/// Check control permission state from a u64 bitmap.
/// Each permission uses 2 bits: 0 = not set, 1 = disable, 2 = enable, 3 = invalid (treated as not set)
/// Returns: Some(true) = enabled, Some(false) = disabled, None = not set or invalid
pub fn get_control_permission(
    permissions: u64,
    permission: hbb_common::rendezvous_proto::control_permissions::Permission,
) -> Option<bool> {
    let index = permission.value();
    if index >= 0 && index < 32 {
        let shift = index * 2;
        let value = (permissions >> shift) & 0b11;
        match value {
            1 => Some(false), // disable
            2 => Some(true),  // enable
            _ => None,        // 0 = not set, 3 = invalid
        }
    } else {
        None
    }
}

/// Enumerate all non-loopback, non-unspecified IPv4 addresses from ALL network
/// interfaces, including virtual adapters (Tailscale/WireGuard VPN, L2 bridges)
/// that `default_net::get_interfaces()` may skip on Windows.
/// Falls back to `default_net` on non-Windows platforms.
#[cfg(windows)]
pub fn get_all_ipv4_addrs() -> Vec<std::net::Ipv4Addr> {
    use windows::Win32::NetworkManagement::IpHelper::*;
    use windows::Win32::NetworkManagement::Ndis::*;
    use windows::Win32::Networking::WinSock::*;
    use std::net::Ipv4Addr;

    let mut addrs = Vec::new();
    let family = AF_UNSPEC.0 as u32;
    let flags = GAA_FLAG_INCLUDE_ALL_INTERFACES
        | GAA_FLAG_SKIP_ANYCAST
        | GAA_FLAG_SKIP_MULTICAST
        | GAA_FLAG_SKIP_DNS_SERVER;
    const ERROR_BUFFER_OVERFLOW: u32 = 111;

    unsafe {
        let mut buf_size: u32 = 0;
        let ret = GetAdaptersAddresses(family, flags, None, None, &mut buf_size);
        if ret != ERROR_BUFFER_OVERFLOW && ret != 0 {
            return addrs;
        }
        let mut buf = vec![0u8; buf_size as usize];
        let ptr = buf.as_mut_ptr() as *mut IP_ADAPTER_ADDRESSES_LH;
        if GetAdaptersAddresses(family, flags, None, Some(ptr), &mut buf_size) != 0 {
            return addrs;
        }
        let mut cur = ptr;
        while !cur.is_null() {
            let adapter = &*cur;
            if adapter.OperStatus == IfOperStatusUp {
                let mut unicast = adapter.FirstUnicastAddress;
                while !unicast.is_null() {
                    let ua = &*unicast;
                    let sa = ua.Address.lpSockaddr;
                    if !sa.is_null() && (*sa).sa_family == AF_INET {
                        let addr_in = &*(sa as *const SOCKADDR_IN);
                        let ip = Ipv4Addr::from(addr_in.sin_addr.S_un.S_addr.to_ne_bytes());
                        if !ip.is_loopback() && !ip.is_unspecified() && !addrs.contains(&ip) {
                            addrs.push(ip);
                        }
                    }
                    unicast = ua.Next;
                }
            }
            cur = adapter.Next;
        }
    }
    addrs
}

#[cfg(not(windows))]
pub fn get_all_ipv4_addrs() -> Vec<std::net::Ipv4Addr> {
    let mut addrs = Vec::new();
    for interface in default_net::get_interfaces() {
        for ipv4 in &interface.ipv4 {
            if !ipv4.addr.is_loopback() && !ipv4.addr.is_unspecified() && !addrs.contains(&ipv4.addr) {
                addrs.push(ipv4.addr);
            }
        }
    }
    addrs
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbb_common::tokio::{
        self,
        time::{interval, interval_at, sleep, Duration, Instant, Interval},
    };
    use std::collections::HashSet;

    #[inline]
    fn get_timestamp_secs() -> u128 {
        (std::time::SystemTime::UNIX_EPOCH
            .elapsed()
            .unwrap()
            .as_millis()
            + 500)
            / 1000
    }

    fn interval_maker() -> Interval {
        interval(Duration::from_secs(1))
    }

    fn interval_at_maker() -> Interval {
        interval_at(
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        )
    }

    // ThrottledInterval tick at the same time as tokio interval, if no sleeps
    #[allow(non_snake_case)]
    #[tokio::test]
    async fn test_RustDesk_interval() {
        let base_intervals = [interval_maker, interval_at_maker];
        for maker in base_intervals.into_iter() {
            let mut tokio_timer = maker();
            let mut tokio_times = Vec::new();
            let mut timer = rustdesk_interval(maker());
            let mut times = Vec::new();
            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        if tokio_times.len() >= 10 && times.len() >= 10 {
                            break;
                        }
                        times.push(get_timestamp_secs());
                    }
                    _ = tokio_timer.tick() => {
                        if tokio_times.len() >= 10 && times.len() >= 10 {
                            break;
                        }
                        tokio_times.push(get_timestamp_secs());
                    }
                }
            }
            assert_eq!(times, tokio_times);
        }
    }

    #[tokio::test]
    async fn test_tokio_time_interval_sleep() {
        let mut timer = interval_maker();
        let mut times = Vec::new();
        sleep(Duration::from_secs(3)).await;
        loop {
            tokio::select! {
                _ = timer.tick() => {
                    times.push(get_timestamp_secs());
                    if times.len() == 5 {
                        break;
                    }
                }
            }
        }
        let times2: HashSet<u128> = HashSet::from_iter(times.clone());
        assert_eq!(times.len(), times2.len() + 3);
    }

    // ThrottledInterval tick less times than tokio interval, if there're sleeps
    #[allow(non_snake_case)]
    #[tokio::test]
    async fn test_RustDesk_interval_sleep() {
        let base_intervals = [interval_maker, interval_at_maker];
        for (i, maker) in base_intervals.into_iter().enumerate() {
            let mut timer = rustdesk_interval(maker());
            let mut times = Vec::new();
            sleep(Duration::from_secs(3)).await;
            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        times.push(get_timestamp_secs());
                        if times.len() == 5 {
                            break;
                        }
                    }
                }
            }
            // No multiple ticks in the `interval` time.
            // Values in "times" are unique and are less than normal tokio interval.
            // See previous test (test_tokio_time_interval_sleep) for comparison.
            let times2: HashSet<u128> = HashSet::from_iter(times.clone());
            assert_eq!(times.len(), times2.len(), "test: {}", i);
        }
    }

    #[test]
    fn test_duration_multiplication() {
        let dur = Duration::from_secs(1);

        assert_eq!(dur * 2, Duration::from_secs(2));
        assert_eq!(
            Duration::from_secs_f64(dur.as_secs_f64() * 0.9),
            Duration::from_millis(900)
        );
        assert_eq!(
            Duration::from_secs_f64(dur.as_secs_f64() * 0.923),
            Duration::from_millis(923)
        );
        assert_eq!(
            Duration::from_secs_f64(dur.as_secs_f64() * 0.923 * 1e-3),
            Duration::from_micros(923)
        );
        assert_eq!(
            Duration::from_secs_f64(dur.as_secs_f64() * 0.923 * 1e-6),
            Duration::from_nanos(923)
        );
        assert_eq!(
            Duration::from_secs_f64(dur.as_secs_f64() * 0.923 * 1e-9),
            Duration::from_nanos(1)
        );
        assert_eq!(
            Duration::from_secs_f64(dur.as_secs_f64() * 0.5 * 1e-9),
            Duration::from_nanos(1)
        );
        assert_eq!(
            Duration::from_secs_f64(dur.as_secs_f64() * 0.499 * 1e-9),
            Duration::from_nanos(0)
        );
    }

    #[test]
    fn test_is_public() {
        // Test URLs containing "rustdesk.com/"
        assert!(is_public("https://rustdesk.com/"));
        assert!(is_public("https://www.rustdesk.com/"));
        assert!(is_public("https://api.rustdesk.com/v1"));
        assert!(is_public("https://API.RUSTDESK.COM/v1"));
        assert!(is_public("https://rustdesk.com/path"));

        // Test URLs ending with "rustdesk.com"
        assert!(is_public("rustdesk.com"));
        assert!(is_public("https://rustdesk.com"));
        assert!(is_public("https://RustDesk.com"));
        assert!(is_public("http://www.rustdesk.com"));
        assert!(is_public("https://api.rustdesk.com"));

        // Test non-public URLs
        assert!(!is_public("https://example.com"));
        assert!(!is_public("https://custom-server.com"));
        assert!(!is_public("http://192.168.1.1"));
        assert!(!is_public("localhost"));
        assert!(!is_public("https://rustdesk.computer.com"));
        assert!(!is_public("rustdesk.comhello.com"));
    }

    #[test]
    fn test_should_use_tcp_proxy_for_api_url() {
        assert!(should_use_tcp_proxy_for_api_url(
            "https://admin.example.com/api/login",
            "https://admin.example.com"
        ));
        assert!(should_use_tcp_proxy_for_api_url(
            "https://admin.example.com:21114/api/login",
            "https://admin.example.com"
        ));
        assert!(!should_use_tcp_proxy_for_api_url(
            "https://api.telegram.org/bot123/sendMessage",
            "https://admin.example.com"
        ));
        assert!(!should_use_tcp_proxy_for_api_url(
            "https://admin.rustdesk.com/api/login",
            "https://admin.rustdesk.com"
        ));
        assert!(!should_use_tcp_proxy_for_api_url(
            "https://admin.example.com/api/login",
            "not a url"
        ));
        assert!(!should_use_tcp_proxy_for_api_url(
            "not a url",
            "https://admin.example.com"
        ));
    }

    #[test]
    fn test_get_tcp_proxy_addr_normalizes_bare_ipv6_host() {
        struct RestoreCustomRendezvousServer(String);

        impl Drop for RestoreCustomRendezvousServer {
            fn drop(&mut self) {
                Config::set_option(
                    keys::OPTION_CUSTOM_RENDEZVOUS_SERVER.to_string(),
                    self.0.clone(),
                );
            }
        }

        let _restore = RestoreCustomRendezvousServer(Config::get_option(
            keys::OPTION_CUSTOM_RENDEZVOUS_SERVER,
        ));
        Config::set_option(
            keys::OPTION_CUSTOM_RENDEZVOUS_SERVER.to_string(),
            "1:2".to_string(),
        );

        assert_eq!(get_tcp_proxy_addr(), format!("[1:2]:{RENDEZVOUS_PORT}"));
    }

    #[tokio::test]
    async fn test_http_request_via_tcp_proxy_rejects_invalid_header_json() {
        let result = http_request_via_tcp_proxy("not a url", "get", None, "{").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_http_request_via_tcp_proxy_rejects_non_object_header_json() {
        let err = http_request_via_tcp_proxy("not a url", "get", None, "[]")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("HTTP header information parsing failed!"));
    }

    #[test]
    fn test_parse_json_header_entries_preserves_single_content_type() {
        let headers = parse_json_header_entries(
            r#"{"Content-Type":"text/plain","Authorization":"Bearer token"}"#,
        )
        .unwrap();

        assert_eq!(
            headers
                .iter()
                .filter(|entry| entry.name.eq_ignore_ascii_case("Content-Type"))
                .count(),
            1
        );
        assert_eq!(
            headers
                .iter()
                .find(|entry| entry.name.eq_ignore_ascii_case("Content-Type"))
                .map(|entry| entry.value.as_str()),
            Some("text/plain")
        );
    }

    #[test]
    fn test_parse_json_header_entries_does_not_add_default_content_type() {
        let headers = parse_json_header_entries(r#"{"Authorization":"Bearer token"}"#).unwrap();

        assert!(!headers
            .iter()
            .any(|entry| entry.name.eq_ignore_ascii_case("Content-Type")));
    }

    #[test]
    fn test_parse_simple_header_respects_custom_content_type() {
        let headers = parse_simple_header("Content-Type: text/plain");

        assert_eq!(
            headers
                .iter()
                .filter(|entry| entry.name.eq_ignore_ascii_case("Content-Type"))
                .count(),
            1
        );
        assert_eq!(
            headers
                .iter()
                .find(|entry| entry.name.eq_ignore_ascii_case("Content-Type"))
                .map(|entry| entry.value.as_str()),
            Some("text/plain")
        );
    }

    #[test]
    fn test_parse_simple_header_preserves_non_content_type_header() {
        let headers = parse_simple_header("Authorization: Bearer token");

        assert!(headers.iter().any(|entry| {
            entry.name.eq_ignore_ascii_case("Authorization")
                && entry.value.as_str() == "Bearer token"
        }));
        assert_eq!(
            headers
                .iter()
                .filter(|entry| entry.name.eq_ignore_ascii_case("Content-Type"))
                .count(),
            1
        );
        assert_eq!(
            headers
                .iter()
                .find(|entry| entry.name.eq_ignore_ascii_case("Content-Type"))
                .map(|entry| entry.value.as_str()),
            Some("application/json")
        );
    }

    #[test]
    fn test_tcp_proxy_log_target_redacts_query_only() {
        assert_eq!(
            tcp_proxy_log_target("https://example.com/api/heartbeat?token=secret"),
            "https://example.com/api/heartbeat"
        );
    }

    #[test]
    fn test_tcp_proxy_log_target_brackets_ipv6_host_with_port() {
        assert_eq!(
            tcp_proxy_log_target("https://[2001:db8::1]:21114/api/heartbeat?token=secret"),
            "https://[2001:db8::1]:21114/api/heartbeat"
        );
    }

    #[test]
    fn test_http_proxy_response_to_json() {
        let mut resp = HttpProxyResponse {
            status: 200,
            body: br#"{"ok":true}"#.to_vec().into(),
            ..Default::default()
        };
        resp.headers.push(HeaderEntry {
            name: "Content-Type".into(),
            value: "application/json".into(),
            ..Default::default()
        });

        let json = http_proxy_response_to_json(resp).unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["status_code"], 200);
        assert_eq!(value["headers"]["content-type"], "application/json");
        assert_eq!(value["body"], r#"{"ok":true}"#);

        let err = http_proxy_response_to_json(HttpProxyResponse {
            error: "dial failed".into(),
            ..Default::default()
        })
        .unwrap_err()
        .to_string();
        assert!(err.contains("TCP proxy error: dial failed"));
    }

    #[test]
    fn test_mouse_event_constants_and_mask_layout() {
        use super::input::*;

        // Verify MOUSE_TYPE constants are unique and within the mask range.
        let types = [
            MOUSE_TYPE_MOVE,
            MOUSE_TYPE_DOWN,
            MOUSE_TYPE_UP,
            MOUSE_TYPE_WHEEL,
            MOUSE_TYPE_TRACKPAD,
            MOUSE_TYPE_MOVE_RELATIVE,
        ];

        let mut seen = std::collections::HashSet::new();
        for t in types.iter() {
            assert!(seen.insert(*t), "Duplicate mouse type: {}", t);
            assert_eq!(
                *t & MOUSE_TYPE_MASK,
                *t,
                "Mouse type {} exceeds mask {}",
                t,
                MOUSE_TYPE_MASK
            );
        }

        // The mask layout is: lower 3 bits for type, upper bits for buttons (shifted by 3).
        let combined_mask = MOUSE_TYPE_DOWN | ((MOUSE_BUTTON_LEFT | MOUSE_BUTTON_RIGHT) << 3);
        assert_eq!(combined_mask & MOUSE_TYPE_MASK, MOUSE_TYPE_DOWN);
        assert_eq!(combined_mask >> 3, MOUSE_BUTTON_LEFT | MOUSE_BUTTON_RIGHT);
    }

    /// v6 熔断器的所有用例串行化用锁。
    ///
    /// 熔断状态是**进程级全局量**（生产代码里就必须如此：熔断要跨连接生效），
    /// 而 Rust 测试默认并行执行同一二进制内的用例。若不串行化，用例之间会
    /// 互相清零/改写对方的计数与时间戳，导致随机失败（flaky）。
    static V6_BREAKER_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// v6 熔断必须与 Phase3 熔断**完全独立**：v6 只是并发抢跑的附加通道，
    /// 它失败绝不能把 v4/Phase3 一起禁掉（那才是真正的成功率损失）。
    ///
    /// 独立性、成功清零、衰减三类断言合并在一个测试里：它们共用同一组全局
    /// 状态，串行执行才可靠（见 V6_BREAKER_TEST_LOCK）。
    #[test]
    fn test_v6_breaker_semantics() {
        let _guard = V6_BREAKER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        // ---- 1) 初始状态 ----
        reset_phase3_state();
        assert!(!should_skip_phase3(), "phase3 初始不应熔断");
        assert!(!should_skip_v6(), "v6 初始不应熔断");

        // ---- 2) v6 连续失败只熔断 v6，绝不牵连 Phase3 ----
        for _ in 0..V6_MAX_RETRIES {
            record_v6_failure();
        }
        assert!(should_skip_v6(), "v6 连续失败达阈值后应熔断");
        assert!(
            !should_skip_phase3(),
            "v6 熔断绝不能连带禁掉 Phase3/v4 —— 那是实打实的成功率损失"
        );

        // ---- 3) v6 成功一次即清零 ----
        record_v6_success();
        assert!(!should_skip_v6(), "v6 成功后应恢复尝试");

        // ---- 4) 反向：Phase3 失败也不应影响 v6 判定 ----
        // PHASE3_MAX_RETRIES = 1，因此一次失败即跳闸，断言有实际区分度。
        record_phase3_failure();
        assert!(should_skip_phase3(), "phase3 一次失败即熔断（MAX_RETRIES=1）");
        assert!(!should_skip_v6(), "Phase3 失败不应影响 v6 判定");

        // ---- 5) record_v6_failure 的窗口衰减 ----
        reset_phase3_state();
        record_v6_success();
        record_v6_failure(); // count = 1
        // 把"上次失败时刻"推到窗口之外，模拟很久以前失败过
        if let Ok(mut g) = V6_LAST_FAIL_AT.lock() {
            *g = Some(
                std::time::Instant::now() - V6_RETRY_WINDOW - std::time::Duration::from_secs(1),
            );
        }
        // 窗口已过：本次失败应重新从 1 起算，而不是累加到阈值
        record_v6_failure();
        assert!(
            !should_skip_v6(),
            "窗口外的旧失败不应累计到熔断阈值（应重新从 1 起算）"
        );

        // ---- 6) should_skip_v6 的衰减放行分支 ----
        // 先真正跳闸，再把时间戳推到窗口外：should_skip_v6 应清零并放行。
        record_v6_failure();
        assert!(should_skip_v6(), "重新累计到阈值后应再次跳闸");
        if let Ok(mut g) = V6_LAST_FAIL_AT.lock() {
            *g = Some(
                std::time::Instant::now() - V6_RETRY_WINDOW - std::time::Duration::from_secs(1),
            );
        }
        assert!(
            !should_skip_v6(),
            "熔断窗口过期后应放行（衰减），且应顺带清零计数"
        );
        // 清零后必须重新累计满 V6_MAX_RETRIES 次才再次跳闸
        // （证明是真清零，而不只是这一次返回 false）
        record_v6_failure();
        assert!(!should_skip_v6(), "衰减清零后，单次失败不应立即再次跳闸");
        for _ in 1..V6_MAX_RETRIES {
            record_v6_failure();
        }
        assert!(should_skip_v6(), "衰减清零后重新累计满阈值应再次跳闸");

        // 收尾：复位，避免影响同二进制内其他用例
        reset_phase3_state();
        record_v6_success();
    }
}
