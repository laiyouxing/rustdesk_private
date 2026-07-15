use std::{
    collections::HashMap,
    future::Future,
    net::{Ipv4Addr, SocketAddr, ToSocketAddrs},
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
        sync::{mpsc, oneshot},
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

/// Timestamp of the last Phase3 punch failure, and retry counter.
/// Up to 3 retries are allowed with exponential backoff (via auto-reconnect).
static LAST_PHASE3_FAIL_AT: std::sync::Mutex<Option<std::time::Instant>> =
    std::sync::Mutex::new(None);
static PHASE3_RETRY_COUNT: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);
const PHASE3_MAX_RETRIES: u32 = 3;

/// Record that Phase3 succeeded, clearing failure record and retry counter.
/// On subsequent reconnections, Phase3 will be attempted again.
pub fn record_phase3_success() {
    PHASE3_RETRY_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
    if let Ok(mut guard) = LAST_PHASE3_FAIL_AT.lock() {
        guard.take();
    }
}

/// Record that Phase3 failed. Increments retry counter; after
/// PHASE3_MAX_RETRIES failures, Phase3 is permanently skipped until
/// user actively disconnects (reset) or Phase3 succeeds.
pub fn record_phase3_failure() {
    PHASE3_RETRY_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    if let Ok(mut guard) = LAST_PHASE3_FAIL_AT.lock() {
        *guard = Some(std::time::Instant::now());
    }
}

/// Returns true if Phase3 should be skipped.
/// Allows up to PHASE3_MAX_RETRIES attempts; auto-reconnect exponential
/// backoff provides natural spacing between retries (1s→1.5s→2.3s→…).
pub fn should_skip_phase3() -> bool {
    PHASE3_RETRY_COUNT.load(std::sync::atomic::Ordering::SeqCst) >= PHASE3_MAX_RETRIES
}

/// Reset Phase3 state so the next connection will try punching again.
/// Called when the user explicitly closes the connection.
pub fn reset_phase3_state() {
    PHASE3_RETRY_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
    if let Ok(mut guard) = LAST_PHASE3_FAIL_AT.lock() {
        guard.take();
    }
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
    } else {
        "ubuntu"
    };
    let url = format!("{}/api/version/latest?platform={}", api_server, platform);
    let proxy_conf = Config::get_socks();
    let tls_url = get_url_for_tls(&url, &proxy_conf);
    let tls_type = get_cached_tls_type(tls_url);
    let is_tls_not_cached = tls_type.is_none();
    let tls_type = tls_type.unwrap_or(TlsType::Rustls);
    let client = create_http_client_async(tls_type, true);
    let resp = match client.get(&url).send().await {
        Ok(r) => r,
        Err(_) if is_tls_not_cached => {
            let client = create_http_client_async(TlsType::NativeTls, true);
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
fn get_stun_servers_v4() -> Vec<String> {
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
pub async fn stun_query_with_socket(
    socket: &UdpSocket,
) -> ResultType<(SocketAddr, String)> {
    use hbb_common::futures::future::FutureExt;
    use hbb_common::rand::{self, Rng};

    const SINGLE_RECV_TIMEOUT: Duration = Duration::from_secs(2);
    const TOTAL_TIMEOUT: Duration = Duration::from_secs(6);

    async fn try_one(socket: &UdpSocket, stun: &str) -> ResultType<(SocketAddr, String)> {
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
        let (n, _) = tokio::time::timeout(SINGLE_RECV_TIMEOUT, socket.recv_from(&mut buf))
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

    // Race all STUN servers concurrently, collect first 2 successful results
    // (same logic as try_one above, accessible outside the closure)
    let work = async {
        let servers = get_stun_servers_v4();
        let futs = servers
            .iter()
            .map(|stun| {
                tokio::time::timeout(TOTAL_TIMEOUT, try_one(socket, stun.as_str())).boxed()
            })
            .collect::<Vec<_>>();
        let mut results = Vec::new();
        // Take first 2 successful replies (or all if fewer succeed)
        for fut in futs {
            if results.len() >= 2 {
                break;
            }
            if let Ok(Ok(ok)) = fut.await {
                results.push(ok);
            }
        }
        if results.is_empty() {
            bail!("All STUN servers failed");
        }
        // If 2+ servers agree on the mapped address, use it (high confidence).
        // Otherwise trust the first reply.
        if results.len() >= 2 && results[0].0 == results[1].0 {
            Ok(results[0].clone())
        } else {
            Ok(results[0].clone())
        }
    };

    tokio::time::timeout(TOTAL_TIMEOUT, work)
        .await
        .map_err(|_| anyhow!("STUN total timeout"))?
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






/// Returns true if punch succeeded, false otherwise.
///
/// Merged with Phase 3: after STUN, sends our address out through
/// `phase3_out_tx` so io_loop can forward it to the peer via relay.
/// Receives peer's Phase 3 addresses through `phase3_peer_rx` and adds
/// them to the target list, so both sides punch through the same socket.
pub async fn relay_upgrade_task(
    peer_addrs: Vec<SocketAddr>,
    notify: Arc<hbb_common::tokio::sync::Notify>,
    direct_stream: Arc<hbb_common::tokio::sync::Mutex<Option<Stream>>>,
    kcp_handle: Arc<std::sync::Mutex<Option<crate::kcp_stream::KcpStream>>>,
    punch_port: u16,
    phase3_out_tx: mpsc::Sender<std::net::SocketAddr>,
    phase3_peer_rx: Arc<std::sync::Mutex<Vec<std::net::SocketAddr>>>,
    phase3_tcp_rx: Arc<std::sync::Mutex<Vec<std::net::SocketAddr>>>,
) -> bool {
    use crate::kcp_stream::KcpStream;

    // #2: if we haven't determined NAT type yet, try STUN-based detection.
    if Config::get_nat_type() == 0 {
        if let Ok(true) = detect_symmetric_nat().await {
            log::info!("relay_upgrade_task: detected SYMMETRIC NAT, switching to relay-only");
            Config::set_nat_type(NatType::SYMMETRIC as _);
            return false;
        } else if Config::get_nat_type() == 0 {
            Config::set_nat_type(NatType::ASYMMETRIC as _);
        }
    }

    // "以时间换资源"：中继已建立，不急。拉长总预算到 3 分钟，每个目标间隔 1s，
    // 空包只发 2-3 个，让 Phase3 几乎不影响中继操作。
    const TOTAL_BUDGET: Duration = Duration::from_secs(180);
    const PREDICTED_SCAN_RANGE: u16 = 50;
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

    // Brief initial wait for relay to stabilize (reduced from 2s→0.5s)
    hbb_common::tokio::time::sleep(Duration::from_millis(500)).await;

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
    // Also create IPv6 socket for dual-stack punching (best-effort)
    let socket_v6 = UdpSocket::bind(SocketAddr::from(([0u16, 0, 0, 0, 0, 0, 0, 0], 0u16)))
        .await
        .ok()
        .map(Arc::new);
    if socket_v6.is_some() {
        log::info!("RelayUpgrade: created IPv6 socket for dual-stack punching");
    }

    // Multi-STUN: query multiple times to detect port increment pattern
    // for Symmetric NAT port prediction.
    let mut targets = peer_addrs.clone();
    let mut our_addr: Option<std::net::SocketAddr> = None;
    let mut stun_ports: Vec<u16> = Vec::new();
    let mut selected_server: Option<String> = None;

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
                        log::info!("RelayUpgrade STUN #{}: mapped {} (port {}, server {})",
                            i + 1, addr, addr.port(), srv);
                        if let Ok(mut public) = PUBLIC_ADDR.lock() {
                            *public = addr.to_string();
                        }
                    } else {
                        log::info!("RelayUpgrade STUN #{}: port {} via {}",
                            i + 1, addr.port(), srv);
                    }
                    stun_ports.push(addr.port());
                    break; // success, exit retry loop
                }
                Err(e) => {
                    if retries > 0 {
                        let delay = Duration::from_millis(200 * (3 - retries) as u64);
                        log::info!("RelayUpgrade STUN #{} retrying in {:?} ({}/2): {:?}",
                            i + 1, delay, 3 - retries, e);
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
                                        log::info!("RelayUpgrade STUN #{} fallback vs {} failed: {:?}",
                                            i + 1, alt_srv, e);
                                    }
                                }
                            }
                            if !fallback_ok {
                                log::info!("RelayUpgrade STUN #{} all fallback servers failed", i + 1);
                            }
                        }
                        continue;
                    }
                    // All retries exhausted, no locked server to fall back from
                    if our_addr.is_none() {
                        log::info!("RelayUpgrade STUN #{} failed after retries: {:?}", i + 1, e);
                    } else {
                        log::info!("RelayUpgrade STUN #{} failed, stopping prediction: {:?}", i + 1, e);
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



    // Phase 3: send our public address to peer through relay.
    // We delay this until AFTER delta measurement so we can include
    // the predicted port for symmetric NAT (BUGFIX: on symmetric NAT,
    // the STUN-discovered port differs from the port the peer must punch to).
    // IPv6 and TCP listener are sent immediately (they don't need delta).
    if crate::get_ipv6_punch_enabled() {
        if let Some(ipv6_addr) = get_cached_ipv6_addr() {
            let _ = phase3_out_tx.try_send(ipv6_addr);
            log::info!("Phase3: sent IPv6 address to relay loop: {}", ipv6_addr);
        }
    }
    // Send TCP listener address for TCP simultaneous open fallback.
    if let Some(tcp_port) = tcp_listener_port {
        let tcp_addr = std::net::SocketAddr::new(
            our_addr.map(|a| a.ip()).unwrap_or(std::net::Ipv4Addr::UNSPECIFIED.into()),
            tcp_port,
        );
        let _ = phase3_out_tx.try_send(tcp_addr);
        log::info!("Phase3: sent TCP listener address: {}", tcp_addr);
    }

    // Measure our symmetric NAT delta by querying different STUN servers.
    // On symmetric NAT, the mapped port changes for each different destination.
    // P2: Try multiple alternative servers and take the consensus delta
    // (most common non-zero value) instead of relying on a single server.
    let our_delta: i16 = if stun_ports.len() >= 2 && our_addr.is_some() {
        let base_port = stun_ports[0] as i16;
        let alt_servers = get_stun_servers_v4();
        let mut deltas: Vec<i16> = Vec::new();
        // Try up to 4 different servers, collect all non-zero deltas
        for alt in alt_servers.iter() {
            // Skip the primary server (delta to self is always 0)
            if selected_server.as_ref().map_or(false, |s| s == alt) {
                continue;
            }
            if let Some(alt_addr) = alt.to_socket_addrs().ok()
                .and_then(|mut i| i.find(|a| a.is_ipv4()))
            {
                let _ = socket.connect(alt_addr).await;
                socket.send(&[]).await.ok();
                match stun_query_single_server(&socket, alt).await {
                    Ok((alt_result, _)) => {
                        let delta = alt_result.port() as i16 - base_port;
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
            use std::collections::HashMap;
            let mut freq: HashMap<i16, usize> = HashMap::new();
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
    // BUGFIX: Send our address NOW (after delta measurement) so we can include
    // the predicted port for symmetric NAT. On cone NAT both ports are the same.
    // On symmetric NAT the peer needs to try: STUN_port (for our us→STUN mapping)
    // AND STUN_port+our_delta (for our us→peer mapping).
    if let Some(addr) = our_addr {
        let _ = phase3_out_tx.try_send(addr);
        log::info!("Phase3: sent our address to relay loop: {}", addr);
        if our_delta != 0 {
            let predicted_port = addr.port().wrapping_add(our_delta as u16);
            if predicted_port > 0 && predicted_port != addr.port() {
                let predicted_addr = std::net::SocketAddr::new(addr.ip(), predicted_port);
                let _ = phase3_out_tx.try_send(predicted_addr);
                log::info!("Phase3: sent predicted our address for symmetric NAT: {}", predicted_addr);
            }
        }
    }

    for _round in 0..6 {
        if started.elapsed() >= TOTAL_BUDGET {
            log::info!("RelayUpgrade: total budget ({}s) exceeded, giving up", TOTAL_BUDGET.as_secs());
            return false;
        }

        // Check for peer Phase 3 addresses and add them to targets.
        // Instead of blind ±50 port scan, use delta-based prediction:
        //   - exact peer port (most likely for asymmetric NAT)
        //   - predicted port (peer_port + our_delta, most likely for symmetric)
        //   - narrow scan range ±PREDICTED_SCAN_RANGE around base port
        if let Ok(mut peer_addrs) = phase3_peer_rx.lock() {
            for addr in peer_addrs.drain(..) {
                let base_port = addr.port();
                // 1) Exact peer port (before closure to avoid borrow conflict)
                if !targets.contains(&addr) {
                    targets.push(addr);
                    log::info!("Phase3: added peer address {} to targets", addr);
                }
                // 2) Predicted port: base_port + our_delta (symmetric NAT heuristic)
                if our_delta != 0 {
                    let predicted = base_port.wrapping_add(our_delta as u16);
                    if predicted > 0 && predicted != base_port {
                        let mut scan_addr = addr;
                        scan_addr.set_port(predicted);
                        if !targets.contains(&scan_addr) {
                            targets.push(scan_addr);
                        }
                    }
                    log::info!("Phase3: predicted port {} (base {} + delta {})",
                        predicted, base_port, our_delta);
                }
                // 3) Narrow scan range around base port
                for offset in 1..=PREDICTED_SCAN_RANGE {
                    for &p in &[base_port.wrapping_add(offset), base_port.wrapping_sub(offset)] {
                        if p > 0 && p != base_port {
                            let mut scan_addr = addr;
                            scan_addr.set_port(p);
                            if !targets.contains(&scan_addr) {
                                targets.push(scan_addr);
                            }
                        }
                    }
                }
                log::info!("Phase3: predicted scan for {}: {} targets (range ±{}, delta {})",
                    addr, targets.len(), PREDICTED_SCAN_RANGE, our_delta);
            }
        }
        // Try each target address with KCP (UDP) hole punching.
        // Inter-target delay prevents flooding the network and impacting relay traffic.
        for &target in &targets {
            if started.elapsed() >= TOTAL_BUDGET {
                return false;
            }
            let socket = if target.is_ipv6() {
                match socket_v6.as_ref() {
                    Some(s) => s.clone(),
                    None => continue,
                }
            } else {
                socket_v4.clone()
            };
            if socket.connect(target).await.is_err() {
                continue;
            }
            // 串行慢慢试：每个目标间隔 1s，让中继操作不被干扰
            hbb_common::tokio::time::sleep(Duration::from_millis(1000)).await;

            // Minimal burst: 2 packets at 5ms spacing (relay already established, no rush)
            for _ in 0..2 {
                if started.elapsed() >= TOTAL_BUDGET {
                    return false;
                }
                socket.send(&[]).await.ok();
                hbb_common::tokio::time::sleep(Duration::from_millis(5)).await;
            }

            // Race KCP connect vs accept
            let socket_for_accept = socket.clone();
            let mut connect_fut = Box::pin(
                KcpStream::connect(socket.clone(), Duration::from_secs(3)));
            let mut accept_fut = Box::pin(async move {
                KcpStream::accept(socket_for_accept, Duration::from_secs(3), None).await
            });
            let punched = tokio::select! {
                res = &mut connect_fut => {
                    match res {
                        Ok((kcp, stream)) => {
                            if let Ok(mut h) = kcp_handle.lock() {
                                *h = Some(kcp);
                            }
                            let mut guard = direct_stream.lock().await;
                            *guard = Some(stream);
                            notify.notify_one();
                            true
                        }
                        Err(_) => false,
                    }
                }
                res = &mut accept_fut => {
                    match res {
                        Ok((kcp, stream)) => {
                            if let Ok(mut h) = kcp_handle.lock() {
                                *h = Some(kcp);
                            }
                            let mut guard = direct_stream.lock().await;
                            *guard = Some(stream);
                            notify.notify_one();
                            true
                        }
                        Err(_) => false,
                    }
                }
            };
            if punched {
                log::info!("RelayUpgrade: KCP punch succeeded after {:?}", started.elapsed());
                return true;
            }
        }

        // If KCP failed this round, try TCP simultaneous open on the peer's
        // dedicated TCP listener port (exchanged via PunchPeerAddr). This is
        // far more reliable than trying TCP on KCP/UDP ports.
        // Also try WebSocket (WS/WSS) connect as some firewalls allow HTTP
        // upgrade traffic while blocking raw TCP on non-standard ports.
        if let Some(ref listener) = tcp_listener {
            let tcp_targets: Vec<std::net::SocketAddr> = if let Ok(addrs) = phase3_tcp_rx.lock() {
                addrs.clone()
            } else { Vec::new() };
            for &tcp_target in &tcp_targets {
                if started.elapsed() >= TOTAL_BUDGET { break; }
                if tcp_target.is_ipv6() { continue; }
                let ports = [0, 1, -1, 2, -2, 5, -5];
                for &port_offset in &ports {
                    if started.elapsed() >= TOTAL_BUDGET { break; }
                    let mut target = tcp_target;
                    target.set_port(tcp_target.port().wrapping_add_signed(port_offset));
                    if target.port() == 0 { continue; }
                    let listener_clone = listener;
                    let ws_url = format!("ws://{}:{}", target.ip(), target.port());
                    let tcp_res: Option<tokio::net::TcpStream> = tokio::select! {
                        // Accept raw TCP connection from peer
                        res = listener_clone.accept() => {
                            match res {
                                Ok((stream, _)) => {
                                    log::info!("RelayUpgrade TCP: accept from {}", target);
                                    Some(stream)
                                }
                                Err(_) => None,
                            }
                        }
                        // Connect to peer via raw TCP
                        res = tokio::time::timeout(Duration::from_secs(2),
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
                        // Connect to peer via WebSocket (bypasses firewalls that block raw TCP)
                        res = tokio::time::timeout(Duration::from_secs(2),
                            tokio_tungstenite::connect_async(&ws_url)) => {
                            match res {
                                Ok(Ok((_ws, _))) => {
                                    log::info!("RelayUpgrade WS: connect to {} succeeded!", target);
                                    tokio::net::TcpStream::connect(target).await.ok()
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
                        log::info!("RelayUpgrade: punch succeeded via TCP/WS to {} after {:?}",
                            target, started.elapsed());
                        return true;
                    }
                }
            }
        }

        // 轮间休息 2s，让中继操作不被 Phase3 干扰
        hbb_common::tokio::time::sleep(Duration::from_millis(2000)).await;
        // Also check for new Phase 3 addresses during gap
        if let Ok(mut peer_addrs) = phase3_peer_rx.lock() {
            for addr in peer_addrs.drain(..) {
                if !targets.contains(&addr) {
                    targets.push(addr);
                    log::info!("Phase3: added peer address {} to targets (during gap)", addr);
                }
                let base_port = addr.port();
                for offset in 1..=PREDICTED_SCAN_RANGE {
                    let ports = [
                        base_port.wrapping_add(offset),
                        base_port.wrapping_sub(offset),
                    ];
                    for &p in &ports {
                        if p > 0 && p != base_port {
                            let mut scan_addr = addr;
                            scan_addr.set_port(p);
                            if !targets.contains(&scan_addr) {
                                targets.push(scan_addr);
                            }
                        }
                    }
                }
            }
        }
    }
    log::info!("RelayUpgrade finished without success in {:?}", started.elapsed());
    false
}


/// Host-side Phase 3 punch: called when the host receives PunchPeerAddr
/// through the relay connection. Uses the same optimizations as
/// relay_upgrade_task (burst + connect/accept race + keep-alive).
///
/// `punch_socket` is an optional persistent UDP socket created before the
/// STUN query. When provided, STUN discovery and hole punching share the
/// same socket, so the NAT mapping is consistent. When `None`, a new socket
/// is created internally (fallback for edge cases).
pub async fn relay_phase3_punch_to_peer(
    peer_addr: std::net::SocketAddr,
    kcp_handle: Arc<std::sync::Mutex<Option<crate::kcp_stream::KcpStream>>>,
    punch_socket: Option<Arc<tokio::net::UdpSocket>>,
) -> ResultType<Stream> {
    use crate::kcp_stream::KcpStream;

    // Use the persistent punch socket if available, otherwise create a new one.
    let socket = if let Some(s) = punch_socket {
        s
    } else {
        let upnp_local = crate::common::get_upnp_local_port();
        let bind_addr = if peer_addr.is_ipv6() {
            SocketAddr::from(([0u16, 0, 0, 0, 0, 0, 0, 0], 0u16))
        } else if upnp_local > 0 {
            SocketAddr::from(([0u8; 4], upnp_local))
        } else {
            SocketAddr::from(([0u8; 4], 0u16))
        };
        Arc::new(UdpSocket::bind(bind_addr).await?)
    };

    // Create TCP listener for TCP simultaneous open fallback.
    // Use UPnP local port if available.
    let upnp_local = crate::common::get_upnp_local_port();
    let tcp_listener = if upnp_local > 0 {
        tokio::net::TcpListener::bind(format!("0.0.0.0:{}", upnp_local)).await.ok()
    } else {
        tokio::net::TcpListener::bind("0.0.0.0:0").await.ok()
    };

    const MAX_TIME: Duration = Duration::from_secs(30);
    let started = std::time::Instant::now();

    // Measure our symmetric NAT delta to predict peer's port offset.
    let our_delta: i16 = {
        let servers_v4 = get_stun_servers_v4();
        if servers_v4.is_empty() {
            0
        } else {
            // STUN to first server to get base port
            if let Ok((base_addr, _)) = stun_query_single_server(&socket, &servers_v4[0]).await {
                let base_port = base_addr.port() as i16;
                // Try a different server to measure delta
                let alt = servers_v4.get(1).unwrap_or(&servers_v4[0]);
                if let Some(alt_addr) = alt.to_socket_addrs().ok().and_then(|mut i| i.find(|a| a.is_ipv4())) {
                    let _ = socket.connect(alt_addr).await;
                    socket.send(&[]).await.ok();
                    if let Ok((alt_addr, _)) = stun_query_single_server(&socket, alt).await {
                        let delta = alt_addr.port() as i16 - base_port;
                        log::info!("Phase3(Host): symmetric delta measured: {} (base {}, alt {})",
                            delta, base_port, alt_addr.port());
                        delta
                    } else { 0 }
                } else { 0 }
            } else { 0 }
        }
    };

    // Build port offset list: prioritized by likelihood.
    // - 0: exact match (asymmetric NAT)
    // - our_delta: prediction for symmetric NAT
    // - ±1, ±2, ±3: small drift
    // - ±5, ±10, ±20: larger drift
    let mut port_offsets: Vec<i16> = vec![0];
    if our_delta != 0 && !port_offsets.contains(&our_delta) {
        port_offsets.push(our_delta);
    }
    for d in [1i16, -1, 2, -2, 3, -3, 5, -5, 10, -10, 20, -20] {
        if !port_offsets.contains(&d) {
            port_offsets.push(d);
        }
    }

    for round in 0..5 {
        if started.elapsed() >= MAX_TIME {
            bail!("Phase3(Host) punch timed out after {:?}", started.elapsed());
        }

        let offsets: &[i16] = if round == 0 { &port_offsets[..5.min(port_offsets.len())] } else { &port_offsets };

        for &offset in offsets {
            if started.elapsed() >= MAX_TIME {
                bail!("Phase3(Host) punch timed out");
            }
            let mut target = peer_addr;
            let new_port = (target.port() as i32 + offset as i32) as u16;
            if new_port == 0 { continue; }
            target.set_port(new_port);

            if socket.connect(target).await.is_err() { continue; }

            for _ in 0..20 {
                socket.send(&[]).await.ok();
                hbb_common::tokio::time::sleep(Duration::from_millis(5)).await;
            }

            let socket_for_accept = socket.clone();
            let mut connect_fut = Box::pin(
                KcpStream::connect(socket.clone(), Duration::from_secs(3)));
            let mut accept_fut = Box::pin(async move {
                KcpStream::accept(socket_for_accept, Duration::from_secs(3), None).await
            });
            let result = tokio::select! {
                res = &mut connect_fut => {
                    match res {
                        Ok((kcp, stream)) => {
                            if let Ok(mut h) = kcp_handle.lock() {
                                *h = Some(kcp);
                            }
                            log::info!("Phase3(Host) KCP succeeded via connect after {:?}", started.elapsed());
                            Some(stream)
                        }
                        Err(_) => None,
                    }
                }
                res = &mut accept_fut => {
                    match res {
                        Ok((kcp, stream)) => {
                            if let Ok(mut h) = kcp_handle.lock() {
                                *h = Some(kcp);
                            }
                            log::info!("Phase3(Host) KCP succeeded via accept after {:?}", started.elapsed());
                            Some(stream)
                        }
                        Err(_) => None,
                    }
                }
            };
            if let Some(stream) = result {
                return Ok(stream);
            }
        }

        // After KCP rounds, try TCP simultaneous open on remaining budget.
        if let Some(ref listener) = tcp_listener {
            if started.elapsed() >= MAX_TIME { break; }
            const TCP_SCAN_MAX: i16 = 20;
            let tcp_offsets: &[i16] = if round == 0 { &[0] }
                else { &[0, 1, -1, 2, -2, 5, -5, 10, -10, TCP_SCAN_MAX, -TCP_SCAN_MAX] };
            for &offset in tcp_offsets {
                if started.elapsed() >= MAX_TIME { break; }
                let mut tcp_target = peer_addr;
                let new_port = (tcp_target.port() as i32 + offset as i32) as u16;
                if new_port == 0 { continue; }
                tcp_target.set_port(new_port);

                let ws_url = format!("ws://{}:{}", tcp_target.ip(), tcp_target.port());
                let result: Option<tokio::net::TcpStream> = tokio::select! {
                    res = listener.accept() => {
                        match res {
                            Ok((stream, _)) => {
                                log::info!("Phase3(Host) TCP: accept from {}", tcp_target);
                                Some(stream)
                            }
                            Err(_) => None,
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
                    res = tokio::time::timeout(Duration::from_secs(3),
                        tokio_tungstenite::connect_async(&ws_url)) => {
                        match res {
                            Ok(Ok((_ws, _))) => {
                                log::info!("Phase3(Host) WS: connect to {} succeeded!", tcp_target);
                                tokio::net::TcpStream::connect(tcp_target).await.ok()
                            }
                            _ => None,
                        }
                    }
                };
                if let Some(stream) = result {
                    log::info!("Phase3(Host) TCP/WS simultaneous open succeeded to {}!", tcp_target);
                    return Ok(Stream::from(stream, tcp_target));
                }
            }
        }
    }

    bail!("Phase3(Host) punch finished without success");
}

/// Detect NAT type by sending two STUN binding requests from the same socket
/// to the same STUN server, but to two different destination ports (or two servers).
/// If the mapped port differs -> Symmetric (NAT4).
/// If the mapped port is the same -> Cone (NAT1-3).
/// #2 enhancement: when only one STUN server is reachable, send two requests
/// to the same server and compare the mapped ports. The server may NAT the
/// response from different source ports, which can still differentiate symmetric.
pub async fn detect_symmetric_nat() -> ResultType<bool> {

    // Create a single socket for both queries so we can compare port mappings.
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let socket = Arc::new(socket);

    // Resolve the first STUN server.
    let servers_v4 = get_stun_servers_v4();
    let stun_str = match servers_v4.first() {
        Some(s) => s.clone(),
        None => return Ok(false),
    };
    let base_addr: SocketAddr = match stun_str.to_socket_addrs()?.find(|x| x.is_ipv4()) {
        Some(a) => a,
        None => return Ok(false),
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
            log::warn!("detect_symmetric_nat: first query failed: {:?}", e);
            return Ok(false);
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
        Err(_) => p1, // second query failed; assume cone
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
}
