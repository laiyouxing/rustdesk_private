# RustDesk 定制内容清单

> 用于复刻到官方新版本时逐项检查。
> 所有改动基于 1.4.8.1 分支，合并到 main 后可在任何版本上复刻。

---

## □ 1. 自定义服务器编译期注入

**目的**：编译时将自建服务器地址和 Key 写进二进制，客户端开箱即用

### 改 libs/hbb_common/src/config.rs

```rust
// 约第120行 — 改为 option_env! 读取环境变量
pub const RS_PUB_KEY: &str = match option_env!("RS_PUB_KEY") {
    Some(v) => v,
    None => "OeVuKk5nlHiXp+APNn0Y3pC1Iwpwn44JGqrQCsWqmBw=",
};
pub const RENDEZVOUS_SERVERS: &[&str] = &[match option_env!("RENDEZVOUS_SERVER") {
    Some(v) => v,
    None => "rs-ny.rustdesk.com",
}];
pub const API_SERVER_DEFAULT: &str = match option_env!("API_SERVER") {
    Some(v) => v,
    None => "https://admin.rustdesk.com",
};
```

### 改 src/common.rs

```rust
// 约第1084行 — 默认 API Server 改为引用常量
config::API_SERVER_DEFAULT.to_owned()
```

### 改 .github/workflows/flutter-build.yml

```yaml
# 全局 env 区块增加（约第49行）
RS_PUB_KEY: "${{ secrets.RS_PUB_KEY }}"
RENDEZVOUS_SERVER: "${{ secrets.RENDEZVOUS_SERVER }}"
API_SERVER: "${{ secrets.API_SERVER }}"
```

---

## □ 2. 禁用官方更新检查和提示

**目的**：完全去除官方版本检查、更新提示和自动下载

### 改 src/common.rs

```rust
// 约第942行 — 直接 return
pub fn check_software_update() {
    // Disabled: no official update checks for custom builds
}
```

### 改 src/updater.rs

```rust
// 文件顶部加（约第1行）
#![allow(dead_code)]

// 约第78行 — 不再启动后台线程
fn start_auto_update_check() -> Sender<UpdateMsg> {
    let (tx, _rx) = channel();
    // Disabled: no auto update for custom builds
    tx
}
```

### 改 flutter/lib/desktop/pages/connection_page.dart

```dart
// 约第81行 — 替换整个 setupServerWidget 函数体
setupServerWidget() => SizedBox.shrink();
```

---

## □ 3. NAT 穿透优化

**目的**：提升自定义服务器的直连成功率

### 改 src/common.rs

```rust
// 约第1101行 — 自建服务器默认开启打洞
pub fn get_local_option(key: &str) -> String {
    let v = LocalConfig::get_option(key);
    if key == keys::OPTION_ENABLE_UDP_PUNCH || key == keys::OPTION_ENABLE_IPV6_PUNCH {
        if v.is_empty() {
            return "Y".to_owned(); // Enable by default
        }
    }
    v
}

// 约第2499行 — 打洞参数优化
let mut retry_interval = Duration::from_millis(10);     // 20ms → 10ms
const MAX_TIME: Duration = Duration::from_secs(30);     // 20s  → 30s
retry_interval = std::cmp::min(
    Duration::from_millis((retry_interval.as_millis() as f64 * 1.3) as u64),  // 1.5 → 1.3
    MAX_INTERVAL
);
```

### 改 src/client.rs

```rust
// 约第704行 — 多端口并发打洞（在 udp_socket_nat 的 if let 块内追加）
for extra_type in ["UDP+1", "UDP+2"] {
    if let Ok(extra_socket) = hbb_common::tokio::net::UdpSocket::bind(
        SocketAddr::new(
            if peer.is_ipv4() {
                std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
            } else {
                std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)
            },
            0,
        ),
    )
    .await
    {
        let extra_socket = Arc::new(extra_socket);
        let extra_timeout = connect_timeout;
        connect_futures.push(
            udp_nat_connect(extra_socket, extra_type, extra_timeout).boxed(),
        );
    }
}
```

---

## □ 4. 修复编译错误（上游遗留）

**目的**：上游重构删除了 `matches_permanent_password_plain` 但调用代码还在

### 改 libs/hbb_common/src/config.rs

```rust
// 在 has_permanent_password() 后面追加（约第1414行）
pub fn matches_permanent_password_plain(plain: &str) -> bool {
    // Check preset/hardcoded password first
    let (preset_storage, preset_salt) = Self::get_preset_password_storage_and_salt();
    if preset_permanent_password_storage_matches_plain(&preset_storage, &preset_salt, plain) {
        return true;
    }
    // Then check local stored password
    let (local_storage, local_salt) = Self::get_local_permanent_password_storage_and_salt();
    if local_storage.is_empty() || plain.is_empty() {
        return false;
    }
    if !local_salt.is_empty() {
        if let Some(stored_h1) = decode_permanent_password_h1_from_storage(&local_storage) {
            let h1 = compute_permanent_password_h1(plain, &local_salt);
            return sodiumoxide::utils::memcmp(&h1, &stored_h1);
        }
    }
    local_storage == plain
}
```

> 注：官方新版本可能已修复此问题，先编译看有没有报错再决定。

---

## □ 5. 关闭定时构建

### 改 .github/workflows/flutter-nightly.yml

```yaml
# 移除 schedule 块，只保留 workflow_dispatch
on:
  workflow_dispatch:
```

---

## ■ GitHub Secrets 设置

在仓库 → Settings → Secrets and variables → Actions → New repository secret 添加：

| Secret 名称 | 说明 | 示例 |
|------------|------|------|
| `RS_PUB_KEY` | 自定义服务器公钥 | （你的公钥值） |
| `RENDEZVOUS_SERVER` | 会合服务器地址 | `183.24.70.210` |
| `API_SERVER` | API 服务器地址 | `http://183.24.70.210` |

---

## 复刻步骤（官方新版本）

1. 把新版本代码拉到本地
2. 逐项对照上面标 □ 的位置改
3. 本地编译测试通过
4. 推送触发 GitHub Actions 构建
