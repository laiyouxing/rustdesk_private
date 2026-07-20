# NAT 穿透方案文档

> 方案：**先中继后升级**（Relay-then-Phase3 Upgrade）
>
> 删除了：预连接打洞（`handle_punch_hole` / `punch_udp_hole` 旧流程）、方案C（STUN via RegisterPeer）、ReSTUN、WS 打洞、时间戳同步
>
> 注意：`PunchHoleRequest` **仍然发送一次**（client.rs `_start_inner`），用于让 hbbs 分配 relay_server；删除的只是"连接前先打洞"的流程。

---

## 架构图

```
控制端 (Connector)                          主机 (Host)
  │                                             │
  │ 1. PunchHoleRequest (仅用于 relay 分配)      │
  │ ── TCP → hbbs ───────────────────────────────│
  │                                             │
  │ 2. PunchHole / RelayResponse                │
  │ ◄────────────────────────────────────────────│
  │                                             │
  │ 3. TCP → hbbr (relay)                       │
  │◄═══ 中继连接已建立 (Relay/WebSocket) ═══════►│
  │                                             │
  │ 4. relay_upgrade_task (独立线程,可取消)      │ 4. Phase3 STUN (后台)
  │  ├── 5轮STUN (同socket,锁定服务器)           │  ├── 共享持久 socket STUN
  │  ├── 算delta、发地址给io_loop                 │  └── 发地址给主循环
  │  └── 6轮打洞循环 (180s预算)                  │
  │       │                                       │
  │ 5. PunchPeerAddr(本端STUN/TCP/IPv6地址) ──中继──►│
  │                                             │
  │ 6. ◄── PunchPeerAddr(对端STUN/TCP地址) ──中继──│
  │       │                                       │
  │ 7. 加入targets（delta预测+±50盲扫）           │ 7. 地址入队(过滤0.0.0.0/IPv6)
  │  ├── socket.connect(target)                  │  └── 唯一打洞任务 relay_phase3_punch_to_peer
  │  ├── 2空包 burst                             │       ├── 共享持久 socket + 端口偏移扫描(±50)
  │  ├── 单endpoint竞速KCP connect/accept        │       ├── 20空包 burst
  │  │   (控制端优先connect)                      │       ├── 单endpoint竞速KCP connect/accept
  │  └── 成功→notify→切换直连                     │       │   (主机优先accept)
  │       │                                       │       └── 成功→notify→切换直连
  │ 8. ◄══════════ KCP/UDP 直连 ═══════════════►  │
  │     换流时 set_key 重建加密(序号归零,两侧同步)    │
  │     数据不再经过中继                             │
```

## 数据流

### 建立连接

```
_start_inner
  ├── 发送 PunchHoleRequest 一次  ← 仅为获取 hbbs 分配的 relay_server
  ├── 等待 PunchHole / RelayResponse
  └── create_relay               ← 连接 hbbr 中继服务器
        └── 返回已加密的 relay Stream
```

### 后台打洞（控制端）

```
relay_upgrade_task (独立线程 + 独立 current_thread 运行时, CancellationToken 可取消)
  │
  ├── NAT 类型检测 (detect_symmetric_nat) — fail-open
  │     ├── 同一 socket 两次 STUN 到不同目标
  │     ├── 端口相同 → Ok(false) = Cone → 写 nat_type=ASYMMETRIC, 继续打洞
  │     ├── 端口不同 → Ok(true) = Symmetric → 写 nat_type=SYMMETRIC, 放弃升级
  │     └── 任一查询失败 → Err(不确定) → 不写配置(保持未知,下次重测), 继续打洞
  │
  ├── 创建主 socket (绑定 UPnP 端口 || udp_nat_port || 随机端口) + IPv6 socket(尽力)
  │
  ├── 5轮 STUN (同 socket)
  │     ├── 第1轮: stun_query_with_socket (并发查3台)
  │     ├── 第2-5轮: stun_query_single_server (锁定同一台, 测 symmetric delta)
  │     └── delta 共识: 多台备选服务器取最频繁非零 delta
  │
  ├── Phase3 发本端地址给对端
  │     └── phase3_out_tx.try_send(addr) → io_loop → PunchPeerAddr → 中继 → 主机
  │         (IPv6 地址、TCP listener 地址、STUN 地址、delta 预测地址)
  │
  └── 6轮打洞循环 (总预算 180s, 每轮开头/每个target/TCP fallback 开头检查 cancel)
        ├── 读取 phase3_peer_rx (对端地址) + delta预测 + ±50端口盲扫
        ├── 遍历 targets
        │     ├── socket.connect(target) + 立即1个空包(防映射老化)
        │     ├── 间隔1s, 再 2空包 burst (5ms间隔)
        │     └── 单 endpoint 上 KcpStream::race (3s超时, 控制端 prefer_connect)
        │           ├── 成功 → notify → 升级直连 → return true
        │           └── 失败 → 下一 target
        ├── TCP fallback: 对 phase3_tcp_rx 目标做 TCP 同时打开
        │     └── connect 与"校验来源IP的accept"竞速 (各3s, 端口 0,±1,±2,±5)
        └── 轮间休息 2s (期间补收对端新地址)
```

### 后台打洞（主机侧）

```
收到 PunchHole（rendezvous_mediator）:
  └── 不打洞, 只回 PunchHoleSent 给 hbbs (带回 relay 测速排序结果)

收到 PunchPeerAddr（connection.rs）:
  ├── 地址过滤: 0.0.0.0 / IPv6 直接忽略; 已升级则忽略
  ├── 地址入队 phase3_targets (唯一打洞任务消费, 避免多任务共享socket互抢)
  └── 首次地址时 spawn 唯一 relay_phase3_punch_to_peer (JoinHandle 保存, on_close 时 abort)
        ├── 共享持久 socket (STUN 与打洞同 socket, NAT 映射一致)
        ├── 测 symmetric delta, 端口偏移 [0, delta, ±1,±2,±3,±5,±10,±20,±30,±40,±50]
        │     (第1轮只用前5个高优先级偏移)
        ├── 每个 target: socket.connect + 20空包 burst (5ms间隔)
        ├── 单 endpoint 上 KcpStream::race (3s超时, 主机 prefer_accept)
        ├── TCP fallback: connect 与"校验来源IP的accept"竞速 (各3s)
        └── 成功 → punch_stream + notify → 升级直连 (预算 180s, 与控制端对齐)
```

### 升级直连

```
io_loop select!:
  punch_notify.notified() =>
    ├── guard = punch_stream.lock().take()
    ├── 先发空 Message 让 relay 刷新挂起消息
    ├── saved_key = peer.take_key(); peer = new_peer
    ├── peer.set_key(enc.0)   ← 用相同 key 重建 Encrypt, 收发序号归零
    │     (换流瞬间丢弃的在途报文若保留旧序号会使新流序号永久错位;
    │      两侧都在换流时刻归零, 新流上序号自洽)
    ├── KcpStream 移入局部 kcp 变量   ← 断连时发 close_reason,
    │     否则对端要等 60s KCP 超时 (KCP 无连接关闭事件)
    ├── update_direct(true)
    ├── set_connection_type("UDP")
    └── set_punch_status("succeeded")
```

## NAT 类型检测（fail-open）

`detect_symmetric_nat` 只在有明确结论时返回值：

| 情形 | 返回 | 调用处行为 |
|------|------|-----------|
| 两次查询同端口 | `Ok(false)` (Cone) | 写 `nat_type=ASYMMETRIC`, 继续打洞 |
| 两次查询不同端口 | `Ok(true)` (Symmetric) | 写 `nat_type=SYMMETRIC`, 放弃升级走中继 |
| 任一查询失败/无服务器 | `Err` (不确定) | **不写配置**（保持未知，下次连接重测），按未知继续打洞 |

避免一次 STUN 抖动把对称 NAT 用户永久误判为 Cone（误判结果曾被永久缓存进配置）。

## 端口盲扫

收到对端 Phase3 地址后，按 delta 预测 + ±50 端口扩展：

```
对端 Phase3 地址: 1.2.3.4:5678
  ↓
加入 targets:
  1.2.3.4:5678          ← 原端口 (asymmetric NAT 命中)
  1.2.3.4:5678+delta    ← delta 预测 (symmetric NAT 命中, delta≠0 时)
  1.2.3.4:5679..5728    ← +1..+50
  1.2.3.4:5677..5628    ← -1..-50
```

Symmetric NAT 每目标端口递增可能较大，±50 覆盖绝大多数情况；delta 由多台
STUN 服务器共识得出。端口计算在 i32 域进行并检查 1..=65535，避免 u16 回绕。

## targets 来源

| 来源 | 内容 | 时机 |
|------|------|------|
| Phase3 对端地址 | 对端IP : STUN端口 | 收到 PunchPeerAddr 时 |
| delta 预测端口 | 对端IP : STUN端口+delta | 扩展 Phase3 地址时 (delta≠0) |
| ±50 端口盲扫 | 对端IP : STUN端口±1~±50 | 扩展 Phase3 地址时 |
| TCP fallback 目标 | 对端IP : TCP listener端口 (±0,1,2,5) | 每轮 KCP 失败后 |
| 间隙补扫 | 同 UDP | 每轮 2s 间隙检查队列 |

## STUN 查询

```
stun_query_with_socket:            stun_query_single_server:
  ┌───────────────────────────┐     ┌───────────────────────────┐
  │ 并发查 stun.qq.com        │     │ 定向查指定服务器          │
  │ 并发查 stun.miwifi.com    │     │ (用于多轮 STUN 的第二轮起)│
  │ 并发查 stun.hstun.com     │     └───────────────────────────┘
  │ 取最先返回的2个结果       │
  │ 一致 → 高置信度           │
  │ 否则 → 用第一个           │
  └───────────────────────────┘
```

## TCP fallback

KCP/UDP 打洞失败后，对每个 TCP 目标（端口 0,±1,±2,±5 偏移）做 TCP 同时打开：

- `connect(target)` 与 `accept_from_ip(listener, target.ip())` 竞速，各 3s 超时
- accept 只接受来源 IP 匹配的连接，其余丢弃继续等
- 无 WebSocket、无时间戳同步
- 控制端 TCP 目标限量保新（最多 8 条）；主机端第 1 轮只试原端口

## 任务生命周期与熔断

- 控制端：`relay_upgrade_task` 跑在独立线程（独立 current_thread tokio 运行时），
  持 `CancellationToken`（`Remote.phase3_cancel`）。重连 spawn 新任务前先取消旧 token；
  io_loop 退出时也取消一次。任务在每轮/每 target/TCP fallback 开头检查取消。
- 主机端：唯一打洞任务的 `JoinHandle` 存于 `Connection.phase3_task`，
  `on_close` 时 `abort()`。
- 熔断：Phase3 连续失败 3 次（`record_phase3_failure`）后跳过后续打洞
  （`should_skip_phase3`）。**只有用户主动断开**（`send_close_reason` →
  `reset_phase3_state`）或打洞成功才重置计数；自动重连不重置。
- 会话类型门控：只有 DEFAULT_CONN / VIEW_CAMERA 的 Relay/WebSocket 连接做 Phase3；
  FILE_TRANSFER / PORT_FORWARD / TERMINAL 不做。

## 文件清单

| 文件 | 修改 | 说明 |
|------|------|------|
| `src/client.rs` | `_start_inner` | 仍发一次 PunchHoleRequest（仅取 relay 分配），随后直接 request_relay |
| `src/client/io_loop.rs` | `io_loop`, `handle_msg_from_peer` | Phase3 地址收发、spawn 打洞线程（cancel token）、换流（set_key 序号归零）、升级后补 kcp 以发 close_reason、conn_type 门控 |
| `src/common.rs` | `relay_upgrade_task`, `relay_phase3_punch_to_peer`, `detect_symmetric_nat` | 多轮STUN、delta预测+±50盲扫、单endpoint KCP 竞速、TCP fallback、NAT 检测 fail-open、Phase3 熔断计数 |
| `src/kcp_stream.rs` | `KcpStream::race` | 单 endpoint 上 connect/accept 竞速（`prefer_connect` 区分控制端/主机） |
| `src/rendezvous_mediator.rs` | 消息处理 | PunchHole 不打洞，只回 PunchHoleSent（relay 测速排序）；旧 handle_punch_hole/punch_udp_hole 已删除 |
| `src/server/connection.rs` | 主机侧 | 共享持久 socket STUN、地址过滤入队、唯一打洞任务（JoinHandle，on_close abort）、换流 |
| `src/ui_session_interface.rs` | 重连循环 | 不在自动重连时重置 Phase3 熔断（只有用户主动断开才重置） |

## NAT 类型期望成功率

| 本端 → 对端 | 成功率 | 说明 |
|------------|--------|------|
| Cone ↔ 任意 | ~99% | 端口固定，STUN 值准确 |
| NAT3 ↔ NAT3 | ~95% | 双方端口固定 |
| NAT3 ↔ NAT4 | ~60-70% | NAT3 端口稳定，NAT4 盲扫命中 |
| NAT4 ↔ NAT4 | ~30-50% | 双方端口都变，依赖双方盲扫 |
