# NAT 穿透方案文档

> 方案：**先中继后升级**（Relay-then-Phase3 Upgrade）
> 
> 删除了：预连接穿透（PunchHoleRequest）、方案C（STUN via RegisterPeer）、ReSTUN

---

## 架构图

```
控制端 (Connector)                          主机 (Host)
  │                                             │
  │ 1. request_relay                             │
  │ ── TCP → hbbs ───────────────────────────────│
  │                                             │
  │ 2. RelayResponse                             │
  │ ◄────────────────────────────────────────────│
  │                                             │
  │ 3. TCP → hbbr (relay)                       │
  │◄═══ 中继连接已建立 (Relay/WebSocket) ═══════►│
  │                                             │
  │ 4. relay_upgrade_task (后台)                  │ 4. Phase3 STUN (后台)
  │  ├── 5轮STUN (同socket,同服务器)              │  ├── 1次STUN
  │  ├── 算delta、发地址给io_loop                  │  └── 发地址给主循环
  │  └── 10轮打洞循环 (30s预算)                   │
  │       │                                       │
  │ 5. PunchPeerAddr(本端STUN地址) ──中继──►     │
  │                                             │
  │ 6. ◄── PunchPeerAddr(对端STUN地址) ──中继──  │
  │       │                                       │
  │ 7. 加入targets（含±10端口盲扫）                │ 7. spawn relay_phase3_punch_to_peer
  │  ├── socket.connect(target)                  │  ├── 新socket + 端口偏移扫描
  │  ├── 20空包 burst                            │  ├── 20空包 burst
  │  ├── KCP connect/accept 竞速                  │  └── KCP connect/accept 竞速
  │  └── 成功后 notify → 切换直连                 │      成功后 notify → 切换直连
  │       │                                       │
  │ 8. ◄══════════ KCP/UDP 直连 ═══════════════►  │
  │     数据不再经过中继                             │
```

## 数据流

### 建立连接

```
_start_inner
  └── request_relay              ← 直接请求中继，不再发 PunchHoleRequest
        ├── 连接 hbbs
        ├── 发送 RequestRelay 消息
        ├── 等待 RelayResponse
        └── create_relay          ← 连接 hbbr 中继服务器
              └── 返回已加密的 relay Stream
```

### 后台打洞（控制端）

```
relay_upgrade_task (spawn 后执行 30s)
  │
  ├── NAT 类型检测 (detect_symmetric_nat)
  │     ├── 同一 socket 两次 STUN 到不同目标
  │     ├── 端口相同 → Cone NAT → 继续打洞
  │     └── 端口不同 → Symmetric → 放弃升级
  │
  ├── 创建主 socket (绑定 punch_port || 随机端口)
  │
  ├── 5轮 STUN (同 socket, 同服务器)
  │     ├── 第1轮: stun_query_with_socket (并发查3台)
  │     ├── 第2-5轮: stun_query_single_server (同一台)
  │     └── 记录端口序列, 算 delta, 仅用于日志
  │
  ├── Phase3 发本端地址给对端
  │     └── phase3_out_tx.try_send(addr) → io_loop → PunchPeerAddr → 中继 → 主机
  │
  └── 10轮打洞循环
        ├── 读取 phase3_peer_rx (对端地址) + 扩展±10端口
        ├── 遍历 targets
        │     ├── socket.connect(target)
        │     ├── 20空包 burst (5ms间隔)
        │     └── KCP connect/accept 竞速 (3s超时)
        │           ├── 成功 → notify → 升级直连 → return true
        │           └── 失败 → 下一 target
        └── 500ms 间隙 (keep-alive 空包+补扫 Phase3)
```

### 后台打洞（主机侧）

```
收到 PunchHole（rendezvous_mediator）:  已禁用，直接忽略
收到 PunchPeerAddr（connection.rs）:
  └── spawn relay_phase3_punch_to_peer
        ├── 新 socket (0.0.0.0:0)
        ├── 5轮 × 端口偏移 [0, ±1, ±2, ±3, ±5, ±10]
        │     ├── socket.connect(target)
        │     ├── 20空包 burst
        │     └── KCP connect/accept 竞速
        └── 成功 → punch_stream + notify → 升级直连
```

### 升级直连

```
io_loop select!:
  punch_notify.notified() =>
    ├── guard = punch_stream.lock()
    ├── peer = new_peer (替换中继流)
    ├── update_direct(true)
    ├── set_connection_type("UDP")
    └── set_punch_status("succeeded")
```

## 端口盲扫

收到对端 Phase3 地址后，扩展 ±10 端口范围：

```
对端 Phase3 地址: 1.2.3.4:5678
  ↓
加入 targets:
  1.2.3.4:5678    ← 原端口
  1.2.3.4:5679    ← +1
  1.2.3.4:5677    ← -1
  1.2.3.4:5680    ← +2
  1.2.3.4:5676    ← -2
  ...             ...
  1.2.3.4:5688    ← +10
  1.2.3.4:5668    ← -10
  (共21个 target)
```

Symmetric NAT 通常每连接递增 1-5，±10 覆盖绝大多数情况。

## targets 来源

| 来源 | 内容 | 时机 |
|------|------|------|
| Phase3 对端地址 | 对端IP : STUN端口 | 收到 PunchPeerAddr 时 |
| ±10 端口盲扫 | 对端IP : STUN端口±1~±10 | 扩展 Phase3 地址时 |
| 间隙补扫 | 同上 | 每轮间隙检查队列 |

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

## 文件清单

| 文件 | 修改 | 说明 |
|------|------|------|
| `src/client.rs` | `_start_inner` | 删除 PunchHoleRequest 循环，直接 request_relay |
| `src/client/io_loop.rs` | `io_loop`, `handle_msg_from_peer` | Phase3 地址发送/接收、relay_upgrade_task 启动 |
| `src/common.rs` | `relay_upgrade_task`, `relay_phase3_punch_to_peer` | 多轮STUN、端口盲扫、KCP竞速 |
| `src/rendezvous_mediator.rs` | 消息处理 | PunchHole 已禁用 |
| `src/server/connection.rs` | 主机侧 | Phase3 地址发送/接收（未改） |

## NAT 类型期望成功率

| 本端 → 对端 | 成功率 | 说明 |
|------------|--------|------|
| Cone ↔ 任意 | ~99% | 端口固定，STUN 值准确 |
| NAT3 ↔ NAT3 | ~95% | 双方端口固定 |
| NAT3 ↔ NAT4 | ~60-70% | NAT3 端口稳定，NAT4 盲扫命中 |
| NAT4 ↔ NAT4 | ~30-50% | 双方端口都变，依赖双方盲扫 |
