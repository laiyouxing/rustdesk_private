import 'dart:async';
import 'dart:math';

import 'package:flutter/foundation.dart';
import 'package:flutter_hbb/models/platform_model.dart';
import 'package:uuid/uuid.dart';

import 'sync_store.dart';

/// 一个远程终端会话的标识（peer 会话 + 终端编号）。
class SyncTerminalRef {
  final UuidValue sessionId;
  final int terminalId;

  const SyncTerminalRef(this.sessionId, this.terminalId);

  @override
  bool operator ==(Object other) =>
      other is SyncTerminalRef &&
      other.sessionId == sessionId &&
      other.terminalId == terminalId;

  @override
  int get hashCode => Object.hash(sessionId, terminalId);

  @override
  String toString() => 'SyncTerminalRef($sessionId, $terminalId)';
}

/// 同步操作（批量操控）控制器。
///
/// 语义：打开「同步」后，在一个远程会话上产生的键盘、鼠标按键/滚轮、相对鼠标位移，
/// 会原样复制到其它已加入同步组的会话；终端输入同理，会复制到本窗口内其它终端。
///
/// 跨窗口的做法见 [SyncStore]：每个窗口注册自己的 sessionId 并定期刷新心跳，
/// 广播时按 sessionId 直接调用 Rust 侧的 `sessionInputKey` / `sessionSendMouse`，
/// 不依赖目标窗口的 Dart 对象。
class SyncController extends ChangeNotifier {
  SyncController._();

  static final SyncController instance = SyncController._();

  /// 心跳间隔：窗口之间靠刷新文件里的时间戳互相发现。
  static const Duration heartbeatInterval = Duration(seconds: 4);

  /// 超过这个时长没有心跳的成员视为已退出。
  static const Duration staleAfter = Duration(seconds: 15);

  static const String _kEnabledFlag = 'enabled';

  final SyncStore _store = SyncStore();

  final String _instanceKey =
      'i${DateTime.now().microsecondsSinceEpoch}_${Random().nextInt(1 << 31)}';

  final Set<SyncTerminalRef> _terminals = <SyncTerminalRef>{};

  bool _initialized = false;
  bool _enabled = false;
  bool get enabled => _enabled;

  /// 本窗口已登记的会话（一个窗口里可以有多个远程标签页）。
  final Map<UuidValue, String> _registered = <UuidValue, String>{};
  Timer? _heartbeat;
  Timer? _flagPoller;

  Set<SyncTerminalRef> get terminals =>
      Set<SyncTerminalRef>.unmodifiable(_terminals);

  /// 首次使用时读取全局开关并开始轮询（其它窗口切换后能跟上）。
  Future<void> ensureInitialized() async {
    if (_initialized) return;
    _initialized = true;
    try {
      _enabled = await _store.readFlag(_kEnabledFlag) ?? false;
    } catch (_) {}
    _flagPoller ??= Timer.periodic(heartbeatInterval, (_) async {
      try {
        final value = await _store.readFlag(_kEnabledFlag);
        if (value != null && value != _enabled) {
          _enabled = value;
          notifyListeners();
        }
      } catch (_) {}
    });
    notifyListeners();
  }

  Future<void> setEnabled(bool value) async {
    await ensureInitialized();
    if (_enabled == value) return;
    _enabled = value;
    notifyListeners();
    try {
      await _store.writeFlag(_kEnabledFlag, value);
    } catch (_) {}
  }

  Future<void> toggle() => setEnabled(!_enabled);

  /// 把本窗口的某个远程会话登记进同步组（进入远程会话时调用）。
  ///
  /// 一个窗口里通常有多个远程标签页，每个标签页的 sessionId 不同，所以按会话
  /// 逐个登记；否则同时开着的标签页只有最后打开的那个能收发同步输入。
  Future<void> registerSession(UuidValue sessionId,
      {String peerId = ''}) async {
    await ensureInitialized();
    _registered[sessionId] = peerId;
    await _writeSelf();
    _heartbeat ??= Timer.periodic(heartbeatInterval, (_) => _writeSelf());
  }

  /// 成员文件的 key：窗口实例 + 会话 id，保证同窗口的多个会话互不覆盖。
  String _memberKey(UuidValue sessionId) =>
      '${_instanceKey}_${sessionId.toString()}';

  Future<void> _writeSelf() async {
    if (_registered.isEmpty) return;
    final ts = DateTime.now().millisecondsSinceEpoch;
    for (final entry in _registered.entries) {
      try {
        await _store.writeMember(_memberKey(entry.key), <String, dynamic>{
          'session_id': entry.key.toString(),
          'peer_id': entry.value,
          'ts': ts,
        });
      } catch (_) {}
    }
  }

  /// 会话结束时注销（离开远程会话时调用）。
  ///
  /// 不传 [sessionId] 表示把本窗口登记过的所有会话一起注销。
  Future<void> unregisterSession([UuidValue? sessionId]) async {
    final targets =
        sessionId == null ? _registered.keys.toList() : <UuidValue>[sessionId];
    for (final id in targets) {
      _registered.remove(id);
      try {
        await _store.removeMember(_memberKey(id));
      } catch (_) {}
    }
    if (_registered.isEmpty) {
      _heartbeat?.cancel();
      _heartbeat = null;
    }
  }

  /// 当前仍在线的同步组成员（不含 [excludeSessionId]）。
  Future<List<UuidValue>> members({UuidValue? excludeSessionId}) async {
    final result = <UuidValue>[];
    final now = DateTime.now().millisecondsSinceEpoch;
    try {
      for (final item in await _store.readMembers()) {
        final raw = item['session_id'];
        final ts = item['ts'];
        if (raw is! String || raw.isEmpty) continue;
        if (ts is! int) continue;
        if (now - ts > staleAfter.inMilliseconds) continue;
        UuidValue sessionId;
        try {
          sessionId = UuidValue(raw);
        } catch (_) {
          continue;
        }
        if (excludeSessionId != null && sessionId == excludeSessionId) continue;
        if (!result.contains(sessionId)) result.add(sessionId);
      }
    } catch (_) {}
    return result;
  }

  /// 键盘：把一次按键复制给同组的其它会话。
  Future<void> broadcastInputKey({
    required UuidValue sourceSessionId,
    required String name,
    required bool down,
    required bool press,
    required bool alt,
    required bool ctrl,
    required bool shift,
    required bool command,
  }) async {
    if (!_enabled) return;
    for (final sessionId in await members(excludeSessionId: sourceSessionId)) {
      try {
        await bind.sessionInputKey(
          sessionId: sessionId,
          name: name,
          down: down,
          press: press,
          alt: alt,
          ctrl: ctrl,
          shift: shift,
          command: command,
        );
      } catch (_) {}
    }
  }

  /// 鼠标：把已经编码好的消息原样复制给同组的其它会话。
  ///
  /// 相对鼠标位移（`move_relative`）与分辨率无关，按键/滚轮与坐标无关，都可以直接复用。
  /// 绝对坐标（`x`/`y` 且无 `type`）无法跨分辨率复制，调用方不要传入。
  Future<void> broadcastMousePayload(
      UuidValue sourceSessionId, String payload) async {
    if (!_enabled) return;
    // 相对鼠标模式的开关标记只对本地有效，不要转发。
    if (payload.contains('relative_mouse_mode')) return;
    for (final sessionId in await members(excludeSessionId: sourceSessionId)) {
      try {
        await bind.sessionSendMouse(sessionId: sessionId, msg: payload);
      } catch (_) {}
    }
  }

  /// 登记一个终端会话（同一个窗口内的多个终端之间互相同步）。
  void registerTerminal(UuidValue sessionId, int terminalId) {
    _terminals.add(SyncTerminalRef(sessionId, terminalId));
    notifyListeners();
  }

  void unregisterTerminal(UuidValue sessionId, int terminalId) {
    _terminals.remove(SyncTerminalRef(sessionId, terminalId));
    notifyListeners();
  }

  /// 终端：把一次输入（含粘贴的多行内容）复制给同组的其它终端。
  Future<void> broadcastTerminalInput({
    required UuidValue sourceSessionId,
    required int sourceTerminalId,
    required String data,
  }) async {
    if (!_enabled) return;
    if (data.isEmpty) return;
    final targets = _terminals
        .where((t) => !(t.sessionId == sourceSessionId &&
            t.terminalId == sourceTerminalId))
        .toList(growable: false);
    for (final t in targets) {
      try {
        await bind.sessionSendTerminalInput(
          sessionId: t.sessionId,
          terminalId: t.terminalId,
          data: data,
        );
      } catch (_) {}
    }
  }
}
