import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart' show Clipboard;
import 'package:flutter/widgets.dart' show Size;
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

/// 同步组里的一个成员（另一个窗口里的远程会话）。
///
/// [width]/[height] 是该会话当前远程桌面的像素尺寸，用来把绝对坐标
/// 按比例换算到不同分辨率的机器上；终端会话没有画面，尺寸为 0。
class SyncMember {
  final UuidValue sessionId;
  final int width;
  final int height;
  final List<int> terminalIds;

  /// 该会话文件传输窗口当前所在的远程目录（空的表示它没有打开文件传输）。
  final String fileDir;

  const SyncMember(
    this.sessionId, {
    this.width = 0,
    this.height = 0,
    this.terminalIds = const <int>[],
    this.fileDir = '',
  });
}

class _LocalSession {
  final String peerId;
  final Size? Function()? remoteSize;
  final String fileDir;

  const _LocalSession(this.peerId, this.remoteSize, {this.fileDir = ''});

  _LocalSession merge({
    String? peerId,
    Size? Function()? remoteSize,
    String? fileDir,
  }) =>
      _LocalSession(
        (peerId != null && peerId.isNotEmpty) ? peerId : this.peerId,
        remoteSize ?? this.remoteSize,
        fileDir:
            (fileDir != null && fileDir.isNotEmpty) ? fileDir : this.fileDir,
      );
}

/// 一个待同步的文件/目录（文件传输同步用）。
class SyncFileItem {
  final String path;
  final String name;
  final bool isDirectory;

  const SyncFileItem({
    required this.path,
    required this.name,
    required this.isDirectory,
  });
}

/// 同步操作（批量操控）控制器。
///
/// 语义：打开「同步」后，在一个远程会话上产生的键盘、鼠标按键/滚轮、相对鼠标位移
/// 以及绝对鼠标移动（按比例归一化）会复制到其它已加入同步组的会话；终端输入会
/// 复制给同组的其它终端。
///
/// 跨窗口的做法见 [SyncStore]：每个窗口把本窗口的会话（含心跳、远程分辨率、终端
/// 列表）写进临时目录，成员列表每 4 秒刷新一次并缓存在内存里；广播时直接用
/// sessionId 调用 Rust 侧的 `sessionInputKey` / `sessionSendMouse` /
/// `sessionSendTerminalInput`，不依赖目标窗口的 Dart 对象。
class SyncController extends ChangeNotifier {
  SyncController._();

  static final SyncController instance = SyncController._();

  /// 心跳间隔：窗口之间靠刷新文件里的时间戳互相发现。
  static const Duration heartbeatInterval = Duration(seconds: 4);

  /// 超过这个时长没有心跳的成员视为已退出。
  static const Duration staleAfter = Duration(seconds: 15);

  /// 绝对鼠标移动的最小广播间隔（约 60Hz），避免一次拖动把目标端打爆。
  static const int absMoveMinGapMs = 16;

  static const String _kEnabledFlag = 'enabled';

  final SyncStore _store = SyncStore();

  final String _instanceKey =
      'i${DateTime.now().microsecondsSinceEpoch}_${Random().nextInt(1 << 31)}';

  /// 本窗口登记过的会话（一个窗口里可以有多个远程标签页）。
  final Map<UuidValue, _LocalSession> _local = <UuidValue, _LocalSession>{};

  /// 终端登记（本窗口 + 其它窗口读回来的都在这里去重）。
  final Set<SyncTerminalRef> _terminals = <SyncTerminalRef>{};

  /// 成员缓存：来自各窗口的成员文件，每 [heartbeatInterval] 刷新一次。
  List<SyncMember> _membersCache = const <SyncMember>[];

  bool _initialized = false;
  bool _enabled = false;
  bool get enabled => _enabled;

  Timer? _heartbeat;
  Timer? _flagPoller;
  int _lastAbsMoveMs = 0;

  /// 当前同步组成员（不含本会话，调用方自行按 sourceSessionId 过滤）。
  List<SyncMember> get members => _membersCache;

  Set<SyncTerminalRef> get terminals =>
      Set<SyncTerminalRef>.unmodifiable(_terminals);

  /// 首次使用时读取全局开关、清理过期成员并开始轮询。
  Future<void> ensureInitialized() async {
    if (_initialized) return;
    _initialized = true;
    try {
      _enabled = await _store.readFlag(_kEnabledFlag) ?? false;
    } catch (_) {}
    try {
      // 客户端异常退出会留下成员文件，这里顺手清掉过期条目。
      await _store.sweepMembers(staleAfter * 4);
    } catch (_) {}
    await _refreshMembers();
    _flagPoller ??= Timer.periodic(heartbeatInterval, (_) async {
      try {
        final value = await _store.readFlag(_kEnabledFlag);
        if (value != null && value != _enabled) {
          _enabled = value;
          notifyListeners();
        }
      } catch (_) {}
    });
    _heartbeat ??= Timer.periodic(heartbeatInterval, (_) {
      _writeSelf();
      _refreshMembers();
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

  /// 把本窗口的某个远程会话登记进同步组（进入远程会话 / 打开终端时调用）。
  ///
  /// 同一会话可能被登记两次（远程画面页给分辨率，终端页不带分辨率），
  /// 这里做合并：已有分辨率回调时保留，peerId 为空时用新值补上。
  Future<void> registerSession(
    UuidValue sessionId, {
    String peerId = '',
    Size? Function()? remoteSize,
    String fileDir = '',
  }) async {
    await ensureInitialized();
    final existing = _local[sessionId];
    _local[sessionId] = existing == null
        ? _LocalSession(peerId, remoteSize, fileDir: fileDir)
        : existing.merge(
            peerId: peerId, remoteSize: remoteSize, fileDir: fileDir);
    await _writeSelf();
    await _refreshMembers();
  }

  /// 文件传输：登记某个会话当前所在的远程目录（切换目录时调用）。
  Future<void> updateFileTransferDir(UuidValue sessionId, String path) async {
    final existing = _local[sessionId];
    if (existing == null) {
      await registerSession(sessionId, fileDir: path);
      return;
    }
    if (existing.fileDir == path) return;
    _local[sessionId] = existing.merge(fileDir: path);
    await _writeSelf();
    await _refreshMembers();
  }

  /// 成员文件的 key：窗口实例 + 会话 id，保证同窗口的多个会话互不覆盖。
  String _memberKey(UuidValue sessionId) =>
      '${_instanceKey}_${sessionId.toString()}';

  Future<void> _writeSelf() async {
    if (_local.isEmpty) return;
    final ts = DateTime.now().millisecondsSinceEpoch;
    for (final entry in _local.entries) {
      Size? size;
      try {
        size = entry.value.remoteSize?.call();
      } catch (_) {}
      final terminalIds = _terminals
          .where((t) => t.sessionId == entry.key)
          .map((t) => t.terminalId)
          .toList(growable: false);
      try {
        await _store.writeMember(_memberKey(entry.key), <String, dynamic>{
          'session_id': entry.key.toString(),
          'peer_id': entry.value.peerId,
          'ts': ts,
          'w': size?.width.round() ?? 0,
          'h': size?.height.round() ?? 0,
          'terminals': terminalIds,
          'file_dir': entry.value.fileDir,
        });
      } catch (_) {}
    }
  }

  /// 从各窗口的成员文件重建成员缓存（过滤过期条目）。
  Future<void> _refreshMembers() async {
    final now = DateTime.now().millisecondsSinceEpoch;
    final byId = <String, SyncMember>{};
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
        final w = item['w'];
        final h = item['h'];
        final rawTerminals = item['terminals'];
        final terminalIds = <int>[];
        if (rawTerminals is List) {
          for (final t in rawTerminals) {
            if (t is int) terminalIds.add(t);
          }
        }
        byId[sessionId.toString()] = SyncMember(
          sessionId,
          width: w is int ? w : 0,
          height: h is int ? h : 0,
          terminalIds: terminalIds,
          fileDir: item['file_dir'] is String ? item['file_dir'] as String : '',
        );
      }
    } catch (_) {}
    _membersCache = List<SyncMember>.unmodifiable(byId.values);
  }

  /// 会话结束时注销（离开远程会话 / 关闭终端时调用）。
  ///
  /// 不传 [sessionId] 表示把本窗口登记过的所有会话一起注销。
  Future<void> unregisterSession([UuidValue? sessionId]) async {
    final targets =
        sessionId == null ? _local.keys.toList() : <UuidValue>[sessionId];
    for (final id in targets) {
      _local.remove(id);
      try {
        await _store.removeMember(_memberKey(id));
      } catch (_) {}
    }
    await _refreshMembers();
  }

  // ---- 画面墙群控 ----
  //
  // 画面墙的格子是只读预览，不参与全局同步成员（不写成员文件）。群控由墙自己
  // 指定「主控格 + 被控格」，目标集合放在这里，与全局同步开关互相独立。

  final List<SyncMember> _wallTargets = <SyncMember>[];
  bool _wallGroupEnabled = false;

  bool get wallGroupEnabled => _wallGroupEnabled;

  /// 设置画面墙群控的目标；[enabled] 为 false 或目标为空时等于关闭。
  void setWallGroup({
    required bool enabled,
    required List<SyncMember> targets,
  }) {
    _wallGroupEnabled = enabled && targets.isNotEmpty;
    _wallTargets
      ..clear()
      ..addAll(targets);
    notifyListeners();
  }

  void clearWallGroup() =>
      setWallGroup(enabled: false, targets: const <SyncMember>[]);

  /// 本次广播的目标：全局同步成员 + 画面墙群控目标，去重并排除源会话。
  List<SyncMember> _resolveTargets(UuidValue sourceSessionId) {
    final byId = <String, SyncMember>{};
    if (_enabled) {
      for (final member in _membersCache) {
        byId[member.sessionId.toString()] = member;
      }
    }
    if (_wallGroupEnabled) {
      for (final member in _wallTargets) {
        byId[member.sessionId.toString()] = member;
      }
    }
    byId.remove(sourceSessionId.toString());
    return byId.values.toList(growable: false);
  }

  /// 键盘：把一次按键复制给同组的其它会话。
  void broadcastInputKey({
    required UuidValue sourceSessionId,
    required String name,
    required bool down,
    required bool press,
    required bool alt,
    required bool ctrl,
    required bool shift,
    required bool command,
  }) {
    final targets = _resolveTargets(sourceSessionId);
    if (targets.isEmpty) return;
    for (final member in targets) {
      try {
        bind.sessionInputKey(
          sessionId: member.sessionId,
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
  /// 绝对坐标（`x`/`y` 且无 `type`）走 [broadcastAbsoluteMove]，不要传到这里。
  void broadcastMousePayload(UuidValue sourceSessionId, String payload) {
    // 相对鼠标模式的开关标记只对本地有效，不要转发。
    if (payload.contains('relative_mouse_mode')) return;
    if (payload.contains('"x"') || payload.contains('"y"')) return;
    final targets = _resolveTargets(sourceSessionId);
    for (final member in targets) {
      try {
        bind.sessionSendMouse(sessionId: member.sessionId, msg: payload);
      } catch (_) {}
    }
  }

  /// 原样把鼠标消息复制给同组会话，不做任何过滤。
  ///
  /// 画面墙主控用：坐标已在墙侧按本地预览坐标换算好，点击/滚轮这类消息直接复制
  /// 即可（绝对移动另走 [broadcastAbsoluteMove] 做分辨率归一化）。
  void broadcastMouseRaw(UuidValue sourceSessionId, String payload) {
    for (final member in _resolveTargets(sourceSessionId)) {
      try {
        bind.sessionSendMouse(sessionId: member.sessionId, msg: payload);
      } catch (_) {}
    }
  }

  /// 绝对鼠标移动：按比例归一化后广播，不同分辨率的机器也能落在同一相对位置。
  ///
  /// [x]/[y] 是源会话远程桌面的像素坐标，[srcWidth]/[srcHeight] 是它的分辨率。
  void broadcastAbsoluteMove({
    required UuidValue sourceSessionId,
    required int srcWidth,
    required int srcHeight,
    required int x,
    required int y,
  }) {
    if (!_enabled && !_wallGroupEnabled) return;
    if (srcWidth <= 0 || srcHeight <= 0) return;
    final now = DateTime.now().millisecondsSinceEpoch;
    if (now - _lastAbsMoveMs < absMoveMinGapMs) return;
    _lastAbsMoveMs = now;
    for (final member in _resolveTargets(sourceSessionId)) {
      // 尺寸太小说明是占位值（16×10），换算出来会把鼠标压到左上角，直接跳过。
      if (member.width < 100 || member.height < 100) continue;
      final targetX = (x * member.width / srcWidth).round();
      final targetY = (y * member.height / srcHeight).round();
      try {
        bind.sessionSendMouse(
          sessionId: member.sessionId,
          msg: json.encode({'x': '$targetX', 'y': '$targetY'}),
        );
      } catch (_) {}
    }
  }

  /// 登记一个终端会话（同一个窗口内的多个终端之间互相同步）。
  void registerTerminal(UuidValue sessionId, int terminalId) {
    _terminals.add(SyncTerminalRef(sessionId, terminalId));
    _writeSelf();
    _refreshMembers();
    notifyListeners();
  }

  void unregisterTerminal(UuidValue sessionId, int terminalId) {
    _terminals.remove(SyncTerminalRef(sessionId, terminalId));
    final stillHasTerminal = _terminals.any((t) => t.sessionId == sessionId);
    final local = _local[sessionId];
    // 只登记过终端的会话（没有远程画面 → 没有分辨率回调）在最后一个终端关闭后注销；
    // 远程画面页登记过的会话保留，画面还在用。
    if (!stillHasTerminal && local != null && local.remoteSize == null) {
      unregisterSession(sessionId);
    } else {
      _writeSelf();
      _refreshMembers();
    }
    notifyListeners();
  }

  /// 终端：把一次输入（含粘贴的多行内容）复制给同组的其它终端。
  void broadcastTerminalInput({
    required UuidValue sourceSessionId,
    required int sourceTerminalId,
    required String data,
  }) {
    if (!_enabled && !_wallGroupEnabled) return;
    if (data.isEmpty) return;
    for (final member in _resolveTargets(sourceSessionId)) {
      for (final terminalId in member.terminalIds) {
        if (member.sessionId == sourceSessionId &&
            terminalId == sourceTerminalId) {
          continue;
        }
        try {
          bind.sessionSendTerminalInput(
            sessionId: member.sessionId,
            terminalId: terminalId,
            data: data,
          );
        } catch (_) {}
      }
    }
  }

  /// 剪贴板同步：把本地剪贴板文本直接推给同组的其它会话。
  ///
  /// 语义是「粘贴到所有机器」：走 `sessionInputString`（等价于把这段文字敲进去），
  /// 因为 Dart 侧没有设置对端剪贴板的 FFI，而各会话自己的剪贴板通道只同步
  /// 本地剪贴板、不会把文本搬到别的会话。
  Future<void> broadcastClipboardPaste(UuidValue sourceSessionId) async {
    if (!_enabled && !_wallGroupEnabled) return;
    String? text;
    try {
      final data = await Clipboard.getData(Clipboard.kTextPlain);
      text = data?.text;
    } catch (_) {
      return;
    }
    if (text == null || text.isEmpty) return;
    for (final member in _resolveTargets(sourceSessionId)) {
      try {
        bind.sessionInputString(sessionId: member.sessionId, value: text);
      } catch (_) {}
    }
  }

  /// 文件传输同步：把同一批本地文件/目录推到其它已打开文件传输的会话。
  ///
  /// 每台机器都落到它自己当前所在的远程目录（同一个文件名），所以源端和目标端
  /// 的目录结构不必相同。
  void broadcastFileTransfer({
    required UuidValue sourceSessionId,
    required List<SyncFileItem> items,
    required bool includeHidden,
  }) {
    if (!_enabled && !_wallGroupEnabled) return;
    if (items.isEmpty) return;
    // 每台目标各建一个独立的传输任务 id：不能复用本地 job id，也不能多个文件共用一个，
    // 否则对端的传输任务会互相覆盖。取一个远离本地小计数器的区间。
    final baseActId =
        0x40000000 + (DateTime.now().millisecondsSinceEpoch & 0x3fffffff);
    for (final member in _resolveTargets(sourceSessionId)) {
      final dir = member.fileDir;
      if (dir.isEmpty) continue;
      final windows = dir.contains('\\') || RegExp(r'^[A-Za-z]:').hasMatch(dir);
      for (var i = 0; i < items.length; i++) {
        final item = items[i];
        try {
          bind.sessionSendFiles(
            sessionId: member.sessionId,
            actId: (baseActId + i) & 0x7fffffff,
            path: item.path,
            to: _joinPath(dir, item.name, windows),
            fileNum: 0,
            includeHidden: includeHidden,
            isRemote: false,
            isDir: item.isDirectory,
          );
        } catch (_) {}
      }
    }
  }

  static String _joinPath(String dir, String name, bool windows) {
    if (dir.endsWith('\\') || dir.endsWith('/')) return '$dir$name';
    return '$dir${windows ? '\\' : '/'}$name';
  }
}
