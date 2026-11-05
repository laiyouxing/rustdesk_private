import 'dart:async';
import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:flutter_hbb/common.dart';
import 'package:flutter_hbb/common/shared_state.dart';
import 'package:flutter_hbb/models/model.dart';
import 'package:flutter_hbb/models/peer_model.dart';
import 'package:flutter_hbb/models/sync_controller.dart';
import 'package:flutter_hbb/models/wall_store.dart';
import 'package:flutter_hbb/utils/multi_window_manager.dart';
import 'package:get/get.dart';

/// 单个画面墙格子的连接超时：超过这个时间还没有第一帧就提示需要密码/可重试。
const Duration _kConnectTimeout = Duration(seconds: 12);

/// 收到错误的宽限期：这段时间内还没有第一帧，才把这一格标记为连接失败。
const Duration kErrorGrace = Duration(seconds: 4);

typedef _WallPeer = WallPeerEntry;

/// 画面墙：一屏网格预览多台设备的实时画面（只读，不接收输入）。
///
/// 每格是一个独立的远程会话：会真实占用带宽与解码资源，格数等于并发会话数。
class WallPage extends StatefulWidget {
  const WallPage({super.key});

  @override
  State<WallPage> createState() => _WallPageState();
}

class _WallPageState extends State<WallPage> {
  final SyncController _sync = SyncController.instance;
  final List<_WallPeer> _peers = <_WallPeer>[];
  bool _loaded = false;

  @override
  void initState() {
    super.initState();
    _loadPeers();
  }

  Future<void> _loadPeers() async {
    final peers = await WallStore.load();
    if (!mounted) return;
    setState(() {
      _peers
        ..clear()
        ..addAll(peers);
      _loaded = true;
    });
  }

  Future<void> _savePeers() => WallStore.save(_peers);

  Future<void> _addPeer() async {
    final idController = TextEditingController();
    final passwordController = TextEditingController();
    String? error;
    // 密码默认隐藏，旁边给一个眼睛按钮，方便核对刚输入的内容。
    var obscurePassword = true;
    final added = await showDialog<bool>(
      context: context,
      builder: (dialogContext) => StatefulBuilder(
        builder: (dialogContext, setDialogState) => AlertDialog(
          title: const Text('添加设备'),
          content: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              TextField(
                controller: idController,
                autofocus: true,
                decoration: InputDecoration(
                  labelText: '设备 ID',
                  hintText: '234 485 258（复制出来带空格也可以）',
                  errorText: error,
                  errorMaxLines: 2,
                ),
              ),
              TextField(
                controller: passwordController,
                obscureText: obscurePassword,
                decoration: InputDecoration(
                  labelText: '密码（可选）',
                  hintText: '对方设了固定密码时填这里，免得一直转圈',
                  suffixIcon: IconButton(
                    tooltip: obscurePassword ? '显示密码' : '隐藏密码',
                    icon: Icon(obscurePassword
                        ? Icons.visibility_off_outlined
                        : Icons.visibility_outlined),
                    onPressed: () =>
                        setDialogState(() => obscurePassword = !obscurePassword),
                  ),
                ),
              ),
              const SizedBox(height: 8),
              Align(
                alignment: Alignment.centerLeft,
                child: TextButton.icon(
                  onPressed: () {
                    Navigator.of(dialogContext).pop(false);
                    _pickFromAddressBook();
                  },
                  icon: const Icon(Icons.menu_book_outlined, size: 18),
                  label: const Text('从地址簿选择'),
                ),
              ),
            ],
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(dialogContext).pop(false),
              child: const Text('取消'),
            ),
            TextButton(
              onPressed: () async {
                final err = await WallStore.add(idController.text,
                    password: passwordController.text);
                if (!mounted) return;
                if (err != null) {
                  setDialogState(() => error = err);
                  return;
                }
                Navigator.of(context).pop(true);
              },
              child: const Text('添加'),
            ),
          ],
        ),
      ),
    );
    idController.dispose();
    passwordController.dispose();
    if (added != true || !mounted) return;
    await _loadPeers();
  }

  /// 从地址簿挑一台已有设备，省得手抄 ID。
  Future<void> _pickFromAddressBook() async {
    final peers = <Peer>[];
    try {
      peers.addAll(gFFI.abModel.currentAbPeers);
    } catch (_) {}
    if (!mounted) return;
    if (peers.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('地址簿是空的，先手动填设备 ID')));
      return;
    }
    final selected = await showDialog<String>(
      context: context,
      builder: (dialogContext) => SimpleDialog(
        title: const Text('从地址簿选择'),
        children: [
          SizedBox(
            width: 420,
            height: 380,
            child: ListView(
              children: [
                for (final peer in peers)
                  ListTile(
                    dense: true,
                    leading: const Icon(Icons.desktop_windows_outlined,
                        size: 20),
                    title: Text(
                      peer.alias.isNotEmpty ? peer.alias : peer.id,
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                    ),
                    subtitle: Text(peer.id),
                    onTap: () => Navigator.of(dialogContext).pop(peer.id),
                  ),
              ],
            ),
          ),
        ],
      ),
    );
    if (selected == null || !mounted) return;
    final err = await WallStore.add(selected);
    if (!mounted) return;
    if (err != null) {
      ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(err)));
      return;
    }
    await _loadPeers();
  }

  Future<void> _removePeer(String id) async {
    setState(() => _peers.removeWhere((peer) => peer.id == id));
    await _savePeers();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('画面墙'),
        actions: [
          AnimatedBuilder(
            animation: _sync,
            builder: (context, _) {
              final enabled = _sync.enabled;
              return TextButton.icon(
                onPressed: () => _sync.toggle(),
                icon: Icon(
                  enabled ? Icons.sync : Icons.sync_disabled,
                  size: 18,
                  color: enabled ? Colors.blueAccent : null,
                ),
                label: Text(enabled ? '同步：开' : '同步：关'),
              );
            },
          ),
          IconButton(
            tooltip: '添加设备',
            onPressed: _addPeer,
            icon: const Icon(Icons.add),
          ),
        ],
      ),
      body: !_loaded
          ? const Center(child: CircularProgressIndicator())
          : _peers.isEmpty
              ? _buildEmpty()
              : _buildGrid(),
    );
  }

  Widget _buildEmpty() {
    return Center(
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          const Icon(Icons.grid_view, size: 56, color: Colors.grey),
          const SizedBox(height: 12),
          const Text('还没有设备，点右上角 + 添加'),
          const SizedBox(height: 8),
          Text(
            '每台设备都会单独连一次，占用带宽和解码资源',
            style: TextStyle(fontSize: 12, color: Colors.grey.shade600),
          ),
        ],
      ),
    );
  }

  Widget _buildGrid() {
    return LayoutBuilder(
      builder: (context, constraints) {
        final columns = (constraints.maxWidth / 360).floor().clamp(1, 4);
        return GridView.builder(
          padding: const EdgeInsets.all(12),
          gridDelegate: SliverGridDelegateWithFixedCrossAxisCount(
            crossAxisCount: columns,
            mainAxisSpacing: 12,
            crossAxisSpacing: 12,
            childAspectRatio: 16 / 10,
          ),
          itemCount: _peers.length,
          itemBuilder: (context, index) {
            final peer = _peers[index];
            return _WallTile(
              key: ValueKey(peer.id),
              peer: peer,
              onRemove: () => _removePeer(peer.id),
            );
          },
        );
      },
    );
  }
}

class _WallTile extends StatefulWidget {
  final _WallPeer peer;
  final VoidCallback onRemove;

  const _WallTile({
    super.key,
    required this.peer,
    required this.onRemove,
  });

  @override
  State<_WallTile> createState() => _WallTileState();
}

class _WallTileState extends State<_WallTile> {
  FFI? _ffi;
  Timer? _timeoutTimer;
  Timer? _pendingFailTimer;
  String? _pendingError;
  String? _error;
  bool _gotFirstImage = false;

  @override
  void initState() {
    super.initState();
    _connect();
  }

  void _connect() {
    _timeoutTimer?.cancel();
    _pendingFailTimer?.cancel();
    _pendingError = null;
    setState(() {
      _error = null;
      _gotFirstImage = false;
    });
    final peerId = widget.peer.id;
    initSharedStates(peerId);
    final ffi = FFI(null);
    _ffi = ffi;
    // 画面墙格子：连接错误只在这一格里显示，不弹全局对话框。
    ffi.wallMode = true;
    ffi.onWallMsgBox = (title, text) {
      // 首次尝试失败后 Rust 会自动重试并可能马上成功，所以先不急着判失败。
      _noteErrorMessage(_describeErrorMessage(title, text));
    };
    ffi.imageModel.addCallbackOnFirstImage((_) {
      if (!mounted) return;
      _timeoutTimer?.cancel();
      _pendingFailTimer?.cancel();
      _pendingError = null;
      setState(() {
        _gotFirstImage = true;
        // 之前报的错（首次尝试失败等）现在已被自动重连救回来，清掉。
        _error = null;
      });
    });
    ffi.start(
      peerId,
      password: widget.peer.password.isEmpty ? null : widget.peer.password,
    );
    ffi.ffiModel.updateEventListener(ffi.sessionId, peerId);
    _timeoutTimer = Timer(_kConnectTimeout, () {
      if (!mounted || _gotFirstImage) return;
      _failWith(widget.peer.password.isEmpty
          ? '连接超时：对方可能设了密码，或当前不可达'
          : '连接超时：密码可能不对，或当前不可达');
    });
  }

  Future<void> _disposeSession() async {
    _timeoutTimer?.cancel();
    _timeoutTimer = null;
    _pendingFailTimer?.cancel();
    _pendingFailTimer = null;
    final ffi = _ffi;
    _ffi = null;
    if (ffi == null) return;
    try {
      ffi.textureModel.onRemotePageDispose(true);
      ffi.imageModel.disposeImage();
      await ffi.close();
    } catch (_) {}
  }

  Future<void> _retry() async {
    await _disposeSession();
    if (!mounted) return;
    _connect();
  }

  /// 收到错误先记下来，等 [kErrorGrace] 还没有第一帧才真正判失败。
  ///
  /// Rust 侧首次连接失败会自己重试（打洞/中继切换），经常一两秒后就通了；
  /// 立刻判失败会让用户先看到「失败」再看到「连上了」。
  void _noteErrorMessage(String message) {
    if (!mounted || _error != null) return;
    _pendingError = message;
    _pendingFailTimer?.cancel();
    _pendingFailTimer = Timer(kErrorGrace, () {
      if (!mounted || _gotFirstImage) return;
      _failWith(_pendingError ?? message);
    });
  }

  /// 这一格连接失败/超时：只在本格显示错误，并停掉会话避免后台反复重连。
  void _failWith(String message) {
    if (!mounted || _error != null) return;
    _timeoutTimer?.cancel();
    _pendingFailTimer?.cancel();
    setState(() => _error = message);
    Future<void>.delayed(const Duration(seconds: 1), () {
      if (mounted && _error != null) _disposeSession();
    });
  }

  /// 把 Rust 侧的错误整理成格子里能显示的一句话。
  String _describeErrorMessage(String title, String text) {
    final msg = text.trim().isNotEmpty ? text.trim() : title.trim();
    if (msg.isEmpty) {
      return widget.peer.password.isEmpty
          ? '连接失败：对方可能设了密码，或当前不可达'
          : '连接失败：密码可能不对，或当前不可达';
    }
    return msg.length > 160 ? '${msg.substring(0, 160)}…' : msg;
  }

  @override
  void dispose() {
    _timeoutTimer?.cancel();
    _timeoutTimer = null;
    _pendingFailTimer?.cancel();
    _pendingFailTimer = null;
    final ffi = _ffi;
    _ffi = null;
    if (ffi != null) {
      try {
        ffi.textureModel.onRemotePageDispose(true);
        ffi.imageModel.disposeImage();
        ffi.close();
      } catch (_) {}
    }
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Card(
      clipBehavior: Clip.antiAlias,
      child: Stack(
        fit: StackFit.expand,
        children: [
          _buildContent(),
          Positioned(
            left: 6,
            top: 6,
            child: Container(
              padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
              decoration: BoxDecoration(
                color: Colors.black54,
                borderRadius: BorderRadius.circular(4),
              ),
              child: Text(
                widget.peer.id,
                style: const TextStyle(color: Colors.white, fontSize: 11),
              ),
            ),
          ),
          Positioned(
            right: 0,
            top: 0,
            child: IconButton(
              tooltip: '从画面墙移除',
              iconSize: 16,
              color: Colors.white70,
              onPressed: widget.onRemove,
              icon: const Icon(Icons.close),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildContent() {
    final error = _error;
    if (error != null) {
      return Center(
        child: Padding(
          padding: const EdgeInsets.all(12),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              const Icon(Icons.error_outline, color: Colors.orange, size: 28),
              const SizedBox(height: 8),
              Text(
                error,
                textAlign: TextAlign.center,
                style: const TextStyle(fontSize: 12),
              ),
              const SizedBox(height: 8),
              OutlinedButton(
                onPressed: _retry,
                child: const Text('重试'),
              ),
            ],
          ),
        ),
      );
    }
    final ffi = _ffi;
    if (ffi == null) return const SizedBox.shrink();
    return ClipRect(
      child: FittedBox(
        fit: BoxFit.contain,
        child: SizedBox(
          width: _displayWidth(ffi).toDouble(),
          height: _displayHeight(ffi).toDouble(),
          child: Obx(() {
            final textureId = ffi.textureModel.getTextureId(0).value;
            if (textureId == 0) {
              return const Center(
                child: SizedBox(
                  width: 20,
                  height: 20,
                  child: CircularProgressIndicator(strokeWidth: 2),
                ),
              );
            }
            return Texture(textureId: textureId);
          }),
        ),
      ),
    );
  }

  int _displayWidth(FFI ffi) {
    try {
      final displays = ffi.ffiModel.pi.displays;
      if (displays.isEmpty) return 16;
      final index = _displayIndex(ffi, displays.length);
      final width = displays[index].width;
      return width > 0 ? width : 16;
    } catch (_) {
      return 16;
    }
  }

  int _displayHeight(FFI ffi) {
    try {
      final displays = ffi.ffiModel.pi.displays;
      if (displays.isEmpty) return 10;
      final index = _displayIndex(ffi, displays.length);
      final height = displays[index].height;
      return height > 0 ? height : 10;
    } catch (_) {
      return 10;
    }
  }

  int _displayIndex(FFI ffi, int length) {
    var index = ffi.ffiModel.pi.currentDisplay;
    if (index < 0 || index >= length) index = 0;
    return index;
  }
}

/// 以独立窗口打开画面墙。
///
/// 不要用 `Navigator.push`：画面墙会顶掉主窗口的首页内容，返回时首页偶尔渲染成空白。
Future<void> openWallWindow() async {
  final argument = jsonEncode({'type': WindowType.Wall.index});
  try {
    await rustDeskWinManager.newSessionWindow(
      WindowType.Wall,
      '',
      argument,
      <int>[],
      false,
    );
  } catch (e) {
    debugPrint('open wall window failed: $e');
  }
}
