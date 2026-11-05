import 'dart:async';
import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:flutter_hbb/common/shared_state.dart';
import 'package:flutter_hbb/models/model.dart';
import 'package:flutter_hbb/models/sync_controller.dart';
import 'package:flutter_hbb/models/sync_store.dart';
import 'package:get/get.dart';

/// 设备列表的存储键（JSON 数组：[{"id": "...", "password": "..."}]）。
const String _kWallPeersKey = 'wall_peers';

/// 单个画面墙格子的连接超时：超过这个时间还没有第一帧就提示需要密码/可重试。
const Duration _kConnectTimeout = Duration(seconds: 12);

class _WallPeer {
  final String id;
  final String password;

  const _WallPeer(this.id, this.password);
}

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
    final raw = await SyncStore().readString(_kWallPeersKey);
    final peers = <_WallPeer>[];
    if (raw != null && raw.trim().isNotEmpty) {
      var parsed = false;
      try {
        final decoded = jsonDecode(raw);
        if (decoded is List) {
          parsed = true;
          for (final item in decoded) {
            if (item is! Map) continue;
            final id = item['id'];
            final password = item['password'];
            if (id is String && id.trim().isNotEmpty) {
              peers.add(_WallPeer(
                  id.trim(), password is String ? password : ''));
            }
          }
        }
      } catch (_) {}
      if (!parsed) {
        // 兼容早期用逗号分隔的格式。
        for (final part in raw.split(',')) {
          final id = part.trim();
          if (id.isNotEmpty) peers.add(_WallPeer(id, ''));
        }
      }
    }
    if (!mounted) return;
    setState(() {
      _peers
        ..clear()
        ..addAll(peers);
      _loaded = true;
    });
  }

  Future<void> _savePeers() async {
    final data = <Map<String, dynamic>>[
      for (final peer in _peers) {'id': peer.id, 'password': peer.password}
    ];
    await SyncStore().writeString(_kWallPeersKey, jsonEncode(data));
  }

  Future<void> _addPeer() async {
    final idController = TextEditingController();
    final passwordController = TextEditingController();
    final added = await showDialog<bool>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('添加设备'),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            TextField(
              controller: idController,
              autofocus: true,
              decoration: const InputDecoration(
                labelText: '设备 ID',
                hintText: '对方主界面显示的 9 位 ID',
              ),
            ),
            TextField(
              controller: passwordController,
              obscureText: true,
              decoration: const InputDecoration(
                labelText: '密码（可选）',
                hintText: '对方设了固定密码时填这里，免得一直转圈',
              ),
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(false),
            child: const Text('取消'),
          ),
          TextButton(
            onPressed: () => Navigator.of(context).pop(true),
            child: const Text('添加'),
          ),
        ],
      ),
    );
    final id = idController.text.trim();
    final password = passwordController.text;
    idController.dispose();
    passwordController.dispose();
    if (added != true || id.isEmpty) return;
    if (_peers.any((peer) => peer.id == id)) {
      if (!mounted) return;
      ScaffoldMessenger.of(context)
          .showSnackBar(const SnackBar(content: Text('这台设备已经在墙上了')));
      return;
    }
    setState(() => _peers.add(_WallPeer(id, password)));
    await _savePeers();
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
  String? _error;
  bool _gotFirstImage = false;

  @override
  void initState() {
    super.initState();
    _connect();
  }

  void _connect() {
    _timeoutTimer?.cancel();
    setState(() {
      _error = null;
      _gotFirstImage = false;
    });
    final peerId = widget.peer.id;
    initSharedStates(peerId);
    final ffi = FFI(null);
    _ffi = ffi;
    ffi.imageModel.addCallbackOnFirstImage((_) {
      if (!mounted) return;
      _timeoutTimer?.cancel();
      setState(() => _gotFirstImage = true);
    });
    ffi.start(
      peerId,
      password: widget.peer.password.isEmpty ? null : widget.peer.password,
    );
    ffi.ffiModel.updateEventListener(ffi.sessionId, peerId);
    _timeoutTimer = Timer(_kConnectTimeout, () {
      if (!mounted || _gotFirstImage) return;
      setState(() {
        _error = widget.peer.password.isEmpty
            ? '连接超时：对方可能设了密码，或当前不可达'
            : '连接超时：密码可能不对，或当前不可达';
      });
    });
  }

  Future<void> _disposeSession() async {
    _timeoutTimer?.cancel();
    _timeoutTimer = null;
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

  @override
  void dispose() {
    _timeoutTimer?.cancel();
    _timeoutTimer = null;
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
