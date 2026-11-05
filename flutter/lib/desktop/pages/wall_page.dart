import 'package:flutter/material.dart';
import 'package:get/get.dart';

import '../../common/shared_state.dart';
import '../../models/model.dart';
import '../../models/sync_controller.dart';
import '../../models/sync_store.dart';

/// 画面墙：一个页面同时看多台远程设备的实时画面（只读预览），
/// 并在顶部提供「同步操作」总开关。
class WallPage extends StatefulWidget {
  const WallPage({super.key});

  @override
  State<WallPage> createState() => _WallPageState();
}

class _WallPageState extends State<WallPage> {
  final SyncStore _store = SyncStore();
  final SyncController _sync = SyncController.instance;
  final List<String> _peers = <String>[];

  bool _loaded = false;
  bool _syncEnabled = false;

  @override
  void initState() {
    super.initState();
    _sync.addListener(_onSyncChanged);
    _sync.ensureInitialized().then((_) {
      if (mounted) setState(() => _syncEnabled = _sync.enabled);
    });
    _loadPeers();
  }

  @override
  void dispose() {
    _sync.removeListener(_onSyncChanged);
    super.dispose();
  }

  void _onSyncChanged() {
    if (!mounted) return;
    setState(() => _syncEnabled = _sync.enabled);
  }

  Future<void> _loadPeers() async {
    var list = <String>[];
    try {
      final raw = await _store.readString('wall_peers') ?? '';
      list = raw
          .split(',')
          .map((e) => e.trim())
          .where((e) => e.isNotEmpty)
          .toList();
    } catch (_) {}
    if (!mounted) return;
    setState(() {
      _peers
        ..clear()
        ..addAll(list);
      _loaded = true;
    });
  }

  Future<void> _savePeers() async {
    try {
      await _store.writeString('wall_peers', _peers.join(','));
    } catch (_) {}
  }

  Future<void> _addPeer() async {
    final controller = TextEditingController();
    final id = await showDialog<String>(
      context: context,
      builder: (ctx) => AlertDialog(
        title: const Text('添加设备'),
        content: TextField(
          controller: controller,
          autofocus: true,
          decoration: const InputDecoration(hintText: '输入远程设备 ID'),
          onSubmitted: (value) => Navigator.of(ctx).pop(value),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(ctx).pop(),
            child: const Text('取消'),
          ),
          TextButton(
            onPressed: () => Navigator.of(ctx).pop(controller.text),
            child: const Text('添加'),
          ),
        ],
      ),
    );
    final trimmed = (id ?? '').trim();
    if (trimmed.isEmpty || _peers.contains(trimmed)) return;
    setState(() => _peers.add(trimmed));
    await _savePeers();
  }

  Future<void> _removePeer(String peerId) async {
    setState(() => _peers.remove(peerId));
    await _savePeers();
  }

  Widget _buildEmpty() {
    return Center(
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          const Text('画面墙还没有设备'),
          const SizedBox(height: 12),
          OutlinedButton.icon(
            onPressed: _addPeer,
            icon: const Icon(Icons.add),
            label: const Text('添加设备'),
          ),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('画面墙'),
        actions: [
          const Text('同步操作'),
          Switch(
            value: _syncEnabled,
            onChanged: (value) => _sync.setEnabled(value),
          ),
          IconButton(
            icon: const Icon(Icons.add),
            tooltip: '添加设备',
            onPressed: _addPeer,
          ),
          const SizedBox(width: 4),
        ],
      ),
      body: !_loaded
          ? const Center(child: CircularProgressIndicator())
          : _peers.isEmpty
              ? _buildEmpty()
              : GridView.builder(
                  padding: const EdgeInsets.all(8),
                  gridDelegate: const SliverGridDelegateWithFixedCrossAxisCount(
                    crossAxisCount: 3,
                    mainAxisSpacing: 8,
                    crossAxisSpacing: 8,
                    childAspectRatio: 16 / 10,
                  ),
                  itemCount: _peers.length,
                  itemBuilder: (context, index) {
                    final peerId = _peers[index];
                    return _WallTile(
                      key: ValueKey(peerId),
                      peerId: peerId,
                      onRemove: () => _removePeer(peerId),
                    );
                  },
                ),
    );
  }
}

/// 单个画面墙格子：独立建立一条远程会话，只渲染画面，不接收输入。
class _WallTile extends StatefulWidget {
  const _WallTile({
    super.key,
    required this.peerId,
    required this.onRemove,
  });

  final String peerId;
  final VoidCallback onRemove;

  @override
  State<_WallTile> createState() => _WallTileState();
}

class _WallTileState extends State<_WallTile> {
  late final FFI _ffi;
  bool _failed = false;
  String _error = '';

  @override
  void initState() {
    super.initState();
    _ffi = FFI();
    try {
      initSharedStates(widget.peerId);
    } catch (_) {}
    try {
      _ffi.start(widget.peerId);
      _ffi.ffiModel.updateEventListener(_ffi.sessionId, widget.peerId);
    } catch (e) {
      _failed = true;
      _error = '$e';
    }
  }

  @override
  void dispose() {
    try {
      _ffi.textureModel.onRemotePageDispose(true);
    } catch (_) {}
    try {
      _ffi.close();
    } catch (_) {}
    try {
      removeSharedStates(widget.peerId);
    } catch (_) {}
    super.dispose();
  }

  double _remoteAspect() {
    try {
      final displays = _ffi.ffiModel.pi.getCurDisplays();
      if (displays.isNotEmpty) {
        final display = displays.first;
        if (display.width > 0 && display.height > 0) {
          return display.width / display.height;
        }
      }
    } catch (_) {}
    return 16 / 9;
  }

  Widget _buildPreview() {
    return Obx(() {
      final textureId = _ffi.textureModel.getTextureId(0).value;
      if (textureId <= 0) {
        return const Center(
          child: SizedBox(
            width: 18,
            height: 18,
            child: CircularProgressIndicator(strokeWidth: 2),
          ),
        );
      }
      return Center(
        child: AspectRatio(
          aspectRatio: _remoteAspect(),
          child: Texture(textureId: textureId),
        ),
      );
    });
  }

  @override
  Widget build(BuildContext context) {
    return Container(
      decoration: BoxDecoration(
        color: Colors.black87,
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: Colors.white24),
      ),
      clipBehavior: Clip.antiAlias,
      child: Stack(
        fit: StackFit.expand,
        children: [
          _buildPreview(),
          Positioned(
            left: 6,
            top: 4,
            child: Text(
              widget.peerId,
              style: const TextStyle(color: Colors.white70, fontSize: 11),
            ),
          ),
          Positioned(
            right: 0,
            top: 0,
            child: IconButton(
              iconSize: 16,
              tooltip: '移除',
              icon: const Icon(Icons.close, color: Colors.white54),
              onPressed: widget.onRemove,
            ),
          ),
          if (_failed)
            Positioned(
              left: 6,
              right: 6,
              bottom: 6,
              child: Text(
                '连接失败: $_error',
                maxLines: 2,
                overflow: TextOverflow.ellipsis,
                style: const TextStyle(color: Colors.redAccent, fontSize: 11),
              ),
            ),
        ],
      ),
    );
  }
}
