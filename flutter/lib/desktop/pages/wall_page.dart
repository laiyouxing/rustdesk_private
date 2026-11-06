import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:desktop_multi_window/desktop_multi_window.dart';
import 'package:flutter/material.dart';
import 'package:flutter_hbb/common.dart';
import 'package:flutter_hbb/consts.dart';
import 'package:flutter_hbb/common/shared_state.dart';
import 'package:flutter_hbb/common/widgets/remote_input.dart';
import 'package:flutter_hbb/models/model.dart';
import 'package:flutter_hbb/models/peer_model.dart';
import 'package:flutter_hbb/models/platform_model.dart';
import 'package:flutter_hbb/models/sync_controller.dart';
import 'package:flutter_hbb/models/wall_store.dart';
import 'package:flutter_hbb/main.dart';
import 'package:flutter_hbb/utils/multi_window_manager.dart';
import 'package:get/get.dart';
import 'package:uuid/uuid.dart';

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

class _WallPageState extends State<WallPage> with MultiWindowListener {
  final SyncController _sync = SyncController.instance;
  final List<_WallPeer> _peers = <_WallPeer>[];
  bool _loaded = false;

  /// 多选（批量操作）模式：格子上出现勾选圈，点格子选中/取消。
  bool _selectMode = false;
  final Set<String> _selected = <String>{};

  /// 批量设置：页面写、格子读（监听后按需应用）。
  final ValueNotifier<_BatchSettings?> _batchSettings =
      ValueNotifier<_BatchSettings?>(null);

  @override
  void initState() {
    super.initState();
    // 多窗口的标题栏 × 会被 preventClose 拦住，必须自己监听 close 事件再关窗，
    // 否则点 × 没有任何反应（主窗口 / 远程桌面窗口都是这么处理的）。
    DesktopMultiWindow.addListener(this);
    _loadPeers();
  }

  @override
  void onWindowClose() {
    _sync.clearWallGroup();
    unawaited(_closeWindow());
  }

  Future<void> _closeWindow() async {
    try {
      final controller = WindowController.fromWindowId(kWindowId!);
      await controller.setPreventClose(false);
      await controller.close();
    } catch (e) {
      debugPrint('close wall window failed: $e');
    }
  }

  @override
  void dispose() {
    DesktopMultiWindow.removeListener(this);
    _sync.clearWallGroup();
    _batchSettings.dispose();
    super.dispose();
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
                    onPressed: () => setDialogState(
                        () => obscurePassword = !obscurePassword),
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
  ///
  /// 不能只看 `currentAbPeers`：那只是当前选中的那一本地址簿。
  /// 「别人共享给我的设备」在另外的共享地址簿里，所以这里遍历全部地址簿并去重。
  Future<void> _pickFromAddressBook() async {
    // 记录每台设备来自哪本地址簿，「共享」单独标一下，方便确认选的是别人分享的那台。
    final items = <({Peer peer, String book, bool shared})>[];
    try {
      final seen = <String>{};
      for (final ab in gFFI.abModel.addressbooks.values) {
        final shared = !ab.isPersonal();
        for (final peer in ab.peers) {
          if (peer.id.isEmpty || !seen.add(peer.id)) continue;
          items.add((peer: peer, book: ab.name(), shared: shared));
        }
      }
    } catch (_) {}
    // 别名在前，其次按 ID，方便在长列表里找。
    items.sort((a, b) {
      final byAlias =
          a.peer.alias.toLowerCase().compareTo(b.peer.alias.toLowerCase());
      return byAlias != 0 ? byAlias : a.peer.id.compareTo(b.peer.id);
    });
    if (!mounted) return;
    if (items.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(const SnackBar(
          content: Text('地址簿是空的（个人和共享地址簿里都没有设备），先手动填设备 ID')));
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
                for (final item in items)
                  ListTile(
                    dense: true,
                    leading:
                        const Icon(Icons.desktop_windows_outlined, size: 20),
                    title: Text(
                      item.peer.alias.isNotEmpty ? item.peer.alias : item.peer.id,
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                    ),
                    subtitle: Text(
                      item.shared
                          ? '${item.peer.id} · ${item.book}'
                          : item.peer.id,
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                    ),
                    trailing: item.shared
                        ? const Text('共享',
                            style: TextStyle(fontSize: 11, color: Colors.grey))
                        : null,
                    onTap: () =>
                        Navigator.of(dialogContext).pop(item.peer.id),
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
    setState(() {
      _peers.removeWhere((peer) => peer.id == id);
      _selected.remove(id);
      if (_peers.isEmpty) _selectMode = false;
    });
    await _savePeers();
  }

  bool get _allSelected =>
      _peers.isNotEmpty && _selected.length == _peers.length;

  // ---- 画面墙群控 ----
  //
  // 墙上先勾选若干格作为「被控」，再把其中一格点成「主控」；主控格接管键鼠，
  // 输入会复制给其它被控格。目标集合交给 SyncController，与全局同步开关无关。

  bool _groupControl = false;
  String? _masterId;
  final Map<String, SyncMember> _tileSessions = <String, SyncMember>{};

  void _enterGroupControl() {
    setState(() {
      _groupControl = true;
      _selectMode = true;
      _masterId = null;
      _selected.clear();
    });
    _syncWallTargets();
  }

  void _exitGroupControl() {
    _sync.clearWallGroup();
    setState(() {
      _groupControl = false;
      _masterId = null;
      _selectMode = false;
      _selected.clear();
    });
  }

  void _setMaster(String peerId) {
    setState(() {
      _masterId = peerId;
      _selected.add(peerId);
    });
    _syncWallTargets();
  }

  /// 某一格连上后回报会话 id 与远程分辨率，群控要用它做坐标换算。
  void _onTileSession(String peerId, UuidValue sessionId, int width, int height) {
    _tileSessions[peerId] = SyncMember(sessionId, width: width, height: height);
    _syncWallTargets();
  }

  /// 把当前勾选的格子（排除主控）同步给 SyncController。
  void _syncWallTargets() {
    final targets = <SyncMember>[];
    for (final id in _selected) {
      if (id == _masterId) continue;
      final member = _tileSessions[id];
      if (member != null) targets.add(member);
    }
    _sync.setWallGroup(
      enabled: _groupControl && _masterId != null,
      targets: targets,
    );
  }

  void _enterSelectMode() {
    setState(() {
      _selectMode = true;
      _selected.clear();
    });
  }

  void _exitSelectMode() {
    _sync.clearWallGroup();
    setState(() {
      _selectMode = false;
      _groupControl = false;
      _masterId = null;
      _selected.clear();
    });
  }

  void _toggleSelect(String id) {
    setState(() {
      if (!_selected.remove(id)) _selected.add(id);
      if (_masterId != null && !_selected.contains(_masterId)) {
        _masterId = null;
      }
    });
    _syncWallTargets();
  }

  void _toggleSelectAll() {
    setState(() {
      if (_allSelected) {
        _selected.clear();
        _masterId = null;
      } else {
        _selected
          ..clear()
          ..addAll(_peers.map((peer) => peer.id));
      }
    });
    _syncWallTargets();
  }

  /// 批量从画面墙移除。只影响画面墙，不会去动设备本身。
  Future<void> _deleteSelected() async {
    if (_selected.isEmpty) return;
    final count = _selected.length;
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (dialogContext) => AlertDialog(
        title: const Text('批量删除'),
        content: Text('从画面墙移除 $count 台设备？\n只影响画面墙，设备本身不受影响。'),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(dialogContext).pop(false),
            child: const Text('取消'),
          ),
          TextButton(
            onPressed: () => Navigator.of(dialogContext).pop(true),
            child: const Text('删除'),
          ),
        ],
      ),
    );
    if (confirmed != true || !mounted) return;
    final removed = Set<String>.of(_selected);
    setState(() {
      _peers.removeWhere((peer) => removed.contains(peer.id));
      _selected.clear();
      if (_peers.isEmpty) _selectMode = false;
    });
    await _savePeers();
  }

  /// 把「⋮」里的设置（画面质量/分辨率/预览屏幕/密码）批量应用到选中的格子。
  Future<void> _openBatchSettings() async {
    if (_selected.isEmpty) return;
    final presetResolutions = const <String>[
      '1920x1080',
      '1600x900',
      '1366x768',
      '1280x720',
      '1024x768',
    ];
    var quality = '';
    var resolution = '';
    var displayIndex = -1;
    var setPassword = false;
    final passwordController = TextEditingController();
    final result = await showDialog<_BatchSettings>(
      context: context,
      builder: (dialogContext) => StatefulBuilder(
        builder: (dialogContext, setDialogState) => AlertDialog(
          title: Text('批量设置 ${_selected.length} 台'),
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text('画面质量',
                    style: TextStyle(fontSize: 12, color: Colors.grey)),
                const SizedBox(height: 4),
                Wrap(
                  spacing: 8,
                  runSpacing: 4,
                  children: [
                    for (final entry in const <String, String>{
                      '': '不修改',
                      kRemoteImageQualityLow: '流畅',
                      kRemoteImageQualityBalanced: '平衡',
                      kRemoteImageQualityBest: '高清',
                    }.entries)
                      ChoiceChip(
                        label: Text(entry.value),
                        selected: quality == entry.key,
                        onSelected: (_) =>
                            setDialogState(() => quality = entry.key),
                      ),
                  ],
                ),
                const SizedBox(height: 12),
                const Text('分辨率',
                    style: TextStyle(fontSize: 12, color: Colors.grey)),
                const SizedBox(height: 4),
                DropdownButton<String>(
                  isExpanded: true,
                  value: resolution,
                  items: [
                    const DropdownMenuItem<String>(
                        value: '', child: Text('不修改')),
                    for (final preset in presetResolutions)
                      DropdownMenuItem<String>(
                          value: preset, child: Text(preset)),
                  ],
                  onChanged: (value) =>
                      setDialogState(() => resolution = value ?? ''),
                ),
                const SizedBox(height: 12),
                const Text('预览屏幕',
                    style: TextStyle(fontSize: 12, color: Colors.grey)),
                const SizedBox(height: 4),
                DropdownButton<int>(
                  isExpanded: true,
                  value: displayIndex,
                  items: [
                    const DropdownMenuItem<int>(value: -1, child: Text('不修改')),
                    for (var i = 0; i < 4; i++)
                      DropdownMenuItem<int>(
                          value: i, child: Text('屏幕 ${i + 1}')),
                  ],
                  onChanged: (value) =>
                      setDialogState(() => displayIndex = value ?? -1),
                ),
                CheckboxListTile(
                  contentPadding: EdgeInsets.zero,
                  value: setPassword,
                  onChanged: (value) =>
                      setDialogState(() => setPassword = value ?? false),
                  title: const Text('统一设置密码'),
                  subtitle: const Text('留空表示不需要密码；保存后这些格子会重连',
                      style: TextStyle(fontSize: 11)),
                  controlAffinity: ListTileControlAffinity.leading,
                ),
                if (setPassword)
                  TextField(
                    controller: passwordController,
                    obscureText: true,
                    decoration: const InputDecoration(
                      labelText: '密码',
                      hintText: '留空表示清除密码',
                    ),
                  ),
              ],
            ),
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(dialogContext).pop(null),
              child: const Text('取消'),
            ),
            TextButton(
              onPressed: () {
                int? width;
                int? height;
                if (resolution.isNotEmpty) {
                  final parts = resolution.split('x');
                  if (parts.length == 2) {
                    width = int.tryParse(parts[0]);
                    height = int.tryParse(parts[1]);
                  }
                }
                Navigator.of(dialogContext).pop(
                  _BatchSettings(
                    quality: quality.isEmpty ? null : quality,
                    display: displayIndex < 0 ? null : displayIndex,
                    width: width,
                    height: height,
                    password:
                        setPassword ? passwordController.text.trim() : null,
                  ),
                );
              },
              child: const Text('应用'),
            ),
          ],
        ),
      ),
    );
    passwordController.dispose();
    if (result == null || result.isEmpty || !mounted) return;
    final targets = Set<String>.of(_selected);
    // 改密码要落盘并重建格子（新密码对新连接才有效）。
    if (result.password != null) {
      for (final id in targets) {
        await WallStore.updatePassword(id, result.password!);
      }
      await _loadPeers();
      if (!mounted) return;
      // 密码变了的格子已经换新会话，等它们连上后再下发其余设置。
      await Future<void>.delayed(const Duration(milliseconds: 600));
      if (!mounted) return;
    }
    _batchSettings.value = result;
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text('已下发设置到 ${targets.length} 台'
            '（还没连上的格子会在连上后自动应用）'),
      ),
    );
  }

  Future<void> _editPassword(WallPeerEntry peer) async {
    final controller = TextEditingController(text: peer.password);
    var obscure = true;
    final result = await showDialog<String>(
      context: context,
      builder: (dialogContext) => StatefulBuilder(
        builder: (dialogContext, setDialogState) => AlertDialog(
          title: Text('设置密码：${peer.id}'),
          content: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              TextField(
                controller: controller,
                obscureText: obscure,
                autofocus: true,
                decoration: InputDecoration(
                  labelText: '密码',
                  hintText: '留空表示这台设备不需要密码',
                  suffixIcon: IconButton(
                    tooltip: obscure ? '显示密码' : '隐藏密码',
                    icon: Icon(obscure
                        ? Icons.visibility_off_outlined
                        : Icons.visibility_outlined),
                    onPressed: () => setDialogState(() => obscure = !obscure),
                  ),
                ),
              ),
              const SizedBox(height: 8),
              const Align(
                alignment: Alignment.centerLeft,
                child: Text(
                  '保存后会重新连接这一格',
                  style: TextStyle(fontSize: 12, color: Colors.grey),
                ),
              ),
            ],
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(dialogContext).pop(null),
              child: const Text('取消'),
            ),
            TextButton(
              onPressed: () => Navigator.of(dialogContext).pop(''),
              child: const Text('清除密码'),
            ),
            TextButton(
              onPressed: () => Navigator.of(dialogContext).pop(controller.text),
              child: const Text('保存'),
            ),
          ],
        ),
      ),
    );
    controller.dispose();
    if (result == null || !mounted) return;
    final saved = await WallStore.updatePassword(peer.id, result.trim());
    if (!saved) return;
    await _loadPeers();
  }

  @override
  Widget build(BuildContext context) {
    // AppBar 背景是主题主色（蓝），TextButton 默认文字也是主色，会变成蓝字蓝底看不见，
    // 所以标题栏里的按钮统一显式指定白色文字。
    final appBarButtonStyle = TextButton.styleFrom(
      foregroundColor: Colors.white,
      disabledForegroundColor: Colors.white38,
    );
    return Scaffold(
      appBar: AppBar(
        title: const Text('画面墙'),
        actions: _selectMode
            ? [
                TextButton(
                  style: appBarButtonStyle,
                  onPressed: _toggleSelectAll,
                  child: Text(_allSelected ? '取消全选' : '全选'),
                ),
                if (_groupControl)
                  TextButton(
                    style: appBarButtonStyle,
                    onPressed: _exitGroupControl,
                    child: Text(
                      _masterId == null
                          ? '群控：点格子选被控，长按设主控'
                          : '结束群控（主控 ${_masterId!}）',
                      style: const TextStyle(color: Colors.orange),
                    ),
                  )
                else
                  TextButton(
                    style: appBarButtonStyle,
                    onPressed: _enterGroupControl,
                    child: const Text('群控'),
                  ),
                TextButton(
                  style: appBarButtonStyle,
                  onPressed: _selected.isEmpty ? null : _openBatchSettings,
                  child: Text('设置 ${_selected.length}'),
                ),
                TextButton(
                  style: appBarButtonStyle,
                  onPressed: _selected.isEmpty ? null : _deleteSelected,
                  child: Text('删除 ${_selected.length}'),
                ),
                IconButton(
                  tooltip: '退出',
                  onPressed: _exitSelectMode,
                  icon: const Icon(Icons.close),
                ),
              ]
            : [
                AnimatedBuilder(
                  animation: _sync,
                  builder: (context, _) {
                    final enabled = _sync.enabled;
                    return TextButton.icon(
                      style: appBarButtonStyle,
                      onPressed: () => _sync.toggle(),
                      icon: Icon(
                        enabled ? Icons.sync : Icons.sync_disabled,
                        size: 18,
                        color: enabled ? Colors.lightGreenAccent : null,
                      ),
                      label: Text(enabled ? '同步：开' : '同步：关'),
                    );
                  },
                ),
                IconButton(
                  tooltip: '批量操作',
                  onPressed: _enterSelectMode,
                  icon: const Icon(Icons.checklist),
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
              // 密码变了也要重建（新 key 会重新建连接）。
              key: ValueKey('${peer.id}|${peer.password}'),
              peer: peer,
              onRemove: () => _removePeer(peer.id),
              onEditPassword: () => _editPassword(peer),
              selectMode: _selectMode,
              selected: _selected.contains(peer.id),
              onToggleSelect: () => _toggleSelect(peer.id),
              batchSettings: _batchSettings,
              groupControl: _groupControl,
              isMaster: _masterId == peer.id,
              onMakeMaster: () => _setMaster(peer.id),
              onSession: _onTileSession,
            );
          },
        );
      },
    );
  }
}

/// 批量设置：一次把「⋮」里的设置应用到多个选中格子。
///
/// 每个字段为 null 表示该项不动。
class _BatchSettings {
  final String? quality;
  final int? display;
  final int? width;
  final int? height;
  final String? password;

  const _BatchSettings({
    this.quality,
    this.display,
    this.width,
    this.height,
    this.password,
  });

  bool get isEmpty =>
      quality == null && display == null && width == null && password == null;
}

class _WallTile extends StatefulWidget {
  final _WallPeer peer;
  final VoidCallback onRemove;
  final VoidCallback onEditPassword;
  final bool selectMode;
  final bool selected;
  final VoidCallback onToggleSelect;
  final ValueNotifier<_BatchSettings?> batchSettings;

  /// 群控模式：非主控格是「被控」（收输入），主控格接管键鼠。
  final bool groupControl;
  final bool isMaster;
  final VoidCallback onMakeMaster;

  /// 这一格连上后把 sessionId 与远程分辨率回报给墙（群控换算坐标要用）。
  final void Function(String peerId, UuidValue sessionId, int width, int height)
      onSession;

  const _WallTile({
    super.key,
    required this.peer,
    required this.onRemove,
    required this.onEditPassword,
    this.selectMode = false,
    this.selected = false,
    required this.onToggleSelect,
    required this.batchSettings,
    this.groupControl = false,
    this.isMaster = false,
    required this.onMakeMaster,
    required this.onSession,
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

  /// 这一格预览的是对端的哪块屏幕（多屏设备可以切换）。
  int _display = 0;

  /// 当前画面质量（low / balanced / best），用于菜单打勾。
  String _quality = '';

  /// 还没连上时收到的批量设置，等这一格出画面后再下发。
  _BatchSettings? _pendingBatch;

  /// 主控格用它接管键盘焦点。
  final FocusNode _keyFocusNode = FocusNode(debugLabel: 'wall-master');

  /// 把会话 id 与当前预览屏的分辨率回报给墙，群控换算鼠标坐标要用。
  void _reportSession() {
    final ffi = _ffi;
    if (ffi == null) return;
    widget.onSession(
      widget.peer.id,
      ffi.sessionId,
      _displayWidth(ffi),
      _displayHeight(ffi),
    );
  }

  @override
  void initState() {
    super.initState();
    widget.batchSettings.addListener(_onBatchSettings);
    final pending = widget.batchSettings.value;
    if (pending != null && widget.selected) _pendingBatch = pending;
    _connect();
  }

  @override
  void didUpdateWidget(covariant _WallTile oldWidget) {
    super.didUpdateWidget(oldWidget);
    // 刚被设成主控：把键盘焦点拿过来，否则按键进不了远程会话。
    if (widget.groupControl && widget.isMaster && !oldWidget.isMaster) {
      WidgetsBinding.instance.addPostFrameCallback((_) {
        if (mounted) _keyFocusNode.requestFocus();
      });
    }
  }

  /// 页面下发了批量设置：只有被选中的格子执行。
  void _onBatchSettings() {
    final settings = widget.batchSettings.value;
    if (settings == null || !widget.selected) return;
    if (_gotFirstImage) {
      unawaited(_applyBatchSettings(settings));
    } else {
      // 还没出画面：会话可能还没建好，先记下来，等第一帧到了再下发。
      _pendingBatch = settings;
    }
  }

  Future<void> _applyBatchSettings(_BatchSettings settings) async {
    final ffi = _ffi;
    if (ffi == null || _error != null || !mounted) return;
    try {
      if (settings.quality != null) {
        await bind.sessionSetImageQuality(
          sessionId: ffi.sessionId,
          value: settings.quality!,
        );
        if (mounted) setState(() => _quality = settings.quality!);
      }
      final display = settings.display;
      if (display != null && display < _displayCount(ffi)) {
        if (!mounted) return;
        setState(() => _display = display);
        await bind.sessionSwitchDisplay(
          isDesktop: true,
          sessionId: ffi.sessionId,
          value: Int32List.fromList([display]),
        );
      }
      final width = settings.width;
      final height = settings.height;
      if (width != null && height != null) {
        var target = ffi.ffiModel.pi.currentDisplay;
        if (target == kAllDisplayValue || target < 0) target = 0;
        await bind.sessionChangeResolution(
          sessionId: ffi.sessionId,
          display: target,
          width: width,
          height: height,
        );
      }
    } catch (_) {}
  }

  void _connect() {
    _timeoutTimer?.cancel();
    _pendingFailTimer?.cancel();
    _pendingError = null;
    _display = 0;
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
      // 会话真正连上后再读质量、下发还没应用的批量设置。
      _reportSession();
      unawaited(_loadQuality());
      final pending = _pendingBatch;
      _pendingBatch = null;
      if (pending != null) unawaited(_applyBatchSettings(pending));
    });
    ffi.start(
      peerId,
      password: widget.peer.password.isEmpty ? null : widget.peer.password,
    );
    ffi.ffiModel.updateEventListener(ffi.sessionId, peerId);
    unawaited(_loadQuality());
    _timeoutTimer = Timer(_kConnectTimeout, () {
      if (!mounted || _gotFirstImage) return;
      _failWith(widget.peer.password.isEmpty
          ? '连接超时：对方可能设了密码，或当前不可达'
          : '连接超时：密码可能不对，或当前不可达');
    });
  }

  /// 读取当前画面质量，供「⋮ → 画面质量」打勾。
  Future<void> _loadQuality() async {
    final ffi = _ffi;
    if (ffi == null) return;
    try {
      final value = await bind.sessionGetImageQuality(sessionId: ffi.sessionId);
      if (!mounted || _ffi != ffi) return;
      final quality = value ?? '';
      if (quality != _quality) setState(() => _quality = quality);
    } catch (_) {}
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
    widget.batchSettings.removeListener(_onBatchSettings);
    _keyFocusNode.dispose();
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
    final card = Card(
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
              child: Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  if (widget.selectMode)
                    Padding(
                      padding: const EdgeInsets.only(right: 4),
                      child: Icon(
                        widget.selected
                            ? Icons.check_circle
                            : Icons.radio_button_unchecked,
                        size: 14,
                        color: widget.selected
                            ? Colors.lightBlueAccent
                            : Colors.white70,
                      ),
                    ),
                  Text(
                    widget.peer.id,
                    style: const TextStyle(color: Colors.white, fontSize: 11),
                  ),
                ],
              ),
            ),
          ),
          if (widget.groupControl)
            Positioned(
              left: 6,
              bottom: 6,
              child: Container(
                padding:
                    const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
                decoration: BoxDecoration(
                  color: widget.isMaster ? Colors.orange : Colors.black54,
                  borderRadius: BorderRadius.circular(4),
                ),
                child: Text(
                  widget.isMaster ? '主控' : '被控',
                  style: const TextStyle(color: Colors.white, fontSize: 11),
                ),
              ),
            ),
          if (!widget.selectMode)
            Positioned(
              right: 0,
              top: 0,
              child: Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  _buildMenu(),
                  IconButton(
                    tooltip: '从画面墙移除',
                    iconSize: 16,
                    color: Colors.white70,
                    onPressed: widget.onRemove,
                    icon: const Icon(Icons.close),
                  ),
                ],
              ),
            ),
        ],
      ),
    );
    if (!widget.selectMode) return card;
    if (widget.groupControl) {
      // 群控：点一下选中/取消被控；长按把这格设成主控。
      if (widget.isMaster) return card;
      return GestureDetector(
        behavior: HitTestBehavior.opaque,
        onTap: widget.onToggleSelect,
        onLongPress: widget.onMakeMaster,
        child: card,
      );
    }
    return GestureDetector(
      behavior: HitTestBehavior.opaque,
      onTap: widget.onToggleSelect,
      child: card,
    );
  }

  /// 每格右上角的「⋮」菜单：密码、分辨率、预览屏幕、重连。
  Widget _buildMenu() {
    final ffi = _ffi;
    PeerInfo? pi;
    if (ffi != null && _error == null) {
      try {
        pi = ffi.ffiModel.pi;
      } catch (_) {}
    }
    final resolutions = <Resolution>[];
    if (pi != null) {
      try {
        resolutions.addAll(pi.resolutions.take(8));
      } catch (_) {}
    }
    final displayCount = pi == null ? 1 : _displayCount(ffi!);

    final items = <PopupMenuEntry<String>>[
      const PopupMenuItem<String>(
        value: 'password',
        child: Text('设置密码'),
      ),
    ];
    if (resolutions.isNotEmpty) {
      items.add(const PopupMenuDivider());
      items.add(const PopupMenuItem<String>(
        enabled: false,
        child: Text('分辨率', style: TextStyle(fontSize: 12)),
      ));
      for (final r in resolutions) {
        items.add(PopupMenuItem<String>(
          value: 'res:${r.width}x${r.height}',
          child: Text('${r.width}x${r.height}'),
        ));
      }
    }
    if (displayCount > 1) {
      items.add(const PopupMenuDivider());
      items.add(const PopupMenuItem<String>(
        enabled: false,
        child: Text('预览屏幕', style: TextStyle(fontSize: 12)),
      ));
      for (var i = 0; i < displayCount; i++) {
        final current = i == _displayIndex(ffi!, displayCount);
        items.add(PopupMenuItem<String>(
          value: 'display:$i',
          child: Text(current ? '屏幕 ${i + 1} ✓' : '屏幕 ${i + 1}'),
        ));
      }
    }
    items.add(const PopupMenuDivider());
    items.add(const PopupMenuItem<String>(
      enabled: false,
      child: Text('画面质量', style: TextStyle(fontSize: 12)),
    ));
    for (final entry in const <String, String>{
      kRemoteImageQualityLow: '流畅',
      kRemoteImageQualityBalanced: '平衡',
      kRemoteImageQualityBest: '高清',
    }.entries) {
      items.add(PopupMenuItem<String>(
        value: 'quality:${entry.key}',
        child: Text(_quality == entry.key ? '${entry.value} ✓' : entry.value),
      ));
    }
    items.add(const PopupMenuDivider());
    items.add(const PopupMenuItem<String>(
      value: 'retry',
      child: Text('重新连接'),
    ));
    items.add(const PopupMenuDivider());
    items.add(const PopupMenuItem<String>(
      value: 'remove',
      child: Text('从画面墙移除'),
    ));

    return PopupMenuButton<String>(
      tooltip: '更多操作',
      padding: EdgeInsets.zero,
      iconSize: 16,
      icon: const Icon(Icons.more_vert, color: Colors.white70),
      onSelected: _onMenuSelected,
      itemBuilder: (context) => items,
    );
  }

  Future<void> _onMenuSelected(String value) async {
    final ffi = _ffi;
    if (value == 'password') {
      widget.onEditPassword();
      return;
    }
    if (value == 'retry') {
      await _retry();
      return;
    }
    if (value == 'remove') {
      widget.onRemove();
      return;
    }
    if (ffi == null) return;
    if (value.startsWith('quality:')) {
      final quality = value.substring('quality:'.length);
      try {
        await bind.sessionSetImageQuality(
          sessionId: ffi.sessionId,
          value: quality,
        );
        if (mounted) setState(() => _quality = quality);
      } catch (_) {}
      return;
    }
    if (value.startsWith('display:')) {
      final index = int.tryParse(value.substring('display:'.length)) ?? 0;
      setState(() => _display = index);
      _reportSession();
      try {
        await bind.sessionSwitchDisplay(
          isDesktop: true,
          sessionId: ffi.sessionId,
          value: Int32List.fromList([index]),
        );
      } catch (_) {}
      return;
    }
    if (value.startsWith('res:')) {
      final parts = value.substring('res:'.length).split('x');
      if (parts.length != 2) return;
      final width = int.tryParse(parts[0]);
      final height = int.tryParse(parts[1]);
      if (width == null || height == null) return;
      final pi = ffi.ffiModel.pi;
      var display = pi.currentDisplay;
      if (display == kAllDisplayValue || display < 0) display = 0;
      try {
        await bind.sessionChangeResolution(
          sessionId: ffi.sessionId,
          display: display,
          width: width,
          height: height,
        );
      } catch (_) {}
      return;
    }
  }

  /// 主控格：给画面套上输入层，键鼠事件走本格会话的 `InputModel`。
  ///
  /// `InputModel` 在发按键/鼠标时会把事件复制给同步组成员，而画面墙群控已经把
  /// 被控格登记成成员，所以这里不需要再单独写一遍广播逻辑。
  /// 输入层必须放在按远程分辨率撑开的 `SizedBox` 内部，指针坐标才等于远程像素坐标。
  Widget _buildInputRegion(FFI ffi, Widget child) {
    if (!widget.groupControl || !widget.isMaster) return child;
    return RawKeyFocusScope(
      focusNode: _keyFocusNode,
      onFocusChange: (focused) => ffi.inputModel.enterOrLeave(focused),
      inputModel: ffi.inputModel,
      child: RawTouchGestureDetectorRegion(
        ffi: ffi,
        isCamera: false,
        child: RawPointerMouseRegion(
          inputModel: ffi.inputModel,
          child: child,
        ),
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
          child: _buildInputRegion(
            ffi,
            Obx(() {
              final textureId = ffi.textureModel
                  .getTextureId(_displayIndex(ffi, _displayCount(ffi)))
                  .value;
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
    var index = _display;
    if (index < 0 || index >= length) {
      index = ffi.ffiModel.pi.currentDisplay;
    }
    if (index < 0 || index >= length) index = 0;
    return index;
  }

  int _displayCount(FFI ffi) {
    try {
      final count = ffi.ffiModel.pi.displays.length;
      return count > 0 ? count : 1;
    } catch (_) {
      return 1;
    }
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
