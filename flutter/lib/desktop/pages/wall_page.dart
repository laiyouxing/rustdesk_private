import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:desktop_multi_window/desktop_multi_window.dart';
import 'package:flutter/material.dart';
import 'package:flutter_hbb/common.dart';
import 'package:flutter_hbb/consts.dart';
import 'package:flutter_hbb/common/shared_state.dart';
import 'package:flutter_hbb/common/widgets/remote_input.dart';
import 'package:flutter_hbb/models/ab_model.dart';
import 'package:flutter_hbb/models/model.dart';
import 'package:flutter_hbb/models/peer_model.dart';
import 'package:flutter_hbb/models/platform_model.dart';
import 'package:flutter_hbb/models/sync_controller.dart';
import 'package:flutter_hbb/models/wall_store.dart';
import 'package:flutter_hbb/main.dart';
import 'package:flutter_hbb/utils/multi_window_manager.dart';
import 'package:get/get.dart';
import 'package:uuid/uuid.dart';

/// 单个画面墙格子的连接超时。
///
/// 不要设得太短：这套网络里中继协商 + LAN 测试 + api-server 打洞回退经常要
/// 10~16 秒才拿到第一帧（日志里 relay 请求到 handle_peer_info 实测 ~16s）。
/// 12 秒就判失败会把本来能连上的会话掐掉，表现成「密码不对/对方不可达」。
const Duration _kConnectTimeout = Duration(seconds: 45);

/// 连接偏慢时先给一个「还在连接」的提示，但不判失败、不断会话。
const Duration _kSlowHintAfter = Duration(seconds: 8);

/// 收到错误的宽限期：这段时间内还没有第一帧，才把这一格标记为连接失败。
///
/// Rust 侧首连失败会自己重连（打洞/换中继），这条网络里常见要 10 秒以上才通，
/// 4 秒的宽限期会把还在重试的会话提前判死，所以放宽到 15 秒。
/// 密码类错误不适用宽限期——密码不对重试多少次也没用，直接提示去设置密码。
const Duration kErrorGrace = Duration(seconds: 15);

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

  /// 关窗只做一次（× 与「关闭」入口可能都会触发）。
  bool _closing = false;

  /// 多选（批量操作）模式：格子上出现勾选圈，点格子选中/取消。
  bool _selectMode = false;
  final Set<String> _selected = <String>{};

  /// 批量设置：页面写、格子读（监听后按需应用）。
  final ValueNotifier<_BatchSettings?> _batchSettings =
      ValueNotifier<_BatchSettings?>(null);

  @override
  void initState() {
    super.initState();
    // 多窗口的标题栏 × 会被 preventClose 拦住，必须自己监听 close 事件再关窗。
    // 事件走 desktop_multi_window 的 windowEventChannel，只有设置过 method handler
    // 才会把 onEvent 分发给 MultiWindowListener（其他子窗口的标签页里都会设）。
    DesktopMultiWindow.addListener(this);
    rustDeskWinManager.setMethodHandler((call, fromWindowId) async {});
    // 地址簿里带着设备别名，加载完再刷一次列表，格子上才是「别名：ID」。
    unawaited(_ensureAddressBookLoaded().then((_) {
      if (mounted) unawaited(_loadPeers());
    }));
    _loadPeers();
  }

  /// 画面墙跑在独立进程里，地址簿本来只在主窗口初始化，这里补一次。
  ///
  /// 先读本地缓存（主窗口同步过的地址簿都在里面）；缓存也没有、且已登录时再拉一次服务端。
  Future<void> _ensureAddressBookLoaded() async {
    try {
      if (gFFI.abModel.addressbooks.isEmpty) {
        await gFFI.abModel.loadCache();
      }
      if (gFFI.abModel.addressbooks.isEmpty) {
        gFFI.userModel.refreshCurrentUser();
        if (gFFI.userModel.isLogin) {
          await gFFI.abModel
              .pullAb(force: ForcePullAb.listAndCurrent, quiet: true);
        }
      }
    } catch (e) {
      debugPrint('wall: load address book failed: $e');
    }
  }

  @override
  void onWindowClose() {
    _closeWallWindow();
  }

  /// 关窗。
  ///
  /// 关键事实（读 desktop_multi_window / window_manager fork 源码确认）：
  ///   - 子窗口是**同一个进程**里的新 Flutter 引擎，`main.dart` 对每个子窗口都
  ///     `setPreventClose(true)`，点 × 时原生只发一条 `close` 事件给 Dart；
  ///   - 原生 WM_CLOSE 里先 `EmitEvent("close")`、再判断 preventClose，
  ///     所以收到事件后把 preventClose 置 false 再 `close()`，窗口就会真的销毁；
  ///   - `WindowController` 走 desktop_multi_window 自己的通道（子引擎一定注册了插件），
  ///     不像 window_manager 那样要先用 `ensureInitialized()` 记录窗口句柄。
  /// 注意：**不能**在这里 `exit(0)`——子窗口和主窗口是同一个进程，
  /// 退出进程会把整个 RustDesk 一起结束。
  void _closeWallWindow() {
    if (_closing) return;
    _closing = true;
    try {
      _sync.clearWallGroup();
    } catch (_) {}
    // 先把每格已建立的远程会话关掉，避免窗口销毁后连接还挂在服务器上。
    for (final member in _tileSessions.values) {
      try {
        unawaited(bind.sessionClose(sessionId: member.sessionId));
      } catch (_) {}
    }
    unawaited(_closeWindowController());
  }

  Future<void> _closeWindowController() async {
    try {
      final controller = WindowController.fromWindowId(kWindowId!);
      await controller
          .setPreventClose(false)
          .timeout(const Duration(milliseconds: 300));
      await controller.close().timeout(const Duration(milliseconds: 300));
    } catch (e) {
      debugPrint('wall: close window failed: $e');
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
    var peers = await WallStore.load();
    // 地址簿是最新的别名来源：墙上存的别名可能过期，进来时顺手刷新。
    final aliases = _aliasesFromAddressBook();
    if (aliases.isNotEmpty) {
      if (await WallStore.syncAliases(aliases)) {
        peers = await WallStore.load();
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

  /// 从所有地址簿（含别人共享的）收集「设备 ID → 别名」。
  Map<String, String> _aliasesFromAddressBook() {
    final aliases = <String, String>{};
    try {
      for (final ab in gFFI.abModel.addressbooks.values) {
        for (final peer in ab.peers) {
          if (peer.id.isEmpty || peer.alias.isEmpty) continue;
          aliases[peer.id] = peer.alias;
        }
      }
    } catch (_) {}
    return aliases;
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
    // 画面墙是独立进程，地址簿可能还没加载过，点之前再补一次。
    await _ensureAddressBookLoaded();
    if (!mounted) return;
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
    // 画面墙在独立进程里，在线状态不会被周期刷新，这里主动查一次再弹框。
    try {
      await bind.queryOnlines(ids: items.map((e) => e.peer.id).toList());
      await Future.delayed(const Duration(milliseconds: 700));
    } catch (_) {}
    // 别名在前，其次按 ID，方便在长列表里找；在线的排前面。
    items.sort((a, b) {
      if (a.peer.online != b.peer.online) return a.peer.online ? -1 : 1;
      final byAlias =
          a.peer.alias.toLowerCase().compareTo(b.peer.alias.toLowerCase());
      return byAlias != 0 ? byAlias : a.peer.id.compareTo(b.peer.id);
    });
    if (!mounted) return;
    if (items.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('地址簿是空的（个人和共享地址簿里都没有设备），先手动填设备 ID')));
      return;
    }
    final selected = await showDialog<String>(
      context: context,
      builder: (dialogContext) => SimpleDialog(
        title: Text(
            '从地址簿选择（在线 ${items.where((i) => i.peer.online).length}/${items.length}）'),
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
                      item.peer.alias.isNotEmpty
                          ? item.peer.alias
                          : item.peer.id,
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
                    trailing: Row(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        if (item.shared)
                          const Text('共享',
                              style:
                                  TextStyle(fontSize: 11, color: Colors.grey)),
                        const SizedBox(width: 8),
                        Tooltip(
                          message: item.peer.online ? '在线' : '离线',
                          child: CircleAvatar(
                            radius: 4,
                            backgroundColor:
                                item.peer.online ? Colors.green : Colors.grey,
                          ),
                        ),
                      ],
                    ),
                    onTap: () => Navigator.of(dialogContext).pop(item.peer.id),
                  ),
              ],
            ),
          ),
        ],
      ),
    );
    if (selected == null || !mounted) return;
    // 从地址簿选的时候顺便把别名记下来，格子上就能显示「别名：ID」。
    final err = await WallStore.add(
      selected,
      alias: _aliasesFromAddressBook()[selected] ?? '',
    );
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
      _tileSessions.remove(id);
      if (_masterId == id) _masterId = null;
      if (_peers.isEmpty) {
        _selectMode = false;
        _groupControl = false;
      }
    });
    _syncWallTargets();
    await _savePeers();
  }

  bool get _allSelected =>
      _peers.isNotEmpty && _selected.length == _peers.length;

  // ---- 画面墙群控 ----
  //
  // 进入群控后，点某一格就把它设成「主控」；**其余已连上的格子自动成为被控**，
  // 主控格接管键鼠，输入复制给它们。不需要再手动勾选被控（少一步，也不会出现
  // 「选了主控却没有被控」的空转）。目标集合交给 SyncController，与全局同步开关无关。

  bool _groupControl = false;
  String? _masterId;
  final Map<String, SyncMember> _tileSessions = <String, SyncMember>{};

  void _enterGroupControl() {
    setState(() {
      _groupControl = true;
      _selectMode = false;
      _selected.clear();
      _masterId = null;
    });
    _syncWallTargets();
  }

  void _exitGroupControl() {
    _sync.clearWallGroup();
    setState(() {
      _groupControl = false;
      _masterId = null;
    });
  }

  /// 群控里点一格：设为主控；再点同一格取消主控（回到「选主控」状态）。
  void _toggleMaster(String peerId) {
    setState(() {
      _masterId = _masterId == peerId ? null : peerId;
    });
    _syncWallTargets();
  }

  /// 当前会被广播的格子数（已连上、且不是主控）。
  int get _slaveCount {
    var count = 0;
    for (final peer in _peers) {
      if (peer.id == _masterId) continue;
      if (_tileSessions.containsKey(peer.id)) count++;
    }
    return count;
  }

  /// 某一格连上后回报会话 id 与远程分辨率，群控要用它做坐标换算。
  void _onTileSession(
      String peerId, UuidValue sessionId, int width, int height) {
    _tileSessions[peerId] = SyncMember(sessionId, width: width, height: height);
    _syncWallTargets();
  }

  /// 把「除主控外的全部已连上格子」同步给 SyncController。
  void _syncWallTargets() {
    final targets = <SyncMember>[];
    if (_groupControl && _masterId != null) {
      for (final peer in _peers) {
        if (peer.id == _masterId) continue;
        final member = _tileSessions[peer.id];
        if (member != null) targets.add(member);
      }
    }
    _sync.setWallGroup(
      enabled: _groupControl && _masterId != null && targets.isNotEmpty,
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
    });
    _syncWallTargets();
  }

  void _toggleSelectAll() {
    setState(() {
      if (_allSelected) {
        _selected.clear();
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
        // 三种模式互斥，标题栏只显示当前模式该有的按钮：
        // 普通（群控 / 批量操作 / 添加）→ 群控（状态 + 结束）→ 多选（全选 / 设置 / 删除 / 退出）。
        actions: _groupControl
            ? _buildGroupControlActions()
            : _selectMode
                ? _buildSelectActions(appBarButtonStyle)
                : _buildNormalActions(appBarButtonStyle),
      ),
      body: !_loaded
          ? const Center(child: CircularProgressIndicator())
          : _peers.isEmpty
              ? _buildEmpty()
              : _buildGrid(),
    );
  }

  /// 普通模式：只留三个入口。
  ///
  /// 这里**不再放全局「同步」开关**：那个开关是给普通远程窗口之间互相同步用的，
  /// 画面墙有独立的群控，两个开关并排只会让人分不清谁在生效。
  List<Widget> _buildNormalActions(ButtonStyle style) => [
        TextButton(
          style: style,
          onPressed: _enterGroupControl,
          child: const Text('群控'),
        ),
        IconButton(
          tooltip: '批量操作（多选后可设置 / 删除）',
          onPressed: _enterSelectMode,
          icon: const Icon(Icons.checklist),
        ),
        IconButton(
          tooltip: '添加设备',
          onPressed: _addPeer,
          icon: const Icon(Icons.add),
        ),
      ];

  /// 群控模式：左侧只放状态文字（主控 / 被控数量），右侧只有「结束群控」。
  List<Widget> _buildGroupControlActions() => [
        Center(
          child: ConstrainedBox(
            constraints: const BoxConstraints(maxWidth: 460),
            child: Text(
              _masterId == null
                  ? '群控：点一格当主控，其余格子自动被控'
                  : '群控中：主控 $_masterId · 被控 $_slaveCount 台'
                      '${_slaveCount == 0 ? '（等其它格子连上）' : ''}',
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
              style: const TextStyle(color: Colors.orange, fontSize: 13),
            ),
          ),
        ),
        const SizedBox(width: 8),
        TextButton(
          style: TextButton.styleFrom(foregroundColor: Colors.white),
          onPressed: _exitGroupControl,
          child: const Text('结束群控'),
        ),
      ];

  /// 多选（批量操作）模式：全选 → 设置 → 删除 → 退出。
  List<Widget> _buildSelectActions(ButtonStyle style) => [
        TextButton(
          style: style,
          onPressed: _toggleSelectAll,
          child: Text(_allSelected ? '取消全选' : '全选'),
        ),
        TextButton(
          style: style,
          onPressed: _selected.isEmpty ? null : _openBatchSettings,
          child: Text('设置 ${_selected.length}'),
        ),
        TextButton(
          style: style,
          onPressed: _selected.isEmpty ? null : _deleteSelected,
          child: Text('删除 ${_selected.length}'),
        ),
        IconButton(
          tooltip: '退出多选',
          onPressed: _exitSelectMode,
          icon: const Icon(Icons.close),
        ),
      ];

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

  /// 网格布局：普通模式每格一样大；群控时主控格占 2×2（列数不够时退回 1×1）。
  ///
  /// 用 Stack + 绝对定位自己排，是因为 `GridView` 不支持跨格；这样主控格能真正
  /// 放大到四格大小，方便在里面操作键鼠。
  Widget _buildGrid() {
    return LayoutBuilder(
      builder: (context, constraints) {
        const gap = 12.0;
        const padding = 12.0;
        final available = constraints.maxWidth - padding * 2;
        if (available <= 0) return const SizedBox.shrink();
        final columns = (available / 360).floor().clamp(1, 4);
        final cellWidth = (available - gap * (columns - 1)) / columns;
        final cellHeight = cellWidth * 10 / 16;
        final placements = _placeTiles(columns);
        var rows = 1;
        for (final placement in placements) {
          final bottom = placement.row + placement.rowSpan;
          if (bottom > rows) rows = bottom;
        }
        final contentHeight = rows * cellHeight + (rows - 1) * gap;
        return SingleChildScrollView(
          padding: const EdgeInsets.all(padding),
          child: SizedBox(
            height: contentHeight,
            child: Stack(
              children: [
                for (final placement in placements)
                  Positioned(
                    left: placement.col * (cellWidth + gap),
                    top: placement.row * (cellHeight + gap),
                    width: placement.colSpan * cellWidth +
                        (placement.colSpan - 1) * gap,
                    height: placement.rowSpan * cellHeight +
                        (placement.rowSpan - 1) * gap,
                    child: _buildTile(placement.peer),
                  ),
              ],
            ),
          ),
        );
      },
    );
  }

  /// 行优先排布：主控占 2×2 放在左上角，其它格子依次填剩下的空格。
  List<_WallPlacement> _placeTiles(int columns) {
    final placements = <_WallPlacement>[];
    final occupied = <String>{};
    bool free(int row, int col) => !occupied.contains('$row,$col');
    void take(int row, int col, int rowSpan, int colSpan) {
      for (var dr = 0; dr < rowSpan; dr++) {
        for (var dc = 0; dc < colSpan; dc++) {
          occupied.add('${row + dr},${col + dc}');
        }
      }
    }

    _WallPeer? master;
    if (_groupControl && _masterId != null) {
      for (final peer in _peers) {
        if (peer.id == _masterId) {
          master = peer;
          break;
        }
      }
    }
    if (master != null) {
      final span = columns >= 2 ? 2 : 1;
      placements
          .add(_WallPlacement(master, 0, 0, colSpan: span, rowSpan: span));
      take(0, 0, span, span);
    }

    var cursorRow = 0;
    var cursorCol = 0;
    for (final peer in _peers) {
      if (master != null && identical(peer, master)) continue;
      while (true) {
        if (cursorCol >= columns) {
          cursorCol = 0;
          cursorRow++;
        }
        if (free(cursorRow, cursorCol)) break;
        cursorCol++;
      }
      placements.add(_WallPlacement(peer, cursorCol, cursorRow));
      take(cursorRow, cursorCol, 1, 1);
      cursorCol++;
    }
    return placements;
  }

  Widget _buildTile(_WallPeer peer) {
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
      isMaster: _groupControl && _masterId == peer.id,
      onMakeMaster: () => _toggleMaster(peer.id),
      onSession: _onTileSession,
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
      quality == null &&
      display == null &&
      width == null &&
      height == null &&
      password == null;
}

class _WallPlacement {
  final _WallPeer peer;
  final int col;
  final int row;
  final int colSpan;
  final int rowSpan;

  const _WallPlacement(this.peer, this.col, this.row,
      {this.colSpan = 1, this.rowSpan = 1});
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
  Timer? _slowTimer;
  Timer? _pendingFailTimer;
  String? _pendingError;
  String? _error;
  bool _gotFirstImage = false;

  /// 连接偏慢（超过 [_kSlowHintAfter] 还没出图）：只提示，不判失败。
  bool _slow = false;

  /// 这一格预览的是对端的哪块屏幕（多屏设备可以切换）。
  int _display = 0;

  /// 当前画面质量（low / balanced / best），用于菜单打勾。
  String _quality = '';

  /// 还没连上时收到的批量设置，等这一格出画面后再下发。
  _BatchSettings? _pendingBatch;

  /// 主控格用它接管键盘焦点。
  final FocusNode _keyFocusNode = FocusNode(debugLabel: 'wall-master');

  /// 格子左上角的标题：有别名就显示「别名：ID」，没有就只显示 ID。
  String get _tileTitle {
    final alias = widget.peer.alias;
    return alias.isEmpty ? widget.peer.id : '$alias：${widget.peer.id}';
  }

  /// 把会话 id 与当前预览屏的分辨率回报给墙，群控换算鼠标坐标要用。
  ///
  /// 显示器信息还没到时不要回报：`_displayWidth/_displayHeight` 的 16×10 只是
  /// 布局占位，拿来换算会把鼠标坐标压到左上角一个点。
  void _reportSession() {
    final ffi = _ffi;
    if (ffi == null) return;
    try {
      final displays = ffi.ffiModel.pi.displays;
      if (displays.isEmpty) return;
      final index = _displayIndex(ffi, displays.length);
      final display = displays[index];
      if (display.width < 100 || display.height < 100) return;
      widget.onSession(
        widget.peer.id,
        ffi.sessionId,
        display.width,
        display.height,
      );
    } catch (_) {}
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
      _slowTimer?.cancel();
      _pendingFailTimer?.cancel();
      _pendingError = null;
      setState(() {
        _gotFirstImage = true;
        _slow = false;
        // 之前报的错（首次尝试失败等）现在已被自动重连救回来，清掉。
        _error = null;
      });
      // 会话真正连上后再读质量、下发还没应用的批量设置。
      _reportSession();
      // 显示器信息偶尔比首帧晚到，补报一次，避免群控拿不到目标分辨率。
      Timer(const Duration(seconds: 2), () {
        if (mounted && _ffi == ffi && _gotFirstImage) _reportSession();
      });
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
    _slowTimer?.cancel();
    _slow = false;
    _slowTimer = Timer(_kSlowHintAfter, () {
      if (!mounted || _gotFirstImage || _error != null) return;
      setState(() => _slow = true);
    });
    _timeoutTimer = Timer(_kConnectTimeout, () {
      if (!mounted || _gotFirstImage) return;
      _failWith(widget.peer.password.isEmpty
          ? '连接超时：设备可能不在线，或网络不通'
          : '连接超时：设备可能不在线、网络不通，或密码不对');
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
    _slowTimer?.cancel();
    _slowTimer = null;
    _slow = false;
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
    // 密码不对重试也没用：直接判失败，让用户去「⋮ → 设置密码」改。
    if (_looksLikePasswordError(message)) {
      _failWith(message);
      return;
    }
    _pendingError = message;
    _pendingFailTimer?.cancel();
    _pendingFailTimer = Timer(kErrorGrace, () {
      if (!mounted || _gotFirstImage) return;
      _failWith(_pendingError ?? message);
    });
  }

  /// 消息里带 password/密码/2FA 之类字样时，视为需要人工处理，不进宽限期。
  bool _looksLikePasswordError(String message) {
    final lower = message.toLowerCase();
    return lower.contains('password') ||
        message.contains('密码') ||
        lower.contains('2fa') ||
        lower.contains('uac');
  }

  /// 这一格连接失败/超时：只在本格显示错误，并停掉会话避免后台反复重连。
  void _failWith(String message) {
    if (!mounted || _error != null) return;
    _timeoutTimer?.cancel();
    _slowTimer?.cancel();
    _pendingFailTimer?.cancel();
    setState(() {
      _slow = false;
      _error = message;
    });
    Future<void>.delayed(const Duration(seconds: 1), () {
      if (mounted && _error != null) _disposeSession();
    });
  }

  /// 把 Rust 侧的错误整理成格子里能显示的一句话。
  String _describeErrorMessage(String title, String text) {
    final msg = text.trim().isNotEmpty ? text.trim() : title.trim();
    if (msg.isEmpty) {
      return widget.peer.password.isEmpty
          ? '连接失败：需要密码或设备不可达'
          : '连接失败：密码不对或设备不可达';
    }
    final short = msg.length > 160 ? '${msg.substring(0, 160)}…' : msg;
    final lower = short.toLowerCase();
    // Rust 侧弹的密码提示在这里被拦下来了（画面墙不弹全局对话框），补一句该去哪设。
    if (lower.contains('password') || short.contains('密码')) {
      return '$short（可点右上角 ⋮ → 设置密码）';
    }
    return short;
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
                    _tileTitle,
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
                padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
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
    // 群控模式：点非主控格就把它设成主控；主控格本身不能再包手势，
    // 否则会把点击/移动吞掉，输入进不到 InputModel。
    if (widget.groupControl) {
      if (widget.isMaster) return card;
      return GestureDetector(
        behavior: HitTestBehavior.opaque,
        onTap: widget.onMakeMaster,
        child: card,
      );
    }
    if (!widget.selectMode) return card;
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
    // 点主控格就把键盘焦点要回来：窗口刚激活、或用户先点了别的格子时，
    // 没有这一步按键会一直进不到主控会话。
    return Listener(
      onPointerDown: (_) => _keyFocusNode.requestFocus(),
      child: RawKeyFocusScope(
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
                return Center(
                  child: Column(
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      const SizedBox(
                        width: 20,
                        height: 20,
                        child: CircularProgressIndicator(strokeWidth: 2),
                      ),
                      if (_slow) ...[
                        const SizedBox(height: 8),
                        const Text(
                          '还在连接…（中继/打洞协商中）',
                          style: TextStyle(fontSize: 12),
                        ),
                      ],
                    ],
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
