import 'dart:convert';

import 'sync_store.dart';

/// 画面墙上的一台设备。
class WallPeerEntry {
  final String id;
  final String password;

  /// 地址簿里的设备别名（没有就留空，格子上只显示 ID）。
  final String alias;

  const WallPeerEntry(this.id, [this.password = '', this.alias = '']);

  WallPeerEntry copyWith({String? password, String? alias}) => WallPeerEntry(
        id,
        password ?? this.password,
        alias ?? this.alias,
      );

  Map<String, dynamic> toJson() =>
      {'id': id, 'password': password, 'alias': alias};

  @override
  bool operator ==(Object other) => other is WallPeerEntry && other.id == id;

  @override
  int get hashCode => id.hashCode;
}

/// 画面墙设备列表的持久化。
///
/// 存在跨窗口共享的临时目录里（和同步操作的成员文件同一个根），
/// 这样设备卡片菜单和画面墙页面能读写同一份列表。
class WallStore {
  static const String _key = 'wall_peers';

  /// 去掉复制 ID 时带出来的空格、横线、下划线。
  ///
  /// RustDesk 主界面「复制 ID」给出的是 `234 485 258` 这种分组形式，
  /// 直接连会失败，所以入库前统一清洗。
  static String normalizeId(String raw) =>
      raw.replaceAll(RegExp(r'[\s\-–—_]+'), '');

  /// RustDesk 设备 ID 是纯数字，长度 5~20 位（官方 9 位，自定义可能更长）。
  static bool isValidId(String id) => RegExp(r'^\d{5,20}$').hasMatch(id);

  static Future<List<WallPeerEntry>> load() async {
    final entries = <WallPeerEntry>[];
    String raw = '';
    try {
      raw = (await SyncStore().readString(_key)) ?? '';
    } catch (_) {}
    if (raw.isEmpty) return entries;
    var parsed = false;
    try {
      final decoded = jsonDecode(raw);
      if (decoded is List) {
        parsed = true;
        for (final item in decoded) {
          if (item is Map) {
            final id = item['id'];
            final password = item['password'];
            final alias = item['alias'];
            if (id is String) {
              final normalized = normalizeId(id);
              if (normalized.isNotEmpty) {
                entries.add(WallPeerEntry(
                  normalized,
                  password is String ? password : '',
                  alias is String ? alias : '',
                ));
              }
            }
          } else if (item is String) {
            final normalized = normalizeId(item);
            if (normalized.isNotEmpty) entries.add(WallPeerEntry(normalized));
          }
        }
      }
    } catch (_) {}
    if (!parsed) {
      // 兼容最早期用逗号分隔的格式。
      for (final part in raw.split(',')) {
        final normalized = normalizeId(part);
        if (normalized.isNotEmpty) entries.add(WallPeerEntry(normalized));
      }
    }
    final seen = <String>{};
    return [
      for (final entry in entries)
        if (seen.add(entry.id)) entry
    ];
  }

  static Future<void> save(List<WallPeerEntry> peers) async {
    final data = [for (final peer in peers) peer.toJson()];
    try {
      await SyncStore().writeString(_key, jsonEncode(data));
    } catch (_) {}
  }

  /// 添加一台设备。[rawId] 可以是 `234 485 258` 这种带空格的写法。
  ///
  /// [alias] 是地址簿里的设备别名，用来在格子上显示「别名：ID」。
  /// 返回 null 表示成功，否则返回给用户看的错误原因。
  static Future<String?> add(
    String rawId, {
    String password = '',
    String alias = '',
  }) async {
    final id = normalizeId(rawId);
    if (id.isEmpty) return '请填写设备 ID';
    if (!isValidId(id)) return '设备 ID 只能是数字（复制出来带空格也可以）';
    final peers = await load();
    if (peers.any((peer) => peer.id == id)) {
      return '这台设备已经在墙上了';
    }
    peers.add(WallPeerEntry(id, password, alias));
    await save(peers);
    return null;
  }

  /// 用地址簿里的最新别名刷新墙上的设备名（地址簿里改过名字也能跟上）。
  ///
  /// 只改别名，不动密码；返回是否有变化。
  static Future<bool> syncAliases(Map<String, String> aliases) async {
    if (aliases.isEmpty) return false;
    final peers = await load();
    var changed = false;
    for (var i = 0; i < peers.length; i++) {
      final alias = aliases[peers[i].id];
      if (alias == null || alias.isEmpty || alias == peers[i].alias) continue;
      peers[i] = peers[i].copyWith(alias: alias);
      changed = true;
    }
    if (changed) await save(peers);
    return changed;
  }

  static Future<void> remove(String id) async {
    final peers = await load();
    peers.removeWhere((peer) => peer.id == id);
    await save(peers);
  }

  /// 修改某台设备的固定密码（传空串表示清除密码）。
  ///
  /// 返回 false 表示这台设备已经不在墙上了。
  static Future<bool> updatePassword(String id, String password) async {
    final peers = await load();
    final index = peers.indexWhere((peer) => peer.id == id);
    if (index < 0) return false;
    peers[index] = peers[index].copyWith(password: password);
    await save(peers);
    return true;
  }
}
