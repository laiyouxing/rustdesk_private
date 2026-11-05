import 'dart:convert';
import 'dart:io';

/// 同步操作成员的跨窗口共享存储（桌面/移动端实现）。
///
/// 目录结构（系统临时目录下）：
///
/// ```
/// rustdesk_sync_group/
///   members/<instance>.json   # 每个打开了远程会话的窗口一行，带心跳时间戳
///   enabled.json              # 全局同步开关
///   wall_peers.json           # 画面墙的设备列表
/// ```
class SyncStore {
  static const String _rootName = 'rustdesk_sync_group';

  Directory get _root => Directory(
      '${Directory.systemTemp.path}${Platform.pathSeparator}$_rootName');

  Directory get _members =>
      Directory('${_root.path}${Platform.pathSeparator}members');

  Future<void> _ensure() async {
    try {
      if (!await _members.exists()) {
        await _members.create(recursive: true);
      }
    } catch (_) {
      // 临时目录不可用时静默降级：同步功能退化为「只影响本窗口」。
    }
  }

  Future<void> writeMember(String key, Map<String, dynamic> data) async {
    await _ensure();
    try {
      final file = File('${_members.path}${Platform.pathSeparator}$key.json');
      await file.writeAsString(jsonEncode(data), flush: true);
    } catch (_) {}
  }

  Future<void> removeMember(String key) async {
    try {
      final file = File('${_members.path}${Platform.pathSeparator}$key.json');
      if (await file.exists()) {
        await file.delete();
      }
    } catch (_) {}
  }

  Future<List<Map<String, dynamic>>> readMembers() async {
    final items = <Map<String, dynamic>>[];
    try {
      if (!await _members.exists()) return items;
      await for (final entity in _members.list(followLinks: false)) {
        if (entity is! File) continue;
        if (!entity.path.endsWith('.json')) continue;
        try {
          final decoded = jsonDecode(await entity.readAsString());
          if (decoded is Map<String, dynamic>) {
            items.add(decoded);
          }
        } catch (_) {}
      }
    } catch (_) {}
    return items;
  }

  /// 清理超过 [maxAge] 没有心跳的成员文件（客户端异常退出会留下它们）。
  ///
  /// 解析失败的文件不删：可能是别的窗口正在写，删掉会丢成员。
  Future<void> sweepMembers(Duration maxAge) async {
    try {
      if (!await _members.exists()) return;
      final now = DateTime.now().millisecondsSinceEpoch;
      await for (final entity in _members.list(followLinks: false)) {
        if (entity is! File) continue;
        if (!entity.path.endsWith('.json')) continue;
        try {
          final decoded = jsonDecode(await entity.readAsString());
          final ts = decoded is Map ? decoded['ts'] : null;
          if (ts is int && now - ts > maxAge.inMilliseconds) {
            await entity.delete();
          }
        } catch (_) {}
      }
    } catch (_) {}
  }

  Future<void> writeFlag(String name, bool value) async {
    await _ensure();
    try {
      final file = File('${_root.path}${Platform.pathSeparator}$name.json');
      await file.writeAsString(jsonEncode({'value': value}));
    } catch (_) {}
  }

  Future<bool?> readFlag(String name) async {
    try {
      final file = File('${_root.path}${Platform.pathSeparator}$name.json');
      if (!await file.exists()) return null;
      final decoded = jsonDecode(await file.readAsString());
      if (decoded is Map && decoded['value'] is bool) {
        return decoded['value'] as bool;
      }
    } catch (_) {}
    return null;
  }

  Future<void> writeString(String name, String value) async {
    await _ensure();
    try {
      final file = File('${_root.path}${Platform.pathSeparator}$name.json');
      await file.writeAsString(jsonEncode({'value': value}));
    } catch (_) {}
  }

  Future<String?> readString(String name) async {
    try {
      final file = File('${_root.path}${Platform.pathSeparator}$name.json');
      if (!await file.exists()) return null;
      final decoded = jsonDecode(await file.readAsString());
      if (decoded is Map && decoded['value'] is String) {
        return decoded['value'] as String;
      }
    } catch (_) {}
    return null;
  }
}
