/// Web 端空实现：浏览器里没有多窗口会话，同步操作不适用。
class SyncStore {
  Future<void> writeMember(String key, Map<String, dynamic> data) async {}

  Future<void> removeMember(String key) async {}

  Future<List<Map<String, dynamic>>> readMembers() async =>
      <Map<String, dynamic>>[];

  Future<void> writeFlag(String name, bool value) async {}

  Future<bool?> readFlag(String name) async => null;

  Future<void> writeString(String name, String value) async {}

  Future<String?> readString(String name) async => null;
}
