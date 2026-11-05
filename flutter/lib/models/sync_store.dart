// 同步操作（批量操控）的跨窗口共享存储。
//
// 桌面端每个远程会话跑在独立的 Flutter engine（多窗口）里，Dart 单例不跨窗口，
// 因此用「本机临时目录 + 心跳时间戳」互相发现：每个窗口把自己当前的 sessionId
// 写进一个文件，并每几秒刷新一次时间戳；读取方过滤掉过期的条目即可。
//
// Web 端没有多窗口，也不需要该功能，条件导出为空实现。
export 'sync_store_io.dart' if (dart.library.html) 'sync_store_web.dart';
