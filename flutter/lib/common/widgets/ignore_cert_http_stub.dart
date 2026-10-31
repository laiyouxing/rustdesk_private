import 'package:http/http.dart' as http;

/// Web 端：浏览器环境由用户/浏览器负责证书信任，
/// 直接使用默认 http 请求即可。
Future<http.Response> getHttpIgnoreCert(
  Uri uri,
  Map<String, String> headers, {
  Duration? timeout,
}) async {
  final future = http.get(uri, headers: headers);
  return timeout != null ? await future.timeout(timeout) : await future;
}
