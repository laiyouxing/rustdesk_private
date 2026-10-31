import 'dart:io';
import 'package:http/http.dart' as http;
import 'package:http/io_client.dart';

/// 桌面/移动端：请求 api-server 时忽略自签名证书错误。
///
/// 原因：api-server 使用自签名证书，Rust 侧 http 请求通过
/// `accept_invalid_certs(true)` 接受该证书；而 Flutter 的 dart:io
/// HttpClient 默认严格校验证书，会因自签名证书 TLS 握手失败，
/// 导致订阅检查等请求被误判失败（表现为"订阅已过期"）。
///
/// 这里显式设置 `badCertificateCallback` 放行，与 Rust 侧行为保持一致。
Future<http.Response> getHttpIgnoreCert(
  Uri uri,
  Map<String, String> headers, {
  Duration? timeout,
}) async {
  final client = HttpClient()
    ..badCertificateCallback = ((cert, host, port) => true);
  try {
    final future = IOClient(client).get(uri, headers: headers);
    return timeout != null ? await future.timeout(timeout) : await future;
  } finally {
    client.close(force: true);
  }
}
