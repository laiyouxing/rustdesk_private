import 'dart:async';
import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_hbb/common.dart';
import 'package:flutter_hbb/desktop/widgets/tabbar_widget.dart';
import 'package:flutter_hbb/models/model.dart';
import 'package:flutter_hbb/models/platform_model.dart';
import 'package:get/get.dart';

const double _kColumn1Width = 30;
const double _kColumn4Width = 100;
const double _kRowHeight = 60;
const double _kTextLeftMargin = 20;

class _PortForward {
  int localPort;
  String remoteHost;
  int remotePort;

  _PortForward.fromJson(List<dynamic> json)
      : localPort = json[0] as int,
        remoteHost = json[1] as String,
        remotePort = json[2] as int;
}

class PortForwardPage extends StatefulWidget {
  PortForwardPage({
    Key? key,
    required this.id,
    required this.password,
    required this.tabController,
    required this.isRDP,
    required this.isSharedPassword,
    this.forceRelay,
    this.connToken,
  }) : super(key: key);
  final String id;
  final String? password;
  final DesktopTabController tabController;
  final bool isRDP;
  final bool? forceRelay;
  final bool? isSharedPassword;
  final String? connToken;
  final SimpleWrapper<State<PortForwardPage>?> _lastState = SimpleWrapper(null);

  FFI get ffi => (_lastState.value! as _PortForwardPageState)._ffi;

  @override
  State<PortForwardPage> createState() {
    final state = _PortForwardPageState();
    _lastState.value = state;
    return state;
  }
}

class _PortForwardPageState extends State<PortForwardPage>
    with AutomaticKeepAliveClientMixin {
  final TextEditingController localPortController = TextEditingController();
  final TextEditingController remoteHostController = TextEditingController();
  final TextEditingController remotePortController = TextEditingController();
  RxList<_PortForward> pfs = RxList.empty(growable: true);
  /// 隧道运行状态：链路类型（直连/中继）、活跃连接数、累计流量等。
  /// 数据由 Rust 端经 main_get_common("port-forward-status") 下发。
  final Rx<Map<String, dynamic>> tunnelStat = <String, dynamic>{}.obs;
  Timer? _statTimer;
  late FFI _ffi;

  @override
  void initState() {
    super.initState();
    _ffi = FFI(null);
    _ffi.start(widget.id,
        isPortForward: true,
        password: widget.password,
        isSharedPassword: widget.isSharedPassword,
        forceRelay: widget.forceRelay,
        connToken: widget.connToken,
        isRdp: widget.isRDP);
    Get.put<FFI>(_ffi, tag: 'pf_${widget.id}');
    debugPrint("Port forward page init success with id ${widget.id}");
    // 定时刷新隧道状态（直连/中继、流量）。1 秒一次足够，开销可忽略。
    _statTimer = Timer.periodic(const Duration(seconds: 1), (_) {
      if (!mounted) return;
      refreshTunnelStatus();
    });
    refreshTunnelStatus();
    // Call onSelected in post frame callback, since we cannot guarantee that the callback will not call setState.
    WidgetsBinding.instance.addPostFrameCallback((_) {
      widget.tabController.onSelected?.call(widget.id);
    });
  }

  @override
  void dispose() {
    _statTimer?.cancel();
    _statTimer = null;
    _ffi.close();
    _ffi.dialogManager.dismissAll();
    Get.delete<FFI>(tag: 'pf_${widget.id}');
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    super.build(context);
    return Scaffold(
      backgroundColor: Theme.of(context).scaffoldBackgroundColor,
      body: FutureBuilder(future: () async {
        if (!widget.isRDP) {
          refreshTunnelConfig();
        }
      }(), builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.done) {
          return Container(
            decoration: BoxDecoration(
                border: Border.all(
                    width: 20,
                    color: Theme.of(context).scaffoldBackgroundColor)),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.stretch,
              children: [
                buildPrompt(context),
                Flexible(
                  child: Container(
                    decoration: BoxDecoration(
                        color: Theme.of(context).colorScheme.background,
                        border: Border.all(width: 1, color: MyTheme.border)),
                    child:
                        widget.isRDP ? buildRdp(context) : buildTunnel(context),
                  ),
                ),
              ],
            ),
          );
        }
        return const Offstage();
      }),
    );
  }

  buildPrompt(BuildContext context) {
    return Obx(() => Offstage(
          offstage: pfs.isEmpty && !widget.isRDP,
          child: Container(
              height: 45,
              color: const Color(0xFF007F00),
              child: Column(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: [
                    Text(
                      translate('Listening ...'),
                      style: const TextStyle(fontSize: 16, color: Colors.white),
                    ),
                    Text(
                      translate('not_close_tcp_tip'),
                      style: const TextStyle(
                          fontSize: 10, color: Color(0xFFDDDDDD), height: 1.2),
                    )
                  ])).marginOnly(bottom: 8),
        ));
  }

  buildTunnel(BuildContext context) {
    text(String label) => Expanded(
        child: Text(translate(label)).marginOnly(left: _kTextLeftMargin));

    return Theme(
      data: Theme.of(context).copyWith(
        colorScheme: Theme.of(context).colorScheme,
      ),
      child: Obx(() => Column(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: [
              buildTunnelStatus(context),
              buildTunnelHint(context),
              Expanded(
                child: ListView.builder(
                    controller: ScrollController(),
                    itemCount: pfs.length + 2,
                    itemBuilder: ((context, index) {
                      if (index == 0) {
                        return Container(
                          height: 25,
                          color: Theme.of(context).scaffoldBackgroundColor,
                          child: Row(children: [
                            text('Local Port'),
                            const SizedBox(width: _kColumn1Width),
                            text('Remote Host'),
                            text('Remote Port'),
                            SizedBox(
                                width: _kColumn4Width,
                                child: Text(translate('Action')))
                          ]),
                        );
                      } else if (index == 1) {
                        return buildTunnelAddRow(context);
                      } else {
                        return buildTunnelDataRow(
                            context, pfs[index - 2], index - 2);
                      }
                    })),
              ),
            ],
          )),
    );
  }

  /// 隧道当前状态条：链路类型（直连 / 中继）、活跃连接数、累计流量。
  ///
  /// 用户最关心的是「到底走直连还是绕了中继」和「有没有在跑」，
  /// 之前界面完全看不到这些，只能靠猜。
  Widget buildTunnelStatus(BuildContext context) {
    final s = tunnelStat.value;
    final active = (s['active'] as num?)?.toInt() ?? 0;
    final direct = s['direct'] == true;
    final secure = s['secure'] == true;
    final bytes = (s['bytes'] as num?)?.toInt() ?? 0;
    final localPort = (s['local_port'] as num?)?.toInt() ?? 0;
    final remoteHost = (s['remote_host'] as String?) ?? '';
    final remotePort = (s['remote_port'] as num?)?.toInt() ?? 0;
    final lastError = (s['last_error'] as String?) ?? '';

    final bool noConn = active == 0;
    final color = noConn
        ? Colors.grey
        : (direct ? const Color(0xFF1B8A3A) : const Color(0xFFC77700));
    final linkText = noConn ? '暂无连接' : (direct ? '直连' : '中继');
    String sizeText = '';
    if (bytes >= 1024 * 1024) {
      sizeText = '${(bytes / (1024 * 1024)).toStringAsFixed(2)} MB';
    } else if (bytes >= 1024) {
      sizeText = '${(bytes / 1024).toStringAsFixed(1)} KB';
    } else {
      sizeText = '$bytes B';
    }

    return Container(
      width: double.infinity,
      padding: const EdgeInsets.fromLTRB(_kTextLeftMargin, 8, 10, 8),
      color: Theme.of(context).scaffoldBackgroundColor,
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(children: [
            Container(
              width: 8,
              height: 8,
              decoration: BoxDecoration(color: color, shape: BoxShape.circle),
            ),
            const SizedBox(width: 6),
            Text(
              linkText,
              style: TextStyle(
                  fontSize: 13,
                  fontWeight: FontWeight.w600,
                  color: color),
            ),
            const SizedBox(width: 10),
            Text(
              noConn ? '等待连接…' : '活跃 $active 条',
              style: TextStyle(
                  fontSize: 12, color: Theme.of(context).hintColor),
            ),
            if (!noConn) ...[
              const SizedBox(width: 10),
              // 加密状态：两端都支持时隧道数据端到端加密，否则退回裸流。
              Container(
                padding:
                    const EdgeInsets.symmetric(horizontal: 6, vertical: 1),
                decoration: BoxDecoration(
                  color: secure
                      ? const Color(0xFFE6F4EA)
                      : const Color(0xFFFCE8E6),
                  borderRadius: BorderRadius.circular(4),
                ),
                child: Text(
                  secure ? '已加密' : '未加密',
                  style: TextStyle(
                      fontSize: 11,
                      color: secure
                          ? const Color(0xFF1B8A3A)
                          : const Color(0xFFB3261E)),
                ),
              ),
            ],
            const Spacer(),
            Text(
              '累计 $sizeText',
              style: TextStyle(
                  fontSize: 12, color: Theme.of(context).hintColor),
            ),
          ]),
          if (!noConn && localPort > 0) ...[
            const SizedBox(height: 3),
            Text(
              '本地 127.0.0.1:$localPort  →  $remoteHost:$remotePort',
              style: TextStyle(
                  fontSize: 11,
                  fontFamily: 'monospace',
                  color: Theme.of(context).textTheme.bodySmall?.color),
            ),
          ],
          if (lastError.isNotEmpty) ...[
            const SizedBox(height: 3),
            Text(
              '最近失败：$lastError',
              style: const TextStyle(fontSize: 11, color: Color(0xFFB3261E)),
            ),
          ],
        ],
      ),
    );
  }

  void refreshTunnelStatus() {
    try {
      final raw = bind.mainGetCommonSync(key: 'port-forward-status');
      if (raw.isNotEmpty) {
        final obj = jsonDecode(raw);
        if (obj is Map<String, dynamic>) {
          tunnelStat.value = obj;
        }
      }
    } catch (e) {
      // 老版本后端没有这个 key，忽略即可。
    }
  }

  /// 端口转发用法说明。
  ///
  /// 此前界面只有三个空输入框，用户不知道
  /// 「本地端口 / 远程主机 / 远程端口」分别该填什么。
  Widget buildTunnelHint(BuildContext context) {
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.fromLTRB(_kTextLeftMargin, 8, 10, 6),
      color: Theme.of(context).scaffoldBackgroundColor,
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(translate('PortForwardTip'),
              style: TextStyle(
                  fontSize: 12,
                  color: Theme.of(context).textTheme.bodySmall?.color)),
          const SizedBox(height: 2),
          Text(translate('PortForwardExample'),
              style:
                  TextStyle(fontSize: 12, color: Theme.of(context).hintColor)),
        ],
      ),
    );
  }

  buildTunnelAddRow(BuildContext context) {
    var portInputFormatter = [
      FilteringTextInputFormatter.allow(RegExp(
          r'^([0-9]|[1-9]\d|[1-9]\d{2}|[1-9]\d{3}|[1-5]\d{4}|6[0-4]\d{3}|65[0-4]\d{2}|655[0-2]\d|6553[0-5])$'))
    ];

    return Container(
      height: _kRowHeight,
      decoration:
          BoxDecoration(color: Theme.of(context).colorScheme.background),
      child: Row(children: [
        buildTunnelInputCell(context,
            controller: localPortController,
            inputFormatters: portInputFormatter,
            hint: translate('PortForwardLocalPortHint')),
        const SizedBox(
            width: _kColumn1Width, child: Icon(Icons.arrow_forward_sharp)),
        buildTunnelInputCell(context,
            controller: remoteHostController, hint: 'localhost'),
        buildTunnelInputCell(context,
            controller: remotePortController,
            inputFormatters: portInputFormatter,
            hint: translate('PortForwardRemotePortHint')),
        ElevatedButton(
          onPressed: () async {
            int? localPort = int.tryParse(localPortController.text);
            int? remotePort = int.tryParse(remotePortController.text);
            if (localPort != null &&
                remotePort != null &&
                (remoteHostController.text.isEmpty ||
                    remoteHostController.text.trim().isNotEmpty)) {
              await bind.sessionAddPortForward(
                  sessionId: _ffi.sessionId,
                  localPort: localPort,
                  remoteHost: remoteHostController.text.trim().isEmpty
                      ? 'localhost'
                      : remoteHostController.text.trim(),
                  remotePort: remotePort);
              localPortController.clear();
              remoteHostController.clear();
              remotePortController.clear();
              refreshTunnelConfig();
            }
          },
          child: Text(
            translate('Add'),
          ),
        ).marginSymmetric(horizontal: 10),
      ]),
    );
  }

  buildTunnelInputCell(BuildContext context,
      {required TextEditingController controller,
      List<TextInputFormatter>? inputFormatters,
      String? hint}) {
    return Expanded(
      child: Padding(
          padding: const EdgeInsets.all(10.0),
          child: TextField(
              controller: controller,
              inputFormatters: inputFormatters,
              decoration: InputDecoration(
                hintText: hint,
              )).workaroundFreezeLinuxMint()),
    );
  }

  Widget buildTunnelDataRow(BuildContext context, _PortForward pf, int index) {
    text(String label) => Expanded(
        child: Text(label, style: const TextStyle(fontSize: 20))
            .marginOnly(left: _kTextLeftMargin));

    return Container(
      height: _kRowHeight,
      decoration: BoxDecoration(
          color: index % 2 == 0
              ? MyTheme.currentThemeMode() == ThemeMode.dark
                  ? const Color(0xFF202020)
                  : const Color(0xFFF4F5F6)
              : Theme.of(context).colorScheme.background),
      child: Row(children: [
        text(pf.localPort.toString()),
        const SizedBox(width: _kColumn1Width),
        text(pf.remoteHost),
        text(pf.remotePort.toString()),
        SizedBox(
          width: _kColumn4Width,
          child: IconButton(
            icon: const Icon(Icons.close),
            onPressed: () async {
              await bind.sessionRemovePortForward(
                  sessionId: _ffi.sessionId, localPort: pf.localPort);
              refreshTunnelConfig();
            },
          ),
        ),
      ]),
    );
  }

  void refreshTunnelConfig() async {
    String peer = bind.mainGetPeerSync(id: widget.id);
    Map<String, dynamic> config = jsonDecode(peer);
    List<dynamic> infos = config['port_forwards'] as List;
    List<_PortForward> result = List.empty(growable: true);
    for (var e in infos) {
      result.add(_PortForward.fromJson(e));
    }
    pfs.value = result;
  }

  buildRdp(BuildContext context) {
    text1(String label) => Expanded(
        child: Text(translate(label)).marginOnly(left: _kTextLeftMargin));
    text2(String label) => Expanded(
            child: Text(
          label,
          style: const TextStyle(fontSize: 20),
        ).marginOnly(left: _kTextLeftMargin));
    return Theme(
      data: Theme.of(context)
          .copyWith(colorScheme: Theme.of(context).colorScheme),
      child: ListView.builder(
          controller: ScrollController(),
          itemCount: 2,
          itemBuilder: ((context, index) {
            if (index == 0) {
              return Container(
                height: 25,
                color: Theme.of(context).scaffoldBackgroundColor,
                child: Row(children: [
                  text1('Local Port'),
                  const SizedBox(width: _kColumn1Width),
                  text1('Remote Host'),
                  text1('Remote Port'),
                ]),
              );
            } else {
              return Container(
                height: _kRowHeight,
                decoration: BoxDecoration(
                    color: Theme.of(context).colorScheme.background),
                child: Row(children: [
                  Expanded(
                    child: Align(
                      alignment: Alignment.centerLeft,
                      child: SizedBox(
                        width: 120,
                        child: ElevatedButton(
                          onPressed: () =>
                              bind.sessionNewRdp(sessionId: _ffi.sessionId),
                          child: Text(
                            translate('New RDP'),
                          ),
                        ).marginSymmetric(vertical: 10),
                      ).marginOnly(left: 20),
                    ),
                  ),
                  const SizedBox(
                      width: _kColumn1Width,
                      child: Icon(Icons.arrow_forward_sharp)),
                  text2('localhost'),
                  text2('RDP'),
                ]),
              );
            }
          })),
    );
  }

  @override
  bool get wantKeepAlive => true;
}
