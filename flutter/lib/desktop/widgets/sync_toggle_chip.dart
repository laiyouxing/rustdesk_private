import 'package:flutter/material.dart';
import 'package:flutter_hbb/models/sync_controller.dart';
import 'package:uuid/uuid.dart';

/// 远程会话左下角的「同步操作」开关。
///
/// 打开后，本会话的键盘、鼠标按键/滚轮、相对鼠标位移会被复制到其它同样处于
/// 同步状态的会话（可能是其它窗口里的会话）。
class SyncToggleChip extends StatefulWidget {
  const SyncToggleChip({
    super.key,
    required this.sessionId,
    this.peerId = '',
  });

  final UuidValue sessionId;
  final String peerId;

  @override
  State<SyncToggleChip> createState() => _SyncToggleChipState();
}

class _SyncToggleChipState extends State<SyncToggleChip> {
  final SyncController _sync = SyncController.instance;

  @override
  void initState() {
    super.initState();
    _sync.addListener(_onSyncChanged);
    _sync.ensureInitialized();
    _sync.registerSession(widget.sessionId, peerId: widget.peerId);
  }

  @override
  void dispose() {
    _sync.removeListener(_onSyncChanged);
    _sync.unregisterSession(widget.sessionId);
    super.dispose();
  }

  void _onSyncChanged() {
    if (mounted) setState(() {});
  }

  @override
  Widget build(BuildContext context) {
    final on = _sync.enabled;
    final color = on ? const Color(0xFF4FC3F7) : Colors.white70;
    return Material(
      color: Colors.black54,
      borderRadius: BorderRadius.circular(6),
      child: InkWell(
        borderRadius: BorderRadius.circular(6),
        onTap: () => _sync.toggle(),
        child: Padding(
          padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 6),
          child: Row(
            mainAxisSize: MainAxisSize.min,
            children: [
              Icon(on ? Icons.sync : Icons.sync_disabled,
                  size: 16, color: color),
              const SizedBox(width: 6),
              Text(
                on ? '同步中' : '同步',
                style: TextStyle(fontSize: 12, color: color),
              ),
            ],
          ),
        ),
      ),
    );
  }
}
