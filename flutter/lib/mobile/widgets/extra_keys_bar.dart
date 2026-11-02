import 'package:flutter/material.dart';
import 'package:flutter_hbb/common.dart';
import 'package:flutter_hbb/models/platform_model.dart';

/// 移动端「扩展按键条」。
///
/// 背景：手机上没有物理键盘，而原实现只有软键盘 + 一个 `Ctrl+Alt+Del` 菜单项，
/// Ctrl / Alt / Win / F1~F12 / 方向键 / 组合快捷键全都按不出来——这是"虚拟键盘难用"
/// 的根本原因。
///
/// 设计参考 UU 远程一类工具的按键条：
///  - 修饰键（Ctrl/Alt/Shift/Win）做成**可锁存的开关**：点一下 = 只对下一个键生效，
///    长按 = 锁定（再点取消），选中态高亮；
///  - 常用编辑键 / 方向键 / 翻页键常驻；
///  - F1~F12 单独一行；
///  - 一排高频组合键（复制/粘贴/撤销/切窗口/关闭窗口/显示桌面/锁屏/任务管理器/截屏）。
///
/// 所有按键都通过 `sessionInputKey` 直接下发，不经过软键盘，也不需要物理键盘。
class ExtraKeysBar extends StatefulWidget {
  const ExtraKeysBar({
    Key? key,
    required this.sessionId,
    this.onClose,
  }) : super(key: key);

  final SessionID sessionId;
  final VoidCallback? onClose;

  @override
  State<ExtraKeysBar> createState() => _ExtraKeysBarState();
}

class _ExtraKeysBarState extends State<ExtraKeysBar> {
  bool _ctrl = false;
  bool _alt = false;
  bool _shift = false;
  bool _win = false;

  /// 锁存：长按修饰键后保持按下状态，直到再次点击
  bool _ctrlLock = false;
  bool _altLock = false;
  bool _shiftLock = false;
  bool _winLock = false;

  bool get _anyModifier => _ctrl || _alt || _shift || _win;

  void _resetOneShot() {
    setState(() {
      if (!_ctrlLock) _ctrl = false;
      if (!_altLock) _alt = false;
      if (!_shiftLock) _shift = false;
      if (!_winLock) _win = false;
    });
  }

  /// 发送一个按键。[mods] 为额外的固定修饰（例如 Ctrl+C 按钮自带 ctrl）。
  void _send(String vk,
      {bool ctrl = false,
      bool alt = false,
      bool shift = false,
      bool win = false}) {
    bind.sessionInputKey(
      sessionId: widget.sessionId,
      name: vk,
      down: false,
      press: true,
      alt: alt || _alt,
      ctrl: ctrl || _ctrl,
      shift: shift || _shift,
      command: win || _win,
    );
    if (_anyModifier) _resetOneShot();
  }

  Widget _key(
    String label,
    VoidCallback onTap, {
    VoidCallback? onLongPress,
    bool active = false,
    double? width,
  }) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 2),
      child: SizedBox(
        width: width ?? 44,
        height: 40,
        child: Material(
          color: active ? MyTheme.accent : Colors.white10,
          borderRadius: BorderRadius.circular(6),
          child: InkWell(
            borderRadius: BorderRadius.circular(6),
            onTap: onTap,
            onLongPress: onLongPress,
            child: Center(
              child: Text(
                label,
                style: TextStyle(
                  color: active ? Colors.white : Colors.white70,
                  fontSize: 13,
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }

  Widget _modKey(String label, bool value, bool locked, VoidCallback onTap,
      VoidCallback onLongPress) {
    return _key(
      locked ? '$label 🔒' : label,
      onTap,
      onLongPress: onLongPress,
      active: value,
      width: locked ? 62 : 48,
    );
  }

  Widget _row(List<Widget> children) => SingleChildScrollView(
        scrollDirection: Axis.horizontal,
        padding: const EdgeInsets.symmetric(vertical: 2),
        child: Row(mainAxisSize: MainAxisSize.min, children: children),
      );

  @override
  Widget build(BuildContext context) {
    return Container(
      color: const Color(0xE6000000),
      padding: const EdgeInsets.fromLTRB(6, 4, 6, 4),
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          // 第一行：修饰键 + 编辑键 + 关闭
          _row([
            _modKey('Ctrl', _ctrl, _ctrlLock, () {
              setState(() => _ctrl = !_ctrl);
              if (!_ctrl) _ctrlLock = false;
            }, () {
              setState(() {
                _ctrlLock = !_ctrlLock;
                _ctrl = _ctrlLock;
              });
            }),
            _modKey('Alt', _alt, _altLock, () {
              setState(() => _alt = !_alt);
              if (!_alt) _altLock = false;
            }, () {
              setState(() {
                _altLock = !_altLock;
                _alt = _altLock;
              });
            }),
            _modKey('Shift', _shift, _shiftLock, () {
              setState(() => _shift = !_shift);
              if (!_shift) _shiftLock = false;
            }, () {
              setState(() {
                _shiftLock = !_shiftLock;
                _shift = _shiftLock;
              });
            }),
            _modKey('Win', _win, _winLock, () {
              setState(() => _win = !_win);
              if (!_win) _winLock = false;
            }, () {
              setState(() {
                _winLock = !_winLock;
                _win = _winLock;
              });
            }),
            _key('Esc', () => _send('VK_ESCAPE')),
            _key('Tab', () => _send('VK_TAB')),
            _key('⌫', () => _send('VK_BACK')),
            _key('Enter', () => _send('VK_RETURN'), width: 56),
            _key('Del', () => _send('VK_DELETE')),
            if (widget.onClose != null)
              _key('收起', () => widget.onClose!.call(), width: 56),
          ]),
          // 第二行：方向键 + 翻页/行首尾
          _row([
            _key('←', () => _send('VK_LEFT')),
            _key('↑', () => _send('VK_UP')),
            _key('↓', () => _send('VK_DOWN')),
            _key('→', () => _send('VK_RIGHT')),
            _key('Home', () => _send('VK_HOME')),
            _key('End', () => _send('VK_END')),
            _key('PgUp', () => _send('VK_PRIOR'), width: 52),
            _key('PgDn', () => _send('VK_NEXT'), width: 52),
            _key('Ins', () => _send('VK_INSERT')),
          ]),
          // 第三行：F1~F12
          _row([
            for (var i = 1; i <= 12; i++)
              _key('F$i', () => _send('VK_F$i'), width: 40),
          ]),
          // 第四行：高频组合键
          _row([
            _key('复制', () => _send('VK_C', ctrl: true), width: 52),
            _key('粘贴', () => _send('VK_V', ctrl: true), width: 52),
            _key('剪切', () => _send('VK_X', ctrl: true), width: 52),
            _key('全选', () => _send('VK_A', ctrl: true), width: 52),
            _key('撤销', () => _send('VK_Z', ctrl: true), width: 52),
            _key('保存', () => _send('VK_S', ctrl: true), width: 52),
            _key('切窗口', () => _send('VK_TAB', alt: true), width: 64),
            _key('关窗口', () => _send('VK_F4', alt: true), width: 64),
            _key('显示桌面', () => _send('VK_D', win: true), width: 72),
            _key('锁屏', () => _send('VK_L', win: true), width: 56),
            _key('任务管理器',
                () => _send('VK_ESCAPE', ctrl: true, shift: true),
                width: 80),
            _key('截屏', () => _send('VK_SNAPSHOT'), width: 56),
            _key('Ctrl+Alt+Del',
                () => bind.sessionCtrlAltDel(sessionId: widget.sessionId),
                width: 96),
          ]),
        ],
      ),
    );
  }
}
