import 'package:flutter/material.dart';

import '../../models/input_model.dart';

/// 左侧竖向「鼠标滚轮」滑条。
///
/// 手机/平板没有鼠标滚轮，看网页、文档、长列表时只能双指缩放后再慢慢拖。
/// 这里在屏幕左缘放一根拖动条：手指上下滑过的距离按 [pxPerTick] 像素一格
/// 换算成滚轮事件发给远程；松手后滑块自动回中，方便反复拖。
///
/// 横屏、竖屏都挂在同一个 Stack 上，跟着布局走，不需要两套代码。
class MobileWheelScrollBar extends StatefulWidget {
  const MobileWheelScrollBar({
    super.key,
    required this.inputModel,
    this.pxPerTick = 10,
    this.thumbHeight = 46,
    this.width = 30,
  });

  /// 当前远程会话的输入模型，滚轮事件通过它的 `scroll()` 发出。
  final InputModel inputModel;

  /// 滑过多少逻辑像素等于一格滚轮（越小滚得越快）。
  final double pxPerTick;

  /// 滑块高度。
  final double thumbHeight;

  /// 整条滑条的宽度。
  final double width;

  @override
  State<MobileWheelScrollBar> createState() => _MobileWheelScrollBarState();
}

class _MobileWheelScrollBarState extends State<MobileWheelScrollBar> {
  static const Duration _kResetDuration = Duration(milliseconds: 220);
  static const double _kThumbInset = 4;

  /// 滑块相对轨道中心的偏移（像素），拖到哪就停在哪，松手回中。
  double _thumbDy = 0;

  /// 允许的最大偏移，随布局高度变化。
  double _maxThumbDy = 0;

  bool _dragging = false;

  /// 还没凑够一格滚轮的剩余像素。
  double _pending = 0;

  void _onDragStart(DragStartDetails details) {
    setState(() {
      _dragging = true;
      _pending = 0;
    });
  }

  void _onDragUpdate(DragUpdateDetails details) {
    final dy = details.delta.dy;
    if (dy == 0) return;
    setState(() {
      _thumbDy = (_thumbDy + dy).clamp(-_maxThumbDy, _maxThumbDy);
      _pending += dy;
    });
    _flushWheel();
  }

  void _onDragStop() {
    setState(() {
      _dragging = false;
      _thumbDy = 0;
      _pending = 0;
    });
  }

  void _flushWheel() {
    final ticks = (_pending / widget.pxPerTick).truncate();
    if (ticks == 0) return;
    _pending -= ticks * widget.pxPerTick;
    _sendWheel(ticks);
  }

  /// 一次最多补 8 格，快速滑动时不至于把事件刷爆。
  void _sendWheel(int ticks) {
    final steps = ticks.abs().clamp(1, 8);
    // 正数 = 滚轮向下，与三指拖动/鼠标滚轮的约定一致（见 remote_input.dart）。
    final direction = ticks.isNegative ? -1 : 1;
    for (var i = 0; i < steps; i++) {
      try {
        widget.inputModel.scroll(direction);
      } catch (_) {}
    }
  }

  @override
  Widget build(BuildContext context) {
    return LayoutBuilder(builder: (context, constraints) {
      final trackHeight =
          constraints.maxHeight.isFinite ? constraints.maxHeight : 240.0;
      final thumbHeight = widget.thumbHeight.clamp(
        28.0,
        (trackHeight - _kThumbInset * 4).clamp(28.0, trackHeight),
      );
      _maxThumbDy = ((trackHeight - thumbHeight) / 2 - _kThumbInset)
          .clamp(0.0, double.infinity);
      final centerTop = (trackHeight - thumbHeight) / 2;
      final thumbTop = (centerTop + _thumbDy)
          .clamp(_kThumbInset, trackHeight - thumbHeight - _kThumbInset);

      return GestureDetector(
        behavior: HitTestBehavior.opaque,
        onVerticalDragStart: _onDragStart,
        onVerticalDragUpdate: _onDragUpdate,
        onVerticalDragEnd: (_) => _onDragStop(),
        onVerticalDragCancel: _onDragStop,
        child: Container(
          width: widget.width,
          decoration: const BoxDecoration(
            color: Color(0x991B1B1B),
            borderRadius: BorderRadius.horizontal(right: Radius.circular(10)),
          ),
          child: Stack(
            children: [
              const Positioned(
                top: 2,
                left: 0,
                right: 0,
                child: Icon(Icons.keyboard_arrow_up,
                    size: 16, color: Colors.white54),
              ),
              const Positioned(
                bottom: 2,
                left: 0,
                right: 0,
                child: Icon(Icons.keyboard_arrow_down,
                    size: 16, color: Colors.white54),
              ),
              AnimatedPositioned(
                duration: _dragging ? Duration.zero : _kResetDuration,
                curve: Curves.easeOut,
                top: thumbTop,
                left: 4,
                right: 4,
                height: thumbHeight,
                child: Container(
                  decoration: BoxDecoration(
                    color: _dragging
                        ? const Color(0xFF4C8DFF)
                        : Colors.white.withOpacity(0.72),
                    borderRadius: BorderRadius.circular(6),
                  ),
                  child: const Center(
                    child: Icon(Icons.unfold_more,
                        size: 14, color: Color(0xFF1B1B1B)),
                  ),
                ),
              ),
            ],
          ),
        ),
      );
    });
  }
}
