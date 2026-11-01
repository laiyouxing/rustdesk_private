/// 中国省级行政区划（34 个），用于"所属地区"选择。
///
/// 用途：中继就近调度。客户端在设置页选择后写入选项 `relay-region`，
/// 选路时对**同地域**中继给一点偏好（A+B 实测延迟仍是主导因素）。
///
/// 与 api-server 的 `service/region.go` 保持同一套 code，便于后台批量下发
/// 与客户端自选互相对齐。
class RegionItem {
  final String code;
  final String name;
  const RegionItem(this.code, this.name);
}

const kRegions = <RegionItem>[
  RegionItem('BJ', '北京'),
  RegionItem('TJ', '天津'),
  RegionItem('HE', '河北'),
  RegionItem('SX', '山西'),
  RegionItem('NM', '内蒙古'),
  RegionItem('LN', '辽宁'),
  RegionItem('JL', '吉林'),
  RegionItem('HL', '黑龙江'),
  RegionItem('SH', '上海'),
  RegionItem('JS', '江苏'),
  RegionItem('ZJ', '浙江'),
  RegionItem('AH', '安徽'),
  RegionItem('FJ', '福建'),
  RegionItem('JX', '江西'),
  RegionItem('SD', '山东'),
  RegionItem('HA', '河南'),
  RegionItem('HB', '湖北'),
  RegionItem('HN', '湖南'),
  RegionItem('GD', '广东'),
  RegionItem('GX', '广西'),
  RegionItem('HI', '海南'),
  RegionItem('CQ', '重庆'),
  RegionItem('SC', '四川'),
  RegionItem('GZ', '贵州'),
  RegionItem('YN', '云南'),
  RegionItem('XZ', '西藏'),
  RegionItem('SN', '陕西'),
  RegionItem('GS', '甘肃'),
  RegionItem('QH', '青海'),
  RegionItem('NX', '宁夏'),
  RegionItem('XJ', '新疆'),
  RegionItem('TW', '台湾'),
  RegionItem('HK', '香港'),
  RegionItem('MO', '澳门'),
];

/// 选项名：客户端所属地区（省级 code）
const String kOptionRelayRegion = 'relay-region';

/// 由 code 取显示名；未匹配时原样返回（含空串）
String regionName(String code) {
  if (code.isEmpty) return '';
  for (final r in kRegions) {
    if (r.code == code) return r.name;
  }
  return code;
}
