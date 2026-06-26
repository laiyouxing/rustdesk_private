# RustDesk 定制跟进操作说明

## 场景：官方发布新版本，如何升级？

---

## 一、准备工作（仅需做一次）

在 `E:\github\rustdesk_private\` 目录下打开 Git Bash，添加上游仓库：

```bash
cd /e/github/rustdesk_private/rustdesk_private
git remote add upstream https://github.com/rustdesk/rustdesk.git
```

---

## 二、跟新版本（每次发版）

### 方式 A：打补丁（推荐，最快）

```bash
# 下载官方新版源码到临时目录
cd /e/github/rustdesk_private/
mkdir rustdesk_upstream
cd rustdesk_upstream
git init
git remote add origin https://github.com/rustdesk/rustdesk.git
git fetch origin --depth=1 v1.5.x     # v1.5.x 改为实际版本号
git checkout FETCH_HEAD

# 打上定制补丁
git apply /e/github/rustdesk_private/rustdesk_private/RustDesk定制补丁.patch
```

如果提示冲突，则换成手动方式。

### 方式 B：手动对照（冲突时用）

打开 `RustDesk定制清单.md`，逐项对照修改：

```
□ libs/hbb_common/src/config.rs  → option_env! 三个常量
□ src/common.rs                  → API默认值、打洞开关、打洞参数
□ src/client.rs                  → 多端口打洞
□ src/updater.rs                 → 禁用更新
□ flutter/.../connection_page.dart → 去提示文字
□ .github/workflows/flutter-build.yml → 三个 Secrets
□ .github/workflows/flutter-nightly.yml → 关闭定时
```

每次改完 **本地编译一次** 确认无报错，再推送 Actions 构建。

---

## 三、推送并构建

```bash
# 推送到你的 GitHub 仓库
git remote add origin https://github.com/laiyouxing/rustdesk_private.git
git push -u origin main

# 去 Actions 页面手动触发 Flutter Nightly Build
```

---

## 四、检查 Secrets

确认 GitHub 仓库的 Secrets 未丢失（Settings → Secrets and variables → Actions）：

| Secret 名称 | 必须？ |
|------------|--------|
| `RS_PUB_KEY` | ✅ |
| `RENDEZVOUS_SERVER` | ✅ |
| `API_SERVER` | ✅ |

---

## 五、常见问题

**Q：打补丁报冲突怎么办？**
→ 官方新版可能改了同一行代码，打开冲突文件手动解决，对照 `RustDesk定制清单.md` 重新改。

**Q：如何查看 patch 具体改了哪些文件？**
```bash
head -50 RustDesk定制补丁.patch
```

**Q：补丁文件丢了怎么办？**
→ 直接从 1.4.8.1 分支重新生成：
```bash
git checkout 1.4.8.1
git diff 29ecb6e5d -- src/common.rs src/client.rs src/updater.rs \
  libs/hbb_common/src/config.rs \
  flutter/lib/desktop/pages/connection_page.dart \
  .github/workflows/flutter-build.yml \
  .github/workflows/flutter-nightly.yml > RustDesk定制补丁.patch
```
