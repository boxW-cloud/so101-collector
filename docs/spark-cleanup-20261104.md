# Spark 版本清理记录

整理日期：2026-11-04（America/Los_Angeles）。服务器：192.168.0.215。

## 保留的入口

- 当前源码：`/home/nvidia/lerobot-so101-sdk/collection-container-v10r15r1-20261105/`
- 统一快捷入口：`/home/nvidia/lerobot-so101-sdk/collection-current/`（指向当前源码）
- 根目录提示：`/home/nvidia/lerobot-so101-sdk/COLLECTION_CURRENT.md`
- 当前镜像：`so101-collection:20261105-v10r15r1`
- 回退镜像：`so101-collection:20261104-v10r14`，其离线包也保留。
- 采集基础镜像、主臂 v2、当前从臂源码镜像、旧真机控制器及 Isaac 场景依赖保留。

## 已清理

- 13 个旧采集镜像/发布压缩包，共约 12.11 GB；仅删除明确的打包文件，历史源码未整目录删除。
- 旧版采集镜像标签 v10r10、v10r11、v10r12、v10r13、未修复的 v10r15，以及主臂 v1 标签；全部确认没有容器引用。
- 已停止的 `so101-isaac-before-follower-20261104`：可写层中的旧 Isaac 日志约 79.94 GB。删除前核对了新旧容器镜像及九个挂载一致，并备份用户配置、inspect、变更列表及日志片段。
- 已停止的历史采集测试容器 `so101-collection-simulation-retest-20261009`；保留它原来挂载的数据、配置和密钥。

## 核查结果

- 清理前可用空间：约 126.37 GB；清理后：约 323.32 GB；可用空间增加约 196.95 GB。以上为文件系统实测，不是逐项文件大小的加总估计。
- 所有运行中容器的 ID 与 StartedAt 均保持一致，没有重启服务。
- 采集容器仍健康，`GET /collection/status` 返回 IDLE。
- 没有删除持久化采集数据、Docker volume、模型或密钥，没有执行全局 prune。

服务器完整清理记录与私有配置备份：`/home/nvidia/lerobot-so101-sdk/github-collector-v15r1/`。私有备份未上传 GitHub。
