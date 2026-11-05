# SO101 采集容器

当前发布：**v10r15r1**。本仓库主目录只保留这一个版本的采集代码，对应 Spark 实际运行的 `so101-collection:20261105-v10r15r1`。版本标签使用镜像原有日期；本文整理日期为 2026-11-04（America/Los_Angeles）。

采集容器从 Redis 取观测和 action，生成 LeRobot 数据集并后台发送。**v10r15 起不读取主臂、不控制从臂，也不提供采集前对齐接口。** 主臂读取和从臂执行由独立节点负责。

最新外部接入文档：[REST 最小接入指南 v10r15r1](docs/SO101采集容器REST最小接入指南-20261104-v10r15r1.md)，含全部最小请求、返回值解释、视频接口与完整仿真上传实测。旧版 [v10r11 指南](docs/history/SO101采集容器REST最小接入指南-20261029-v10r11.md) 保留原文供查历史。

## 目录

| 路径 | 内容 |
|---|---|
| `image-files/opt/app/` | 采集服务、测试网页、SDK、Redis 配对与上传代码、测试 |
| `image-files/opt/sim-retest/` | 容器入口与启动链 |
| `image-files/opt/converters/` | 冻结的数据转换器 |
| `Dockerfile`、`docker-compose.yml` | 当前采集镜像构建与部署 |
| `.env.example`、`apply-env.py` | 配置模板与生成器；真实 `.env` 不入库 |
| `companions/leader/` | 已上线的主臂 v2 节点，发布 `/so101/action` |
| `companions/controller/` | 对应从臂源码；目前用于 Isaac 内节点，真实从臂新版未部署 |
| `companions/isaac-follower/` | Isaac 内节点守护与历史重建脚本 |
| `docs/control-chains-v10r15r1.md` | 四条控制链路、采集侧路及实际验收范围 |
| `docs/control-chains-v10r15r1.html` | 可离线查看的四条链路图 |
| `release.json` | 当前组件版本、镜像身份与采集源码校验值 |

## 现有 Spark 上查看、修改和构建

本发布针对 **Linux ARM64 / Spark**。GitHub 保存源码和文档，不包含 Docker 镜像二进制、私钥、运行数据、模型及实际上传账号。

构建依赖已有的 `so101-collection-runtime:20261015-v10r6-rsync` 基础镜像。它包含 Python、ROS 2、LeRobot、编码器及 rsync/SSH 等依赖，身份见 `release.json`。基础镜像目前只在已有 Spark；**新机器仅克隆本仓库不能从零构建**，需先从已验证机器导出/加载基础镜像。`Dockerfile.runtime` 是历史依赖增量，不能代替完整基础环境。

```bash
git clone https://github.com/boxW-cloud/so101-collector.git
cd so101-collector
git checkout v10r15r1
python3 tools/verify-source.py
docker image inspect so101-collection-runtime:20261015-v10r6-rsync >/dev/null
./build.sh
```

修改后请使用新版本标签构建，不要覆盖已验收标签；`build.sh` 中的固定标签也需同步修改。主臂和从臂镜像另依赖 `so101-follower:direct-20261026-portfix`，`companions/` 不由采集 Compose 自动启动。

## 部署配置

复制 `.env.example` 为 `.env`，填写机器人、Redis、持久化目录与上传目标。实际密码仅写本机配置，不能提交到 Git。

当前 Spark 工作目录仍是：

```text
/home/nvidia/lerobot-so101-sdk/collection-container-v10r15r1-20261105/
```

现有数据与配置位于 `/home/nvidia/so101-collection/{data,config,secrets,delivery}`；在新检出目录部署时，应显式配置这四个绝对路径，避免生成另一套空目录。配置完成且采集空闲后才执行 `./start.sh`；该命令会更新配置并可能重建容器。

当前 Isaac 是外部已有容器，含独立从臂节点；`start.sh` 会沿用它。Compose 的备用 `sim` profile 不等于完整重建现场的 Isaac 节点、场景及 Perceptor，详见四条链路文档。不要同时启动两个监听相同控制 Topic 的从臂节点。

## 验收状态

- 实测主臂 ROS action 约 30.58 Hz、从臂状态约 4.98 Hz。
- 完整仿真链路实测：开始 → 重采 → 完成本条并继续 → 结束封存 → 现有服务器 SSH 上传 READY；最终 2 条、18 帧，两路 640×480，配对差 0.3～31.0 ms。
- LeRobot 原生格式校验通过，废弃轮次未进入最终数据集；远端 28 个封存文件逐一 hash/大小一致，另有最后写入的 `.ready.json`。结束后恢复 IDLE、关闭供数；服务未重启。
- 修复运行中查询状态误走旧版 API 的问题。
- 真实从臂新版、Composer 页面完整动作、全行程跟随精度未在本轮验收。主臂肩肘超过仿真限位时会被裁剪；七个原动作的限速、力矩限制保留。

最新测试证据见 [完整链路验收摘要](docs/sim-chain-full-v15r1-acceptance.json)，控制分支和精度边界另见 `docs/control-chains-v10r15r1.md`，不把旧版测试总数当成本轮全量回归。版本及清理规则见 [版本管理说明](docs/version-policy.md)。
