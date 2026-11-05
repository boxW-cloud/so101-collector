# v10r16 — ROS 相机发布（2026-11-05）

- 在现有采集容器内新增 so101_camera_publisher，Domain96，发布 /so101/camera/front/image_raw 与 /so101/camera/wrist/image_raw（sensor_msgs/Image，rgb8），以及各自的 frame_info（std_msgs/String）。
- Image 头沿用源 captured_unix_ns；元数据保留原始 monotonic 时间、sequence、generation、帧身份及 RGB hash。未新增时间戳年龄/时差/顺序校验，源端已有检查保留。
- 读取原有图像缓存，不打开摄像头、不依赖 Redis 回写图像；HTTP JPEG / WebSocket 与采集 REST 保持兼容。
- Spark Linux ARM64 相关测试 29 项通过；隔离原生 ROS 测试通过；现场独立订阅收到两路各39帧且元数据匹配，HTTP JPEG与WS双路通过。
- 新容器完整采集上传复测2条17帧，LeRobot原生校验、重采排除与远端文件一致性通过，上传READY。
- 仅替换采集容器，其他运行容器与Perceptor源码未变。Perceptor消费端仍需同事按新版文档接入；本轮不宣称真实从臂或物理相机实测。

新接入指南：docs/SO101-Perceptor关节图像Action统一ROS接入-v10r16-20261105.md。
证据与完整现场帧元数据：docs/so101-ros-images-v10r16-acceptance-20261105.json。
镜像：so101-collection:20261105-v10r16，GitHub只发布源码/文档，镜像二进制仍在Spark。

---

历史发布记录（保留）：

# v10r15r1

采集容器使用独立主臂节点的 action 与 Perceptor 观测完成 Redis 配对、LeRobot 录制及后台发送。修复当前录制中的状态查询误用旧版 API 的问题。

- 主目录仅当前版本；附对应主臂 v2、从臂源码及 Isaac 节点脚本。
- 四条控制链路见 docs/control-chains-v10r15r1.md 与离线 HTML。
- 278 个采集载荷文件与已部署镜像逐项匹配；此次从仓库重新运行相关 HTTP 测试 11 项通过。
- Docker 镜像二进制、实际凭据、采集数据不入库。构建需要 Spark 上已有基础镜像，详见 README。
- Spark 定向清理及保留的回退版本见 docs/spark-cleanup-20261104.md。

当前采集镜像：`so101-collection:20261105-v10r15r1`。真实从臂新版尚未部署；肩肘越过仿真限位会裁剪，不能据此宣称一比一跟随精度通过。

## 完整仿真采集上传复验与接口指南（2026-11-04）

- 当前容器实测开始、重采、完成本条、结束封存与默认服务器 SSH 上传，最终 2 条 / 18 帧；同步队列 READY，远端 `.ready.json` 存在，28 个封存文件逐一 hash/大小一致。
- 新增 `docs/SO101采集容器REST最小接入指南-20261104-v10r15r1.md`，明确移除 align、忽略 is_sim、采集结束不停止独立从臂跟随，以及 202 / 本地封存完成 / 远端 READY 的区别。
- 旧 v10r11 指南逐字节保留在 `docs/history/`；测试摘要位于 `docs/sim-chain-full-v15r1-acceptance.json`。
- 本次仅补充验证与文档，应用源码、镜像标签和运行容器未变。

## Perceptor ROS 订阅接入说明（2026-11-04）

- 新增状态 `/so101/state` 与主臂 `/so101/action` 的接入文档及完整实测消息样本；约 6 秒只读探针收到 30 条 state / 181 条 action。
- 明确从臂 ticks 到观测单位的映射、原始采样时刻、Perceptor 钩子接法，以及主臂节点是现有 teleop 动作队列写入者。
- Perceptor 本轮未修改，仍使用 ARM_INSPECT；改成订阅后的采集消费链路需要接入同事另行验证。Composer 高层动作也不能冒充连续六轴 action 标签。
