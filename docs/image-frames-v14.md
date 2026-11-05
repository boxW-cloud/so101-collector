# 采集容器 v10r14：统一图像帧

本版只改采集容器。Perceptor、相机驱动、从臂节点及已有 Redis 队列格式保持原样。以已构建的 v10r13 源码为基础派生 v14，包含 v13 原有的动作字段规则；v12 和 v13 均保留。

## 流程

```text
SO101：现有相机/Isaac 帧入口 ─┐
                            ├→ RGB uint8[H,W,3] + collection.frame.v1
Bobac：现有 Redis 图片 key ──┘
   → SDK 持久化录制帧（NPZ：像素及来源信息）
   → 完成本条 / 结束本批
   → LeRobot 写入器生成 MP4 + Parquet + meta
   → 同步封存的数据集目录
```

Redis 图片 key 只是临时引用。录进本条的数据保存在本地 NPZ；导出时读本地已提交帧，不再回 Redis 读取 key。未被录制帧率选中的源画面不承诺全部保存。重采仍只废弃当前未提交轮次。

## 每路相机的统一帧记录

记录位于原始录制帧的来源信息 `observation.image_frames.<camera>`，不把像素或 Base64 放进元数据 JSON。

```json
{
  "schema": "collection.frame.v1",
  "camera_key": "front",
  "frame_id": "frame-<来源身份摘要>",
  "identity_basis": "source_sample_id",
  "width": 640,
  "height": 480,
  "encoding": "rgb8",
  "dtype": "uint8",
  "captured_monotonic_ns": 123456789000,
  "received_monotonic_ns": 123456800000,
  "source": {
    "kind": "redis_capture",
    "process_id": "原始发布进程ID",
    "observation_id": "原始观测ID",
    "sample_id": "原始相机样本ID",
    "sampled_at": 123.456,
    "original_source_timestamp": 1700000000.123,
    "time_basis": "receive_approx",
    "image_key": "collection_image:front:sample-001",
    "source_clock": {
      "time_basis": "process_monotonic",
      "clock_anchor": {"monotonic": 100.0, "unix": 1700000000.0}
    }
  }
}
```

例中的尺寸、时刻和身份仅表示字段含义，实际值来自设备。SO101 的 `source.kind` 是 `isaac_ipc` 或 `so101_camera`，使用其原有的主机 monotonic 采集时刻；真实相机共享存储提供的 sequence、captured_unix_ns 也保留。缺少源端样本 ID 时，`identity_basis=camera_and_capture_time`，由相机名和已有采集时刻生成稳定帧身份，不冒充硬件帧序号。

`captured_monotonic_ns` 是原有 SDK 使用的本机采集/映射时刻；`source` 保存原始来源、时间及其时间基准。两者不能混为同一个硬件时钟。原始时间不因过期、未来值、跨时钟或缺少原始时间而在新记录层被重写或拒绝；本版不新增时差、年龄、单调性校验，也不删除 SDK/设备原有的检查。

## 封存目录新增索引

```text
dataset/
├── data/...parquet
├── videos/observation.images.front/...mp4
├── videos/observation.images.wrist/...mp4
├── meta/
│   ├── info.json
│   ├── episodes/...parquet
│   ├── source-frames/episode-000000/frame-00000000.json
│   └── frame-index.jsonl
└── manifest.json
```

`meta/frame-index.jsonl` 每行对应“某条录制中的一帧、某一路相机”：

```json
{
  "episode_index": 0,
  "frame_index": 0,
  "camera_key": "front",
  "frame": {"schema": "collection.frame.v1", "其他字段": "同上"},
  "provenance_file": "meta/source-frames/episode-000000/frame-00000000.json"
}
```

一条录制的 frame_index 与 LeRobot 的逐帧数据对应；多路相机会有各自的索引行。定位 MP4 文件和该条视频范围仍使用原生 `meta/episodes`。`frame-index.jsonl` 与来源 JSON 一起计入 manifest 文件清单及内容 hash。

后续拼接应按 `episode_index` 和 `frame_index` 的顺序读取；`frame_id` 用于识别来源帧，不用它的字符串顺序排序。是否按原始时间重新采样、补帧或丢帧，留到下一步明确规则。

原生 MP4 的播放时间使用配置 FPS；原始采集时间保存在索引与来源文件中。丢帧、抖动、两相机不同步或图像与关节不同时，不会因为新增一个时间戳字段就自动消失，本版不宣称已经做硬件同步。

## 修改位置

- `deployment_runtime/observation_frames.py`：统一帧记录与既有 PNG 传输解码。
- `deployment_runtime/ros_transport.py`：取帧时将统一记录放进录制来源信息。
- `deployment_runtime/redis_capture.py`：保留 Perceptor 原始相机身份、源时间、时钟锚点。
- `deployment_runtime/isaac_adapter.py`、`direct_follower.py`、`camera_service.py`：传递 SO101 现有源帧标识和采集时间。
- `src/lerobot_sdk/data/provenance.py`：导出可追溯的逐帧索引。

核心 `ImageFrame`、`Observation` 的字段没有增加平台字段；开始、重采、完成和结束接口不因本版增加前端参数。

## 部署与验证

镜像：`so101-collection:20261104-v10r14`。

本版代码、Dockerfile、Compose 和启动脚本位于 Spark：

```text
/home/nvidia/lerobot-so101-sdk/collection-container-v10r14-20261104/
```

应用源码在 `image-files/opt/app/`。后续修改/构建/启动使用这个新版本目录。原 `/home/nvidia/so101-collection/` 的 data、config、secrets、delivery 继续被新目录 `.env` 以绝对路径挂载；无需搬动已有数据。

最终镜像内 77 项相关测试通过，包含原生 LeRobot MP4/Parquet 编码读取、两条录制、重采/恢复、Bobac 动作规则、来源时间戳和索引、Linux 启动脚本权限。另在独立、无设备和无网络的容器中验证了默认入口完整启动。真机读写由测试设备替代，Bobac Redis 使用测试源；没有据此宣称真机训练质量或硬件同步已验收。

部署后只读读取了现有 front/wrist 两路 640×480 实帧，确认源时间戳能随统一帧记录保留。实际人工示教采集仍由使用者开始；本轮没有自动创建业务采集批次。

离线包为 `images/so101-collection-20261104-v10r14.tar.gz`，包含应用镜像和构建所需的运行基础镜像。机器导入后，可编辑 `image-files/` 并离线重建应用镜像。`Dockerfile.runtime` 保留为历史运行基础镜像配方，不是本次应用重建的入口。
