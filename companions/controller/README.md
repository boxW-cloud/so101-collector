# SO101 控制器

本目录是 Spark 上的控制器源码和构建目录：`/home/nvidia/so101-controller`。

## 目录说明

- `seeed/seeed_ros.py`：共用动作逻辑、ROS 命令和结果话题，以及后端选择。
- `seeed/seeed_arm.py`：真机串口后端，包含端口恢复逻辑。
- `seeed/sim_arm.py`：Isaac IPC 仿真后端。
- `entry.sh`：镜像内的进程入口和命令行入口。
- `data/real`、`data/sim`：分别保存真机和仿真的预设及请求日志。
- `Dockerfile`、`build.sh`：从当前源码构建镜像。
- `control.json`、`start.sh`：选择模式并启动控制容器。

## 配置与运行

在 `control.json` 中，将 JSON 布尔值 `is_sim` 设为 `false` 使用真机，设为
`true` 使用 Isaac 仿真。该文件还指定镜像、真机串口 `serial_port`、Isaac
目录 `isaac_dir`，以及两种模式各自的数据目录。Isaac 仿真器需单独运行；
仿真模式只挂载 Isaac IPC 和契约文件，真机模式只传入串口设备。
数据目录可写成相对路径，相对于 `start.sh` 所在目录解析；连同源码目录迁移时
无需修改这两个路径。镜像固定使用非 root 用户，启动脚本会在实际启动前准备
所选数据目录及日志文件的权限；目录所有权需要调整时，请用 `sudo` 运行脚本。

修改源码后构建镜像：

```bash
bash /home/nvidia/so101-controller/build.sh
```

检查配置并启动所选模式：

```bash
bash /home/nvidia/so101-controller/start.sh --check
bash /home/nvidia/so101-controller/start.sh
```

`--check` 只检查配置和运行条件，不修改数据或切换容器。实际启动时，脚本会
先检查数据目录可写，再停止旧控制
容器和当前控制容器，再启动单个 `so101-controller` 容器。修改 `is_sim` 后
重新运行 `start.sh` 即可切换模式；两种模式的数据目录相互独立。要临时使用
其他配置文件，可设置 `SO101_CONTROL_CONFIG=/绝对路径/config.json`。

两种模式均使用 ROS Domain 96，通过 `/so101/command` 接收命令，通过
`/so101/result` 发布结果。命令是 `std_msgs/String` 类型的 JSON 字符串，例如：

```json
{"action":"ARM_INSPECT","params":{"request_id":"inspect-1"}}
```

## 仿真已移到 so101-isaac；同一时间只能有一个从臂节点

2026-11-04 起，仿真从臂的 ROS 节点（同一份 `seeed/` 代码）跑在 Isaac 容器 `so101-isaac` 里
（`/home/nvidia/so101-collection/data/isaac-console/follower/`），本容器只在用真机从臂时启动。
两者共用 `/so101/command`：节点启动时等 5 秒（`SO101_COMMAND_GUARD_S`），发现话题上已有别的
`seeed_follower_actions` 就退出（退出码 3），不会两个一起执行同一条命令。改用真机前先执行
`docker exec so101-isaac /sim/follower/run.sh stop`；切回仿真：停掉本容器，再执行
`docker exec so101-isaac /sim/follower/run.sh start`。

## 状态主动推送 `/so101/state`

控制器后台采样线程每采一次关节状态，就把和 `ARM_INSPECT` 相同的结果主动发到
`/so101/state`（同样是 `std_msgs/String` JSON，同一 ROS Domain 96），不用再轮询：

```json
{"action":"ARM_STATE","params":{"status":"SUCCEEDED","result":{ ... 与 ARM_INSPECT 的 result 相同 ... }}}
```

采样周期：仿真 0.2 秒，真机 0.5 秒；可用环境变量 `SO101_FEEDBACK_PERIOD`（秒）覆盖。
推送消息没有 `request_id`，不是哪条命令的回执。`ARM_INSPECT` 照常可用，返回的
`result.native` 同样带上下面这些采样标识：

- `sample_seq`：本次进程内的采样序号，每采一次加 1，重启后从 1 重新开始。
  和 `result.native.session_id`（控制器进程启动时生成）一起唯一标识一次采样。
- `sampled_mono_ns`：采样时刻，主机 CLOCK_MONOTONIC 纳秒；同一台主机上的容器可以直接比较。
- `sampled_unix`：采样完成时的 Unix 时间（秒）。
- `source`：仿真时是这次数据在 Isaac 里的来源。空闲时为
  `{"kind":"isaac_status","steps":…,"time_ns":…}`（Isaac 大约每 1.5 秒才写一次状态文件，
  所以几次推送可能是同一个 `steps`）；动作执行中为 `{"kind":"isaac_sample","time_ns":…}`。
  真机为 `null`。

订阅示例：

```bash
docker exec so101-controller /opt/so101/entry.sh ros topic echo /so101/state --once
```
