# 当前部署补充：无密钥版

当前不生成、读取或验证密钥，请求使用八字段信封，回执无签名。无需配置密钥路径。保留 request_id 幂等、目标、时间及参数校验。

# Seeed SO-101 从臂 · ROS 2 控制包

按《真实从臂控制：ROS 2 Topic与JSON接口规范》（协议 `so101.follower.v1`）给 Seeed SO-101 从臂做的 ROS 2 封装。
一个进程直连串口并在 ROS 2 上收发 JSON，中间没有别的服务，所以**臂的 USB 要插在运行它的那台机器上**。

> **验证状态**：2026-10-12 在真臂上用 `local` 模式把七个元动作都跑成功了（见"真机实测记录"）；ROS 收发层只用桩测过，**没有在真的 DDS 上跑过**。
> 换一台臂或换了舵机之后，第一次上真机请按"第一次上真机的顺序"来。

## 包里有什么

| 路径 | 内容 |
|---|---|
| `seeed_ros.py` | 主程序：签名信封、七个元动作、绑定/租约/心跳、ROS 2 节点，以及不用 ROS 的终端模式 |
| `seeed_arm.py` | Seeed 臂的串口直连控制（找串口、核对身份、读状态、上电、单步运动、卸力） |
| `joint_route.py` | 去预设位时"先动谁、后动谁"的规则 |
| `so101_kinematics.py` | 正/逆运动学：由 (x, y, z, roll, pitch, yaw) 反解五个关节。算法照搬松甲直连版 |
| `models/so101_new_calib.urdf` | 运动学用的 SO-101 模型（TheRobotStudio/SO-ARM100，Apache-2.0，来源见 `models/SOURCE.md`） |
| `seeed_ros_client.py` | 测试工具：在 ROS 2 上发一条签名请求并等回执 |
| `deps/` | 两个依赖，已解包，**不需要 pip**：pyserial 3.5（BSD）、feetech-servo-sdk 1.0.0（Unlicense，提供 `scservo_sdk`）。`seeed_ros.py` 启动时自己把这个目录加进搜索路径 |
| `tests/` | 不接硬件的自测（可选，需要 pytest） |
| `MANIFEST.json` | 每个文件的 SHA256、依赖版本、打包时间 |

## 需要目标机器自己有的（基础环境，不在包里）

- Python 3.10 或更新（3.11、3.12 实际跑过）。
- numpy：只有 `ARM_MOVETO` 和 `MOVE_GRASP` 用到。装了 ROS 2 的环境里都有；没有的话这两个动作返回 `KINEMATICS_UNAVAILABLE`，其余功能照常。
- 跑 ROS 节点和 `seeed_ros_client.py` 时：ROS 2（提供 `rclpy` 和 `std_msgs`），并且已经 `source` 过它的环境。
  `local` 和 `teach` 两种模式不需要 ROS。
- Linux 上当前用户要有串口权限（加入 `dialout` 组，或在容器里用 `--device` 把串口映射进去）。

两个依赖都是纯 Python，不分操作系统和 CPU 架构，x86 和 ARM64 用的是同一份。

## 这个包只认一台臂

`seeed_arm.py` 开头写死了这台臂的控制板 USB 序列号（`5A7C124107`）和六个舵机的零偏、限位。连接时逐项核对，
对不上就拒绝连接。换一台臂要改这两处常量。

## 怎么用

解压后进到这个目录里运行。下面以 Linux 为例；Windows 上把 `python3` 换成 `python`（Windows 一般没有 ROS 2，只用 `teach` 和 `local`）。

```bash
# 1. 示教（只读，不通电、不运动）。把臂摆到位置后各执行一次。节点运行时串口被占着，所以要在启动节点之前做
#    包里带了 seeed_ros_data/config.json 的话，park 和 chest 已经示教过，这一步可以跳过；想换位置就再 teach 一次覆盖
python3 seeed_ros.py teach park             # 停放位：ARM_RELEASE 用
python3 seeed_ros.py teach chest            # 胸前保持位：HOLD_ARM / HOLD_GRASP 用
python3 seeed_ros.py teach gripper_open     # 可选：夹爪张开端点
python3 seeed_ros.py teach gripper_closed   # 可选：夹爪闭合端点

# 2. 不用 ROS，先在终端里按同一套协议走一遍（动作会真的驱动臂）
python3 seeed_ros.py local
#    提示符下依次输入：inspect → bind → confirm → LOOSEN → GRASP → stop → release → q
#    位姿动作：MOVE_GRASP up 0.02        ARM_MOVETO 0.17 0.01 0.12 2.72 0.04 1.69

# 3. 启动 ROS 2 节点
export ROS_DOMAIN_ID=96 RMW_IMPLEMENTATION=rmw_fastrtps_cpp
python3 seeed_ros.py run

# 4. 另开一个终端（同一台机器、同样的环境变量）发请求
python3 seeed_ros_client.py inspect
python3 seeed_ros_client.py bind                                  # 返回 epoch
python3 seeed_ros_client.py confirm_workspace --epoch <epoch>     # 表示你已确认底座固定、周围清空
python3 seeed_ros_client.py execute --epoch <epoch> --action LOOSEN --wait
python3 seeed_ros_client.py execute --epoch <epoch> --action MOVE_GRASP --direction up --distance 0.02 --wait
python3 seeed_ros_client.py execute --epoch <epoch> --action ARM_MOVETO --pose 0.17 0.01 0.12 2.72 0.04 1.69 --wait
python3 seeed_ros_client.py stop --epoch <epoch>
python3 seeed_ros_client.py release_control --epoch <epoch>
```

运行时产生的文件都在 `seeed_ros_data/` 里（可用 `--data-dir` 改）：

| 文件 | 内容 |
|---|---|
| `config.json` | `teach` 记下的预设位和夹爪端点（单位 ticks） |
| `journal.json` | epoch、已处理的 request_id 和每个动作的结果（用于幂等和 `job` 查询） |

退出节点只关串口、**不卸力**，臂继续保持在原地。

包里带的预设位是 2026-10-12 手摆示教的：`chest`（肩 1470 / 肘 2566 / 腕 2966）是末端抬起约 7cm 的姿态，`park`（肩 1879 / 肘 2632 / 腕 2691）是臂向前趴在桌面上的姿态。它们是关节角度，只对这台臂有效；
`park` 是按"底座和桌面在同一平面"记的，底座垫高或换了安装方式要重新 `teach park`，否则会悬空或顶到桌面。

## 接口

和规范一致：`/so101_follower` 下 7 个 Topic（全是 `std_msgs/msg/String`，Reliable / Volatile / KeepLast 16）、九字段信封、
（每 2 秒发一次 `heartbeat`）、约 0.5 秒一次 `feedback`、约 5 秒一次 `catalog`、回执同样带签名。

| Topic | 方向 | 内容 |
|---|---|---|
| `/so101_follower/actions/request` | 平台 → 节点 | 七个元动作，`operation=execute` |
| `/so101_follower/actions/result` | 节点 → 平台 | 受理回执、最终结果或拒绝原因 |
| `/so101_follower/actions/status` | 节点 → 平台 | 已受理动作的运行状态和终态 |
| `/so101_follower/control/request` | 平台 → 节点 | `inspect` `catalog` `bind` `confirm_workspace` `heartbeat` `stop` `job` `release_control` |
| `/so101_follower/control/result` | 节点 → 平台 | 控制请求的回执 |
| `/so101_follower/feedback` | 节点 → 平台 | 连接、六轴位置/目标/通电/温度/电压、故障、租约 |
| `/so101_follower/catalog` | 节点 → 平台 | 七个动作的字段定义和实际执行能力 |

和规范不同、对接方需要知道的地方：

| 项目 | 规范（松甲臂） | 这个包（Seeed 臂） |
|---|---|---|
| robot_id | `so101-follower-com15` | `so101-follower-seeed`（环境变量 `SO101_ROBOT_ID` 可改） |
| device_id | `ch340:COM15:1-1` | `ch343:5A7C124107` |
| 相对移动的 action_id | 新旧两种写法并存 | 只认 `MOVE_GRASP` |
| 故障 | 由控制服务锁存 | 不锁存：`native.fault` 只反映当前仍存在的超温（> 55°C），降温后自己消失 |
| `stop` | 保留末个有界目标 | 五个臂关节的目标收回到当前位置；夹爪的目标保留（否则等于松手）。不开爪、不卸力 |
| 预设位、夹爪端点 | 配置里写米制坐标 | 用 `teach` 记关节 ticks |

七个动作的实际能力：

| action_id | 结果 |
|---|---|
| `LOOSEN` / `GRASP` | 执行。分段走到端点，每段最多 100 ticks；某段没走到就 `TARGET_NOT_REACHED`。没示教端点时取限位寄存器（读数大的一头算张开；真机上哪头是张开要看实物确认） |
| `ARM_RELEASE` | 先 `teach park` 才执行，否则 `POSE_NOT_CONFIGURED`。不开爪、不卸力 |
| `HOLD_ARM` | 先 `teach chest` 才执行，否则 `POSE_NOT_CONFIGURED` |
| `HOLD_GRASP` | 先整体预检（缺胸前位就直接拒绝，不会先闭爪），再闭爪、再去胸前位 |
| `ARM_MOVETO` | 执行。由 `target_position` 的 x/y/z/roll/pitch/yaw 反解五个关节，五个关节同时走过去；走完按编码器读数核对，位置 ≤5mm 且姿态 ≤0.01rad 才算成功，否则 `POSE_NOT_REACHED`。解不出来返回 `IK_POSE_NOT_CONVERGED` 或 `UNREACHABLE`，不会退成只管位置 |
| `MOVE_GRASP` | 执行。末端沿底座坐标系平移：forward/backward = ±X，left/right = ±Y，up/down = ±Z；腕旋保持不动。走完位置 ≤5mm 才算成功，否则 `TARGET_NOT_REACHED` |

位姿相关的约定和限制（算法照搬松甲直连版那一套，见 `so101_kinematics.py`）：

- 坐标是 `base_link → gripper_frame_link`，单位米和弧度，姿态为 `Rz(yaw)·Ry(pitch)·Rx(roll)`。当前末端位姿在 `inspect` / `feedback` 的 `native.tool` 里，挑目标时先看它。
- 这台臂只有五个自由度，不是任意六维位姿都到得了。`ARM_MOVETO` 的六个数必须给全。
- **模型没有在真臂上标定过**：关节角按"量程中点 = 0 弧度"换算，末端参考点取 URDF 里的 `gripper_frame_link`。模型里算得准，真实末端的位置和姿态会有偏差，偏多少要上真机量。
- 五个关节是在关节空间里同步走过去的，末端走的不是直线。途中只检查末端不低于底座平面上方 5mm，**没有碰撞检测**。
- `ARM_MOVETO` 的 `speed`（m/s）只是按比例换成关节速度（0.5 对应默认的 200 ticks/s，上限 400），不是真实线速度。
- 舵机是纯比例控制（P=16、I=0），带负载的关节会停在离目标还差一截的地方（真机肘关节约 30 格，末端差约 1cm）。所以停稳后还没到位时会把舵机目标再往前多给一截，最多补 3 次、每个关节累计不超过 60 格（`TRIM_ROUNDS` / `TRIM_MAX`），实际多给了多少在结果的 `device_result.trim_ticks` 里。扭矩上限寄存器不动；真被东西挡住时推力会比不补偿时大，但仍受扭矩上限限制，补完还不到就报失败并原地保持。
- 到位判据在 `seeed_ros.py` 开头的 `ARRIVAL_M` / `ARRIVAL_RAD`（5mm / 0.01rad），是按编码器读数算的。

几点容易踩的：

- 只配置了 `side=right`（`part_id=2`）。`ARM_RELEASE` 的 side 默认是 `both`，必须明确写 `"side":"right"`。
- 夹爪合到一半停住（比如夹到了东西）按规范算 `TARGET_NOT_REACHED`，所以 `HOLD_GRASP` 夹着实物时会停在闭爪这一步、
  不去抬臂。想让它通过，把 `gripper_closed` 示教成夹住该物体时的位置。
- 夹爪合上不等于抓住了东西：结果里的 `object_grasp_verified` 永远是 `false`。
- 去预设位是一个关节一小段地走（每段最多 120 ticks，等停稳再走下一段），不是一条连续轨迹。
- `teach` 时贴着限位的关节（比如肘折到底）会夹回 30 格余量之内再记，并打印出来；否则这个预设位发不出运动指令。
- 回执 `ok=true` 只说明请求被处理了，动作是否成功要同时看 `result.state` 和 `result.success`。

## 自测（可选）

不接硬件，在模拟舵机总线上把协议和动作跑一遍。需要 pytest：

```bash
python3 -m pytest -q tests
```

## 真机实测记录（2026-10-12，Windows，`local` 模式，COM18）

| 动作 | 结果 |
|---|---|
| `LOOSEN` | 成功：夹爪 2288 → 3446（端点 3459），每段 100 ticks、扭矩上限 10%。3446 这头是不是张开，只有现场的人看得到，记录里没有确认 |
| `GRASP` | 成功：3446 → 2048（端点 2036），空爪 |
| `MOVE_GRASP` | 成功：上移 2cm 误差 0.2mm（肘补偿 26 格）；下移 7cm 误差 2.4mm（没用到补偿） |
| `ARM_MOVETO` | 成功：误差 1.5mm / 0.0065rad（肘补偿 26 格） |
| `HOLD_ARM` | 成功：从放下位到手摆示教的胸前位，各关节残差 ≤2 格（肩补偿 11、肘补偿 29） |
| `ARM_RELEASE` | 成功：回到放下位，残差 ≤3 格，停稳后各关节负载约 0~2% |
| `HOLD_GRASP` | 成功：先闭爪再到胸前位，空爪 |

误差都是按编码器读数算的，不是用尺子量的末端实际位置。没有测过的：夹着实物、动作中途 `stop`、心跳中断、
离这几个姿态较远的位姿、连续长时间运行，以及整个 ROS 收发层。

补偿加上之前的结果：`MOVE_GRASP` 两次差 10.3mm / 11.9mm，`ARM_MOVETO` 差 10.0mm / 0.035rad，都如实报了失败。

## 第一次上真机的顺序

1. `python3 seeed_ros.py teach gripper_closed`：能连上并打印当前位置，说明串口、身份核对和依赖都没问题（这一步只读）。
   如果只是想验证连接，记下的值之后可以再 teach 一次覆盖。
2. `python3 seeed_ros.py local`，输入 `inspect`：核对六个关节的读数和实物姿态对得上。
3. 周围清空、手放在舵机电源上，输入 `bind`、`confirm`，然后 `LOOSEN`：只动夹爪（扭矩上限 10%）。
   如果方向反了（应该张开却合上），用 `teach gripper_open` / `gripper_closed` 把两个端点记一遍。
4. 夹爪没问题后再示教 `park` 和 `chest`，试 `HOLD_ARM` 和 `ARM_RELEASE`。第一次选离当前位置不远的预设。
5. 位姿动作先从小的来：臂在抬起的姿态下，`MOVE_GRASP up 0.01`，用尺子量末端实际抬了多少、往哪个方向。方向和距离对得上，再试 `ARM_MOVETO`（目标取 `inspect` 里 `native.tool` 的值稍作改动）。
6. 以上都对了，再启动 ROS 节点，用 `seeed_ros_client.py` 重复一遍。

## 常见问题

| 现象 | 原因 |
|---|---|
| `没找到 Seeed 臂的控制板` | USB 没插，或者插的不是这台臂的控制板（按 USB 序列号找） |
| `读失败 ID1 地址31` | 控制板连上了，但舵机没应答：舵机电源没开，或总线线没插紧 |
| `串口打开失败` | 别的程序占着这个串口；Linux 上也可能是没有串口权限 |
| `身份/标定和登记的不一样` | 换过舵机或重新标定过，`seeed_arm.py` 里登记的零偏/限位要更新 |
| 回执 `STALE_CONTROL` | 没有 `bind`，或 epoch 不是最近一次 `bind` 返回的（节点重启、`release_control` 之后旧 epoch 都作废） |
| 回执 `LEASE_EXPIRED` | 空闲超过 120 秒没发 `heartbeat`，或动作期间超过 5 秒没发 |
| `ros2 topic list` 看不到 Topic | 两边的 `ROS_DOMAIN_ID` / `RMW_IMPLEMENTATION` 不一致，或开了 `ROS_LOCALHOST_ONLY` 而不在同一台机器上 |
