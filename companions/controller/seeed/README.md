# 当前部署补充：无密钥版

当前不生成、读取或验证密钥，CLI 请求使用八字段信封，直接模式另接受 operation/payload 简化请求，回执无签名。无需配置密钥路径。保留 request_id 幂等、目标、时间及参数校验。

# Seeed 简化直接控制版

默认 SO101_DIRECT_CONTROL=1：无需 bind、epoch、confirm_workspace、heartbeat。
直接 execute 动作和 stop 即可；签名由客户端自动完成。错误、限位、不可达、停止与幂等检查保留。
CLI 接受动作名并自动等待：HOLD_ARM、ARM_RELEASE、LOOSEN、GRASP、HOLD_GRASP、MOVE_GRASP、ARM_MOVETO。
ARM_MOVE 是 MOVE_GRASP 的 CLI 别名，可传 JSON 参数。
原始运动学、硬件说明见 UPSTREAM_README.md；其中手动绑定和心跳步骤仅适用于 SO101_DIRECT_CONTROL=0 的兼容模式。

2026-10-12：夹爪运行时扭矩上限由100/1000提高至500/1000（50%），只调整 gripper，其他关节不变；在下一次夹爪动作上电初始化时写入寄存器48。分段、限位、温度与到位检查保持不变。

在线示教：arm teach park / chest / gripper_open / gripper_closed。使用运行节点已有串口，空闲时读取当前位置并原子保存；不运动、不改变扭矩。相同请求 ID 重试返回首次记录，运动中拒绝。离线入口改为 teach-local，必须先停主控制器。


Framework uses the original perceptor image unchanged. Static requests use std_msgs/String; dynamic requests use existing Float64MultiArray, TransformStamped and String messages. Only controller decoding and robot_config mappings are customized; no adapter or added ROS packages.
