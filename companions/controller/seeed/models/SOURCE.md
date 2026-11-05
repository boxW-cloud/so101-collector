# so101_new_calib.urdf 的来源

- 仓库：https://github.com/TheRobotStudio/SO-ARM100
- 提交：`eecbe3e0a9ebb23e25ad7b2759b03884c6660903`
- 许可证：Apache-2.0
- SHA256：`651ea00a3efe02a3c984b21a1b52273bd1b320ed635b3eae92fd52e99d0a393b`（`so101_kinematics.py` 加载时会核对）

文件原样未改，和松甲直连版（`so101-spark-native-pose6d-20261008/arm_mcp/static/models/so101/`）用的是同一份。
这里只用到其中的关节原点、转轴和限位来做运动学计算，没有带网格文件（STL）。
