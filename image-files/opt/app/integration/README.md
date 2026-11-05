# integration 导航

这里是SDK外层的接线、仿真和验收工具，不是核心SDK。业务实现已经按职责分组；历史试验代码集中到`legacy/`，单元测试移到`tests/integration/`。

| 目录 | 用途 | 常用入口 |
| --- | --- | --- |
| `actions/` | 第17章标准动作包装 | `action_primitives.py` |
| `ros/` | ROS2契约、传输、网关与探针 | `ros_sim_contract.py`、`ros_sim_gateway.py` |
| `simulation/` | Isaac场景、采集启动、回放、推理验收和查看 | `isaac_harness.py`、`isaac_replay.py`、`act_isaac_validation.py` |
| `leader/` | 主臂只读输入及真实主臂→仿真采集 | `native_collection_producer.py`、`leader_collection_validation.py` |
| `calibration/` | 映射、三点插值、测量拟合 | `three_point_preview.py`、`fit_measured_angles.py` |
| `training/` | ACT/π0.5训练与模型验收 | `act_gpu_job.py`、`pi05_training_acceptance.py` |
| `legacy/` | 旧HTTP/文件预览链路与一次性部署脚本 | `leader_preview_*`、`deploy_leader_preview*`；仅查阅历史，不作为当前ROS2采集入口 |

根目录只保留包入口、启动/路径工具、目录索引，以及共用的`contract.json`和`fastdds.xml`。生成缓存已在编辑器中隐藏，不当业务源码整理。

## 本地运行入口

从SDK仓库根目录运行：

```powershell
python integration/run.py --list
python integration/run.py native_collection_producer --help
python integration/run.py isaac_replay --help
```

执行真实脚本时仍需原有依赖、参数和运行环境。上面的`--list`只列出文件，不导入Isaac、不连接串口。历史脚本中可能固定旧端口和旧输出目录，不能因为被重新归类就直接运行。

Python导入名称保持原样：

```python
from integration.ros_sim_contract import make_contract
from integration.action_primitives import ActionWrapper
from integration.leader_collection_input import BorrowedLeader
```

目录分组是物理布局；继续使用上述稳定模块名，避免同时以`integration.ros.ros_sim_contract`等第二种名字加载同一文件。包入口仅增加标准模块查找目录，不提前加载硬件/仿真脚本。

## 部署与构建

仓库中旧的`python integration/<脚本>.py`直达路径改为`run.py`。独立部署仍可导出原有平铺路径：

```powershell
python integration/layout.py --export-flat <新的独立目录>
```

导出不覆盖已有目录，记录逐文件hash，不执行任何设备代码。部署目录包含同名脚本和依赖，故容器中的`/opt/edge/integration/ros_sim_gateway.py`等命令保持不变。

- 训练容器：`tools/training_sim_bundle.py`已支持分组源文件，递归收集依赖，输出旧平铺部署结构。
- 采集容器：使用`tools/collection-upload-container/package_context.py --wheel <SDK.whl> --output <新context.tar.gz>`生成上下文；Dockerfile同时兼容分组/平铺输入，通过文件导出得到运行目录。
- 不要复跑`artifacts/`里写死历史作业目录的部署脚本；历史交付包和已运行镜像保留其冻结版本。新构建需重新生成制品和环境锁。

详细新旧路径及验证记录见[整理报告](../docs/integration-layout.md)。
