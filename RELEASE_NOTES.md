# v10r15r1

采集容器使用独立主臂节点的 action 与 Perceptor 观测完成 Redis 配对、LeRobot 录制及后台发送。修复当前录制中的状态查询误用旧版 API 的问题。

- 主目录仅当前版本；附对应主臂 v2、从臂源码及 Isaac 节点脚本。
- 四条控制链路见 docs/control-chains-v10r15r1.md 与离线 HTML。
- 278 个采集载荷文件与已部署镜像逐项匹配；此次从仓库重新运行相关 HTTP 测试 11 项通过。
- Docker 镜像二进制、实际凭据、采集数据不入库。构建需要 Spark 上已有基础镜像，详见 README。
- Spark 定向清理及保留的回退版本见 docs/spark-cleanup-20261104.md。

当前采集镜像：`so101-collection:20261105-v10r15r1`。真实从臂新版尚未部署；肩肘越过仿真限位会裁剪，不能据此宣称一比一跟随精度通过。
