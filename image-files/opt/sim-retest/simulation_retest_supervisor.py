"""Latest capture API plus the one configured robot's data source; never auto-record.

v10r15（2026-11-05）：采集容器只采集，不再控制从臂、不再读主臂。一台采集容器只采一台机器人，
由 .env 的 COLLECTION_ROBOT 决定（apply-env.py → /config/robot.json），三种都从 Redis 采集队列取数据：
  so101-sim  Isaac 仿真的 SO101：观测来自 Perceptor（状态经 so101-isaac 里的从臂节点，图片取自 8910 视频），
             action 来自主臂节点 so101-leader。另起仿真画面预览（8922，只看画面）。
  so101      真实 SO101：同上；另起摄像头常驻进程（/config/real-cameras.json），给 8910 视频和 Perceptor 用。
  bobac3     机器人上 Perceptor 的 Redis 采集队列。
都是先登记（redis_capture prepare），再起设备进程（redis_capture serve）；设备进程只在 8910 开始采集时打开供数开关、
从队列取数据，结束采集就关掉。准备失败时 8910 照常启动，但没有采集目标，开始采集会明确报错。
"""
import json,os,signal,subprocess,sys,threading,time,urllib.request
from pathlib import Path

ROBOTS=('so101','bobac3','so101-sim')

def configured_robot():
    try:robot=json.loads(Path('/config/robot.json').read_text(encoding='utf-8')).get('robot')
    except (OSError,ValueError,AttributeError):robot=None
    if robot not in ROBOTS:
        print(json.dumps({'note':'/config/robot.json 没有或不对，按仿真 so101-sim 启动（请在部署目录执行 ./apply-env.py）'},ensure_ascii=False),flush=True)
        robot='so101-sim'
    return robot

def no_target():
    """准备失败时用的空绑定：8910 开始采集返回 COLLECTION_TARGET_NOT_CONFIGURED。"""
    path=Path('/tmp/so101-collection-no-target.json');path.write_text('{}\n',encoding='utf-8');return path

def real_cameras():
    """真实 SO101 的摄像头配置（apply-env.py 生成）；没有或没启用返回 None。"""
    path=Path('/config/real-cameras.json')
    try:value=json.loads(path.read_text(encoding='utf-8'))
    except (OSError,ValueError):return None
    return path if value.get('enabled') is True and value.get('cameras') else None

def main():
    root=Path('/data/isaac-console');generation=json.loads((root/'generation.json').read_text())['generation']
    robot=configured_robot();os.environ['SO101_COLLECTION_ROBOT']=robot
    try:
        from deployment_runtime.redis_capture import prepare as prepare_redis
        print(json.dumps({'redis_capture':prepare_redis()},ensure_ascii=False),flush=True)
    except Exception as error:
        print(json.dumps({'redis_capture':{'registered':False,'reason':str(error)}},ensure_ascii=False),flush=True)
    redis_peer=root/'redis-capture/peer.json';redis_targets=root/'redis-capture/collection-project-targets.json'
    redis_on=redis_peer.exists() and redis_targets.exists()
    os.environ['SO101_COLLECTION_TARGETS_JSON']=str(redis_targets if redis_on else no_target())
    if not redis_on:
        print(json.dumps({'note':f'{robot} 没有准备好（见上面的准备结果），8910 照常启动但没有采集目标'},ensure_ascii=False),flush=True)
    cameras=real_cameras() if robot=='so101' else None
    console_config=root/'retest-console.json'
    from deployment_runtime.container_init import prepare
    config,accounts=prepare()
    stop=threading.Event();children={};logs={}
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:stop.set())
    def spawn(name,args):
        logs[name]=(root/('retest-'+name+'.log')).open('ab')
        children[name]=subprocess.Popen([sys.executable,*args],cwd='/opt/app',stdout=logs[name],stderr=logs[name])
    try:
        spawn('api',['-m','deployment_runtime.http','--config',str(config),'--accounts',str(accounts)])
        until=time.monotonic()+45
        while not stop.is_set():
            if children['api'].poll() is not None:raise RuntimeError('API exited')
            try:
                if json.load(urllib.request.urlopen('http://127.0.0.1:8910/healthz',timeout=1)).get('ready'):break
            except Exception:pass
            if time.monotonic()>until:raise TimeoutError('API initialization')
            stop.wait(.2)
        if robot=='so101-sim':   # 只看画面：读 Isaac 写的预览图，不控制 Isaac
            spawn('preview',['/opt/app/integration/simulation/passive_preview.py','--ipc-directory',str(root/'ipc'),'--generation',generation,'--bind','127.0.0.1','--port','8922'])
        if cameras:spawn('camera',['-m','deployment_runtime.camera_service','--config',str(cameras)])
        if redis_on:spawn('redis',['-m','deployment_runtime.redis_capture','serve','--config',str(redis_peer)])
        spawn('console',['-m','collection_console.server','--config',str(console_config)])
        print(json.dumps({'robot':robot,'services':{n:p.pid for n,p in children.items()},'generation':generation,'auto_recording':False,
                          'redis_capture':redis_on,'real_cameras':bool(cameras)}),flush=True)
        while not stop.wait(.2):
            dead={n:p.returncode for n,p in children.items() if p.poll() is not None}
            if dead:raise RuntimeError('Child exited: '+json.dumps(dead))
    finally:
        for name in ['api','console','preview','redis','camera']:
            p=children.get(name)
            if p and p.poll() is None:
                p.terminate()
                try:p.wait(45 if name=='api' else 5)
                except subprocess.TimeoutExpired:p.kill();p.wait(5)
        for stream in logs.values():stream.close()
if __name__=='__main__':main()
