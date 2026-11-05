"""Native Seeed data integration; intentionally uses real Parquet/video encoders."""
import json
import shutil
import threading
import time
from pathlib import Path
import pytest
from lerobot_sdk import *
from lerobot_sdk.testing import FakeRobot,simulation_config
from lerobot_sdk.testing.leader import ScriptedLeader
from lerobot_sdk.adapters.seeed_dataset import SeeedWriter

pytest.importorskip('lerobot',reason='Install collection extra for native data tests')


def until(predicate,timeout=20):
    end=time.monotonic()+timeout
    while not predicate():
        if time.monotonic()>end:raise AssertionError('Condition did not become true')
        time.sleep(.01)


def enough(task,count=3):
    def check():
        assert task.status().state!=CollectionState.RECOVERABLE,task.status().error
        return task.status().current_frames>=count
    until(check)


@pytest.fixture
def station(tmp_path):
    config=simulation_config('collect-test',required_cameras=('front','wrist'),max_observation_age_s=2)
    backend=FakeRobot(resource_id=str(tmp_path),cameras=('front','wrist'))
    robot=RobotSession(config,backend,lock_dir=tmp_path/'robot-lock').connect()
    positions=dict(robot.get_arm_data().positions);positions[JOINT_NAMES[0]]=12
    leader=ScriptedLeader(config,positions,resource_id=str(tmp_path))
    cfg=CollectionConfig('task','native integration',tmp_path,10,'Move object',config.robot_id,
                         config.calibration_hash,config.units,{'front':(12,16,3),'wrist':(12,16,3)},min_free_bytes=1)
    tasks=[]
    def create():
        task=CollectionManager().create_task(cfg);tasks.append(task);return task
    yield robot,leader,cfg,create,tasks
    for task in tasks:
        if task._thread and task._thread.is_alive():
            try:task.stop(False).wait(30)
            except SDKError:pass
            task._thread.join(30)
    robot.close()


def test_create_does_not_touch_hardware_or_writer(station):
    robot,leader,cfg,create,_=station
    task=create()
    assert task.status().state==CollectionState.CREATED and leader.calls==0
    assert not list((task.directory/'attempts').iterdir())
    with pytest.raises(StateError):CollectionManager().create_task(cfg)


def test_native_record_save_redo_and_full_export(station,tmp_path):
    robot,leader,cfg,create,_=station
    task=create();task.start(robot,leader).wait(120)
    enough(task);saved=task.save_episode().wait(30)
    assert saved.episode_index==0 and saved.frames>=3
    with pytest.raises(StateError):task.save_episode()
    task.start_episode().wait(5);enough(task)
    discarded=task._episode_dir
    task.redo_episode().wait(30)
    assert not discarded.exists()
    assert task.status().state==CollectionState.RESETTING and task.status().committed_episodes==1
    time.sleep(.15);assert task.status().current_frames==0
    task.start_episode().wait(5);enough(task)
    stop=task.stop();assert task.stop() is stop
    ref=stop.wait(60)
    assert robot.get_arm_data().sequence>0 and leader.is_connected
    manager=DatasetManager();inspected=manager.inspect(ref)
    assert inspected.episodes==2 and inspected.native_readable
    manifest=json.loads((ref.root/'manifest.json').read_text())
    assert manifest['source_episode_hashes'][0]==saved.content_hash
    out=manager.export(ref,tmp_path/'export').wait(60)
    assert out.content_hash==ref.content_hash
    assert manager.copy_to(ref,out.root).wait(60)==out
    assert (out.root/'manifest.json').read_bytes()==(ref.root/'manifest.json').read_bytes()
    assert len(list((out.root/'videos').rglob('*.mp4')))>=2
    reopened=CollectionManager().open_task(task.directory)
    assert reopened.stop().wait().content_hash==ref.content_hash
    import pyarrow.parquet as pq
    rows=[]
    for path in (out.root/'data').rglob('*.parquet'):rows+=pq.read_table(path).to_pylist()
    assert all(row['action']==row['action.executed'] for row in rows)
    assert any(row['action.requested']!=row['action.executed'] for row in rows)
    assert any(row['observation.state']!=row['action.executed'] for row in rows)


def test_save_in_flight_is_idempotent_timeout_does_not_cancel(station):
    robot,leader,cfg,create,_=station
    task=create();entered=threading.Event();release=threading.Event()
    class Slow(SeeedWriter):
        def save(self):
            entered.set();assert release.wait(15)
            super().save()
    task._writer_factory=Slow
    task.start(robot,leader).wait(120);enough(task)
    op=task.save_episode()
    try:
        assert entered.wait(5)
        assert task.save_episode() is op
        with pytest.raises(WaitTimeout):op.wait(.01)
        assert robot.status().state==SessionState.HOLDING
        with pytest.raises(BusyError):task.redo_episode()
    finally:release.set()
    op.wait(30)
    assert task.status().committed_episodes==1
    task.stop().wait(60)


def test_stop_without_current_save_preserves_committed(station):
    robot,leader,cfg,create,_=station
    task=create();task.start(robot,leader).wait(120);enough(task)
    first=task.save_episode().wait(30)
    task.start_episode().wait(5);enough(task)
    ref=task.stop(False).wait(60)
    result=DatasetManager().inspect(ref)
    assert result.episodes==1 and result.frames==first.frames


def test_save_failure_recovery_preserves_committed_and_current(station):
    robot,leader,cfg,create,tasks=station
    task=create()
    class FailSecond(SeeedWriter):
        count=0
        def save(self):
            self.count+=1
            if self.count==2:raise OSError('injected encoder failure')
            return super().save()
    task._writer_factory=FailSecond
    task.start(robot,leader).wait(120);enough(task);saved=task.save_episode().wait(30)
    task.start_episode().wait(5);enough(task)
    with pytest.raises(OperationFailed):task.save_episode().wait(30)
    assert task.status().state==CollectionState.RECOVERABLE
    assert robot.status().state==SessionState.HOLDING
    assert not list((task.directory/'versions').iterdir())
    pending_frames=task.status().current_frames
    recovered=CollectionManager().open_task(task.directory);tasks.append(recovered)
    calls=leader.calls;assert recovered.status().committed_episodes==1 and leader.calls==calls
    recovered.start(robot,leader).wait(120)
    assert recovered.status().state==CollectionState.RESETTING and leader.calls==calls
    ref=recovered.stop().wait(60)
    assert DatasetManager().inspect(ref).frames==saved.frames+pending_frames


def test_task_lock_and_manual_control_conflict(station):
    robot,leader,cfg,create,_=station
    task=create();task.start(robot,leader).wait(120);enough(task)
    with pytest.raises(BusyError):CollectionManager().open_task(task.directory)
    target=JointTarget(dict(robot.get_arm_data().positions),cfg.units,cfg.calibration_hash)
    with pytest.raises(BusyError):robot.move_to(target)
    task.stop().wait(60)


def test_disk_failure_is_recoverable_not_ready(station,monkeypatch):
    import lerobot_sdk.collection as module
    robot,leader,cfg,create,_=station
    task=create();task.start(robot,leader).wait(120);enough(task)
    def fail(*args):raise OperationFailed('disk full')
    monkeypatch.setattr(module,'disk_check',fail)
    until(lambda:task.status().state==CollectionState.RECOVERABLE)
    task._thread.join(20)
    assert robot.get_arm_data().sequence>0 and robot.status().state==SessionState.HOLDING
    assert not list((task.directory/'versions').iterdir())


def test_corrupt_committed_journal_rejected_before_recovery(station):
    robot,leader,cfg,create,_=station
    task=create();task.start(robot,leader).wait(120);enough(task);task.save_episode().wait(30)
    task.stop().wait(60)
    journal=json.loads((task.directory/'task.json').read_text())
    path=task.directory/journal['commits'][0]['files'][0]['path']
    path.write_bytes(b'corrupted')
    with pytest.raises(ContractMismatch):CollectionManager().open_task(task.directory)


@pytest.fixture
def ready(station):
    robot,leader,cfg,create,_=station
    task=create();task.start(robot,leader).wait(120);enough(task)
    return task.stop().wait(60)


@pytest.mark.parametrize('change',['missing','extra','bytes','manifest'])
def test_inspect_rejects_tampered_snapshot(ready,change):
    file=next((ready.root/'data').rglob('*.parquet'))
    if change=='missing':file.unlink()
    elif change=='extra':(ready.root/'extra.txt').write_text('unlisted')
    elif change=='bytes':file.write_bytes(b'broken')
    else:
        p=ready.root/'manifest.json';m=json.loads(p.read_text());m['fps']=99;p.write_text(json.dumps(m))
    with pytest.raises(ContractMismatch):DatasetManager().inspect(ready)


def test_partial_copy_never_publishes(ready,tmp_path,monkeypatch):
    import lerobot_sdk.datasets as module
    def fail(*args):raise OSError('injected disk failure during copy')
    monkeypatch.setattr(module.shutil,'copyfile',fail)
    destination=tmp_path/'broken-export'
    with pytest.raises(OperationFailed):DatasetManager().export(ready,destination).wait(60)
    assert not destination.exists()
    assert not list(tmp_path.glob('.broken-export.staging-*'))
    assert DatasetManager().inspect(ready).native_readable


def test_export_refuses_overlap_and_conflict(ready,tmp_path):
    manager=DatasetManager()
    with pytest.raises(ContractMismatch):manager.export(ready,ready.root/'nested').wait(60)
    conflict=tmp_path/'conflict';conflict.mkdir();(conflict/'mine.txt').write_text('keep')
    with pytest.raises(SDKError):manager.export(ready,conflict).wait(60)
    assert (conflict/'mine.txt').read_text()=='keep'


def test_staging_cannot_be_inspected(station):
    *_,create,tasks=station
    task=create()
    with pytest.raises(StateError):DatasetManager().inspect(task.directory)


def test_optional_sink_failure_preserves_local_data(ready):
    class Sink:
        def upload(self,ref):raise OSError('storage unavailable')
    with pytest.raises(OperationFailed):DatasetManager().upload(ready,Sink()).wait(60)
    assert DatasetManager().inspect(ready).native_readable


def test_redo_in_flight_reuses_operation(station):
    robot,leader,cfg,create,_=station
    task=create();entered=threading.Event();release=threading.Event()
    class SlowClear(SeeedWriter):
        def clear(self):
            entered.set();assert release.wait(15)
            super().clear()
    task._writer_factory=SlowClear
    task.start(robot,leader).wait(120);enough(task);task.save_episode().wait(30)
    task.start_episode().wait(5);enough(task)
    op=task.redo_episode()
    try:
        assert entered.wait(5)
        assert task.redo_episode() is op
        with pytest.raises(BusyError):task.start_episode()
    finally:release.set()
    op.wait(30)
    task.stop().wait(60)


def test_recovery_after_process_termination(station,tmp_path):
    import subprocess,sys
    robot,leader,cfg,_,tasks=station
    config_file=tmp_path/'config.json'
    config_file.write_text(json.dumps(cfg.to_dict()),encoding='utf-8')
    child_code='''
import sys,json,time,os
from pathlib import Path
from lerobot_sdk import *
from lerobot_sdk.testing import FakeRobot,simulation_config
from lerobot_sdk.testing.leader import ScriptedLeader
cfg=CollectionConfig(**json.loads(Path(sys.argv[1]).read_text()))
rc=simulation_config('collect-test',required_cameras=('front','wrist'),max_observation_age_s=2)
# Each test owns a distinct fake device; parallel/aborted runs must not collide.
identity='crash-child:'+str(cfg.root.resolve())
robot=RobotSession(rc,FakeRobot(resource_id=identity,cameras=('front','wrist'))).connect()
leader=ScriptedLeader(rc,dict(robot.get_arm_data().positions),resource_id=identity)
task=CollectionManager().create_task(cfg)
task.start(robot,leader).wait(120)
while task.status().current_frames<3:time.sleep(.02)
task.save_episode().wait(30)
(cfg.root/'committed.flag').write_text(str(os.getpid()))
while True:time.sleep(1)
'''
    with (tmp_path/'child.log').open('w') as log:
        child=subprocess.Popen([sys.executable,'-c',child_code,str(config_file)],stdout=log,stderr=log)
        try:
            until(lambda:(tmp_path/'committed.flag').exists() or child.poll() is not None,timeout=150)
            assert child.poll() is None,(tmp_path/'child.log').read_text()
        finally:
            # Windows venv python.exe is a redirector; terminate the actual
            # fixture PID it reports, not just its short-lived parent shim.
            if child.poll() is None:
                marker=tmp_path/'committed.flag'
                if marker.exists():
                    import os,signal
                    os.kill(int(marker.read_text()),signal.SIGTERM)
                else:child.terminate()
            child.wait(timeout=20)
    task=CollectionManager().open_task(tmp_path/cfg.name);tasks.append(task)
    assert task.status().state==CollectionState.RECOVERABLE
    assert task.status().committed_episodes==1
    task.start(robot,leader).wait(120)
    assert task.status().state==CollectionState.RESETTING and leader.calls==0
    ref=task.stop().wait(60)
    assert DatasetManager().inspect(ref).episodes==1
