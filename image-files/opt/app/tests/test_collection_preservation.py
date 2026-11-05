"""Durable unsubmitted episodes: real recorder/writer with simulated devices."""
import json
import threading
import time

import pytest

from lerobot_sdk import (CollectionConfig, CollectionManager, CollectionState,
    ContractMismatch, DatasetManager, OperationFailed, RobotSession,
    SessionState, WaitTimeout)
from lerobot_sdk.testing import FakeRobot, simulation_config
from lerobot_sdk.testing.leader import ScriptedLeader

pytest.importorskip('lerobot')


@pytest.fixture
def capture(tmp_path):
    from lerobot_sdk.adapters.seeed_dataset import upstream
    upstream()  # Native imports are not the readiness condition under test.
    config=simulation_config('preservation-test', required_cameras=('front',),
                             max_observation_age_s=2)
    robot=RobotSession(config,FakeRobot(resource_id=str(tmp_path),cameras=('front',)),
                       lock_dir=tmp_path/'locks').connect()
    leader=ScriptedLeader(config,dict(robot.get_arm_data().positions),resource_id=str(tmp_path))
    cfg=CollectionConfig('batch','durable current episode',tmp_path,10,'preserve',
                         config.robot_id,config.calibration_hash,config.units,
                         {'front':(12,16,3)},min_free_bytes=1)
    tasks=[]
    def create():
        task=CollectionManager().create_task(cfg);tasks.append(task);return task
    yield robot,leader,create,tasks
    for task in tasks:
        if task._thread and task._thread.is_alive():
            try:task.stop(False).wait(30)
            except Exception:pass
            task._thread.join(30)
    robot.close()


def test_start_waits_for_first_durable_frame(capture):
    robot,leader,create,_=capture
    task=create();entered=threading.Event();release=threading.Event()
    original=leader.get_action
    def block_first(*,timeout_s):
        entered.set();assert release.wait(20)
        return original(timeout_s=timeout_s)
    leader.get_action=block_first
    operation=task.start(robot,leader)
    try:
        assert entered.wait(20)
        with pytest.raises(WaitTimeout):operation.wait(.01)
    finally:release.set()
    result=operation.wait(30)
    assert result.current_frames>=1
    current=json.loads((task.directory/'task.json').read_text())['current']
    assert len(current['files'])>=1
    assert (task.directory/current['files'][0]['path']).is_file()


def test_first_frame_failure_is_start_failure(capture):
    robot,leader,create,_=capture
    task=create()
    def fail(*,timeout_s):raise TimeoutError('leader unavailable before first frame')
    leader.get_action=fail
    with pytest.raises(OperationFailed,match='leader unavailable'):
        task.start(robot,leader).wait(30)
    task._thread.join(10)
    assert task.status().state==CollectionState.RECOVERABLE
    assert robot.status().state==SessionState.HOLDING


def test_suspend_reopen_resume_keeps_same_unsubmitted_files(capture):
    robot,leader,create,tasks=capture
    task=create();task.start(robot,leader).wait(30)
    task.suspend().wait(30);task._thread.join(10)
    current=json.loads((task.directory/'task.json').read_text())['current']
    assert current['files'] and task.status().committed_episodes==0
    originals=[(task.directory/f['path'],(task.directory/f['path']).read_bytes()) for f in current['files']]
    assert robot.status().state==SessionState.HOLDING
    restored=CollectionManager().open_task(task.directory);tasks.append(restored)
    calls=leader.calls
    restored.start(robot,leader).wait(30)
    assert restored.status().state==CollectionState.RESETTING
    assert leader.calls==calls  # opening/rebuilding cannot replay motion
    resumed=restored.resume_episode().wait(30)
    assert resumed.current_frames>len(originals)
    assert restored._episode_dir==task.directory/current['directory']
    for path,content in originals:assert path.read_bytes()==content
    saved=restored.save_episode().wait(30)
    assert saved.frames>=len(originals)+1 and saved.episode_index==0
    ref=restored.stop().wait(60)
    assert DatasetManager().inspect(ref).frames==saved.frames


@pytest.mark.parametrize('change',['bytes','outside'])
def test_open_task_rejects_corrupt_or_outside_current(capture,tmp_path,change):
    robot,leader,create,_=capture
    task=create();task.start(robot,leader).wait(30)
    task.suspend().wait(30);task._thread.join(10)
    journal=json.loads((task.directory/'task.json').read_text())
    if change=='bytes':(task.directory/journal['current']['files'][0]['path']).write_bytes(b'corrupted')
    else:
        journal['current']['directory']='../foreign'
        (task.directory/'task.json').write_text(json.dumps(journal))
    with pytest.raises(ContractMismatch):CollectionManager().open_task(task.directory)


def test_frozen_commits_cannot_change_live_journal(capture):
    robot,leader,create,_=capture
    task=create();task.start(robot,leader).wait(30)
    task.save_episode().wait(30)
    frozen=task.frozen_commits()
    expected=frozen[0]['files'][0]['sha256']
    frozen[0]['files'][0]['sha256']='changed'
    assert task.frozen_commits()[0]['files'][0]['sha256']==expected
    assert json.loads((task.directory/'task.json').read_text())['commits'][0]['files'][0]['sha256']==expected


def test_preserved_episode_can_resume_with_recreated_robot_session(capture,tmp_path):
    robot,leader,create,tasks=capture
    with robot._lock:robot._sequence=1000
    task=create();task.start(robot,leader).wait(30)
    task.suspend().wait(30);robot.close()
    last=task._last_sequence
    fresh=RobotSession(robot.config,FakeRobot(resource_id=str(tmp_path)+'new',cameras=('front',)),
                       lock_dir=tmp_path/'new-locks').connect()
    try:
        new_leader=ScriptedLeader(fresh.config,dict(fresh.get_arm_data().positions),resource_id=str(tmp_path)+'new')
        restored=CollectionManager().open_task(task.directory);tasks.append(restored)
        restored.start(fresh,new_leader).wait(30)
        assert new_leader.calls==0
        result=restored.resume_episode().wait(30)
        assert result.current_frames>=2 and restored._last_sequence>last
        restored.suspend().wait(30)
    finally:fresh.close()


def test_source_failure_retains_current_and_can_be_suspended_without_restarting(capture):
    robot,leader,create,_=capture
    task=create();task.start(robot,leader).wait(30)
    def fail(*,timeout_s):raise TimeoutError('source disconnected')
    leader.get_action=fail
    task._thread.join(10)
    assert task.status().state==CollectionState.RECOVERABLE
    assert task.status().current_frames>=1
    saved=json.loads((task.directory/'task.json').read_text())['current']['files']
    result=task.suspend().wait(30)
    assert result.current_frames==len(saved)
    assert not task._thread.is_alive()
    assert json.loads((task.directory/'task.json').read_text())['current']['files']==saved
    assert robot.status().state==SessionState.HOLDING


def test_resume_rejects_clock_rollback_without_motion(capture,monkeypatch):
    from dataclasses import replace
    robot,leader,create,tasks=capture
    task=create();task.start(robot,leader).wait(30)
    task.suspend().wait(30)
    restored=CollectionManager().open_task(task.directory);tasks.append(restored)
    restored.start(robot,leader).wait(30)
    calls=leader.calls;original=restored._observation
    def rollback():
        obs=original()
        return replace(obs,arm=replace(obs.arm,captured_monotonic_ns=task._last_stamp))
    monkeypatch.setattr(restored,'_observation',rollback)
    with pytest.raises(ContractMismatch,match='clock'):
        restored.resume_episode().wait(30)
    restored._thread.join(10)
    assert leader.calls==calls and robot.status().state==SessionState.HOLDING
    assert restored._current==task._current


def test_redo_crash_after_delete_never_leaves_journal_pointing_to_missing_files(capture,monkeypatch):
    from types import SimpleNamespace
    import lerobot_sdk.data.collection as module
    robot,leader,create,tasks=capture
    task=create();task.start(robot,leader).wait(30);task.suspend().wait(30)
    directory=task._episode_dir
    task._writer=SimpleNamespace(clear=lambda:None)
    real_remove=module.remove_owned_tree
    def remove_then_crash(path,parent):
        real_remove(path,parent)
        raise SystemExit('process died after deleting the redo directory')
    with monkeypatch.context() as patch:
        patch.setattr(module,'remove_owned_tree',remove_then_crash)
        with pytest.raises(SystemExit):task._discard()
    assert not directory.exists()
    restored=CollectionManager().open_task(task.directory);tasks.append(restored)
    assert restored.status().current_frames==0
    assert restored.frozen_commits()==[]
    restored.start(robot,leader).wait(30)
    assert restored.status().state==CollectionState.RESETTING
    assert restored.start_episode().wait(30).current_frames>=1


def test_unjournaled_fsynced_tail_is_preserved_and_cannot_collide_on_resume(capture,monkeypatch):
    from types import SimpleNamespace
    import numpy as np
    import lerobot_sdk.data.collection as module
    robot,leader,create,tasks=capture
    task=create();task.start(robot,leader).wait(30);task.suspend().wait(30)
    saved=json.loads((task.directory/'task.json').read_text())
    ghost=module.CollectionTask(task.directory,saved)
    frame=module._raw_frame(task.directory/ghost._current[-1]['path'])
    frame['sdk.source_sequence']=frame['sdk.source_sequence']+1
    frame['sdk.captured_monotonic_ns']=frame['sdk.captured_monotonic_ns']+1000
    frame['sdk.sent_monotonic_ns']=np.maximum(frame['sdk.sent_monotonic_ns'],frame['sdk.captured_monotonic_ns'])
    ghost._robot=SimpleNamespace(_collection_step=lambda target:None)
    ghost._leader=leader;ghost._writer=SimpleNamespace(add=lambda value:None)
    ghost._episode_started=time.monotonic()
    ghost._observation=lambda:SimpleNamespace(source_metadata=None,arm=SimpleNamespace(
        sequence=int(frame['sdk.source_sequence'].item()),captured_monotonic_ns=int(frame['sdk.captured_monotonic_ns'].item())))
    before=set(ghost._episode_dir.iterdir())
    with monkeypatch.context() as patch:
        patch.setattr(module,'make_frame',lambda *args:frame)
        patch.setattr(ghost,'_persist',lambda *args,**kwargs:(_ for _ in ()).throw(SystemExit('crash after raw fsync, before current journal')))
        with pytest.raises(SystemExit):ghost._frame()
    tails=set(ghost._episode_dir.iterdir())-before
    assert len(tails)==1
    tail=tails.pop();content=tail.read_bytes()
    restored=CollectionManager().open_task(task.directory);tasks.append(restored)
    assert restored.status().current_frames==len(saved['current']['files'])  # No invented frame acknowledgement.
    restored.start(robot,leader).wait(30)
    assert restored.resume_episode().wait(30).current_frames>len(saved['current']['files'])
    assert tail.read_bytes()==content
    assert all(restored.directory/item['path']!=tail for item in restored._current)


def test_current_journal_refuses_absolute_frame_path_even_inside_task(capture):
    robot,leader,create,_=capture
    task=create();task.start(robot,leader).wait(30);task.suspend().wait(30)
    journal=json.loads((task.directory/'task.json').read_text())
    item=journal['current']['files'][0]
    item['path']=str(task.directory/item['path'])
    (task.directory/'task.json').write_text(json.dumps(journal))
    with pytest.raises(ContractMismatch):CollectionManager().open_task(task.directory)
