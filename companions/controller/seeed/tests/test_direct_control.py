import json
import pytest
import seeed_ros
from seeed_arm import JOINTS
from test_seeed_ros import clock, make, ask, STAND
from seeed_ros import Follower, envelope


@pytest.fixture(autouse=True)
def direct(monkeypatch):
    monkeypatch.setattr(seeed_ros, 'DIRECT_CONTROL', True)


def execute(follower, action, parameters, rid='direct-action'):
    payload={'action_id':action,'actionType':'ExecutionAction','actionCategory':'BuildinAction',
             'part_id':2,'parameters':parameters}
    accepted=ask(follower,'execute',payload,rid)
    return accepted,ask(follower,'job',{'request_id':rid}).get('result')


def test_gripper_uses_50_percent_runtime_torque_limit(clock, monkeypatch, tmp_path):
    follower,bus=make(clock,monkeypatch,tmp_path)
    _,job=execute(follower,'LOOSEN',{'side':'right'})
    assert job['state']=='SUCCEEDED'
    assert JOINTS['gripper'].torque_cap==500
    assert bus.peek(6,48)==500
    assert [JOINTS[name].torque_cap for name in ('shoulder_pan','shoulder_lift','elbow_flex','wrist_flex','wrist_roll')]==[250,400,350,250,150]


def test_xyz_and_pose_without_bind_epoch_confirmation_or_heartbeat(clock, monkeypatch, tmp_path):
    follower,bus=make(clock,monkeypatch,tmp_path,pose={**STAND,'gripper':2429})
    monkeypatch.setattr(seeed_ros,'LEASE_ACTIVE',.01)
    goal=ask(follower,'inspect',{})['result']['native']['tool']
    assert not follower.bound and not follower.workspace_confirmed
    accepted,job=execute(follower,'MOVE_GRASP',{'side':'right','direction':'up','distance':.01},'direct-xyz')
    assert accepted['ok'] and job['success'],job
    params={'target_position':dict(zip(('x','y','z','roll','pitch','yaw'),goal['position_m']+goal['orientation_rpy'])),'speed':.1}
    _,job=execute(follower,'ARM_MOVETO',params,'direct-pose')
    assert job['success'],job
    assert not follower.bound and not follower.workspace_confirmed


def test_stop_needs_no_epoch_even_when_never_bound(clock, monkeypatch, tmp_path):
    follower,bus=make(clock,monkeypatch,tmp_path)
    reply=ask(follower,'stop',{})
    assert reply['ok'] and reply['result']['state']=='STOPPED'
    assert not bus.writes


def test_idempotency_survives_restart_without_epoch(clock, monkeypatch, tmp_path):
    follower,bus=make(clock,monkeypatch,tmp_path)
    _,job=execute(follower,'LOOSEN',{'side':'right'},'direct-once')
    assert job['success']
    writes=len(bus.writes)
    again=Follower(follower.arm,tmp_path,background=False)
    _,retry=execute(again,'LOOSEN',{'side':'right'},'direct-once')
    assert retry==job and len(bus.writes)==writes
    conflict,_=execute(again,'GRASP',{'side':'right'},'direct-once')
    assert conflict['error']['code']=='IDEMPOTENCY_CONFLICT' and len(bus.writes)==writes


def test_fault_and_parameter_checks_still_prevent_writes(clock, monkeypatch, tmp_path):
    follower,bus=make(clock,monkeypatch,tmp_path,temps=lambda t:56)
    _,job=execute(follower,'LOOSEN',{'side':'right'})
    assert job['error_code']=='FAULT_LATCHED' and not bus.writes


def test_unreachable_pose_still_rejected_before_torque(clock, monkeypatch, tmp_path):
    follower,bus=make(clock,monkeypatch,tmp_path,pose={**STAND,'gripper':2429})
    _,job=execute(follower,'ARM_MOVETO',{'target_position':{'x':.9,'y':0,'z':.2,'roll':0,'pitch':0,'yaw':0},'speed':.1})
    assert job['error_code']=='UNREACHABLE' and not bus.writes


def test_direct_mode_keeps_stop_during_motion(clock, monkeypatch, tmp_path):
    (tmp_path/'config.json').write_text(json.dumps({'chest':STAND}))
    follower,bus=make(clock,monkeypatch,tmp_path)
    start=clock.t
    clock.hook=lambda t: t-start>1 and follower.cancel.set()
    _,job=execute(follower,'HOLD_ARM',{'side':'right'})
    clock.hook=None
    assert job['error_code']=='STOPPED' and job['stop']['hold_confirmed']


def test_watchdog_does_not_require_heartbeat_in_direct_mode(clock, monkeypatch, tmp_path):
    follower,bus=make(clock,monkeypatch,tmp_path)
    follower.active='in-progress'
    follower.lease_until=-1
    clock.hook=lambda t:setattr(follower,'closed',True)
    follower._watchdog()
    clock.hook=None
    assert not follower.cancel.is_set()


def test_direct_mode_advertised_without_authentication(clock, monkeypatch, tmp_path):
    follower,bus=make(clock,monkeypatch,tmp_path)
    state=ask(follower,'inspect',{})['result']
    assert state['control_mode']=='direct'
    assert all(state[k] is False for k in ('requires_bind','requires_epoch','requires_heartbeat','requires_workspace_confirmation'))
    control=ask(follower,'catalog',{})['result']['control']
    assert control['mode']=='direct' and not control['requires_epoch']
    packet=envelope('inspect',{},'no-auth')
    assert len(packet)==8
    assert follower.handle(json.dumps(packet),False)['ok']
    assert not bus.writes
