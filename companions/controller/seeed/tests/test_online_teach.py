"""Online teaching reads hardware only, persists safely and never opens another port."""
import json
from pathlib import Path

import pytest
import seeed_ros
from seeed_arm import JOINTS, MARGIN
from test_seeed_ros import clock, make, ask


@pytest.fixture(autouse=True)
def direct(monkeypatch):
    monkeypatch.setattr(seeed_ros, 'DIRECT_CONTROL', True)


@pytest.mark.parametrize('name', seeed_ros.TEACH_PRESETS)
def test_records_each_preset_without_servo_writes(clock, monkeypatch, tmp_path, name):
    (tmp_path/'config.json').write_text(json.dumps({'keep': {'unchanged': True}}))
    follower,bus=make(clock,monkeypatch,tmp_path)
    reply=ask(follower,'teach',{'name':name})
    assert reply['ok']
    result=reply['result']
    assert result['name']==name and result['saved']
    assert result['motion_started'] is False and result['torque_changed'] is False
    assert follower.config()['keep']=={'unchanged':True}
    assert follower.config()[name]==result['value']
    if name.startswith('gripper_'):
        assert result['value']==2429
    else:
        assert set(result['value'])==set(seeed_ros.ARM_JOINTS)
        for joint,value in result['value'].items():
            assert JOINTS[joint].lo+MARGIN<=value<=JOINTS[joint].hi-MARGIN
        assert result['clamped']  # Folded fixture includes values inside the limit margin.
    assert bus.writes==[]


def test_busy_refuses_to_change_config(clock, monkeypatch, tmp_path):
    follower,bus=make(clock,monkeypatch,tmp_path)
    follower.active='moving-action'
    assert ask(follower,'teach',{'name':'park'})['error']['code']=='BUSY'
    assert not (tmp_path/'config.json').exists() and not bus.writes


@pytest.mark.parametrize('payload',[{}, {'name':'unknown'}, {'name':'park','extra':True}])
def test_invalid_payload_cannot_change_config(clock, monkeypatch, tmp_path, payload):
    follower,bus=make(clock,monkeypatch,tmp_path)
    assert ask(follower,'teach',payload)['error']['code']=='INVALID_ARGUMENT'
    assert not (tmp_path/'config.json').exists() and not bus.writes


def test_duplicate_id_reuses_saved_value_and_conflicts_are_rejected(clock, monkeypatch, tmp_path):
    follower,bus=make(clock,monkeypatch,tmp_path)
    first=ask(follower,'teach',{'name':'park'},'record-once')
    monkeypatch.setattr(follower.arm,'positions',lambda: (_ for _ in ()).throw(AssertionError('must not reread')))
    assert ask(follower,'teach',{'name':'park'},'record-once')==first
    assert ask(follower,'teach',{'name':'chest'},'record-once')['error']['code']=='IDEMPOTENCY_CONFLICT'
    assert not bus.writes


def test_fault_refuses_recording(clock, monkeypatch, tmp_path):
    follower,bus=make(clock,monkeypatch,tmp_path,temps=lambda t:56)
    assert ask(follower,'teach',{'name':'park'})['error']['code']=='FAULT_LATCHED'
    assert not (tmp_path/'config.json').exists() and not bus.writes


def test_restart_keeps_record_and_duplicate_receipt(clock, monkeypatch, tmp_path):
    follower,bus=make(clock,monkeypatch,tmp_path)
    first=ask(follower,'teach',{'name':'gripper_open'},'persist-record')
    again=seeed_ros.Follower(follower.arm,tmp_path,background=False)
    assert again.config()['gripper_open']==2429
    assert ask(again,'teach',{'name':'gripper_open'},'persist-record')==first
    assert not bus.writes


def test_failed_atomic_replace_preserves_previous_config(clock, monkeypatch, tmp_path):
    path=tmp_path/'config.json'
    path.write_text('{"park":{"existing":123}}')
    follower,bus=make(clock,monkeypatch,tmp_path)
    original=Path.replace
    def fail_config_replace(self,target):
        if target==path:
            raise OSError('test replacement failure')
        return original(self,target)
    monkeypatch.setattr(Path,'replace',fail_config_replace)
    assert ask(follower,'teach',{'name':'park'})['error']['code']=='INTERNAL_ERROR'
    assert json.loads(path.read_text())=={'park':{'existing':123}}
    assert not bus.writes
