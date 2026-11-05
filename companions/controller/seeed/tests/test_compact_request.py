"""Configuration publishers send compact JSON; protocol defaults belong here."""
import json

import pytest
import seeed_ros
from test_seeed_ros import clock, make


@pytest.fixture(autouse=True)
def direct(monkeypatch):
    monkeypatch.setattr(seeed_ros, 'DIRECT_CONTROL', True)


def test_compact_inspect_and_catalog(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path)
    for operation in ('inspect', 'catalog'):
        reply = follower.handle(json.dumps({'operation': operation, 'payload': {}}), False)
        assert reply['ok'] and reply['request_id'].startswith('request-')
    assert not bus.writes


@pytest.mark.parametrize('name', seeed_ros.TEACH_PRESETS)
def test_compact_teach_and_duplicate(clock, monkeypatch, tmp_path, name):
    follower, bus = make(clock, monkeypatch, tmp_path)
    packet = {'operation': 'teach', 'payload': {'name': name}, 'request_id': 'teach-once'}
    first = follower.handle(json.dumps(packet), False)
    assert first['ok'] and first['result']['saved']
    assert follower.handle(json.dumps(packet), False) == first
    packet['payload']['name'] = 'chest' if name != 'chest' else 'park'
    assert follower.handle(json.dumps(packet), False)['error']['code'] == 'IDEMPOTENCY_CONFLICT'
    assert not bus.writes


@pytest.mark.parametrize('action', ['ARM_RELEASE', 'LOOSEN', 'GRASP', 'HOLD_ARM', 'HOLD_GRASP', 'MOVE_GRASP'])
def test_controller_defaults_single_arm(action):
    result = seeed_ros.verify(seeed_ros.normalize_request({
        'operation': 'execute', 'payload': {'action_id': action}, 'request_id': 'test-defaults'}))
    assert result['payload']['parameters']['side'] == 'right'
    assert result['payload']['part_id'] == 2
    assert result['payload']['actionType'] == 'ExecutionAction'


@pytest.mark.parametrize('parameters', [
    {'target_position': {}}, {'speed': float('nan')}, {},
    {'target_position': dict(x=True,y=0,z=0,roll=0,pitch=0,yaw=0)},
])
def test_invalid_pose_rejected_before_queue(clock, monkeypatch, tmp_path, parameters):
    follower, bus = make(clock, monkeypatch, tmp_path)
    result = follower.handle(json.dumps({'operation': 'execute', 'payload': {
        'action_id': 'ARM_MOVETO', 'parameters': parameters}}), True)
    assert not result['ok'] and follower.active is None and not bus.writes


def test_valid_six_dimensional_pose():
    pose = dict(x=.2,y=0,z=.15,roll=0,pitch=0,yaw=0)
    packet = seeed_ros.normalize_request({'operation': 'execute', 'payload': {
        'action_id': 'ARM_MOVETO', 'parameters': {'target_position': pose, 'speed': .5}}})
    assert packet['payload']['parameters'] == {'target_position': pose, 'speed': .5}


@pytest.mark.parametrize('extra', [{'expires_at': 0}, {'robot_id': 'other'}, {'request_id': ''}])
def test_partial_envelope_or_invalid_id_rejected(extra):
    with pytest.raises(seeed_ros.Error):
        seeed_ros.verify(seeed_ros.normalize_request({'operation': 'inspect', 'payload': {}, **extra}))


def test_compact_disabled_in_leased_mode(monkeypatch):
    monkeypatch.setattr(seeed_ros, 'DIRECT_CONTROL', False)
    with pytest.raises(seeed_ros.Error):
        seeed_ros.normalize_request({'operation': 'inspect', 'payload': {}})


def test_wrong_topic_rejected(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path)
    reply = follower.handle('{"operation":"catalog","payload":{}}', True)
    assert reply['error']['code'] == 'WRONG_TOPIC' and not bus.writes
