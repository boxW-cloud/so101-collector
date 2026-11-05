"""redis_capture：只在采集会话期间消费 Redis 采集队列（capture/v5），转成现有录制格式。用假 Redis 测。"""
import base64
import io
import json
import math
import threading
import time
from pathlib import Path

import numpy as np
import pytest
from PIL import Image

from deployment_runtime import redis_capture as rc
from deployment_runtime.direct_follower import is_direct, sim_like
from deployment_runtime.store import Fault

SHAPE = (40, 64)          # 测试用小图


class FakeRedis:
    """进程内的假 Redis：List（LPUSH / BRPOP / RPOP / LLEN / LINDEX）和 String（GET）。"""

    def __init__(self):
        self.lists, self.strings, self.calls = {}, {}, []
        self.cond = threading.Condition()

    def lpush(self, key, value):
        with self.cond:
            self.lists.setdefault(key, []).insert(0, value)
            self.cond.notify_all()

    def client(self):
        return FakeClient(self)


class FakeClient:
    def __init__(self, redis):
        self.redis = redis

    def call(self, *args):
        r = self.redis
        r.calls.append(args[0])
        command = args[0]
        with r.cond:
            if command == 'LLEN':
                return len(r.lists.get(args[1], []))
            if command == 'RPOP':
                items = r.lists.get(args[1], [])
                return items.pop() if items else None
            if command == 'LINDEX':
                items = r.lists.get(args[1], [])
                return items[int(args[2])] if items else None
            if command == 'GET':
                return r.strings.get(args[1])
            if command == 'SET':
                if 'NX' in args[3:] and args[1] in r.strings:
                    return None
                r.strings[args[1]] = args[2].encode() if isinstance(args[2], str) else args[2]
                return 'OK'
            if command == 'DEL':
                return int(r.strings.pop(args[1], None) is not None)
            if command == 'BRPOP':
                key, timeout = args[1], float(args[2])
                if r.cond.wait_for(lambda: r.lists.get(key), timeout):
                    return [key.encode(), r.lists[key].pop()]
                return None
        raise AssertionError(command)

    def call_many(self, commands):
        return [self.call(*c) for c in commands]

    def close(self):
        pass


class Source:
    """模拟 Perceptor：往观测队列推观测（同时写图片 key），往动作队列推事件。"""

    def __init__(self, redis, process_id='p1'):
        # v10r15：供数开关由采集容器开始时自己打开，这里不再预先打开
        self.redis, self.process_id, self.n, self.ts = redis, process_id, 0, 100.0

    def observation(self, *, image=True, shape=SHAPE, state=None, gap=0, ts=None, push=True):
        self.n += 1
        self.ts = ts if ts is not None else self.ts + 0.2
        key = f'collection_image:front:sample_{self.process_id}_{self.n:012d}'
        if image:
            stream = io.BytesIO()
            Image.fromarray(np.full((*shape, 3), self.n % 256, dtype=np.uint8)).save(stream, format='JPEG')
            self.redis.strings[key] = stream.getvalue()
        message = {'schema_version': 'capture/v5', 'process_id': self.process_id, 'time_basis': 'process_monotonic',
                   'config_hash': 'h', 'data_gap_count': gap, 'observation_id': f'obs_{self.process_id}_{self.n:012d}',
                   'timestamp': self.ts, 'action_id': None, 'association_quality': 'unknown',
                   'state': state or {f'joint{i}': 0.1 * i for i in range(1, 7)}, 'images': {'front': key},
                   'sources': {'front': {'sampled_at': self.ts - 0.02}}, 'training_eligible': False}
        if push:
            self.redis.lpush('collection_observation_queue', json.dumps(message).encode())
        return message

    def deliver(self, message):
        self.redis.lpush('collection_observation_queue', json.dumps(message).encode())

    def action_event(self, action, event, event_id=None, **fields):
        message = {'schema_version': 'capture/v5', 'process_id': self.process_id, 'action_id': action, 'event': event,
                   'event_id': event_id or f'event_{action}_{event}', 'type': 'ARM_MOVE', **fields}
        self.redis.lpush('collection_action_queue', json.dumps(message).encode())

    def teleop(self, observation, positions, requested=None, delay=0.01, **fields):
        """每帧成对的实际下发目标（teleop/action/v1），指向 observation。"""
        message = {'schema_version': 'teleop/action/v1', 'process_id': observation['process_id'],
                   'action_id': 'act' + observation['observation_id'][3:], 'input_observation_id': observation['observation_id'],
                   'time_basis': 'process_monotonic', 'timestamp': observation['timestamp'] + delay,
                   'positions': positions, **({'requested_positions': requested} if requested is not None else {}), **fields}
        self.redis.lpush('collection_action_queue', json.dumps(message).encode())
        return message


def bursty_feed(source, stop, *, period=0.1, hold_at=0.8, hold_for=1.2, state=None):
    """Perceptor 每 period 秒采样一次（时间戳是真实采样时刻），但在 hold_at 秒处送达停 hold_for 秒，之后一起送到。"""
    def run():
        started, held = time.monotonic(), []
        while not stop.is_set():
            now = time.monotonic()
            message = source.observation(ts=now, push=False, state=state)
            if hold_at <= now - started < hold_at + hold_for:
                held.append(message)
            else:
                for old in held:
                    source.deliver(old)
                held.clear()
                source.deliver(message)
            stop.wait(period)
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def config(tmp_path, **overrides):
    value = {**rc.DEFAULTS, 'host': 'fake', 'calibration_hash': 'sha256:' + '0' * 64, 'cameras': {'front': list(SHAPE)},
             'camera_store': str(tmp_path / 'camera'), 'session_root': str(tmp_path / 'sessions'),
             'switch_owner': str(tmp_path / 'switch-owner.json'), 'connect_wait_s': 1.0, 'sample_wait_s': 0.6}
    value.update(overrides)
    return value


@pytest.fixture
def redis():
    return FakeRedis()


def adapter_for(tmp_path, redis, **overrides):
    return rc.RedisCaptureAdapter(config(tmp_path, **overrides), client_factory=redis.client)


def feed(source, count, interval=0.1):
    def run():
        for _ in range(count):
            time.sleep(interval)
            source.observation()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def test_to_slots_maps_six_joints_into_so101_slots():
    slots = rc.to_slots({'joint1': math.pi / 2, 'joint2': 0, 'joint3': 0, 'joint4': 0, 'joint5': -math.pi / 4, 'joint6': 0.0},
                        rc.DEFAULTS['joints'])
    assert slots['shoulder_pan.pos'] == pytest.approx(90) and slots['wrist_roll.pos'] == pytest.approx(-45)
    assert slots['gripper.pos'] == pytest.approx(50)
    assert rc.to_slots({'joint1': 1}, rc.DEFAULTS['joints']) is None
    assert rc.to_slots({**{f'joint{i}': 0 for i in range(1, 7)}, 'joint3': float('nan')}, rc.DEFAULTS['joints']) is None
    assert rc.to_slots({**{f'joint{i}': 0 for i in range(1, 7)}, 'joint6': 7.0}, rc.DEFAULTS['joints']) is None


def test_dataset_schema_writes_robot_joint_names_and_raw_radians():
    from types import SimpleNamespace
    from lerobot_sdk.adapters.seeed_dataset import dataset_vector, joint_names_of, joint_units_of, robot_type_of
    from lerobot_sdk.data.contracts import joint_schema
    schema = joint_schema(rc.dataset_schema_of({**rc.DEFAULTS}))
    config = SimpleNamespace(dataset_schema=schema)
    assert joint_names_of(config) == [f'joint{i}' for i in range(1, 7)] and joint_units_of(config) == ['rad'] * 6
    assert robot_type_of(config) == 'bobac3'
    rng = np.random.default_rng(0)
    for _ in range(50):
        raw = {f'joint{i}': float(v) for i, v in enumerate(rng.uniform(-2 * math.pi, 2 * math.pi, 6), 1)}
        back = dataset_vector(config, rc.to_slots(raw, rc.DEFAULTS['joints']))
        assert back == pytest.approx([raw[f'joint{i}'] for i in range(1, 7)], abs=1e-9)


def test_so101_targets_keep_the_old_dataset_format():
    from types import SimpleNamespace
    from lerobot_sdk import DEGREE_UNITS, JOINT_NAMES
    from lerobot_sdk.adapters.seeed_dataset import features, joint_names_of, robot_type_of
    config = SimpleNamespace(dataset_schema=None, units=DEGREE_UNITS, camera_shapes={'front': (40, 64, 3)})
    assert joint_names_of(config) == list(JOINT_NAMES) and robot_type_of(config) == 'so101_follower'
    assert features(config)['observation.state']['names'] == list(JOINT_NAMES)


def test_bad_dataset_schemas_are_rejected():
    from lerobot_sdk.data.contracts import ConfigurationError, joint_schema, manifest_joint_contract_ok
    good = rc.dataset_schema_of({**rc.DEFAULTS})
    for bad in ({**good, 'robot_type': 'so101_follower'}, {**good, 'joint_names': ['a'] * 6}, {**good, 'units': ['furlong'] * 6},
                {**good, 'from_internal': [[0, 1]] * 6}, {k: v for k, v in good.items() if k != 'units'},
                {**good, 'action': 'later'}, {**good, 'extra': 1}):
        with pytest.raises(ConfigurationError):
            joint_schema(bad)
    assert joint_schema(good)['action'] == 'next_observation_state'          # Bobac3：action = 下一帧实测状态
    without = joint_schema({k: v for k, v in good.items() if k != 'action'})
    assert 'action' not in without                                         # 不写 action 的 schema 和以前一样
    assert manifest_joint_contract_ok('bobac3', good['joint_names'], good['units'])
    assert not manifest_joint_contract_ok('so101_follower', good['joint_names'], good['units'])


def test_redis_target_reuses_local_source_gates():
    import inspect
    from deployment_runtime import sessions
    robot = {'kind': 'physical', 'provenance': rc.PROVENANCE}
    assert is_direct(robot) and sim_like(robot) and rc.is_redis_capture(robot)
    assert not is_direct({'kind': 'physical', 'provenance': 'physical_native_relative_teleop'})
    # 结束采集时：本机数据源不调 stop_teleop，核对时承认它的来源（2026-10-29 实测 finish 卡在这两处）
    assert rc.PROVENANCE in sessions.LOCAL_SOURCES
    code = inspect.getsource(sessions)
    assert "v['source']!=DIRECT_FOLLOWER" not in code and code.count("v['source'] not in LOCAL_SOURCES") == 2
    assert "('physical_native_relative_teleop',*LOCAL_SOURCES)" in code


def test_queues_are_untouched_until_collection_starts(tmp_path, redis):
    source = Source(redis)
    adapter = adapter_for(tmp_path, redis)
    adapter.describe()
    adapter.queue_status()
    for _ in range(3):
        source.observation()
    assert redis.calls == [] and len(redis.lists['collection_observation_queue']) == 3


def test_connect_drops_backlog_then_waits_for_a_fresh_observation(tmp_path, redis):
    source = Source(redis)
    for _ in range(5):
        old = source.observation()
    source.action_event('a0', 'request')
    feeder = feed(source, 3)
    adapter = adapter_for(tmp_path, redis)
    adapter.connect()
    sample = adapter.sample()
    assert sample['clock_mapping']['observation_id'] != old['observation_id']     # 积压的 5 条都丢掉了
    assert adapter.consumer.stats['drained_observation'] == 5 and adapter.consumer.stats['drained_action'] == 1
    feeder.join()
    adapter.preempt_stop(close=True)
    adapter.hold()


def test_no_observations_fail_on_the_first_sample_not_on_connect(tmp_path, redis):
    """connect 失败会让批次卡住（2026-10-29 实测），所以 connect 总是成功，没有数据由第一次 sample 报出来。"""
    adapter = adapter_for(tmp_path, redis, connect_wait_s=0.3)
    adapter.connect()
    with pytest.raises(Fault) as error:
        adapter.sample()
    assert error.value.code == 'SOURCE_STALLED' and 'Perceptor 在出数吗' in str(error.value)
    adapter.close()
    assert adapter.consumer is None


def test_collection_opens_the_supply_switch_and_closes_it_at_the_end(tmp_path, redis):
    """v10r15：开始采集时采集容器自己打开供数开关（记下是自己开的），结束会话时删掉。"""
    source = Source(redis)
    feeder = feed(source, 2)
    adapter = adapter_for(tmp_path, redis)
    assert 'collection_enabled' not in redis.strings
    adapter.connect()
    adapter.sample()
    assert redis.strings['collection_enabled'] == b'1' and (tmp_path / 'switch-owner.json').exists()
    adapter.hold()                                   # 结束这一条：不关开关
    assert redis.strings['collection_enabled'] == b'1'
    adapter.preempt_stop(close=True)
    adapter.hold()                                   # 结束会话：关开关
    assert 'collection_enabled' not in redis.strings and not (tmp_path / 'switch-owner.json').exists()
    feeder.join()


def test_switch_already_on_fails_on_the_first_sample_and_is_left_alone(tmp_path, redis):
    """开关已经是开着的：可能有别的采集在用，不抢、也不替它关；第一次 sample 报出来。"""
    redis.strings['collection_enabled'] = b'1'
    adapter = adapter_for(tmp_path, redis, connect_wait_s=0.3)
    adapter.connect()
    with pytest.raises(Fault) as error:
        adapter.sample()
    assert error.value.code == 'SOURCE_FAILED' and '已经是开着的' in str(error.value) and 'DEL collection_enabled' in str(error.value)
    adapter.close()
    assert redis.strings['collection_enabled'] == b'1' and not (tmp_path / 'switch-owner.json').exists()


def test_switch_left_on_by_a_crash_is_removed_at_startup(tmp_path, redis):
    cfg = config(tmp_path)
    assert rc.release_stale_switch(cfg, redis.client) == 'none'           # 没有记录：别人开的不动
    redis.strings['collection_enabled'] = b'1'
    rc.write_switch_owner(cfg)                                             # 上次是本容器开的，没来得及关
    assert rc.release_stale_switch(cfg, redis.client) == 'deleted_stale_switch'
    assert 'collection_enabled' not in redis.strings and not (tmp_path / 'switch-owner.json').exists()


def test_unreachable_redis_fails_on_the_first_sample(tmp_path):
    def unreachable():
        raise OSError('connection refused')
    adapter = rc.RedisCaptureAdapter(config(tmp_path, connect_wait_s=0.3), client_factory=unreachable)
    adapter.connect()
    with pytest.raises(Fault) as error:
        adapter.sample()
    assert error.value.code == 'SOURCE_FAILED' and '连不上 Redis' in str(error.value)
    adapter.close()


def test_sample_returns_each_observation_once_with_local_increasing_time(tmp_path, redis):
    source = Source(redis)
    feed(source, 1, interval=0.05)
    adapter = adapter_for(tmp_path, redis)
    adapter.connect()
    first = adapter.sample()
    feeder = feed(source, 2, interval=0.15)
    second = adapter.sample()                       # 等到下一条新观测才返回
    third = adapter.sample()
    feeder.join()
    stamps = [first['captured_monotonic_ns'], second['captured_monotonic_ns'], third['captured_monotonic_ns']]
    assert stamps == sorted(stamps) and len(set(stamps)) == 3
    ids = [s['clock_mapping']['observation_id'] for s in (first, second, third)]
    assert len(set(ids)) == 3
    with pytest.raises(Fault) as error:
        adapter.sample()                            # 没有新观测：供数中断
    assert error.value.code == 'SOURCE_STALLED'
    adapter.close()


def test_sample_payload_is_what_the_recording_side_expects(tmp_path, redis):
    source = Source(redis)
    feed(source, 1, interval=0.05)
    adapter = adapter_for(tmp_path, redis)
    adapter.connect()
    sample = adapter.sample()
    assert set(sample['positions']) == {n + '.pos' for n in rc.NAMES}
    image = Image.open(io.BytesIO(base64.b64decode(sample['images']['front']['png'])))
    assert image.format == 'PNG' and image.size == (SHAPE[1], SHAPE[0])
    assert 0 <= sample['captured_monotonic_ns'] - sample['images']['front']['captured_monotonic_ns'] <= 0.03e9
    evidence = sample['clock_mapping']
    assert evidence['source'] == rc.PROVENANCE and evidence['raw_state']['joint1'] == pytest.approx(0.1)
    assert adapter.leader() == {'positions': sample['positions']}
    assert adapter.send(sample['positions']) == sample['positions']
    with pytest.raises(Fault):
        adapter.send({'shoulder_pan.pos': 1})
    adapter.close()


def test_late_burst_delivery_keeps_real_sampling_times(tmp_path, redis):
    """2026-10-29 202 实测：Perceptor 采样均匀，但送达会停约 1 秒后一起到。不能判成供数中断，时间要按真实采样间隔。"""
    source, stop = Source(redis), threading.Event()
    adapter = adapter_for(tmp_path, redis, sample_wait_s=1.4)
    bursty_feed(source, stop, hold_at=0.6, hold_for=1.2)
    try:
        adapter.connect()
        consumer = adapter.consumer
        samples = [adapter.sample() for _ in range(30)]       # 约 3 秒，中间有一次 1.2 秒的送达停顿
    finally:
        stop.set()
        adapter.close()
    stamps = [s['captured_monotonic_ns'] for s in samples]
    perceptor = [s['clock_mapping']['perceptor_timestamp'] for s in samples]
    assert stamps == sorted(stamps) and len(set(stamps)) == len(stamps)
    # 一起到的观测仍按采样间隔：本机时间差 = Perceptor 时间差（送达最快那条定下常数之后）
    local = [(b - a) / 1e9 for a, b in zip(stamps[5:], stamps[6:])]
    remote = [b - a for a, b in zip(perceptor[5:], perceptor[6:])]
    assert local == pytest.approx(remote, abs=0.005)
    assert consumer.max_delay >= 1.0                         # 送达停顿确实发生过，而且记了下来


def test_bad_images_are_skipped_with_a_reason(tmp_path, redis):
    source = Source(redis)
    adapter = adapter_for(tmp_path, redis, connect_wait_s=0.5)
    source.observation(image=False)
    feeder = threading.Timer(0.1, lambda: (source.observation(image=False), source.observation(shape=(10, 10))))
    feeder.start()
    adapter.connect()
    with pytest.raises(Fault) as error:
        adapter.sample()
    assert '10x10' in str(error.value) or '取不到' in str(error.value)
    adapter.close()


def test_episode_hold_keeps_consuming_but_session_close_stops(tmp_path, redis):
    source = Source(redis)
    feed(source, 1, interval=0.05)
    adapter = adapter_for(tmp_path, redis)
    adapter.connect()
    adapter.sample()
    adapter.hold()                                  # 保存 / 重采：不结束会话
    assert adapter.consumer is not None and adapter.consumer.alive()
    adapter.preempt_stop(close=True)
    adapter.hold()                                  # 结束采集
    assert adapter.consumer is None
    time.sleep(0.1)
    for _ in range(3):
        source.observation()
    time.sleep(1.2)
    assert len(redis.lists['collection_observation_queue']) == 3      # 停止后不再取数据


def test_action_events_merge_by_action_id(tmp_path, redis):
    source = Source(redis)
    feed(source, 1, interval=0.05)
    adapter = adapter_for(tmp_path, redis)
    adapter.connect()
    source.action_event('a1', 'request', t_request=1.0)
    source.action_event('a1', 'received', t_request=1.0, t_received=1.1)
    source.action_event('a1', 'received', t_request=1.0, t_received=1.1)          # 同一 event_id 重复
    source.action_event('a1', 'finish', t_request=1.0, t_end=2.0, motion_result='completed')
    source.action_event('a2', 'request')
    time.sleep(0.5)
    consumer = adapter.consumer
    adapter.close()
    merged = json.loads((consumer.directory / 'actions_merged.json').read_text(encoding='utf-8'))
    a1 = next(r for r in merged if r['action_id'] == 'a1')
    assert a1['events'] == ['request', 'received', 'finish'] and a1['t_end'] == 2.0 and a1['motion_result'] == 'completed'
    assert len(merged) == 2 and consumer.stats['duplicate_action_event'] == 1
    summary = json.loads((consumer.directory / 'summary.json').read_text(encoding='utf-8'))
    assert summary['stats']['observations'] >= 1 and summary['actions'] == 2


# ══════════ 2026-11-03：每帧成对的实际下发目标（teleop/action/v1，COLLECTION_REDIS_ACTION=paired） ══════════

def test_paired_action_is_attached_by_observation_id(tmp_path, redis):
    source = Source(redis)
    adapter = adapter_for(tmp_path, redis, action='paired', sample_wait_s=1.0)
    adapter.connect()
    time.sleep(0.2)                                                        # 等开始时丢积压做完，再推新数据
    state = {f'joint{i}': 0.1 * i for i in range(1, 7)}
    first = source.observation(state=state)
    source.teleop(first, {**state, 'joint1': 0.15}, requested={**state, 'joint1': 0.2})
    source.action_event('a1', 'request')                                   # 平台指令事件照旧存档，不参与配对
    sample = adapter.sample()
    paired = sample['source_action']
    assert paired['observation_id'] == first['observation_id'] and paired['delay_s'] == pytest.approx(0.01)
    assert paired['positions']['shoulder_pan.pos'] == pytest.approx(math.degrees(0.15))    # 和观测一样换成 SDK 槽位
    assert paired['requested_positions']['shoulder_pan.pos'] == pytest.approx(math.degrees(0.2))
    assert sample['positions']['shoulder_pan.pos'] == pytest.approx(math.degrees(0.1))     # 观测本身不变
    # 第二条观测没有成对目标：等 action_wait_s 后照样返回观测，只是不带 source_action（SDK 会跳过这一帧）
    second = source.observation(state=state)
    started = time.monotonic()
    sample = adapter.sample()
    assert 'source_action' not in sample and time.monotonic() - started >= rc.DEFAULTS['action_wait_s'] - 0.05
    # 指向别的观测的目标、下发时刻早于观测的目标、关节不全的目标：都不配
    third = source.observation(state=state)
    source.teleop(second, state)                                           # 迟到的、给上一条观测的
    source.teleop(third, state, delay=-0.05)                               # 先下发后观测：不合理
    assert 'source_action' not in adapter.sample()
    fourth = source.observation(state=state)
    source.teleop(fourth, {'joint1': 0.1})
    assert 'source_action' not in adapter.sample()
    source.redis.lpush('collection_action_queue', json.dumps({'schema_version': 'teleop/action/v1', 'process_id': 'p1'}).encode())
    time.sleep(0.3)
    consumer = adapter.consumer
    adapter.close()
    assert consumer.stats['action_paired'] == 1 and consumer.stats['action_missing'] == 1
    assert consumer.stats['action_invalid'] == 2 and consumer.stats['bad_teleop_action'] == 1
    merged = json.loads((consumer.directory / 'actions_merged.json').read_text(encoding='utf-8'))
    assert [r['action_id'] for r in merged] == ['a1']                     # teleop 消息不进指令合并
    events = (consumer.directory / 'action_events.jsonl').read_text(encoding='utf-8').splitlines()
    assert sum('teleop/action/v1' in line for line in events) == 5        # 但都原样存档


def test_next_state_mode_ignores_paired_actions(tmp_path, redis):
    source = Source(redis)
    adapter = adapter_for(tmp_path, redis)                                 # 默认 action=next_state
    adapter.connect()
    time.sleep(0.2)
    observation = source.observation()
    source.teleop(observation, observation['state'])
    assert 'source_action' not in adapter.sample()
    adapter.close()


def test_action_mode_config_is_checked(tmp_path):
    base = {'enabled': True, 'host': 'h'}
    for bad in ({'action': 'later'}, {'action': 'paired', 'action_wait_s': 0.9},
                {'action': 'paired', 'sample_wait_s': 1.5, 'action_wait_s': 0.4}):
        path = tmp_path / 'redis-capture.json'
        path.write_text(json.dumps({**base, **bad}))
        with pytest.raises(ValueError, match='action'):
            rc.load_config(path)
    path.write_text(json.dumps({**base, 'action': 'paired'}))
    config = rc.load_config(path)
    assert rc.dataset_schema_of(config)['action'] == 'source_action'
    assert rc.dataset_schema_of({**config, 'action': 'next_state'})['action'] == 'next_observation_state'


def test_video_store_gets_frames_only_during_collection(tmp_path, redis):
    from deployment_runtime.camera_service import FrameStore
    source = Source(redis)
    feed(source, 1, interval=0.05)
    adapter = adapter_for(tmp_path, redis)
    adapter.connect()
    store = FrameStore(tmp_path / 'camera')
    deadline = time.monotonic() + 2
    while not (tmp_path / 'camera/front.json').exists() and time.monotonic() < deadline:
        time.sleep(.02)
    jpeg, meta = store.read('front', 'jpeg', 5)
    assert jpeg[:2] == b'\xff\xd8' and meta['shape'] == [SHAPE[0], SHAPE[1], 3]
    adapter.close()


def test_stop_confirmation_survives_supply_switched_off_first(tmp_path, redis):
    """平台先关供数再结束采集：hold 之后拿不到新观测时，停止确认用最后一条观测换上当前时间，并标明。"""
    source = Source(redis)
    feed(source, 1, interval=0.05)
    adapter = adapter_for(tmp_path, redis, stop_wait_s=0.2)
    adapter.connect()
    last = adapter.sample()
    adapter.hold()                                  # SDK 开始确认停止（供数已经关了）
    first, second = adapter.sample(), adapter.sample()
    assert first['clock_mapping']['restamped_after_hold'] and second['clock_mapping']['restamped_after_hold']
    assert last['captured_monotonic_ns'] < first['captured_monotonic_ns'] < second['captured_monotonic_ns']
    assert first['positions'] == last['positions']
    adapter.send(last['positions'])                 # 录制恢复：又变回严格模式
    with pytest.raises(Fault) as error:
        adapter.sample()
    assert error.value.code == 'SOURCE_STALLED'
    adapter.close()


def write_runtime(tmp_path, *, enabled=True):
    data = tmp_path / 'data'
    (data / 'isaac-console').mkdir(parents=True)
    (data / 'isaac-console/peer.json').write_text(json.dumps({'domain_id': 173, 'key_file': '/shared/ros-key',
                                                              'namespace': '/so101_isaac_console'}))
    defaults = tmp_path / 'targets.json'
    defaults.write_text(json.dumps({'lab': {'simulation_teleop': {'robot_id': 'sim', 'robot_revision': 'r1'},
                                            'default_mode': 'simulation_teleop'}}))
    conf = tmp_path / 'redis-capture.json'
    conf.write_text(json.dumps({'_说明': 'x', 'enabled': enabled, 'default_mode': 'physical_teleop', 'host': '192.168.0.233',
                                'port': 6379, 'db': 0, 'fps': 4, 'robot_id': 'bobac3-capture',
                                'joints': rc.DEFAULTS['joints'], 'cameras': {'front': [400, 640]}}))
    return data, defaults, conf


def test_prepare_registers_target_and_binds_physical_mode(tmp_path):
    from deployment_runtime.store import Store
    data, defaults, conf = write_runtime(tmp_path)
    store = Store(data / 'registry.sqlite')
    first = rc.prepare(data_root=data, defaults_targets=defaults, config_path=conf, store=store)
    again = rc.prepare(data_root=data, defaults_targets=defaults, config_path=conf, store=store)
    assert first['registered'] == 'new' and again['registered'] == 'existing' and first['revision'] == again['revision']
    targets = json.loads((data / 'isaac-console/redis-capture/collection-project-targets.json').read_text())
    assert targets['lab']['physical_teleop'] == {'robot_id': 'bobac3-capture', 'robot_revision': first['revision']}
    assert set(targets['lab']) == {'default_mode', 'physical_teleop'}               # v10r12：只绑定 Bobac3 这一台
    assert targets['lab']['default_mode'] == 'physical_teleop'
    peer = json.loads((data / 'isaac-console/redis-capture/peer.json').read_text())
    assert peer['namespace'] == '/so101_redis_capture' and peer['adapter']['host'] == '192.168.0.233'
    assert peer['camera_topics']['front']['name'] == '/so101_redis_capture/cameras/front'
    store.close()


def test_waits_must_stay_inside_the_two_second_peer_lease(tmp_path):
    _, _, conf = write_runtime(tmp_path)
    value = json.loads(conf.read_text())
    assert rc.load_config(conf)['sample_wait_s'] == 1.4
    conf.write_text(json.dumps({**value, 'sample_wait_s': 2.0}))
    with pytest.raises(ValueError, match='租约'):
        rc.load_config(conf)


def test_prepare_disabled_removes_stale_runtime(tmp_path):
    from deployment_runtime.store import Store
    data, defaults, conf = write_runtime(tmp_path)
    store = Store(data / 'registry.sqlite')
    rc.prepare(data_root=data, defaults_targets=defaults, config_path=conf, store=store)
    conf.write_text(json.dumps({'enabled': False, 'default_mode': 'simulation_teleop'}))
    assert rc.prepare(data_root=data, defaults_targets=defaults, config_path=conf, store=store) == {'enabled': False}
    assert not (data / 'isaac-console/redis-capture/peer.json').exists()
    store.close()


class LocalBackend:
    """和 ros_transport.RosBackend 做同样的换算（PNG 解码、尺寸核对、clock_mapping 进来源信息），只是不经过 ROS。"""
    kind = 'physical'
    commissioned = True

    def __init__(self, adapter, robot):
        self.adapter, self.robot = adapter, robot
        self.resource_id = 'local:' + robot['robot_id']

    def connect(self, timeout_s):
        self.adapter.connect()

    def sample(self, timeout_s):
        from lerobot_sdk.control.backend import BackendSnapshot
        from deployment_runtime.observation_frames import decode_sample_frames
        v = self.adapter.sample()
        images,records = decode_sample_frames(v['images'],self.robot['camera_shapes'])
        return BackendSnapshot(v['captured_monotonic_ns'], v['positions'], images,
                               {'clock_domain': 'local', 'clock_mapping': v['clock_mapping'],'image_frames':records,
                                **({'source_action': v['source_action']} if v.get('source_action') is not None else {})})

    def send(self, positions, timeout_s):
        return self.adapter.send(dict(positions))

    def hold(self, positions, timeout_s):
        self.adapter.hold()

    def interrupt_stop(self, timeout_s):
        """和 RosBackend 一样有优先停止通道：停止时先让设备进程 hold。"""
        self.adapter.hold()

    def close(self, *, hold, timeout_s):
        self.adapter.preempt_stop(close=True)
        self.adapter.hold()

    def leader(self, timeout_s):
        return self.adapter.leader()


@pytest.mark.parametrize('with_schema,feed_mode', [(True, 'steady'), (False, 'steady'), (True, 'bursty')])
def test_full_recording_through_the_sdk_seals_a_dataset(tmp_path, redis, with_schema, feed_mode):
    """开始 → 录一条 → 保存 → 下一条 → 结束封存 → inspect，和 8910 用的是同一套 SDK 录制代码。
    with_schema=False：不带 dataset_schema 时写出的仍是 SO101 原格式（证明 SO101 目标不受影响）。
    feed_mode=bursty：录制中送达停 1.2 秒后一起到（202 实测情况），整批照样录完。"""
    from lerobot_sdk import CollectionConfig, CollectionManager, DatasetManager, RobotConfig, RobotSession
    cfg = config(tmp_path, cameras={'front': list(SHAPE)}, sample_wait_s=1.4)
    body, revision, peer = rc.build_registration(cfg, {'domain_id': 173, 'key_file': '/k'})
    robot = {**body, 'revision': revision}
    sdk = RobotConfig(**body['sdk_config'])
    adapter = rc.RedisCaptureAdapter({**peer['adapter'], 'camera_store': str(tmp_path / 'camera')}, client_factory=redis.client)
    source = Source(redis)
    stop = threading.Event()
    state = {'joint1': 0.1, 'joint2': 0.2, 'joint3': 0.3, 'joint4': 0.4, 'joint5': 0.5, 'joint6': -0.09}

    def run():
        while not stop.wait(0.1):
            source.observation(state=state)
    if feed_mode == 'bursty':
        bursty_feed(source, stop, state=state, hold_at=1.0, hold_for=1.2)
    else:
        threading.Thread(target=run, daemon=True).start()
    backend = LocalBackend(adapter, robot)
    session = RobotSession(sdk, backend, lock_dir=tmp_path / 'locks', physical_calibration_hash=sdk.calibration_hash).connect()
    try:
        leader = rc.CurrentStateLeader(session)            # 和 sessions 对 Redis 目标的做法一样
        task = CollectionManager().create_task(CollectionConfig(
            name='redis-e2e', description='', root=tmp_path / 'tasks', fps=body['fps'], task='redis e2e',
            robot_id=sdk.robot_id, calibration_hash=sdk.calibration_hash, units=sdk.units,
            camera_shapes={k: tuple(v) for k, v in body['camera_shapes'].items()}, leader_timeout_s=sdk.io_timeout_s,
            dataset_schema=body['dataset_schema'] if with_schema else None))
        task.start(session, leader).wait(30)
        time.sleep(1.5)
        assert task.save_episode().wait(120).frames >= 3
        task.start_episode().wait(60)
        time.sleep(1.2)
        stop.set()                                    # 平台先关了供数，紧接着结束采集
        ref = task.stop(True).wait(300)
        inspection = DatasetManager().inspect(ref)
        assert inspection.episodes == 2 and inspection.native_readable and inspection.camera_keys == ('front',)
        index_rows=[json.loads(line) for line in (Path(ref.root)/'meta/frame-index.jsonl').read_text().splitlines()]
        assert len(index_rows)==inspection.frames
        assert {row['episode_index'] for row in index_rows}=={0,1}
        assert all(row['frame']['source']['kind']=='redis_capture' for row in index_rows)
        assert all(row['frame']['source']['image_key'].startswith('collection_image:') for row in index_rows)
        frame = json.loads(next((Path(ref.root) / 'meta/source-frames/episode-000000').glob('*.json')).read_text())
        assert frame['provenance']['observation']['clock_mapping']['source'] == rc.PROVENANCE
        # 数据集里是机器人自己的关节名、弧度和 robot_type，不是 SO101 的
        import pyarrow.parquet as pq
        info = json.loads((Path(ref.root) / 'meta/info.json').read_text())
        manifest = json.loads((Path(ref.root) / 'manifest.json').read_text())
        row = pq.read_table(next((Path(ref.root) / 'data').rglob('*.parquet'))).to_pylist()[0]
        if with_schema:
            names = [f'joint{i}' for i in range(1, 7)]
            assert info['robot_type'] == 'bobac3' and manifest['robot_type'] == 'bobac3'
            assert manifest['joint_names'] == names and manifest['units'] == ['rad'] * 6 and 'joint_mapping' in manifest
            expected = [0.1, 0.2, 0.3, 0.4, 0.5, -0.09]
        else:
            from lerobot_sdk import JOINT_NAMES
            names = list(JOINT_NAMES)
            assert info['robot_type'] == 'so101_follower' and manifest['robot_type'] == 'so101_follower'
            assert manifest['joint_names'] == names and manifest['units'] == ['deg'] * 5 + ['normalized_0_100']
            assert 'joint_mapping' not in manifest
            expected = list(rc.to_slots(state, rc.DEFAULTS['joints']).values())
        for key in ('observation.state', 'action', 'action.requested', 'action.executed'):
            assert info['features'][key]['names'] == names
        assert row['observation.state'] == pytest.approx(expected, abs=1e-4)
        assert row['action'] == pytest.approx(row['observation.state'], abs=1e-5)   # 关节值不变：下一帧和这一帧一样
    finally:
        stop.set()
        session.close()
    assert adapter.consumer is None                   # 会话关闭后停止消费


def test_bobac3_action_is_the_next_frame_measured_state(tmp_path, redis):
    """2026-11-03：Bobac3 数据集里第 t 帧的 action = 第 t+1 帧的 observation.state；每条最后一帧丢掉。
    Perceptor 的 joint1 每条观测加 0.01，所以能看出 action 确实取的是下一帧。"""
    import pyarrow.parquet as pq
    from lerobot_sdk import CollectionConfig, CollectionManager, DatasetManager, RobotConfig, RobotSession
    cfg = config(tmp_path, cameras={'front': list(SHAPE)}, sample_wait_s=1.4)
    body, revision, peer = rc.build_registration(cfg, {'domain_id': 173, 'key_file': '/k'})
    assert body['dataset_schema']['action'] == 'next_observation_state'
    robot = {**body, 'revision': revision}
    sdk = RobotConfig(**body['sdk_config'])
    adapter = rc.RedisCaptureAdapter({**peer['adapter'], 'camera_store': str(tmp_path / 'camera')}, client_factory=redis.client)
    source = Source(redis)
    stop = threading.Event()

    def run():
        step = 0
        while not stop.wait(0.1):
            step += 1
            source.observation(state={'joint1': 0.01 * step, 'joint2': 0.2, 'joint3': 0.3, 'joint4': 0.4,
                                      'joint5': 0.5, 'joint6': -0.09})
    threading.Thread(target=run, daemon=True).start()
    session = RobotSession(sdk, LocalBackend(adapter, robot), lock_dir=tmp_path / 'locks',
                           physical_calibration_hash=sdk.calibration_hash).connect()
    try:
        task = CollectionManager().create_task(CollectionConfig(
            name='redis-next-action', description='', root=tmp_path / 'tasks', fps=body['fps'], task='next action',
            robot_id=sdk.robot_id, calibration_hash=sdk.calibration_hash, units=sdk.units,
            camera_shapes={k: tuple(v) for k, v in body['camera_shapes'].items()}, leader_timeout_s=sdk.io_timeout_s,
            dataset_schema=body['dataset_schema']))
        task.start(session, rc.CurrentStateLeader(session)).wait(30)
        time.sleep(1.5)
        first = task.save_episode().wait(120).frames
        task.start_episode().wait(60)
        time.sleep(1.2)
        ref = task.stop(True).wait(300)
        stop.set()
        assert first >= 3
        inspection = DatasetManager().inspect(ref)
        assert inspection.episodes == 2 and inspection.native_readable
        manifest = json.loads((Path(ref.root) / 'manifest.json').read_text())
        assert manifest['action_semantics'].startswith('action == action.executed == action.requested == next frame')
        rows = [row for path in sorted((Path(ref.root) / 'data').rglob('*.parquet')) for row in pq.read_table(path).to_pylist()]
        for episode, length in enumerate(manifest['episode_lengths']):
            frames = sorted((r for r in rows if r['episode_index'] == episode), key=lambda r: r['frame_index'])
            assert len(frames) == length >= 2
            for now, following in zip(frames, frames[1:]):
                assert now['action'] == pytest.approx(following['observation.state'], abs=1e-5)
                assert now['action.executed'] == now['action'] and now['action.requested'] == now['action']
                assert following['observation.state'][0] > now['observation.state'][0]       # 确实往前走了
            last = frames[-1]                                    # 最后一帧的 action 是被丢掉的那一帧观测
            assert last['action'][0] > last['observation.state'][0]
            assert last['action'][1:] == pytest.approx(last['observation.state'][1:], abs=1e-5)
        provenance = json.loads(sorted((Path(ref.root) / 'meta/source-frames/episode-000000').glob('*.json'))[0].read_text())
        assert provenance['provenance']['action']['rule'] == 'next_observation_state'
    finally:
        stop.set()
        session.close()


def paired_recording(tmp_path, redis, feeder):
    """COLLECTION_REDIS_ACTION=paired：起 SDK 录制（和 8910 同一套代码），feeder(source, stop) 负责推观测和成对目标。"""
    from lerobot_sdk import CollectionConfig, CollectionManager, RobotConfig, RobotSession
    cfg = config(tmp_path, cameras={'front': list(SHAPE)}, sample_wait_s=1.4, action='paired')
    body, revision, peer = rc.build_registration(cfg, {'domain_id': 173, 'key_file': '/k'})
    assert body['dataset_schema']['action'] == 'source_action'
    sdk = RobotConfig(**body['sdk_config'])
    adapter = rc.RedisCaptureAdapter({**peer['adapter'], 'camera_store': str(tmp_path / 'camera')}, client_factory=redis.client)
    stop = threading.Event()
    threading.Thread(target=feeder, args=(Source(redis), stop), daemon=True).start()
    session = RobotSession(sdk, LocalBackend(adapter, {**body, 'revision': revision}), lock_dir=tmp_path / 'locks',
                           physical_calibration_hash=sdk.calibration_hash).connect()
    task = CollectionManager().create_task(CollectionConfig(
        name='redis-paired', description='', root=tmp_path / 'tasks', fps=body['fps'], task='paired action',
        robot_id=sdk.robot_id, calibration_hash=sdk.calibration_hash, units=sdk.units,
        camera_shapes={k: tuple(v) for k, v in body['camera_shapes'].items()}, leader_timeout_s=sdk.io_timeout_s,
        dataset_schema=body['dataset_schema']))
    return task, session, stop, adapter


def test_paired_actions_become_the_dataset_action(tmp_path, redis):
    """数据集的 action = 数据源报的实际下发目标（这里比状态多 0.05 rad），action.requested = 原本想要的（多 0.1）；
    每 4 条观测有 1 条没给目标：这些观测不会进数据集（SDK 的采样线程比录制快，通常直接越过它们；
    万一录制拿到的是它们，也是跳过不写），绝不拿状态顶替。"""
    import pyarrow.parquet as pq
    from lerobot_sdk import DatasetManager, RobotSession   # noqa: F401

    def feeder(source, stop):
        step = 0
        while not stop.wait(0.1):
            step += 1
            state = {'joint1': 0.01 * step, 'joint2': 0.2, 'joint3': 0.3, 'joint4': 0.4, 'joint5': 0.5, 'joint6': -0.09}
            observation = source.observation(state=state)
            if step % 4:
                source.teleop(observation, {**state, 'joint1': state['joint1'] + 0.05},
                              requested={**state, 'joint1': state['joint1'] + 0.1})
    task, session, stop, adapter = paired_recording(tmp_path, redis, feeder)
    try:
        task.start(session, rc.CurrentStateLeader(session)).wait(30)
        time.sleep(2.0)
        ref = task.stop(True).wait(300)
        stop.set()
        stats = dict(adapter.consumer.stats)
        assert stats['action_paired'] > 0 and stats['action_missing'] > 0     # 确实遇到过没给目标的观测
        inspection = DatasetManager().inspect(ref)
        assert inspection.episodes == 1 and inspection.native_readable
        manifest = json.loads((Path(ref.root) / 'manifest.json').read_text())
        assert manifest['action_semantics'].startswith('action == action.executed == target the source reports')
        rows = [row for path in sorted((Path(ref.root) / 'data').rglob('*.parquet')) for row in pq.read_table(path).to_pylist()]
        assert len(rows) >= 3
        for row in rows:                                   # 每一帧都是配上的：action 都比状态多 0.05
            assert row['action'][0] == pytest.approx(row['observation.state'][0] + 0.05, abs=1e-4)
            assert row['action.requested'][0] == pytest.approx(row['observation.state'][0] + 0.1, abs=1e-4)
            assert row['action.executed'] == row['action']
            assert row['action'][1:] == pytest.approx(row['observation.state'][1:], abs=1e-4)
        frames = [json.loads(p.read_text()) for p in sorted((Path(ref.root) / 'meta/source-frames/episode-000000').glob('*.json'))]
        actions = [f['provenance']['action'] for f in frames]
        assert all(a['rule'] == 'source_action' and a['action_id'].startswith('act_') for a in actions)
        # 每一帧的 action 都来自指向它自己那条观测的目标
        assert all(a['observation_id'] == f['provenance']['observation']['clock_mapping']['observation_id']
                   for a, f in zip(actions, frames))
    finally:
        stop.set()
        session.close()


def test_no_paired_actions_fails_the_start_clearly(tmp_path, redis):
    """供了观测却一直不给成对目标：连续 10 帧配不上就判失败，不会录出一堆拿状态顶替的帧。"""
    def feeder(source, stop):
        while not stop.wait(0.1):
            source.observation()
    task, session, stop, _ = paired_recording(tmp_path, redis, feeder)
    try:
        with pytest.raises(Exception, match='paired source action'):
            task.start(session, rc.CurrentStateLeader(session)).wait(60)
    finally:
        stop.set()
        session.close()


# ══════════ v10r15：SO101（观测来自 Perceptor，action 来自主臂节点 so101-leader，按时间配对） ══════════

SO101_JOINTS = ['shoulder_pan', 'shoulder_lift', 'elbow_flex', 'wrist_flex', 'wrist_roll', 'gripper']


def so101_config(tmp_path, **overrides):
    return config(tmp_path, **{'units': 'lerobot', 'joints': list(SO101_JOINTS), 'robot_id': 'so101-sim-capture',
                               'robot_type': 'so-101-follower_sim', 'action': 'teleop', 'camera_store': None, **overrides})


class So101Source(Source):
    """SO101 的 Perceptor 观测（state 是 SO101 关节名，度 / 0~100；sources.arm_state.sampled_at 是状态采样时刻）
    和主臂节点推的 action（teleop/action/v1，source=teleop_leader，input_observation_id=null）。"""

    def so101_observation(self, state, *, state_at, ts):
        message = self.observation(state=state, ts=ts, push=False)
        message['sources']['arm_state'] = {'sampled_at': state_at}
        self.deliver(message)
        return message

    def leader(self, positions, timestamp, seq, robot_type='so-101-follower_sim'):
        message = {'schema_version': 'teleop/action/v1', 'robot_type': robot_type, 'process_id': 'so101-leader:abc',
                   'action_id': f'abc:{seq:06d}', 'seq': seq, 'source': 'teleop_leader', 'leader_serial': '5A7C124026',
                   'time_basis': 'host_monotonic', 'timestamp': timestamp, 'sampled_unix': 1.8e9,
                   'input_observation_id': None, 'positions': positions, 'units': {}, 'raw': {}, 'rate_hz': 30}
        self.redis.lpush('collection_action_queue', json.dumps(message).encode())
        return message


def test_so101_units_are_kept_and_the_dataset_stays_standard_so101():
    from types import SimpleNamespace
    from lerobot_sdk import DEGREE_UNITS, JOINT_NAMES
    from lerobot_sdk.adapters.seeed_dataset import action_semantics_of, dataset_vector, joint_names_of, robot_type_of
    from lerobot_sdk.data.contracts import joint_schema
    state = dict(zip(SO101_JOINTS, [10.0, -20.0, 30.0, 40.0, -50.0, 25.0]))
    slots = rc.to_slots(state, SO101_JOINTS, 'lerobot')
    assert list(slots.values()) == [10.0, -20.0, 30.0, 40.0, -50.0, 25.0]        # 度 / 0~100 原样进槽位
    for bad in ({**state, 'gripper': 120.0}, {**state, 'elbow_flex': 400.0}, {**state, 'wrist_roll': float('nan')},
                {'shoulder_pan': 1.0}):
        assert rc.to_slots(bad, SO101_JOINTS, 'lerobot') is None
    schema = joint_schema(rc.dataset_schema_of({'units': 'lerobot', 'action': 'teleop'}))
    assert schema == {'action': 'leader_action'}                                  # 只换 action 规则，关节不换
    cfg = SimpleNamespace(dataset_schema=schema, units=DEGREE_UNITS)
    assert joint_names_of(cfg) == list(JOINT_NAMES) and robot_type_of(cfg) == 'so101_follower'
    assert dataset_vector(cfg, slots) == list(slots.values())
    assert action_semantics_of(cfg).startswith('action == action.executed == action.requested == leader arm command')


def test_so101_observation_pairs_with_the_first_leader_action_after_its_state(tmp_path, redis):
    source = So101Source(redis)
    adapter = rc.RedisCaptureAdapter(so101_config(tmp_path), client_factory=redis.client)
    adapter.connect()
    time.sleep(0.3)                                   # 等开始时丢完积压、消费线程起来
    state = dict(zip(SO101_JOINTS, [1.0, 2.0, 3.0, 4.0, 5.0, 50.0]))
    for seq, stamp in enumerate((99.95, 100.01, 100.04, 100.07), 1):              # 状态采样时刻是 100.0
        source.leader({**state, 'shoulder_pan': float(seq)}, stamp, seq)
    time.sleep(0.3)
    source.so101_observation(state, state_at=100.0, ts=100.3)
    sample = adapter.sample()
    action = sample['source_action']
    assert action['seq'] == 2 and action['positions']['shoulder_pan.pos'] == 2.0  # 状态之后的第一条（100.01）
    assert action['requested_positions'] == action['positions'] and action['process_id'] == 'so101-leader:abc'
    assert action['delay_s'] == pytest.approx(0.01) and action['state_sampled_at'] == 100.0
    assert sample['positions']['gripper.pos'] == 50.0 and sample['clock_mapping']['state_sampled_at'] == 100.0
    source.leader(state, 101.5, 5)                    # 状态 101.2 之后的第一条在 0.3 秒后：太晚，这一帧不配
    time.sleep(0.3)
    source.so101_observation(state, state_at=101.2, ts=101.5)
    late = adapter.sample()
    assert 'source_action' not in late and adapter.consumer.stats['action_too_late'] == 1
    assert adapter.frame_store is None                                            # SO101 不往 8910 视频里写帧
    adapter.preempt_stop(close=True)
    adapter.hold()


def test_so101_ignores_leader_actions_for_another_robot(tmp_path, redis):
    source = So101Source(redis)
    adapter = rc.RedisCaptureAdapter(so101_config(tmp_path, action_wait_s=0.1), client_factory=redis.client)
    adapter.connect()
    time.sleep(0.3)
    state = dict(zip(SO101_JOINTS, [1.0, 2.0, 3.0, 4.0, 5.0, 50.0]))
    source.leader(state, 100.01, 1, robot_type='so-101-follower')                # 真机的主臂节点
    time.sleep(0.3)
    source.so101_observation(state, state_at=100.0, ts=100.3)
    sample = adapter.sample()
    stats = adapter.consumer.stats
    assert 'source_action' not in sample and stats['leader_robot_type_mismatch'] == 1 and stats['action_missing'] == 1
    assert 'so-101-follower' in adapter.consumer.last_problem
    adapter.preempt_stop(close=True)
    adapter.hold()


def test_so101_teleop_recording_writes_the_leader_command_as_action(tmp_path, redis):
    """完整录制（和 8910 同一套 SDK 代码）：数据集是标准 SO101 格式，action = 主臂指令（这里比状态多 0.5°），
    来源信息里记着配上的主臂 action 的 seq 和时间差。"""
    import pyarrow.parquet as pq
    from lerobot_sdk import CollectionConfig, CollectionManager, DatasetManager, JOINT_NAMES, RobotConfig, RobotSession
    cfg = so101_config(tmp_path, sample_wait_s=1.4)
    body, revision, peer = rc.build_registration(cfg, {'domain_id': 173, 'key_file': '/k'})
    assert body['dataset_schema'] == {'action': 'leader_action'} and body['kind'] == 'physical'
    sdk = RobotConfig(**body['sdk_config'])
    adapter = rc.RedisCaptureAdapter(peer['adapter'], client_factory=redis.client)
    assert adapter.frame_store is None
    source, stop = So101Source(redis), threading.Event()

    def run():
        step = 0
        while not stop.wait(0.1):
            step += 1
            now = time.monotonic()
            state = dict(zip(SO101_JOINTS, [float(step), 2.0, 3.0, 4.0, 5.0, 50.0]))
            source.leader({**state, 'shoulder_pan': step + 0.5}, now - 0.03, step)  # 状态采样后 0.02 秒的主臂读数
            source.so101_observation(state, state_at=now - 0.05, ts=now)
    threading.Thread(target=run, daemon=True).start()
    session = RobotSession(sdk, LocalBackend(adapter, {**body, 'revision': revision}), lock_dir=tmp_path / 'locks',
                           physical_calibration_hash=sdk.calibration_hash).connect()
    try:
        task = CollectionManager().create_task(CollectionConfig(
            name='so101-teleop', description='', root=tmp_path / 'tasks', fps=body['fps'], task='so101 teleop',
            robot_id=sdk.robot_id, calibration_hash=sdk.calibration_hash, units=sdk.units,
            camera_shapes={k: tuple(v) for k, v in body['camera_shapes'].items()}, leader_timeout_s=sdk.io_timeout_s,
            dataset_schema=body['dataset_schema']))
        task.start(session, rc.CurrentStateLeader(session)).wait(30)
        time.sleep(2.0)
        ref = task.stop(True).wait(300)
        stop.set()
        inspection = DatasetManager().inspect(ref)
        assert inspection.episodes == 1 and inspection.native_readable
        manifest = json.loads((Path(ref.root) / 'manifest.json').read_text())
        assert manifest['robot_type'] == 'so101_follower' and manifest['joint_names'] == list(JOINT_NAMES)
        assert manifest['units'] == ['deg'] * 5 + ['normalized_0_100'] and 'joint_mapping' not in manifest
        assert manifest['action_semantics'].startswith('action == action.executed == action.requested == leader arm command')
        rows = [row for path in sorted((Path(ref.root) / 'data').rglob('*.parquet')) for row in pq.read_table(path).to_pylist()]
        assert len(rows) >= 3
        for row in rows:
            assert row['action'][0] == pytest.approx(row['observation.state'][0] + 0.5, abs=1e-4)
            assert row['action'][1:] == pytest.approx(row['observation.state'][1:], abs=1e-4)
            assert row['action.executed'] == row['action'] and row['action.requested'] == row['action']
        frames = [json.loads(p.read_text()) for p in sorted((Path(ref.root) / 'meta/source-frames/episode-000000').glob('*.json'))]
        actions = [f['provenance']['action'] for f in frames]
        assert all(a['rule'] == 'leader_action' and a['delay_s'] == pytest.approx(0.02, abs=1e-6) for a in actions)
        assert all(a['seq'] >= 1 and a['action_id'].startswith('abc:') for a in actions)
    finally:
        stop.set()
        session.close()
    assert 'collection_enabled' not in redis.strings                             # 结束后供数开关已关
