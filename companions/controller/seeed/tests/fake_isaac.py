"""假 Isaac：按采集容器 isaac_harness.py 的同机文件接口应答（租约、占用、序号、关节范围、sim-status.json），不跑物理引擎。

关节按一阶滞后追目标（每拍追上差距的 FOLLOW），像 Isaac 里关节驱动追目标那样。
"""
import json
import threading
import time
from pathlib import Path

NAMES = ['shoulder_pan.pos', 'shoulder_lift.pos', 'elbow_flex.pos', 'wrist_flex.pos', 'wrist_roll.pos', 'gripper.pos']
LIMITS = {'shoulder_pan.pos': [-110.0, 110.0], 'shoulder_lift.pos': [-100.0, 100.0], 'elbow_flex.pos': [-100.0, 90.0],
          'wrist_flex.pos': [-95.0, 95.0], 'wrist_roll.pos': [-160.0, 160.0], 'gripper.pos': [0.0, 100.0]}
HASH = 'sha256:' + 'd' * 64
RUN = 'run-1'


class FakeIsaac:
    def __init__(self, root, initial, *, rate=200, follow=.25):
        self.root, self.ipc = Path(root), Path(root) / 'ipc'
        self.ipc.mkdir(parents=True, exist_ok=True)
        (self.root / 'contract.json').write_text(json.dumps({
            'robot_id': 'so101-isaac-console', 'calibration_hash': HASH, 'names': NAMES, 'limits': LIMITS,
            'units': {n: 'normalized_0_100' if n == 'gripper.pos' else 'deg' for n in NAMES}}))
        self.measured, self.target = dict(initial), dict(initial)
        self.owner, self.lease_until, self.generation, self.last_sequence, self.seen = None, 0, 0, 0, None
        self.rate, self.follow, self.run = rate, follow, RUN
        self.ops, self.sent = [], []           # 每个请求 (操作, 客户端)；每次 send 的目标
        self.expired = 0
        self.stopping = threading.Event()
        self.lock = threading.Lock()
        self._publish('ready.json', {'calibration_hash': HASH, 'robot_id': 'so101-isaac-console', 'generation': RUN})
        self._status(time.monotonic_ns())
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.stopping.set()
        self.thread.join(2)

    def _publish(self, name, value):
        temporary = self.ipc / (name + '.tmp')
        temporary.write_text(json.dumps(value))
        temporary.replace(self.ipc / name)

    def _status(self, now):
        self._publish('sim-status.json', {'time_ns': now, 'owner': self.owner, 'measured': dict(self.measured),
                                          'target': dict(self.target), 'run_generation': self.run})

    def lease_to(self, client, seconds=3600):
        """模拟采集容器正占着租约。"""
        with self.lock:
            self.owner, self.lease_until = client, time.monotonic_ns() + int(seconds * 1e9)

    def _handle(self, req, now):
        response = {'id': req['id']}
        op = req.get('operation')
        self.ops.append((op, req.get('client')))
        try:
            if now > req['deadline_ns']:
                raise ValueError('Expired command')
            if req['calibration_hash'] != HASH:
                raise ValueError('Calibration hash mismatch')
            if op == 'connect':
                if self.owner and self.owner != req['client']:
                    response['code'] = 'busy'
                    raise ValueError('Simulation already leased')
                self.owner, self.generation, self.last_sequence = req['client'], req['generation'], 0
                self.target = dict(self.measured)
            if req['client'] != self.owner:
                raise ValueError('No current control lease')
            if req['sequence'] <= self.last_sequence or req['generation'] < self.generation:
                raise ValueError('Old sequence/generation')
            self.last_sequence, self.generation, self.lease_until = req['sequence'], req['generation'], now + 2_000_000_000
            if op == 'sample':
                response.update(positions=dict(self.measured), captured_monotonic_ns=now, images={})
            elif op == 'send':
                values = req['positions']
                if set(values) != set(NAMES):
                    raise ValueError('Expected exactly six named joints')
                if any(not LIMITS[k][0] <= v <= LIMITS[k][1] for k, v in values.items()):
                    raise ValueError('Target violates simulation contract')
                self.target = dict(values)
                self.sent.append(dict(values))
                response['sent'] = values
            elif op in ('hold', 'close'):
                self.target = dict(self.measured)
                if op == 'close':
                    self.owner = None
            elif op not in ('connect', 'heartbeat'):
                raise ValueError('Unsupported operation')
        except Exception as error:
            response['error'] = str(error)
        self._publish('response.json', response)

    def _loop(self):
        last_status = 0
        while not self.stopping.is_set():
            now = time.monotonic_ns()
            with self.lock:
                if self.owner and now > self.lease_until:
                    self.target, self.owner = dict(self.measured), None
                    self.expired += 1
                try:
                    req = json.loads((self.ipc / 'request.json').read_text())
                except (FileNotFoundError, json.JSONDecodeError):
                    req = None
                if req and req.get('id') != self.seen:
                    self.seen = req['id']
                    self._handle(req, now)
                for name in NAMES:
                    self.measured[name] += (self.target[name] - self.measured[name]) * self.follow
                if now - last_status > 50_000_000:
                    self._status(now)
                    last_status = now
            time.sleep(1 / self.rate)
