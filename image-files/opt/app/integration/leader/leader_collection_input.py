"""Private bench input transport and borrowed leader; not a core SDK network API.

Windows reads native hardware; Spark alone timestamps a one-use challenge and
expiry. No cross-host monotonic clocks are subtracted. Only the local adapter
returns SDK JointTarget; this service never controls hardware or simulation.
"""
import argparse
import hmac
import json
import math
from pathlib import Path
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

INPUT_HTTP_PROTOCOL = 'HTTP/1.1'


class InputRejected(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code

def atomic(path, data):
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(data), encoding='utf-8')
    temp.replace(path)


class InputGate:
    def __init__(self, profile_hash, ttl=.8, clock=time.monotonic):
        if type(ttl) not in (int, float) or not math.isfinite(ttl) or not .1 <= ttl <= 2:
            raise ValueError('Explicit input TTL must be finite and within [0.1, 2] seconds')
        self.profile_hash, self.ttl, self.clock = profile_hash, ttl, clock
        self.challenge = None
        self.last_sequence = -1

    def issue(self):
        issued = self.clock()
        self.challenge = (secrets.token_hex(16), issued + self.ttl, issued)
        return {'challenge': self.challenge[0], 'valid_for_s': self.ttl,
                'challenge_issued_monotonic': issued}

    def accept(self, packet):
        now = self.clock()
        if not isinstance(packet, dict):
            raise InputRejected('INVALID_PACKET', 'Input packet must be an object')
        if 'request_next_challenge' in packet and type(packet['request_next_challenge']) is not bool:
            raise InputRejected('INVALID_PACKET', 'Next challenge request must be boolean')
        if not self.challenge or packet.get('challenge') != self.challenge[0]:
            raise InputRejected('UNKNOWN_CHALLENGE', 'Unknown or already consumed challenge')
        expiry = self.challenge[1]
        issued = self.challenge[2]
        self.challenge = None  # One use, including rejected packets.
        if now >= expiry:
            raise InputRejected('CHALLENGE_EXPIRED', 'Issued challenge expired')
        seq = packet.get('sequence')
        if type(seq) is not int:
            raise InputRejected('INVALID_PACKET', 'Integer sequence required')
        if seq <= self.last_sequence:
            raise InputRejected('STALE_SEQUENCE', 'Old sequence')
        if packet.get('profile_hash') != self.profile_hash:
            raise InputRejected('PROFILE_MISMATCH', 'Profile mismatch')
        positions = packet.get('native_positions')
        if not isinstance(positions, dict) or set(positions) != set('123456'):
            raise InputRejected('INVALID_PACKET', 'Six named motor positions required')
        if any(type(v) is not int or not 0 <= v <= 4095 for v in positions.values()):
            raise InputRejected('INVALID_PACKET', 'Invalid encoder')
        scan = packet.get('scan_duration_s')
        if type(scan) not in (int, float) or not math.isfinite(scan) or not 0 < scan <= .4:
            raise InputRejected('INVALID_PACKET', 'Invalid scan duration')
        self.last_sequence = seq
        return {**packet, 'received_monotonic': now, 'expires_monotonic': expiry,
                'challenge_issued_monotonic': issued, 'valid_for_s': self.ttl}


class BorrowedLeader:
    """Fresh file snapshot supplied by the authenticated bench receiver.

    ID2/3/4 keep the accepted three-point mapping. ID1/5 retain a +/-20 degree
    relative envelope, re-centered in normalization space at borrow time so a
    changed physical rest pose is not stuck against the old absolute clamp.
    """
    def __init__(self, root, profile, config, anchor_positions, trace=None, *, input_profile_hash=None):
        from lerobot.motors import Motor, MotorNormMode, MotorCalibration
        from lerobot.motors.motors_bus import MotorsBus
        from types import SimpleNamespace
        self.root, self.profile, self.config = Path(root), profile, config
        self.input_profile_hash=input_profile_hash or profile['profile_hash']
        self.resource_id = 'native-leader:' + profile['profile_hash']
        self.trace = trace
        self._last_sequence = -1
        self.anchor_sim = dict(anchor_positions)
        self.anchor_raw = None
        self.last_mapping = None
        motors = {
            name: Motor(axis['motor_id'], 'sts3215', MotorNormMode.RANGE_0_100 if name == 'gripper' else MotorNormMode.RANGE_M100_100)
            for name, axis in profile['axes'].items()}
        calibration = {
            name: MotorCalibration(**axis['spark_normalization_calibration'])
            for name, axis in profile['axes'].items()}
        # Only the installed package's pure normalization method is used. This
        # context has no serial port or device methods and cannot connect motors.
        names = {axis['motor_id']: name for name, axis in profile['axes'].items()}
        self.normalization_context = SimpleNamespace(calibration=calibration, motors=motors,
            apply_drive_mode=False, _id_to_name=names.__getitem__)
        self.normalize = lambda values: MotorsBus._normalize(self.normalization_context, values)

    def _read(self):
        packet = json.loads((self.root / 'input.json').read_text())
        now = time.monotonic()
        if packet['profile_hash'] != self.input_profile_hash or not packet['received_monotonic'] <= now < packet['expires_monotonic']:
            raise TimeoutError('Leader sample expired or profile mismatch')
        if set(packet['native_positions']) != set('123456') or any(type(v) is not int or not 0 <= v <= 4095 for v in packet['native_positions'].values()):
            raise ValueError('Invalid leader snapshot')
        return packet

    @property
    def is_connected(self):
        try:
            self._read()
            return True
        except (OSError, ValueError, KeyError, TimeoutError):
            return False

    def get_action(self, *, timeout_s):
        from lerobot_sdk import JointTarget
        from integration.three_point_preview import mapped_degrees
        end = time.monotonic() + timeout_s
        while True:
            packet = self._read()  # Expired input fails closed, never waits for a replacement.
            if packet['sequence'] > self._last_sequence:
                break
            if time.monotonic() >= end:
                raise TimeoutError('No new leader sequence')
            time.sleep(.005)
        raw = packet['native_positions']
        if self.anchor_raw is None:
            self.anchor_raw = dict(raw)
        compat = {i: 4095 - raw[str(i)] for i in range(1, 7)}
        for i, name in ((1, 'shoulder_pan'), (5, 'wrist_roll')):
            cal = self.profile['axes'][name]['spark_normalization_calibration']
            delta = (raw[str(i)] - self.anchor_raw[str(i)] + 2048) % 4096 - 2048
            compat[i] = round((cal['range_min'] + cal['range_max']) / 2 - delta)
        normalized = self.normalize(compat)  # Installed fixed Seeed package; no connect/configure.
        values = {}
        clipped = []
        for name, axis in self.profile['axes'].items():
            i = axis['motor_id']
            if axis.get('mapping_mode') == 'fixed_three_point':
                value, clamp = mapped_degrees(raw[str(i)], axis)
                if clamp: clipped.append(name)
            elif name == 'gripper':
                value = normalized[i]
            else:
                lo, hi = axis['sim_joint_limits_deg']
                delta = normalized[i] / 200 * (hi - lo)
                value = self.anchor_sim[name + '.pos'] + max(-20, min(20, delta))
                if abs(delta) > 20: clipped.append(name)
            lo, hi = self.config.limits[name + '.pos']
            margin = .5 if name == 'gripper' else 2.0
            limited = max(lo + margin, min(hi - margin, value))
            if limited != value: clipped.append(name + ':simulation_margin')
            values[name + '.pos'] = limited
        target = JointTarget(values, self.config.units, self.config.calibration_hash)
        self.config.validate_target(target)
        self._last_sequence = packet['sequence']
        self.last_mapping={'sequence':packet['sequence'],'raw':dict(raw),'requested':dict(values),'clipped_axes':list(clipped),
            'received_monotonic':packet['received_monotonic'],'expires_monotonic':packet['expires_monotonic'],
            'challenge_issued_monotonic':packet.get('challenge_issued_monotonic'),'valid_for_s':packet.get('valid_for_s')}
        if self.trace:
            self.trace.write(json.dumps({'sequence': packet['sequence'], 'received_monotonic': packet['received_monotonic'],
                'used_monotonic': time.monotonic(), 'expires_monotonic': packet['expires_monotonic'],
                'challenge_issued_monotonic':packet.get('challenge_issued_monotonic'),'valid_for_s':packet.get('valid_for_s'),
                'raw': raw, 'requested': values, 'clipped_axes': clipped}) + '\n')
            self.trace.flush()
        return target


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--profile', type=Path, required=True)
    p.add_argument('--token', type=Path, required=True)
    p.add_argument('--port', type=int, default=8795)
    p.add_argument('--bind', default='127.0.0.1', help='Trusted receiver address; default loopback')
    p.add_argument('--ttl', type=float, default=.8, help='Input challenge validity in [0.1, 2] seconds; default 0.8')
    p.add_argument('--mapped-output', type=Path)
    p.add_argument('--contract', type=Path)
    p.add_argument('--anchor-ready', type=Path)
    p.add_argument('--anchor-generation')
    a = p.parse_args()
    a.root.mkdir(exist_ok=True)
    token = a.token.read_text().strip()
    profile = json.loads(a.profile.read_text())
    try:
        gate = InputGate(profile['profile_hash'], ttl=a.ttl)
    except ValueError as error:
        p.error(str(error))
    mapper = None
    mapping_args = (a.mapped_output, a.contract, a.anchor_ready, a.anchor_generation)
    if any(mapping_args):
        if not all(mapping_args):
            p.error('Mapping requires --mapped-output, --contract, --anchor-ready and --anchor-generation')
        try:
            from integration.leader_input_mapping import NativeInputMapper
        except ModuleNotFoundError:
            from leader_input_mapping import NativeInputMapper
        mapper = NativeInputMapper(a.root, profile, json.loads(a.contract.read_text()),
            json.loads(a.anchor_ready.read_text()), expected_generation=a.anchor_generation,
            output=a.mapped_output, trace=a.root / 'mapping.jsonl')
    lock = threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        protocol_version = INPUT_HTTP_PROTOCOL
        def setup(self):
            super().setup()
            self.connection.settimeout(5)  # Idle connection wait, not source validity.
        def log_message(self, *args): pass
        def reply(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        def allowed(self):
            return hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + token)
        def do_GET(self):
            if not self.allowed(): return self.reply(401, {'error': 'unauthorized'})
            if self.path != '/challenge': return self.reply(404, {})
            with lock: self.reply(200, gate.issue())
        def do_POST(self):
            if not self.allowed(): return self.reply(401, {'error': 'unauthorized'})
            if self.path != '/input': return self.reply(404, {})
            try:
                n = int(self.headers.get('Content-Length', '0'))
                if not 0 < n <= 4096: raise ValueError('Invalid payload length')
                self.connection.settimeout(1)
                try:
                    packet = json.loads(self.rfile.read(n))
                finally:
                    self.connection.settimeout(5)
                with lock:
                    accepted = gate.accept(packet)
                    if mapper:
                        try:
                            mapper.publish(accepted)
                        except TimeoutError as error:
                            raise InputRejected('SOURCE_EXPIRED', 'Source expired before mapped publication') from error
                    else:
                        atomic(a.root / 'input.json', accepted)
                    receipt = {'accepted_sequence': accepted['sequence']}
                    if packet.get('request_next_challenge') is True:
                        receipt['next_challenge'] = gate.issue()
                self.reply(200, receipt)
            except InputRejected as error:
                self.reply(400, {'error': 'input rejected', 'code': error.code})
            except (ValueError, KeyError, TimeoutError):
                self.reply(400, {'error': 'input rejected', 'code': 'INVALID_PACKET'})
    server = ThreadingHTTPServer((a.bind, a.port), Handler)
    server.timeout = .5
    atomic(a.root / 'ready.json', {'bind': a.bind, 'port': a.port, 'profile_hash': gate.profile_hash,
        'ttl_s': gate.ttl, 'mapped_output': str(a.mapped_output) if mapper else None,
        'anchor_generation': a.anchor_generation if mapper else None})
    try:
        while not (a.root / 'shutdown.request').exists(): server.handle_request()
    finally:
        server.server_close()

if __name__ == '__main__': main()
