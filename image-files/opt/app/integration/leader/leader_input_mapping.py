"""Native read-only leader samples mapped to one freshly started Isaac scene.

This is an outer integration adapter. It reuses the accepted BorrowedLeader
normalization without constructing a serial driver or changing sample expiry.
"""
import json
import math
from pathlib import Path
import time

try:
    from integration.leader_collection_input import BorrowedLeader, atomic
except ModuleNotFoundError:
    from leader_collection_input import BorrowedLeader, atomic


class NativeInputMapper:
    def __init__(self, root, profile, contract, ready, *, expected_generation, output, trace):
        from lerobot_sdk import RobotConfig, JointTarget, JOINT_NAMES
        if not expected_generation or ready.get('generation') != expected_generation:
            raise ValueError('Current Isaac startup generation required; do not reuse old ready.json')
        if ready.get('robot_id') != contract['robot_id'] or ready.get('calibration_hash') != contract['calibration_hash']:
            raise ValueError('Isaac ready identity/calibration differs from target contract')
        if set(contract['names']) != set(JOINT_NAMES) or len(contract['names']) != 6:
            raise ValueError('Complete named six-axis contract required')
        self.config = RobotConfig(
            robot_id=contract['robot_id'], calibration_hash=contract['calibration_hash'],
            units=contract['units'], limits={k: tuple(v) for k, v in contract['limits'].items()},
            max_velocity={k: 30. for k in JOINT_NAMES}, max_step={k: 3. for k in JOINT_NAMES},
            tolerance={k: .75 for k in JOINT_NAMES}, gripper_open=80., gripper_closed=20.,
            fps=10, stable_frames=2, action_timeout_s=10, io_timeout_s=2.,
            max_observation_age_s=1., max_sensor_skew_s=.5, required_cameras=())
        self.anchor = dict(ready['initial'])
        self.config.validate_target(JointTarget(self.anchor, self.config.units, self.config.calibration_hash))
        self.generation = expected_generation
        self.root = Path(root); self.root.mkdir(parents=True, exist_ok=True)
        self.output = Path(output); self.output.parent.mkdir(parents=True, exist_ok=True)
        self.trace = Path(trace); self.trace.parent.mkdir(parents=True, exist_ok=True)
        self.profile_hash = profile['profile_hash']
        # Imports used here are pure normalization; no hardware object is created.
        self.leader = BorrowedLeader(self.root, profile, self.config, self.anchor)
        self.last_sequence = -1
        self.last_received = None
        self.max_input_gap_s = 0.

    def publish(self, packet):
        now = time.monotonic()
        seq = packet.get('sequence')
        if type(seq) is not int or seq <= self.last_sequence:
            raise ValueError('Mapped input sequence must increase')
        if packet.get('profile_hash') != self.profile_hash:
            raise ValueError('Leader profile differs from registered mapping')
        received = packet.get('received_monotonic'); expires = packet.get('expires_monotonic')
        if (type(received) not in (int, float) or type(expires) not in (int, float)
                or not math.isfinite(received) or not math.isfinite(expires)
                or not received <= now < expires):
            raise TimeoutError('Native input expired; mapped input lifetime must not be renewed')
        raw = packet.get('native_positions')
        if (not isinstance(raw, dict) or set(raw) != set('123456')
                or any(type(v) is not int or not 0 <= v <= 4095 for v in raw.values())):
            raise ValueError('Six valid native motor positions required')
        atomic(self.root / 'input.json', packet)
        target = self.leader.get_action(timeout_s=.01)
        if time.monotonic() >= expires:
            raise TimeoutError('Input expired during mapping; no mapped output published')
        gap = None if self.last_received is None else received - self.last_received
        if gap is not None:
            self.max_input_gap_s = max(self.max_input_gap_s, gap)
        result = {
            'positions': dict(target.positions), 'units': dict(self.config.units),
            'calibration_hash': self.config.calibration_hash,
            'received_monotonic': received, 'expires_monotonic': expires,
            'challenge_issued_monotonic': packet.get('challenge_issued_monotonic'),
            'valid_for_s': packet.get('valid_for_s'), 'ttl_s': packet.get('valid_for_s'),
            'native_positions': dict(raw), 'sequence': seq, 'profile_hash': self.profile_hash,
            'anchor_generation': self.generation, 'anchor_initial': dict(self.anchor),
            'clipped_axes': self.leader.last_mapping['clipped_axes'],
            'source': 'physical_native_leader_to_isaac_simulation',
            'input_gap_s': gap, 'max_input_gap_s': self.max_input_gap_s,
        }
        atomic(self.output, result)
        self.last_sequence = seq
        self.last_received = received
        with self.trace.open('a', encoding='utf-8') as stream:
            stream.write(json.dumps({**result, 'mapped_monotonic': time.monotonic()}) + '\n')
            stream.flush()
        return result
