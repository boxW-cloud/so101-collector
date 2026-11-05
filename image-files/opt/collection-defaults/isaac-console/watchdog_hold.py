"""Atomic HOLD evidence in the simulation loop; no control claim or motor I/O.

The file IPC is private, same-host test transport. This proof is not a physical
torque measurement or a public remote-control authorization mechanism.
"""
import math


PROTOCOL = 'isaac.watchdog-hold.v1'


class WatchdogHold:
    def __init__(self, run_generation, limits):
        self.run_generation = run_generation
        self.limits = limits
        self.record = None
        self.first = None
        self.latest = None

    def invalidate(self):
        self.record = None
        self.first = None
        self.latest = None

    def expired(self, client, generation, sequence, stamp, target):
        self.invalidate()
        self.record = {'event': 'lease expired; simulator holding',
            'protocol': PROTOCOL, 'run_generation': self.run_generation,
            'expired_client': client, 'control_generation': generation,
            'last_sequence': sequence, 'time_ns': stamp, 'target': dict(target),
            'reason': 'watchdog'}
        return dict(self.record)

    def startup(self, stamp, target):
        self.invalidate()
        self.record = {'event': 'startup; simulator holding',
            'protocol': PROTOCOL, 'run_generation': self.run_generation,
            'expired_client': None, 'control_generation': None,
            'last_sequence': None, 'time_ns': stamp, 'target': dict(target),
            'reason': 'startup'}
        return dict(self.record)

    def _valid_positions(self, values):
        if not isinstance(values, dict) or set(values) != set(self.limits):
            return False
        for name, value in values.items():
            if type(value) not in (int, float) or not math.isfinite(value):
                return False
            lo, hi = self.limits[name]
            # Preserve only the existing small Isaac endpoint precision budget.
            budget = 0 if name == 'gripper.pos' else .001
            if not lo - budget <= value <= hi + budget:
                return False
        return True

    def observe(self, stamp, owner, target, measured):
        if owner is not None:
            self.invalidate()
            return
        if not self.record:
            return
        if (type(stamp) is not int or stamp <= self.record['time_ns'] or
                target != self.record['target'] or
                not self._valid_positions(target) or
                not self._valid_positions(measured)):
            self.first = self.latest = None
            return
        if self.latest and stamp <= self.latest[0]:
            self.first = self.latest = None
            return
        observation = (stamp, dict(measured))
        if (self.first is None or
                any(abs(measured[n] - self.first[1][n]) > .1
                    for n in self.limits)):
            self.first = observation
        self.latest = observation

    def confirm(self, request, owner, target, measured, stamp, now, *, reason='watchdog'):
        """Must run before/after stepping on the single simulator control loop."""
        if owner is not None:
            raise ValueError('Simulator has a current control owner')
        record = self.record
        if not record:
            raise ValueError('No current watchdog HOLD evidence')
        time_key = 'watchdog_time_ns' if reason == 'watchdog' else 'hold_time_ns'
        if (reason not in ('watchdog', 'startup') or record['reason'] != reason or
                request.get('run_generation') != self.run_generation or
                request.get(time_key) != record['time_ns'] or
                type(request.get('sequence')) is not int or
                request['sequence'] < 1 or
                type(request.get('generation')) is not int or
                request['generation'] < 0 or
                not isinstance(request.get('client'), str) or not request['client']):
            raise ValueError('Watchdog HOLD identity or control generation changed')
        if reason == 'watchdog' and (
                request['client'] != record['expired_client'] or
                request['sequence'] <= record['last_sequence'] or
                request['generation'] < record['control_generation']):
            raise ValueError('Watchdog HOLD identity or control generation changed')
        if target != record['target']:
            raise ValueError('Watchdog HOLD target changed')
        if (type(stamp) is not int or type(now) is not int or
                not 0 <= now - stamp <= 2_000_000_000):
            raise ValueError('Watchdog HOLD observation is stale or in the future')
        if (not self._valid_positions(measured) or not self.latest or
                self.latest != (stamp, measured) or not self.first or
                stamp - self.first[0] < 100_000_000):
            raise ValueError('Watchdog HOLD lacks fresh continuous stable feedback')
        # HOLD proves observed stability with an unchanged command, not arrival
        # at that command and not physical support force. Retain move-to's .75
        # tolerance only as an honest, independent result flag.
        errors = {name: measured[name] - target[name] for name in self.limits}
        return {'holding': True, 'action_queue_empty': True,
            'positions': dict(measured), 'target': dict(target),
            'holding_basis': 'stable_feedback_and_unchanged_target',
            'target_error_by_joint': errors,
            'target_reached': all(abs(error) <= .75 for error in errors.values()),
            'captured_monotonic_ns': stamp, 'run_generation': self.run_generation,
            time_key: record['time_ns'], 'protocol': PROTOCOL, 'reason': reason,
            'lease_reacquired': False, 'motion_started': False}
