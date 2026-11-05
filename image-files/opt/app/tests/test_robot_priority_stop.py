"""Optional priority cancellation must reach a backend while normal I/O waits."""
from dataclasses import replace
import threading
import time

import pytest

from lerobot_sdk import (RobotSession, JointTarget, JOINT_NAMES, SessionState,
                         OperationCancelled, OperationFailed, StateError)
from lerobot_sdk.testing import FakeRobot, simulation_config


class InterruptedBackend(FakeRobot):
    def __init__(self):
        super().__init__()
        self.block = None
        self.entered = threading.Event()
        self.release = threading.Event()
        self.interrupted = threading.Event()
        self.interrupt_error = False
        self.stale_return = False
        self.interrupt_calls = []
        self.fail_blocked_sample = False

    def interrupt_stop(self, timeout_s):
        self.interrupt_calls.append((timeout_s, threading.get_ident()))
        self.interrupted.set()
        if self.interrupt_error:
            raise RuntimeError('Priority stop transport unavailable')

    def sample(self, timeout_s):
        result = super().sample(timeout_s)
        if self.block == 'sample':
            self.block = None
            self.entered.set()
            self.release.wait(2)
            if self.fail_blocked_sample:
                raise TimeoutError('Interrupted sample failure')
            if self.stale_return:
                result = replace(result, captured_monotonic_ns=time.monotonic_ns()-9_000_000_000)
        return result

    def send(self, positions, timeout_s):
        result = super().send(positions, timeout_s)
        if self.block == 'send':
            self.block = None
            self.entered.set()
            self.release.wait(2)
        return result


def run_stop(robot, results, errors, *, emergency=False):
    try:
        results.append(robot.emergency_stop('operator') if emergency else robot.stop('operator'))
    except Exception as error:
        errors.append(error)


def target(robot, value=5):
    positions = dict(robot.config.release_pose.positions)
    positions[JOINT_NAMES[0]] = value
    return JointTarget(positions, robot.config.units, robot.config.calibration_hash)


@pytest.mark.parametrize('blocked', ['send', 'sample'])
def test_optional_stop_hook_reaches_backend_before_blocked_io_or_session_lock(tmp_path, blocked):
    backend = InterruptedBackend()
    config = simulation_config(fps=50, io_timeout_s=.2)
    robot = RobotSession(config, backend, lock_dir=tmp_path).connect()
    stopper = None
    results, errors = [], []
    try:
        backend.block = blocked
        handle = robot.move_to(target(robot), duration_s=.5) if blocked == 'send' else None
        assert backend.entered.wait(1)
        before = len(backend.sent)
        stopper = threading.Thread(target=run_stop, args=(robot, results, errors))
        stopper.start()
        assert backend.interrupted.wait(.25), 'Priority hook must not wait for the normal I/O lock'
        assert stopper.is_alive() and results == []  # A hook is not a HOLD proof.
        backend.release.set();stopper.join(2)
        assert not stopper.is_alive() and errors == []
        assert results[0].holding and robot.status().state == SessionState.HOLDING
        if handle:
            with pytest.raises(OperationCancelled):handle.wait(.2)
        time.sleep(.06)
        assert len(backend.sent) == before
        normal_threads = {ident for _, ident in backend.calls}
        assert len(normal_threads) == 1
        assert backend.interrupt_calls[0][1] not in normal_threads
    finally:
        backend.release.set()
        if stopper:stopper.join(2)
        backend.block = None;backend.interrupt_error = False
        robot.close()


def test_late_stale_sample_after_stop_is_discarded_before_fresh_hold_proof(tmp_path):
    backend = InterruptedBackend()
    robot = RobotSession(simulation_config(fps=50, io_timeout_s=.2), backend, lock_dir=tmp_path).connect()
    results, errors = [], []
    try:
        backend.block = 'sample';backend.stale_return = True
        assert backend.entered.wait(1)
        stopper = threading.Thread(target=run_stop, args=(robot, results, errors));stopper.start()
        assert backend.interrupted.wait(.25)
        backend.release.set();stopper.join(2)
        assert not stopper.is_alive() and errors == []
        assert results[0].holding and robot.status().state == SessionState.HOLDING
        assert robot.get_arm_data().age_ms < 200
    finally:
        backend.release.set();backend.block = None
        robot.close()


def test_new_motion_waiting_behind_blocked_send_is_rejected_by_stop_intent(tmp_path):
    backend = InterruptedBackend()
    robot = RobotSession(simulation_config(fps=50, io_timeout_s=.2), backend, lock_dir=tmp_path).connect()
    results, errors, later_errors = [], [], []
    try:
        backend.block = 'send';handle = robot.move_to(target(robot), duration_s=.5)
        assert backend.entered.wait(1)
        stopper = threading.Thread(target=run_stop, args=(robot, results, errors));stopper.start()
        assert backend.interrupted.wait(.25)
        def submit_later():
            try:robot.move_to(target(robot, 2), duration_s=.5)
            except Exception as error:later_errors.append(error)
        later = threading.Thread(target=submit_later);later.start()
        later.join(.15)
        assert not later.is_alive(), 'A new target must reject the stop intent before waiting for ordinary I/O'
        backend.release.set();stopper.join(2);later.join(2)
        assert errors == [] and later_errors
        assert isinstance(later_errors[0], (StateError,))
        with pytest.raises(OperationCancelled):handle.wait(.2)
        assert len(backend.sent) == 1
    finally:
        backend.release.set();backend.block = None
        robot.close()


def test_failed_priority_hook_cannot_report_a_successful_stop(tmp_path):
    backend = InterruptedBackend()
    robot = RobotSession(simulation_config(fps=50, io_timeout_s=.2), backend, lock_dir=tmp_path).connect()
    results, errors = [], []
    try:
        backend.block = 'sample';backend.interrupt_error = True
        assert backend.entered.wait(1)
        stopper = threading.Thread(target=run_stop, args=(robot, results, errors));stopper.start()
        assert backend.interrupted.wait(.25)
        assert results == []
        backend.release.set();stopper.join(2)
        assert results == [] and len(errors) == 1 and isinstance(errors[0], OperationFailed)
        assert robot.status().state == SessionState.FAULT
    finally:
        backend.release.set();backend.block = None;backend.interrupt_error = False
        robot.close()


def test_priority_emergency_stop_remains_latched_after_blocked_send(tmp_path):
    backend = InterruptedBackend()
    robot = RobotSession(simulation_config(fps=50, io_timeout_s=.2), backend, lock_dir=tmp_path).connect()
    results, errors = [], []
    try:
        backend.block = 'send';robot.move_to(target(robot), duration_s=.5)
        assert backend.entered.wait(1)
        stopper = threading.Thread(target=run_stop, args=(robot, results, errors), kwargs={'emergency':True});stopper.start()
        assert backend.interrupted.wait(.25)
        backend.release.set();stopper.join(2)
        assert errors == [] and results[0].holding and results[0].state == SessionState.E_STOP
        with pytest.raises(StateError):robot.move_to(target(robot, 2))
    finally:
        backend.release.set();backend.block = None
        robot.close()


@pytest.mark.parametrize('command', ['open_gripper','close_gripper','release'])
def test_other_manual_commands_reject_pending_stop_without_waiting_for_io(tmp_path, command):
    backend = InterruptedBackend()
    robot = RobotSession(simulation_config(fps=50, io_timeout_s=.2), backend, lock_dir=tmp_path).connect()
    results, errors, rejected = [], [], []
    try:
        backend.block = 'send';robot.move_to(target(robot), duration_s=.5)
        assert backend.entered.wait(1)
        stopper = threading.Thread(target=run_stop, args=(robot, results, errors));stopper.start()
        assert backend.interrupted.wait(.25)
        def invoke():
            try:getattr(robot, command)()
            except Exception as error:rejected.append(error)
        caller = threading.Thread(target=invoke);caller.start();caller.join(.15)
        assert not caller.is_alive(), 'Manual commands must not queue behind an already requested stop'
        assert len(rejected) == 1 and isinstance(rejected[0], StateError)
        backend.release.set();stopper.join(2)
        assert errors == [] and results[0].holding
    finally:
        backend.release.set();backend.block = None
        robot.close()


def test_cancel_of_an_old_completed_handle_does_not_interrupt_a_new_operation(tmp_path):
    backend = InterruptedBackend()
    with RobotSession(simulation_config(fps=50, io_timeout_s=.2), backend, lock_dir=tmp_path) as robot:
        old = robot.move_to(target(robot, .2));old.wait(2)
        fresh = robot.move_to(target(robot, 5), duration_s=.5)
        old.cancel()
        assert not backend.interrupt_calls
        assert not robot._stop_intent.is_set()
        fresh.cancel()
        with pytest.raises(OperationCancelled):fresh.wait(.5)


def test_fault_racing_priority_admission_cleans_intent_and_allows_a_fresh_stop(tmp_path):
    backend = InterruptedBackend()
    robot = RobotSession(simulation_config(fps=50, io_timeout_s=.2), backend, lock_dir=tmp_path).connect()
    fault_entered, allow_fault = threading.Event(), threading.Event()
    original = robot._faulted
    def defer_fault(error):
        fault_entered.set();allow_fault.wait(2);original(error)
    robot._faulted = defer_fault
    results, errors = [], []
    stopper = None
    try:
        backend.block = 'sample';backend.fail_blocked_sample = True
        assert backend.entered.wait(1)
        backend.release.set();assert fault_entered.wait(1)
        stopper = threading.Thread(target=run_stop, args=(robot, results, errors));stopper.start()
        assert backend.interrupted.wait(.25)
        deadline = time.monotonic()+1
        while robot._stop_request is None and time.monotonic()<deadline:time.sleep(.005)
        assert robot._stop_request is not None
        allow_fault.set();stopper.join(2)
        assert errors and results == []
        assert robot.status().state == SessionState.FAULT
        assert robot._priority_stop is None and not robot._stop_intent.is_set()
        previous = len(backend.interrupt_calls)
        assert robot.stop('fresh retry').holding
        assert len(backend.interrupt_calls) == previous+1
    finally:
        allow_fault.set();backend.release.set();backend.block = None;backend.fail_blocked_sample = False
        if stopper:stopper.join(2)
        try:robot.close()
        except OperationFailed:pass
