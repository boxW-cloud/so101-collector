"""Production recorder/runtime with native files; only devices are simulated."""
import json
import threading

import pytest

from lerobot_sdk import (CollectionConfig, CollectionManager, CollectionState,
                         RobotSession, OperationFailed)
from lerobot_sdk.testing import simulation_config
from lerobot_sdk.testing.leader import ScriptedLeader
from deployment_runtime.sessions import SessionRuntime
from test_robot_priority_stop import InterruptedBackend

pytest.importorskip('lerobot')


@pytest.mark.parametrize('failed_interrupt', [False, True])
def test_session_runtime_stop_preempts_blocked_frame_and_preserves_all_journaled_files(tmp_path, failed_interrupt):
    from lerobot_sdk.adapters.seeed_dataset import upstream
    upstream()
    backend = InterruptedBackend();backend.resource_id = str(tmp_path)
    config = simulation_config('priority-recorder', fps=50, io_timeout_s=.3,
                               max_observation_age_s=2)
    robot = RobotSession(config, backend, lock_dir=tmp_path/'locks').connect()
    class UsedLeader(ScriptedLeader):
        def frame_provenance(self):
            return {'source_kind':'simulation','source':'test_fixture','source_sequence':self.calls,
                    'native_positions':dict.fromkeys('123456',2000+self.calls)}
    leader = UsedLeader(config, dict(robot.get_arm_data().positions), resource_id=str(tmp_path))
    collection = CollectionConfig('batch', 'priority-stop fixture', tmp_path, 5, 'record',
        config.robot_id, config.calibration_hash, config.units, {'front':(12,16,3)}, min_free_bytes=1)
    task = CollectionManager().create_task(collection)
    runtime = SessionRuntime(tmp_path/'runtime', lambda *_: backend)
    runtime.live['batch'] = {'robot':robot, 'backend':backend, 'task':task,
        'lock':threading.RLock(), 'starting':False, 'cancelled':False,
        'verification':False, 'mode':'simulation_teleop', 'source':'test_fixture'}
    results, errors = [], []
    stopper = None
    try:
        task.start(robot, leader).wait(120)
        task.save_episode().wait(30)
        task.start_episode().wait(30)
        backend.entered.clear();backend.interrupted.clear();backend.release.clear()
        backend.block = 'send';backend.interrupt_error = failed_interrupt
        assert backend.entered.wait(2)
        before = json.loads((task.directory/'task.json').read_text())
        files = [item for ep in before['commits'] for item in ep['files']] + before['current']['files']
        contents = {item['path']:(task.directory/item['path']).read_bytes() for item in files}
        from lerobot_sdk.data.collection import _raw_frame
        from lerobot_sdk.data.provenance import SOURCE_FIELD
        assert all(SOURCE_FIELD in _raw_frame(task.directory/item['path']) for item in files)
        def stop():
            try:results.append(runtime.suspend_capture('batch'))
            except Exception as error:errors.append(error)
        stopper = threading.Thread(target=stop);stopper.start()
        assert backend.interrupted.wait(.3), 'REST runtime must not wait for frame completion before priority stop'
        assert stopper.is_alive() and results == []
        backend.release.set();stopper.join(5)
        assert not stopper.is_alive()
        task._thread.join(2)
        after = json.loads((task.directory/'task.json').read_text())
        assert after['commits'] == before['commits']
        assert after['current']['files'] == before['current']['files']
        for path, content in contents.items():assert (task.directory/path).read_bytes() == content
        assert task.status().state == CollectionState.RECOVERABLE
        if failed_interrupt:
            assert results == [] and errors and isinstance(errors[0], OperationFailed)
            assert 'batch' in runtime.live
        else:
            assert errors == [] and results[0]['holding'] and results[0]['action_queue_empty']
            assert results[0]['state'] == 'STOPPED' and 'batch' not in runtime.live
            assert task.status().error is None
    finally:
        backend.release.set();backend.block = None;backend.interrupt_error = False
        if stopper:stopper.join(5)
        if task._thread and task._thread.is_alive():
            try:task.suspend().wait(10)
            except Exception:pass
            task._thread.join(5)
        robot.close()
