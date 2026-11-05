"""Native encoder/source sidecars; physical devices are never opened."""
from dataclasses import replace
import json
import threading
import time

import pytest

from lerobot_sdk import CollectionConfig, CollectionManager, RobotSession, DatasetManager, JOINT_NAMES
from lerobot_sdk.testing import FakeRobot, simulation_config
from lerobot_sdk.testing.leader import ScriptedLeader
from lerobot_sdk.data.filesystem import sha_file

pytest.importorskip('lerobot')


def wait(predicate,timeout=10):
    until=time.monotonic()+timeout
    while not predicate():
        assert time.monotonic()<until
        time.sleep(.01)


def test_exact_used_source_raw_json_survives_save_redo_export_and_recovery(tmp_path):
    from lerobot_sdk.adapters.seeed_dataset import upstream
    from lerobot_sdk.data.provenance import SOURCE_FIELD
    from lerobot_sdk.data.collection import _raw_frame
    upstream()
    config=simulation_config('provenance-native',max_observation_age_s=2)
    class Sensors(FakeRobot):
        def sample(self,timeout_s):
            value=super().sample(timeout_s)
            from deployment_runtime.observation_frames import frame_record
            return replace(value,source_metadata={'clock_mapping':{'method':'fixture-clock',
                'source_capture_ns':value.captured_monotonic_ns,'mapped_lower_bound_ns':value.captured_monotonic_ns,
                'exact_hardware_sync':False},'image_frames':{k:frame_record(k,f.rgb,f.captured_monotonic_ns,
                    source={'kind':'isaac_ipc','original_source_timestamp':f.captured_monotonic_ns,
                            'time_basis':'recording_host_monotonic_ns'}) for k,f in value.images.items()}})
    robot=RobotSession(config,Sensors(resource_id=str(tmp_path)),lock_dir=tmp_path/'locks').connect()
    class Leader(ScriptedLeader):
        def get_action(self,*,timeout_s):
            result=super().get_action(timeout_s=timeout_s)
            self.used={'source_kind':'simulation','source_sequence':self.calls,
                'native_positions':dict.fromkeys('123456',2000+self.calls),
                'gains':dict.fromkeys([name.removesuffix('.pos') for name in JOINT_NAMES],.15),
                'active_axes':['shoulder_lift','elbow_flex','wrist_flex'],'runtime_config_sha256':'sha256:'+'a'*64}
            return result
        def frame_provenance(self):return self.used
    leader=Leader(config,dict(robot.get_arm_data().positions),resource_id=str(tmp_path))
    cfg=CollectionConfig('batch','source evidence',tmp_path,5,'record',config.robot_id,
        config.calibration_hash,config.units,{'front':(12,16,3)},min_free_bytes=1)
    task=CollectionManager().create_task(cfg)
    try:
        task.start(robot,leader).wait(120)
        saved=task.save_episode().wait(30)
        raw=task.directory/task.frozen_commits()[0]['files'][0]['path']
        contents=raw.read_bytes();frame=_raw_frame(raw)
        assert SOURCE_FIELD in frame
        used=json.loads(frame[SOURCE_FIELD].item())
        assert used['leader']['source_sequence']==1
        assert used['leader']['native_positions']['1']==2001
        assert used['observation']['clock_mapping']['exact_hardware_sync'] is False
        task.start_episode().wait(30);discarded=task._episode_dir
        task.redo_episode().wait(30)
        assert not discarded.exists() and raw.read_bytes()==contents
        task.start_episode().wait(30)
        task.suspend().wait(30)
        recovered=CollectionManager().open_task(task.directory)
        recovered.start(robot,leader).wait(30)
        assert raw.read_bytes()==contents
        ref=recovered.stop(save_current=True).wait(60)
        metadata=json.loads((ref.root/'manifest.json').read_text())
        source=metadata['source_provenance']
        assert source['frames']>=saved.frames and source['exact_hardware_sync'] is False
        assert source['files'] and all(name in metadata['files'] for name in source['files'])
        sidecar=json.loads((ref.root/source['files'][0]).read_text())
        assert sidecar['raw_frame']['sha256']==sha_file(raw)
        assert sidecar['provenance']['leader']==used['leader']
        assert DatasetManager().inspect(ref).native_readable
        rows=[json.loads(line) for line in (ref.root/'meta/frame-index.jsonl').read_text().splitlines()]
        assert len(rows)==sum(metadata['episode_lengths'])
        assert all(row['frame']['source']['kind']=='isaac_ipc' for row in rows)
        assert all(row['frame']['captured_monotonic_ns']==row['frame']['source']['original_source_timestamp'] for row in rows)
        assert 'meta/frame-index.jsonl' in metadata['files']
    finally:
        for candidate in (task,locals().get('recovered')):
            if candidate is not None and candidate._thread and candidate._thread.is_alive():
                try:candidate.suspend().wait(15)
                except Exception:pass
        robot.close()
