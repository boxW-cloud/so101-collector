"""Core-only tests: optional native dataset packages must remain optional."""
from dataclasses import replace
import sys
import subprocess
import pytest
from lerobot_sdk import CollectionConfig,CollectionManager,SDKError,StateError
from lerobot_sdk.testing import simulation_config


def config(tmp_path,**changes):
    r=simulation_config()
    c=CollectionConfig('task','description',tmp_path,10,'task text',r.robot_id,
                       r.calibration_hash,r.units,{'front':(12,16,3)})
    return replace(c,**changes)


@pytest.mark.parametrize('changes',[
    {'name':'../outside'}, {'name':'/absolute'}, {'fps':0}, {'fps':True},
    {'task':''}, {'camera_shapes':{}}, {'camera_shapes':{'bad/path':(12,16,3)}},
    {'camera_shapes':{'front':(13,16,3)}}, {'calibration_hash':'not-a-hash'},
])
def test_collection_configuration_rejects_invalid_contract(tmp_path,changes):
    with pytest.raises(SDKError):config(tmp_path,**changes)


def test_create_filesystem_failure_is_sdk_error(tmp_path,monkeypatch):
    import lerobot_sdk.collection as module
    def fail(*args):raise OSError('injected metadata disk error')
    monkeypatch.setattr(module,'atomic_json',fail)
    with pytest.raises(SDKError):CollectionManager().create_task(config(tmp_path))
    assert not list(tmp_path.rglob('manifest.json'))


def test_open_malformed_journal_is_sdk_error(tmp_path):
    (tmp_path/'task.json').write_text('{}')
    with pytest.raises(SDKError):CollectionManager().open_task(tmp_path)


def test_sdk_import_does_not_import_torch():
    result=subprocess.run([sys.executable,'-c',
        "import sys,lerobot_sdk; assert 'torch' not in sys.modules; assert 'lerobot' not in sys.modules"],
        capture_output=True,text=True,timeout=30)
    assert result.returncode==0,result.stderr
