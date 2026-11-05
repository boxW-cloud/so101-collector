"""The non-root runtime must be able to exec launch/SSH helper scripts."""
import os
from pathlib import Path
import pytest

@pytest.mark.skipif(os.name!='posix',reason='Exact Linux image permission contract')
@pytest.mark.parametrize('name',[
    '/opt/sim-retest/entrypoint.sh',
    '/opt/app/collection_service/ros_entry.sh',
    '/opt/app/deployment_runtime/sync_askpass.sh',
])
def test_runtime_script_is_executable(name):
    assert Path(name).is_file()
    assert os.access(name,os.R_OK|os.X_OK)
