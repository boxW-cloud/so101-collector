"""Public convenience routes must keep the explicit v2 command contract intact."""
from deployment_runtime.api_contract import FIELDS,openapi
import json


def test_explicit_deployment_file_loads_project_binding_without_changing_input_config(tmp_path,monkeypatch):
    from deployment_runtime.service import Platform
    from test_collection_contract_api import CaptureEngine
    bindings={'lab':{'default_mode':'simulation_teleop','simulation_teleop':{'robot_id':'sim','robot_revision':'v1'}}}
    target=tmp_path/'targets.json';target.write_text(json.dumps(bindings),encoding='utf-8')
    monkeypatch.setenv('SO101_COLLECTION_TARGETS_JSON',str(target))
    original={'role':'collection','root':str(tmp_path/'runtime')}
    platform=Platform(original,sessions=CaptureEngine(tmp_path/'capture'))
    try:
        assert platform.config['collection_targets']==bindings
        assert 'collection_targets' not in original
    finally:platform.close()


def test_project_only_status_and_start_identity_are_documented_as_optional():
    paths=openapi()['paths']
    parameters=paths['/collection/status']['get']['parameters']
    # No-project queries select v9; legacy project queries are still documented.
    assert [p['name'] for p in parameters if p['required']]==[]
    assert {'project','session_id'}<={p['name'] for p in parameters} and 'is_sim' not in {p['name'] for p in parameters}
    start=paths['/collection/sessions']['post']['requestBody']['content']['application/json']['schema']
    assert {'project','mode','name','calibration_revision','request_id'}<=set(start['required'])
    assert not {'robot_id','robot_revision'}&set(start['required'])
    assert start['dependentRequired']=={'robot_id':['robot_revision'],'robot_revision':['robot_id']}
    assert {'robot_id','robot_revision'}<=set(FIELDS['collections.start'][0].split())
    assert any(rule.get('then',{}).get('required')==['scene_id','scene_revision'] for rule in start['allOf'])


def test_there_is_no_alignment_endpoint():
    # v10r15：对齐由操作者自己负责，平台不管
    assert '/collection/align' not in openapi()['paths'] and 'verifications.align' not in FIELDS
