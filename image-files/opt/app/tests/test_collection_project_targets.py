"""Fixed project bindings and raw-request idempotency; real SQLite, software device."""
import copy
import json

import pytest

from deployment_runtime.store import Fault, Principal, digest
from test_collection_contract_api import system


def configure(platform,*,revision='v1'):
    platform.config['collection_targets']={'lab':{'default_mode':'simulation_teleop',
        'simulation_teleop':{'robot_id':'sim','robot_revision':revision},
        'physical_teleop':{'robot_id':'physical','robot_revision':'v1'}}}


def simplified(payload):return {k:v for k,v in payload.items() if k not in ('robot_id','robot_revision')}


def register_version(platform,actor,key='sim',revision='v2',*,physical=False):
    body=copy.deepcopy(platform.store.get('robots','sim','v1',actor)['body'])
    body['robot_id']=body['sdk_config']['robot_id']=key
    if physical:
        body.update(kind='physical',commissioned=True)
        body.pop('scene_id');body.pop('scene_revision')
    platform.register('robots',{'project':'lab','id':key,'revision':revision,'body':body},actor)


def test_project_only_idle_status_resolves_pinned_revision_not_latest(system):
    contract,engine,box,platform,actor,payload=system
    configure(platform);register_version(platform,actor)
    seen=[];contract.probe=lambda key,revision,a: seen.append((key,revision)) or {
        'peer':{'owner':None,'fault':None,'holding':True,'action_queue_empty':True}}
    status=contract.status({'project':'lab'},actor)
    assert {k:status[k] for k in ('state','project','robot_id','robot_revision','target_type','mode')}=={
        'state':'IDLE','project':'lab','robot_id':'sim','robot_revision':'v1',
        'target_type':'simulation','mode':'simulation_teleop'}
    assert seen==[('sim','v1')] and not engine.live


def test_default_start_freezes_resolved_job_and_raw_request_and_does_not_mutate_caller(system):
    contract,engine,box,platform,actor,payload=system;configure(platform)
    raw=simplified(payload);before=copy.deepcopy(raw)
    first=contract.start(raw,'project-start',actor)
    assert raw==before and (first['robot_id'],first['robot_revision'])==('sim','v1')
    job=platform.store.job(first['session_id'],actor)
    assert (job['payload']['robot_id'],job['payload']['robot_revision'])==('sim','v1')
    assert job['fingerprint']==digest(['collections.start','lab',raw])
    register_version(platform,actor);configure(platform,revision='v2')
    assert contract.start(raw,'project-start',actor)==first
    assert list(engine.live)==[first['session_id']]
    configure(platform,revision='not-registered')
    assert contract.start(raw,'project-start',actor)==first


def test_changed_raw_request_conflicts_before_lookup_of_changed_binding(system):
    contract,engine,box,platform,actor,payload=system;configure(platform)
    raw=simplified(payload);first=contract.start(raw,'raw-fingerprint',actor)
    configure(platform,revision='not-registered')
    with pytest.raises(Fault) as error:contract.start({**raw,'name':'different'},'raw-fingerprint',actor)
    assert error.value.code=='IDEMPOTENCY_CONFLICT' and list(engine.live)==[first['session_id']]


def test_failed_start_request_does_not_retarget_after_binding_changes(system):
    contract,engine,box,platform,actor,payload=system;configure(platform)
    raw={**simplified(payload),'calibration_revision':'wrong'}
    with pytest.raises(Fault) as first:contract.start(raw,'failed-frozen-request',actor)
    platform.config['collection_targets']={}
    with pytest.raises(Fault) as retry:contract.start(raw,'failed-frozen-request',actor)
    assert first.value.code==retry.value.code=='CALIBRATION_MISMATCH' and not engine.live


def test_null_mode_is_rejected_instead_of_replaced_by_project_default(system):
    contract,engine,box,platform,actor,payload=system;configure(platform)
    with pytest.raises(Fault) as error:contract.start({**simplified(payload),'mode':None},'null-mode',actor)
    assert error.value.code=='INVALID_MODE' and not engine.live


def test_status_prefers_frozen_active_identity_after_config_binding_changes(system):
    contract,engine,box,platform,actor,payload=system;configure(platform)
    first=contract.start(simplified(payload),'active',actor)
    register_version(platform,actor);configure(platform,revision='v2')
    status=contract.status({'project':'lab'},actor)
    assert status['state']=='RUNNING' and status['session_id']==first['session_id']
    assert (status['robot_id'],status['robot_revision'])==('sim','v1')


def test_project_status_follows_physical_active_session_even_if_default_is_simulation(system):
    contract,engine,box,platform,actor,payload=system;configure(platform)
    register_version(platform,actor,'physical','v1',physical=True)
    raw={'project':'lab','mode':'physical_teleop','name':'physical fixture',
         'calibration_revision':'baseline','workspace_confirmed':True}
    first=contract.start(raw,'physical-start',actor)
    status=contract.status({'project':'lab'},actor)
    assert status['state']=='RUNNING' and status['session_id']==first['session_id']
    assert {k:status[k] for k in ('robot_id','robot_revision','target_type','mode')}=={
        'robot_id':'physical','robot_revision':'v1','target_type':'physical','mode':'physical_teleop'}


def test_pending_finish_status_returns_frozen_target_identity(system):
    contract,engine,box,platform,actor,payload=system;configure(platform)
    first=contract.start(simplified(payload),'finish-identity',actor)
    row=contract._read(first['session_id'],actor)
    row.update(internal_state='STOPPING',finish={'state':'RETRY_WAIT'});contract._write(row)
    status=contract.status({'project':'lab'},actor)
    assert status['state']=='FINISHING'
    assert {k:status[k] for k in ('robot_id','robot_revision','target_type','mode')}=={
        'robot_id':'sim','robot_revision':'v1','target_type':'simulation','mode':'simulation_teleop'}


def test_two_project_targets_require_explicit_disambiguation(system):
    contract,engine,box,platform,actor,payload=system;configure(platform)
    register_version(platform,actor,'other','v1')
    contract.start(payload,'first-target',actor)
    contract.start({**payload,'robot_id':'other'},'second-target',actor)
    with pytest.raises(Fault) as error:contract.status({'project':'lab'},actor)
    assert error.value.code=='PROJECT_TARGET_AMBIGUOUS'
    assert contract.status({'project':'lab','robot_id':'sim','robot_revision':'v1'},actor)['robot_id']=='sim'


def test_project_status_does_not_reveal_another_operator_session(system):
    contract,engine,box,platform,actor,payload=system;configure(platform)
    contract.start(payload,'owned-by-alice',actor)
    with pytest.raises(Fault) as error:
        contract.status({'project':'lab'},Principal('bob',('lab',),('operator',)))
    assert error.value.code=='FORBIDDEN'


@pytest.mark.parametrize('selection',[{'robot_id':'sim'},{'robot_revision':'v1'}])
def test_partial_explicit_identity_is_rejected_in_start_and_status(system,selection):
    contract,engine,box,platform,actor,payload=system;configure(platform)
    with pytest.raises(Fault) as start_error:contract.start({**simplified(payload),**selection},'partial',actor)
    with pytest.raises(Fault) as status_error:contract.status({'project':'lab',**selection},actor)
    assert start_error.value.code==status_error.value.code=='INCOMPLETE_TARGET'
    assert not engine.live and platform.store.claims()==[]


@pytest.mark.parametrize('config',[None,{}, {'lab':{}},
    {'lab':{'default_mode':'simulation_teleop'}},
    {'lab':{'default_mode':'latest','simulation_teleop':{'robot_id':'sim','robot_revision':'v1'}}}])
def test_project_default_requires_explicit_valid_fixed_configuration(system,config):
    contract,engine,box,platform,actor,payload=system
    if config is not None:platform.config['collection_targets']=config
    with pytest.raises(Fault) as error:contract.status({'project':'lab'},actor)
    assert error.value.code in ('COLLECTION_TARGET_NOT_CONFIGURED','COLLECTION_TARGET_CONFIG_INVALID')
    assert not engine.live


@pytest.mark.parametrize('missing',['mode','name','calibration_revision','scene_id','scene_revision'])
def test_simplified_start_does_not_fill_required_business_metadata(system,missing):
    contract,engine,box,platform,actor,payload=system;configure(platform)
    raw=simplified(payload);raw.pop(missing)
    with pytest.raises(Fault) as error:contract.start(raw,'missing-'+missing,actor)
    assert error.value.code=='INVALID_FIELDS' and not engine.live


def test_explicit_legacy_identity_needs_no_default_and_status_mode_can_select_binding(system):
    contract,engine,box,platform,actor,payload=system
    first=contract.start(payload,'legacy-start',actor)
    assert first['robot_revision']=='v1'
    assert contract.status({'project':'lab','robot_id':'sim','robot_revision':'v1'},actor)['mode']=='simulation_teleop'
    contract.stop(first['session_id'],{'project':'lab'},'legacy-stop',actor)
    configure(platform);register_version(platform,actor,'physical','v1',physical=True)
    status=contract.status({'project':'lab','mode':'physical_teleop'},actor)
    assert status['robot_id']=='physical' and status['target_type']=='physical' and status['mode']=='physical_teleop'


def test_resolver_is_pure_and_align_mode_does_not_fill_scene_or_calibration(system):
    from deployment_runtime.collection_targets import resolve_target
    contract,engine,box,platform,actor,payload=system;configure(platform)
    original={'project':'lab'}
    result,robot=resolve_target(platform,original,actor,mode='simulation_teleop')
    assert original=={'project':'lab'} and result=={
        'project':'lab','mode':'simulation_teleop','robot_id':'sim','robot_revision':'v1'}
    assert robot['revision']=='v1' and not engine.live
    with pytest.raises(Fault):resolve_target(platform,{'project':'lab','mode':'physical_teleop'},actor,mode='simulation_teleop')
