"""Private v9 behavior over real SQLite; no device or HTTP schema replacement."""
import copy
import re
import threading

import pytest

from deployment_runtime.store import Fault
from test_collection_contract_api import system


def minimal_start(contract,payload,actor,rid='managed-start'):
    return contract.minimal_start(copy.deepcopy(payload),rid,actor)


def test_managed_marker_is_frozen_in_row_not_runtime_core_payload(system,monkeypatch):
    c,e,b,p,a,payload=system;seen=[];start=e.start
    def capture(key,robot,body,**kwargs):
        seen.append(copy.deepcopy(body));return start(key,robot,body,**kwargs)
    monkeypatch.setattr(e,'start',capture)
    result=minimal_start(c,payload,a)
    row=c._read(result['session_id'],a)
    assert row['api_contract']=='minimal.v9' and row['managed'] is True
    assert 'api_contract' not in seen[0] and 'managed' not in seen[0]
    assert c.minimal_start(payload,'managed-start',a)==result


def test_failed_managed_start_retains_contract_marker(system,monkeypatch):
    c,e,b,p,a,payload=system
    def fail(*args,**kwargs):raise Fault('FIXTURE_START_FAILED','Software startup failed')
    monkeypatch.setattr(e,'start',fail)
    with pytest.raises(Fault,match='Software startup'):minimal_start(c,payload,a)
    row=c.db.execute('SELECT body FROM sessions').fetchone()
    import json
    value=json.loads(row['body'])
    assert value['api_contract']=='minimal.v9' and value['managed'] is True
    assert value['internal_state']=='START_FAILED'


def test_v9_redo_rotates_attempt_once_and_old_attempt_cannot_complete(system):
    c,e,b,p,a,payload=system;first=minimal_start(c,payload,a)
    body={'project':'lab','recording_generation':1}
    redone=c.minimal_redo(first['session_id'],first['episode_id'],body,'managed-redo',a)
    assert redone['episode_id']!=first['episode_id'] and re.fullmatch('[a-f0-9]{32}',redone['episode_id'])
    assert redone['episode_index']==first['episode_index'] and redone['recording_generation']==2
    assert e.live[first['session_id']]['episode_id']==redone['episode_id']
    assert c.minimal_redo(first['session_id'],first['episode_id'],body,'managed-redo',a)==redone
    assert e.discards==[first['episode_id']]
    with pytest.raises(Fault) as error:
        c.minimal_complete(first['session_id'],first['episode_id'],{'project':'lab','recording_generation':2},'stale-attempt',a)
    assert error.value.code=='STALE_EPISODE' and not b.rows
    done=c.minimal_complete(first['session_id'],redone['episode_id'],{'project':'lab','recording_generation':2},'managed-complete',a)
    assert done['committed_episode']['episode_id']==redone['episode_id']
    assert done['episode_index']==1 and done['recording_generation']==1


def test_v8_default_redo_remains_same_attempt_id(system):
    c,e,b,p,a,payload=system;first=c.start(payload,'legacy-start',a)
    row=c._read(first['session_id'],a)
    assert 'api_contract' not in row and 'managed' not in row
    redone=c.redo(first['session_id'],first['episode_id'],{'project':'lab','recording_generation':1},'legacy-redo',a)
    assert redone['episode_id']==first['episode_id'] and redone['recording_generation']==2


@pytest.mark.parametrize('operation',['redo','complete','finish','resume'])
def test_minimal_wrapper_rejects_legacy_session_without_side_effects(system,operation):
    c,e,b,p,a,payload=system;first=c.start(payload,'legacy-start',a)
    if operation=='resume':
        c.stop(first['session_id'],{'project':'lab'},'legacy-stop',a)
        args=(first['session_id'],{'project':'lab'},'minimal-'+operation,a)
    elif operation=='finish':args=(first['session_id'],{'project':'lab'},'minimal-'+operation,a)
    else:args=(first['session_id'],first['episode_id'],{'project':'lab','recording_generation':1},'minimal-'+operation,a)
    with pytest.raises(Fault) as error:getattr(c,'minimal_'+operation)(*args)
    assert error.value.code=='PROTOCOL_MISMATCH' and not b.rows and e.discards==[]


def test_v9_resume_without_fake_workspace_rotates_same_logical_episode(system,monkeypatch):
    c,e,b,p,a,payload=system;first=minimal_start(c,payload,a)
    c.stop(first['session_id'],{'project':'lab'},'backend-stop',a)
    def resume(key,robot,body):
        assert 'workspace_confirmed' not in body
        e.live[key]={'state':'RECORDING','current_frames':3,'committed_episodes':0,
            'episode_id':'runtime-temporary','commits':[],'mode':body['mode']}
        return e.status(key)
    monkeypatch.setattr(e,'resume_capture',resume,raising=False)
    resumed=c.minimal_resume(first['session_id'],{'project':'lab'},'managed-resume',a)
    assert resumed['episode_id']!=first['episode_id'] and resumed['episode_index']==0
    assert resumed['recording_generation']==2 and e.live[first['session_id']]['episode_id']==resumed['episode_id']
    assert c.minimal_resume(first['session_id'],{'project':'lab'},'managed-resume',a)==resumed


@pytest.mark.parametrize('workspace',[None,True])
def test_managed_physical_start_never_uses_workspace_as_takeover_proof(system,workspace):
    c,e,b,p,a,payload=system
    body=copy.deepcopy(p.store.get('robots','sim','v1',a)['body'])
    body.update(kind='physical',commissioned=True,robot_id='physical')
    body['sdk_config']['robot_id']='physical';body.pop('scene_id');body.pop('scene_revision')
    p.register('robots',{'project':'lab','id':'physical','revision':'v1','body':body},a)
    raw={'project':'lab','robot_id':'physical','robot_revision':'v1','mode':'physical_teleop',
        'name':'physical fixture','calibration_revision':'baseline'}
    if workspace is not None:raw['workspace_confirmed']=workspace
    with pytest.raises(Fault) as error:minimal_start(c,raw,a)
    assert error.value.code=='PHYSICAL_READINESS_REQUIRED' and not e.live and not p.store.claims()


def test_body_cannot_spoof_managed_contract_marker(system):
    c,e,b,p,a,payload=system
    with pytest.raises(Fault) as error:c.start({**payload,'api_contract':'minimal.v9','managed':True},'spoof-marker',a)
    assert error.value.code=='INVALID_FIELDS' and not e.live


def test_finish_intent_is_not_overwritten_by_late_managed_start_result(system,monkeypatch):
    c,e,b,p,a,payload=system
    entered=threading.Event();release=threading.Event();seen=[];errors=[]
    original_start=e.start
    def delayed(key,robot,body):
        seen.append(key);entered.set();assert release.wait(5)
        return original_start(key,robot,body)
    cancelled=[]
    monkeypatch.setattr(e,'start',delayed)
    monkeypatch.setattr(e,'cancel_start',cancelled.append,raising=False)
    monkeypatch.setattr(c,'_launch_finish',lambda key:None)
    def run():
        try:minimal_start(c,payload,a)
        except Exception as error:errors.append(error)
    worker=threading.Thread(target=run);worker.start()
    try:
        assert entered.wait(3)
        result=c.minimal_finish(seen[0],{'project':'lab'},'finish-during-start',a)
        assert result['state']=='FINISHING' and cancelled==seen
        release.set();worker.join(5)
        assert not worker.is_alive()
        row=c._read(seen[0],a)
        assert row['finish']['request_id']=='finish-during-start' and row['managed'] is True
        assert errors and errors[0].code=='SESSION_ENDED' and not e.live and not p.store.claims()
    finally:
        release.set();worker.join(5)
