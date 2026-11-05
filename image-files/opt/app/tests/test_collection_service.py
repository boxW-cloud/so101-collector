import copy,json,time,uuid
from pathlib import Path
import pytest
from collection_service.protocol import signed,verified
from collection_service.service import Service

@pytest.fixture
def api(tmp_path,monkeypatch):
    import lerobot_sdk.adapters.seeed_dataset as dataset
    monkeypatch.setattr(dataset,'upstream',lambda:None)
    cfg={'data_root':str(tmp_path),'principals':{'student':{'projects':['lab'],'roles':['collector']},'guest':{'projects':[],'roles':[]}},
        'robot':{'robot_id':'sim','config_revision':'v1','scene':'bench'},'ros':{'domain_id':91},'sinks':{},'sources':{'test':{'kind':'scripted_test'}}}
    service=Service(cfg,{'student':'test-secret','guest':'guest-secret'})
    yield service
    service.shutdown()

def request(service,op,payload,ident=None,principal='student',key=None):
    return service.dispatch(signed({'request_id':ident or uuid.uuid4().hex,'principal':principal,'operation':op,'payload':payload,'issued_at':time.time()},key or service.keys[principal]))

def claim(api):return request(api,'robot.claim',{'robot_id':'sim','project':'lab','config_revision':'v1'})['result']

def test_signature_roles_and_idempotency(api):
    assert request(api,'catalog',{},key='wrong')['error']['code']=='FORBIDDEN'
    assert request(api,'robot.claim',{'robot_id':'sim','project':'lab','config_revision':'v1'},principal='guest')['error']['code']=='FORBIDDEN'
    p={'robot_id':'sim','project':'lab','config_revision':'v1'}
    a=request(api,'robot.claim',p,'same');b=request(api,'robot.claim',p,'same')
    assert a['result']==b['result'];assert verified(a,'test-secret')['ok']
    assert request(api,'robot.claim',{**p,'project':'other'},'same')['error']['code']=='IDEMPOTENCY_CONFLICT'

def test_context_ros_and_unknown_field_fail_before_connect(api):
    c=claim(api);p={k:c[k] for k in ('robot_id','project','config_revision','epoch','control_token')}
    p.update(name='task',description='',scene='bench',ros=api.config['ros'],source='test',fps=10)
    assert request(api,'collection.start',{**p,'epoch':0})['error']['code']=='STALE_CONTROL'
    assert request(api,'collection.start',{**p,'ros':{'domain_id':92}})['error']['code']=='CONFIG_MISMATCH'
    assert request(api,'collection.start',{**p,'shell':'anything'})['error']['code']=='INVALID_FIELDS'
    assert request(api,'collection.start',{**p,'fps':3.5})['error']['code']=='INVALID_FPS'
    assert not api.running

def test_restart_does_not_restore_motion(api,tmp_path):
    claim(api);api.state['sessions']['old']={'state':'RECORDING','pending':'start'}
    api.state['jobs']['job']={'status':'RUNNING'};api.state['datasets']['data']={'upload_state':'UPLOADING'};api.persist()
    other=Service(api.config,api.keys)
    try:
        assert other.state['claim'] is None
        assert other.state['sessions']['old']['state']=='RECOVERABLE'
        assert other.state['jobs']['job']['status']=='INTERRUPTED'
        assert other.state['datasets']['data']['upload_state']=='PENDING_RETRY'
    finally:other.shutdown()

def test_physical_never_uses_simulation_permission(api):
    answer=request(api,'physical.preflight',{})
    assert answer['result']['ready'] is False
    assert request(api,'robot.claim',{'robot_id':'COM15','project':'lab','config_revision':'v1'})['error']['code']=='CONFIG_MISMATCH'

def test_leader_topic_rejects_wrong_clock_stale_and_replay():
    from collection_service.leader_topic import accept_leader
    value={'profile_hash':'profile','clock_id':'boot','sequence':5,'captured_monotonic_ns':1_000_000_000,'native_positions':{str(i):2000 for i in range(1,7)}}
    msg=signed(value,'key')
    assert accept_leader(msg,'key','profile','boot',4,now_ns=1_100_000_000)['sequence']==5
    for clock,seq,now in [('different-host',4,1_100_000_000),('boot',5,1_100_000_000),('boot',4,2_000_000_000)]:
        with pytest.raises(ValueError):accept_leader(msg,'key','profile',clock,seq,now_ns=now)

def test_compressed_ros_contract_does_not_change_legacy_raw():
    from integration.ros_sim_contract import make_contract,validate_contract
    raw=make_contract('sim','hash',[str(i) for i in range(6)],{str(i):[-1,1] for i in range(6)})
    assert validate_contract(raw)['topics']['front']['type']=='sensor_msgs/msg/Image'
    compressed=copy.deepcopy(raw);compressed['schema']='so101.ros2.sim.v2';compressed['camera_transport']={'encoding':'jpeg','quality':90}
    for key in ('front','wrist'):compressed['topics'][key]['type']='sensor_msgs/msg/CompressedImage'
    assert validate_contract(compressed)
    compressed['camera_transport']['quality']=50
    with pytest.raises(ValueError):validate_contract(compressed)
