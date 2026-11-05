import hashlib,json,threading,urllib.request,urllib.error
from pathlib import Path
from contextlib import contextmanager
import pytest
from collection_http.bridge import Bridge,HttpError,response_status
from collection_http.server import make_server

TOKEN='test-credential-not-deployed'
class FakeBridge(Bridge):
    def __init__(self,config):super().__init__(config,{'student':'test-upstream-key'});self.calls=[];self.dataset=None
    def call(self,principal,operation,payload,request_id=None):
        from collection_http.bridge import OPERATIONS
        if operation not in OPERATIONS:raise HttpError(422,'UNSUPPORTED_OPERATION','Unsupported')
        self.calls.append((principal,operation,payload,request_id))
        if operation=='dataset.status':return {'ok':True,'result':self.dataset}
        return {'ok':True,'result':{'operation_id':'op1','status':'QUEUED'}}

@pytest.fixture
def service(tmp_path):
    cfg={'bind':'127.0.0.1','port':0,'upstream_port':18863,'export_root':str(tmp_path),'token_hashes':{'student':hashlib.sha256(TOKEN.encode()).hexdigest()},'allowed_origins':['https://platform.test']}
    bridge=FakeBridge(cfg);server=make_server(cfg,bridge);t=threading.Thread(target=server.serve_forever,daemon=True);t.start()
    yield f'http://127.0.0.1:{server.server_port}',bridge,tmp_path
    server.shutdown();server.server_close();t.join()

def request(service,path,body=None,headers=None,raw=None,method=None):
    url,_,_=service;h={'Authorization':'Bearer '+TOKEN,**(headers or {})}
    if body is not None:raw=json.dumps(body).encode();h['Content-Type']='application/json'
    req=urllib.request.Request(url+path,data=raw,headers=h,method=method)
    try:response=urllib.request.urlopen(req,timeout=3)
    except urllib.error.HTTPError as e:response=e
    with response:return response.status,response.headers,response.read()

def test_auth_no_spoof_or_unknown_motion(service):
    assert request(service,'/v1/catalog',headers={'Authorization':''})[0]==401
    assert not service[1].calls
    assert request(service,'/v1/commands',{'operation':'catalog','payload':{},'request_id':'x','principal':'admin'})[0]==400
    assert request(service,'/v1/commands',{'operation':'ARM_MOVETO','payload':{},'request_id':'x'})[0]==422
    assert request(service,'/v1/catalog',headers={'Origin':'https://other.test'})[0]==403
    assert not service[1].calls

@pytest.mark.parametrize('rid',[None,'',12,'a b','../x'])
def test_explicit_idempotency_key_required(service,rid):
    assert request(service,'/v1/commands',{'operation':'collection.stop','payload':{},'request_id':rid})[0]==400
    assert not service[1].calls

def test_async_ack_and_context_pass_through(service):
    payload={'session_id':'s1','control_token':'opaque-test','epoch':2,'save_current':True}
    body={'operation':'collection.stop','payload':payload,'request_id':'same-retry'}
    assert request(service,'/v1/commands',body)[0]==202
    assert request(service,'/v1/commands',body)[0]==202
    assert service[1].calls==[('student','collection.stop',payload,'same-retry')]*2
    assert request(service,'/v1/operations/job123')[0]==200
    assert service[1].calls[-1][1:3]==('operation.get',{'operation_id':'job123'})

def test_finite_json_and_preflight(service):
    assert request(service,'/v1/commands',raw=b'{"n":NaN}',headers={'Content-Type':'application/json'})[0]==400
    assert request(service,'/v1/catalog',headers={'Origin':'https://platform.test'},method='OPTIONS')[0]==204
    assert request(service,'/healthz',headers={'Authorization':''})[0]==200

def exported(service):
    _,bridge,root=service;folder=root/'ds1';(folder/'videos').mkdir(parents=True);content=b'0123456789';(folder/'videos/front.mp4').write_bytes(content)
    manifest={'files':{'videos/front.mp4':{'bytes':10,'sha256':'sha256:'+hashlib.sha256(content).hexdigest()}},'dataset_id':'ds1'}
    manifest['content_hash']='sha256:'+hashlib.sha256(json.dumps(manifest,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
    (folder/'manifest.json').write_text(json.dumps(manifest))
    bridge.dataset={'local_state':'LOCAL_READY','content_hash':manifest['content_hash']}
    return folder

def test_download_manifest_and_resume(service):
    exported(service)
    assert request(service,'/v1/datasets/ds1/manifest')[0]==200
    code,headers,raw=request(service,'/v1/datasets/ds1/files/videos/front.mp4',headers={'Range':'bytes=2-5'})
    assert code==206 and raw==b'2345' and headers['Content-Range']=='bytes 2-5/10'
    assert request(service,'/v1/datasets/ds1/files/videos/front.mp4',headers={'Range':'bytes=12-'})[0]==416
    assert request(service,'/v1/datasets/ds1/files/videos/front.mp4',method='HEAD')[2]==b''

def test_download_rejects_traversal_unlisted_modified_and_unsealed(service):
    folder=exported(service)
    assert request(service,'/v1/datasets/ds1/files/%2e%2e/secret')[0]==400
    assert request(service,'/v1/datasets/ds1/files/not-listed')[0]==404
    (folder/'videos/front.mp4').write_bytes(b'bad')
    assert request(service,'/v1/datasets/ds1/files/videos/front.mp4')[0]==409
    service[1].dataset['local_state']='RECORDING'
    assert request(service,'/v1/datasets/ds1/manifest')[0]==409

def test_upstream_timeout_keeps_request_id(monkeypatch):
    b=Bridge({'upstream_port':18863}, {'student':'test-key'})
    def fail(*args,**kwargs):raise urllib.error.URLError('test timeout')
    monkeypatch.setattr(b.opener,'open',fail)
    with pytest.raises(HttpError) as e:b.call('student','collection.stop',{},'stable-request')
    assert e.value.status==504 and e.value.extra['outcome_unknown'] and e.value.extra['request_id']=='stable-request'

def test_error_http_status_mapping():
    assert response_status({'ok':False,'error':{'code':'BUSY'}})==409
    assert response_status({'ok':False,'error':{'code':'NOT_FOUND'}})==404
    assert response_status({'ok':True,'result':{'status':'SUCCEEDED'}})==200
