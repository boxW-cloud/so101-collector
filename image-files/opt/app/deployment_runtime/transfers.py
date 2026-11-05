"""Registered sink upload; destination URLs/credentials are not caller supplied."""
import json,urllib.request,urllib.parse
from pathlib import Path
from .store import Fault

def push(platform,p,a,operation_id):
    source=platform.store.get('datasets',p['dataset_id'],p['version'],a)
    if source['project']!=p['project'] or source['body']['content_hash']!=p['content_hash']:raise Fault('DATASET_VERSION_MISMATCH','Upload source project/hash differs')
    sink=platform.config.get('sinks',{}).get(p['sink_id'])
    if not sink:raise Fault('SINK_NOT_CONFIGURED','Select a registered destination')
    mapping=sink.get('principals',{}).get(a.name)
    if mapping:sink={**sink,**mapping,'principal':a.name}
    if sink.get('principal')!=a.name or sink.get('project',p['project'])!=p['project']:raise Fault('PRINCIPAL_MAPPING_REQUIRED','Configure a same-principal/project destination credential before upload')
    if sink.get('kind')=='directory':
        from .collection_destinations import _registered,push_destination
        return push_destination(platform,p,a,operation_id,_registered(sink,p['sink_id'],a,p['project']))
    token=Path(sink['token_file']).read_text().strip();opener=urllib.request.build_opener(urllib.request.ProxyHandler({}));manifest=source['body']['manifest']
    def call(method,path,value=None,raw=None):
        req=urllib.request.Request(sink['url'].rstrip('/')+path,method=method,data=json.dumps(value).encode() if value is not None else raw,
            headers={'Authorization':'Bearer '+token,'Content-Type':'application/json' if value is not None else 'application/octet-stream'})
        with opener.open(req,timeout=120) as response:v=json.load(response)
        if not v.get('ok'):raise Fault('UPLOAD_FAILED','Destination rejected transfer')
        return v['result']
    identity=call('GET','/v2/identity')
    if identity.get('principal')!=a.name or p['project'] not in identity.get('projects',[]):raise Fault('SINK_IDENTITY_MISMATCH','Destination credential identity/project differs; no data uploaded')
    t=call('POST','/v2/transfers',{'project':p['project'],'manifest':manifest});key=t['transfer_id']
    if t['state']!='READY':
        for name in manifest['files']:
            path=platform.datasets.file(p['dataset_id'],p['version'],name,a);offset=t['offsets'][name]
            with path.open('rb') as stream:
                stream.seek(offset)
                while raw:=stream.read(1024**2):
                    state=call('PUT',f'/v2/transfers/{key}/files?'+urllib.parse.urlencode({'path':name,'offset':offset}),raw=raw);offset+=len(raw)
                    platform.store.event(operation_id,'upload_progress',{'transfer_id':key,'completed_bytes':state['completed_bytes'],'total_bytes':state['total_bytes']})
    ready=call('POST',f'/v2/transfers/{key}/finish',{})
    if ready['body']['content_hash']!=p['content_hash'] or ready['owner']!=a.name or ready['project']!=p['project']:raise Fault('HASH_MISMATCH','Remote published hash/ownership differs')
    return {'upload_state':'READY','business_complete':True,'upload_verified':True,'content_hash':p['content_hash'],'target_dataset_id':ready['id'],'target_version':ready['revision'],'storage_ref':p['sink_id']+':'+ready['id']+'/'+ready['revision']}
