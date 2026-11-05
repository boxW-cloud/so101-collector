"""Bridge the existing verified conversion transaction to the new directory."""
import json,re,sys,threading
from pathlib import Path
from collection_service.conversion import ConversionOperations
from .store import Fault,ident

class Converter(ConversionOperations):
    converter_hash='sha256:94bc600b613be43133aabcddde64ca3188048a09cd84e293afe4d5a79614e24c'
    converter_python=sys.executable
    def __init__(self,root):
        from lerobot_sdk import DatasetManager
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True);self.dm=DatasetManager();self.lock=threading.RLock();self.state={'datasets':{}}
    def persist(self):pass  # The atomic published artifact is registered by Platform after validation.
    def converter_command(self,executable,program,scratch):
        return [executable,'-I',str(Path(__file__).with_name('converter_compat.py')),'--script',str(program),'--repo-id','local/source','--root',str(scratch)]

def validate_output_scope(p,a):
    owner=p.get('output_owner',a.name);project=p.get('output_project',p['project'])
    if owner!=a.name or project!=p['project']:
        raise Fault('UNSUPPORTED_OUTPUT_SCOPE','Conversion output currently must remain with the requesting owner and project')
    return owner,project


def convert(platform,p,a,*,operation_id=None):
    from lerobot_sdk import DatasetManager
    owner,project=validate_output_scope(p,a)
    log_ref=f'/v2/operations/{ident(operation_id)}/conversion-log' if operation_id is not None else None
    source=platform.store.get('datasets',p['dataset_id'],p['version'],a)['body']
    if source['content_hash']!=p['content_hash']:raise Fault('HASH_MISMATCH','Source immutable hash differs')
    cfg=platform.config.get('converter')
    if not cfg or cfg['version']!=p['converter_version'] or p['target_format']!='gr00t-v2.1':raise Fault('CONVERTER_NOT_READY','Frozen converter version/target not configured')
    worker=Converter(platform.root/'conversion');ref=DatasetManager().inspect(source['root']).ref
    fields={'source_ref':{name:p[name] for name in ('dataset_id','version','content_hash')},
            'output_owner':owner,'output_project':project,'converter_version':cfg['version'],'log_ref':log_ref}
    result=None
    try:
        result=worker.convert_logged(ref,{**cfg,'python':sys.executable},{'output_owner':owner,'project':project})
        ready=platform.datasets.register_local(result['root'],result['content_hash'],a,project,[worker.root])
        return {**result,**fields,'registered':ready,'source_content_hash':source['content_hash']}
    except Exception as exc:
        # convert_logged creates the log before raising; preserve its original ID.
        log_id=result.get('log_id') if result else None
        if log_id is None:
            matches=re.findall(r'(?:^|[\s;])log_id=([0-9a-f]{32})(?=$|[\s;])',str(exc))
            log_id=matches[-1] if matches else None
        fault=exc if isinstance(exc,Fault) else Fault(getattr(exc,'code','CONVERSION_FAILED'),str(exc))
        fault.details={**getattr(fault,'details',{}),**fields,'log_id':log_id}
        if fault is exc:raise
        raise fault from exc
