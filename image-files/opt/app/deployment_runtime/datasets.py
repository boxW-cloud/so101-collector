"""Immutable dataset intake with resumable verified files and native validation."""
import hashlib,json,os,shutil,threading,uuid
from pathlib import Path,PurePosixPath
from .store import Fault,digest,ident,canonical

def file_hash(p):
    with Path(p).open('rb') as f:return 'sha256:'+hashlib.file_digest(f,'sha256').hexdigest()
def atomic(path,value):
    p=Path(path);tmp=p.with_suffix('.tmp');tmp.write_text(canonical(value),encoding='utf-8');tmp.replace(p)
def relative(value):
    p=PurePosixPath(value)
    if not value or '\\' in value or ':' in value or p.is_absolute() or any(x in ('','.','..') for x in value.split('/')) or value=='manifest.json':raise Fault('INVALID_PATH','Only manifest-listed relative file names allowed',400)
    return p
def verify_manifest(m):
    ident(m.get('dataset_id'));ident(m.get('version'))
    if m.get('content_hash')!=digest({k:v for k,v in m.items() if k!='content_hash'}):raise Fault('HASH_MISMATCH','Manifest hash mismatch')
    if not isinstance(m.get('files'),dict) or not m['files'] or len(m['files'])>100000:raise Fault('INVALID_MANIFEST','Invalid file inventory')
    total=0
    for name,f in m['files'].items():
        relative(name)
        if type(f['bytes'])!=int or f['bytes']<0 or not isinstance(f['sha256'],str) or len(f['sha256'])!=71:raise Fault('INVALID_MANIFEST','Invalid file size/hash')
        total+=f['bytes']
    if total>1024**4:raise Fault('DATASET_LIMIT','Dataset exceeds deployment intake bound')
    return total
def native_inspect(path):
    from lerobot_sdk import DatasetManager
    m=json.loads((path/'manifest.json').read_text(encoding='utf-8'))
    if m.get('format')=='gr00t-v2.1':
        from collection_service.conversion import ConvertedInspector
        return ConvertedInspector().inspect(path)
    return DatasetManager().inspect(path)

class Datasets:
    def __init__(self,store,root,inspect=native_inspect):
        self.store=store;self.root=Path(root).resolve();self.inspect=inspect;self.lock=threading.RLock()
        for d in ('staging','versions','transfers'):(self.root/d).mkdir(parents=True,exist_ok=True)
    def begin(self,manifest,principal,project):
        principal.require(project);size=verify_manifest(manifest)
        if shutil.disk_usage(self.root).free<size+64*1024**2:raise Fault('DISK_FULL','Insufficient local staging space')
        key=digest([principal.name,project,manifest['content_hash']])[7:]
        with self.lock:
            record=self.root/'transfers'/(key+'.json')
            if not record.exists():
                stage=self.root/'staging'/key;stage.mkdir(exist_ok=False)
                atomic(record,{'transfer_id':key,'owner':principal.name,'project':project,'manifest':manifest,'state':'UPLOADING'})
        return self.status(key,principal)
    def _record(self,key,principal):
        ident(key);p=self.root/'transfers'/(key+'.json')
        if not p.exists():raise Fault('NOT_FOUND','Transfer not found',404)
        v=json.loads(p.read_text(encoding='utf-8'));principal.require(v['project'])
        if v['owner']!=principal.name:raise Fault('FORBIDDEN','Transfer owner differs',403)
        return p,v
    def status(self,key,principal):
        _,v=self._record(key,principal);offsets={}
        for name,spec in v['manifest']['files'].items():
            p=self.root/('versions' if v['state']=='READY' else 'staging')/(v.get('directory',key))/name
            offsets[name]=p.stat().st_size if p.is_file() else 0
        return {'transfer_id':key,'state':v['state'],'offsets':offsets,'completed_bytes':sum(offsets.values()),'total_bytes':sum(f['bytes'] for f in v['manifest']['files'].values()),'dataset_id':v['manifest']['dataset_id'],'version':v['manifest']['version']}
    def write(self,key,name,offset,data,principal):
        with self.lock:
            _,v=self._record(key,principal);relative(name)
            if v['state']!='UPLOADING':raise Fault('IMMUTABLE','Published transfers cannot be changed')
            if name not in v['manifest']['files']:raise Fault('INVALID_PATH','File not in manifest')
            if type(offset)!=int or offset<0 or len(data)>4*1024**2:raise Fault('INVALID_CHUNK','Invalid offset/chunk',400)
            path=self.root/'staging'/key/name;path.parent.mkdir(parents=True,exist_ok=True)
            if path.is_symlink() or not path.resolve().is_relative_to(self.root/'staging'/key):raise Fault('INVALID_PATH','Links forbidden')
            size=path.stat().st_size if path.exists() else 0
            if offset+len(data)>v['manifest']['files'][name]['bytes']:raise Fault('SIZE_MISMATCH','Chunk exceeds expected file length')
            if offset<size:
                with path.open('rb') as f:f.seek(offset);old=f.read(len(data))
                if old!=data:raise Fault('OFFSET_CONFLICT','Previously acknowledged bytes differ')
            elif offset!=size:raise Fault('OFFSET_CONFLICT','Resume from reported offset')
            else:
                with path.open('ab') as f:f.write(data);f.flush();os.fsync(f.fileno())
            return self.status(key,principal)
    def finish(self,key,principal):
        with self.lock:
            record,v=self._record(key,principal);m=v['manifest']
            if v['state']=='READY':return self.store.get('datasets',m['dataset_id'],m['version'],principal)
            stage=self.root/'staging'/key;directory=key;target=self.root/'versions'/directory
            # A crash after rename may leave a complete unregistered target.
            candidate=target if target.exists() else stage
            for name,f in m['files'].items():
                p=candidate/name
                if p.is_symlink() or not p.resolve().is_relative_to(candidate) or not p.is_file() or p.stat().st_size!=f['bytes']:raise Fault('INCOMPLETE','Transfer incomplete')
                if file_hash(p)!=f['sha256']:raise Fault('HASH_MISMATCH','Transferred file hash differs')
            atomic(candidate/'manifest.json',m);self.inspect(candidate)
            if candidate==stage:stage.rename(target)
            body={'dataset_id':m['dataset_id'],'version':m['version'],'content_hash':m['content_hash'],'robot_id':m.get('robot_id'),
                  'format':m.get('format','lerobot-v3.0'),'schema':m.get('schema'),'local_state':'LOCAL_READY','root':str(target),'manifest':m}
            result=self.store.register('datasets',m['dataset_id'],m['version'],v['project'],principal,body)
            v.update(state='READY',directory=directory);atomic(record,v);return result
    def file(self,key,revision,name,principal):
        value=self.store.get('datasets',key,revision,principal)['body'];root=Path(value['root']);m=value['manifest']
        if name=='manifest.json':return root/name
        relative(name)
        if name not in m['files']:raise Fault('NOT_FOUND','File not in published inventory',404)
        path=root/name
        if path.is_symlink() or not path.resolve().is_relative_to(root) or file_hash(path)!=m['files'][name]['sha256']:raise Fault('HASH_MISMATCH','Published data changed')
        return path
    def register_local(self,path,expected_hash,principal,project,allowed_roots):
        path=Path(path).resolve()
        if not any(path.is_relative_to(Path(r).resolve()) for r in allowed_roots):raise Fault('INVALID_PATH','Source not in configured import roots')
        m=json.loads((path/'manifest.json').read_text(encoding='utf-8'));verify_manifest(m)
        if m['content_hash']!=expected_hash:raise Fault('HASH_MISMATCH','Requested immutable version hash differs')
        t=self.begin(m,principal,project)
        if t['state']=='READY':return self.finish(t['transfer_id'],principal)
        for name in m['files']:
            f=path/name
            if f.is_symlink() or not f.resolve().is_relative_to(path):raise Fault('INVALID_PATH','Source links forbidden')
            offset=t['offsets'][name]
            with f.open('rb') as stream:
                stream.seek(offset)
                while chunk:=stream.read(1024**2):self.write(t['transfer_id'],name,offset,chunk,principal);offset+=len(chunk)
        return self.finish(t['transfer_id'],principal)
