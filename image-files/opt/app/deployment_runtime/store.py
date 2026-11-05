"""Durable version directory, operation journal and exclusive resource claims."""
from dataclasses import dataclass
import hashlib,json,re,sqlite3,threading,time,uuid
from pathlib import Path

def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
def digest(v):return 'sha256:'+hashlib.sha256(canonical(v).encode()).hexdigest()
def ident(v):
    if not isinstance(v,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}',v):raise Fault('INVALID_ID','ASCII identifier required')
    return v
class Fault(Exception):
    def __init__(self,code,message,status=409):super().__init__(message);self.code=code;self.status=status
@dataclass(frozen=True)
class Principal:
    name:str
    projects:tuple
    roles:tuple
    def require(self,project,role=None):
        if project not in self.projects or role and role not in self.roles:raise Fault('FORBIDDEN','Project or role denied',403)

class Store:
    def __init__(self,path):
        Path(path).parent.mkdir(parents=True,exist_ok=True);self.lock=threading.RLock()
        self.db=sqlite3.connect(path,check_same_thread=False,isolation_level=None);self.db.row_factory=sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''CREATE TABLE IF NOT EXISTS resources(kind TEXT,id TEXT,revision TEXT,project TEXT,owner TEXT,hash TEXT,body TEXT,created REAL,PRIMARY KEY(kind,id,revision));
CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,kind TEXT,owner TEXT,project TEXT,request_id TEXT,fingerprint TEXT,state TEXT,payload TEXT,result TEXT,error TEXT,created REAL,updated REAL,UNIQUE(owner,request_id));
CREATE TABLE IF NOT EXISTS claims(resource TEXT PRIMARY KEY,job_id TEXT,epoch TEXT,state TEXT);
CREATE TABLE IF NOT EXISTS grants(kind TEXT,id TEXT,revision TEXT,subject TEXT,PRIMARY KEY(kind,id,revision,subject));
CREATE TABLE IF NOT EXISTS events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT,kind TEXT,body TEXT,created REAL);''')
        self.db.execute("UPDATE jobs SET state='INTERRUPTED',error=? WHERE state IN ('QUEUED','RUNNING','CANCELLING')",(canonical({'code':'RESTARTED','message':'No replay; reconcile device before reuse'}),))
        self.db.execute("UPDATE claims SET state='RECONCILE_REQUIRED'")
    def close(self):self.db.close()
    def register(self,kind,key,revision,project,principal,body):
        principal.require(project);ident(key);ident(revision);h=digest(body)
        with self.lock:
            old=self.db.execute('SELECT * FROM resources WHERE kind=? AND id=? AND revision=?',(kind,key,revision)).fetchone()
            if old:
                if old['project']!=project or old['owner']!=principal.name:raise Fault('FORBIDDEN','Resource belongs to another owner/project',403)
                if old['hash']!=h:raise Fault('VERSION_CONFLICT','Registered version is immutable')
            else:self.db.execute('INSERT INTO resources VALUES(?,?,?,?,?,?,?,?)',(kind,key,revision,project,principal.name,h,canonical(body),time.time()))
        return self.get(kind,key,revision,principal)
    def get(self,kind,key,revision,principal):
        with self.lock:r=self.db.execute('SELECT * FROM resources WHERE kind=? AND id=? AND revision=?',(kind,key,revision)).fetchone()
        if not r:raise Fault('NOT_FOUND','Registered version not found',404)
        principal.require(r['project'])
        if r['owner']!=principal.name and kind not in ('robots','tasks','agents') and not self.granted(kind,key,revision,principal.name):raise Fault('FORBIDDEN','Version belongs to another owner',403)
        return {**dict(r),'body':json.loads(r['body'])}
    def list(self,kind,principal,*,project,offset=0,limit=50,robot_id=None):
        principal.require(project)
        if type(offset)!=int or type(limit)!=int or offset<0 or not 1<=limit<=200:raise Fault('INVALID_PAGE','Invalid offset/limit')
        with self.lock:rows=self.db.execute('SELECT * FROM resources WHERE kind=? AND project=? ORDER BY created,id,revision',(kind,project)).fetchall()
        values=[{**dict(r),'body':json.loads(r['body'])} for r in rows if kind in ('robots','tasks','agents') or r['owner']==principal.name or self.granted(kind,r['id'],r['revision'],principal.name)]
        if robot_id:values=[r for r in values if r['body'].get('robot_id')==robot_id]
        return {'items':values[offset:offset+limit],'total':len(values),'offset':offset,'next_offset':offset+limit if offset+limit<len(values) else None}
    def granted(self,kind,key,revision,subject):
        with self.lock:return bool(self.db.execute('SELECT 1 FROM grants WHERE kind=? AND id=? AND revision=? AND subject=?',(kind,key,revision,subject)).fetchone())
    def grant(self,kind,key,revision,principal,subject):
        entry=self.get(kind,key,revision,principal);ident(subject)
        if entry['owner']!=principal.name or kind not in ('datasets','models'):raise Fault('FORBIDDEN','Only artifact owner grants read access',403)
        with self.lock:self.db.execute('INSERT OR IGNORE INTO grants VALUES(?,?,?,?)',(kind,key,revision,subject))
        return {'kind':kind,'id':key,'revision':revision,'subject':subject,'access':'read','project':entry['project']}
    def prior_job(self,kind,payload,request_id,principal,project):
        principal.require(project);ident(request_id)
        with self.lock:prior=self.db.execute('SELECT id,fingerprint FROM jobs WHERE owner=? AND request_id=?',(principal.name,request_id)).fetchone()
        if prior:
            if prior['fingerprint']!=digest([kind,project,payload]):raise Fault('IDEMPOTENCY_CONFLICT','request_id conflict')
            return self.job(prior['id'],principal)
        return None
    def create_job(self,kind,payload,request_id,principal,project,resource=None,request_payload=None,key=None):
        # key：v10 开始采集由上游下发的 id 当作作业/会话 ID；不传时仍由容器生成 uuid。
        principal.require(project);ident(request_id);fingerprint=digest([kind,project,payload if request_payload is None else request_payload]);now=time.time()
        if key is not None:ident(key)
        with self.lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:
                prior=self.db.execute('SELECT id,fingerprint FROM jobs WHERE owner=? AND request_id=?',(principal.name,request_id)).fetchone()
                if prior:
                    if prior['fingerprint']!=fingerprint:raise Fault('IDEMPOTENCY_CONFLICT','request_id conflict')
                    self.db.execute('COMMIT');return self.job(prior['id'],principal),False
                if resource and self.db.execute('SELECT 1 FROM claims WHERE resource=?',(resource,)).fetchone():raise Fault('BUSY','Resource busy or requires stop reconciliation')
                if key is None:key=uuid.uuid4().hex
                elif self.db.execute('SELECT 1 FROM jobs WHERE id=?',(key,)).fetchone():raise Fault('ID_ALREADY_USED','This id has already been used by an earlier collection',409)
                self.db.execute('INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(key,kind,principal.name,project,request_id,fingerprint,'QUEUED',canonical(payload),None,None,now,now))
                if resource:self.db.execute('INSERT INTO claims VALUES(?,?,?,?)',(resource,key,uuid.uuid4().hex,'ACTIVE'))
                self.db.execute('COMMIT')
            except BaseException:self.db.execute('ROLLBACK');raise
        return self.job(key,principal),True
    def job(self,key,principal):
        with self.lock:r=self.db.execute('SELECT * FROM jobs WHERE id=?',(key,)).fetchone()
        if not r:raise Fault('NOT_FOUND','Operation not found',404)
        principal.require(r['project'])
        if r['owner']!=principal.name:raise Fault('FORBIDDEN','Operation owner differs',403)
        value=dict(r)
        for k in ('payload','result','error'):value[k]=json.loads(value[k]) if value[k] else None
        value['operation_id']=value.pop('id');return value
    def update_job(self,key,state,*,result=None,error=None):
        with self.lock:self.db.execute('UPDATE jobs SET state=?,result=?,error=?,updated=? WHERE id=?',(state,canonical(result) if result is not None else None,canonical(error) if error else None,time.time(),key))
    def release(self,resource,job_id,*,holding):
        with self.lock:
            if holding:self.db.execute('DELETE FROM claims WHERE resource=? AND job_id=?',(resource,job_id))
            else:self.db.execute("UPDATE claims SET state='STOP_UNCONFIRMED' WHERE resource=? AND job_id=?",(resource,job_id))
    def claims(self):
        with self.lock:return [dict(r) for r in self.db.execute('SELECT * FROM claims')]
    def event(self,job_id,kind,body):
        with self.lock:self.db.execute('INSERT INTO events(job_id,kind,body,created) VALUES(?,?,?,?)',(job_id,kind,canonical(body),time.time()))
    def events(self,key,principal,after=0):
        self.job(key,principal)
        with self.lock:rows=self.db.execute('SELECT * FROM events WHERE job_id=? AND sequence>? ORDER BY sequence LIMIT 200',(key,after)).fetchall()
        return [{**dict(r),'body':json.loads(r['body'])} for r in rows]
