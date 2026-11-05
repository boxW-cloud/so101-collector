"""Persistent per-episode outbox, independent of collection target ownership.

Only the trusted execute callback touches a configured receiver. Enqueue never
does network work, and READY requires a persisted receiver verification receipt.
"""
import json
import re
import sqlite3
import threading
import time
from pathlib import Path
from contextlib import contextmanager

from lerobot_sdk.core.locking import DeviceLock
from lerobot_sdk.data.filesystem import canonical
from .store import Fault


IDENTITY_FIELDS=('session_id','project','operator','robot_id','target_type','mode',
                 'episode_id','episode_index','recording_generation','manifest_version','upload_operation')


def _json(value):
    return canonical(value).decode('utf-8')


class UploadRejected(Exception):
    """An explicit matching platform validation rejection, not a network retry."""


class UploadPending(Exception):
    """The independent durable sync worker is progressing normally."""


# 同步队列在推送/等平台确认时，外层 outbox 每隔这么久查一次队列状态（只读 SQLite，很便宜）。
PENDING_POLL_S = 5


class DurableUploads:
    def __init__(self,root,execute,retry_delay_s=1,max_retry_delay_s=60):
        self.root=Path(root).resolve();self.root.mkdir(parents=True,exist_ok=True)
        self.path=self.root/'collection-uploads.sqlite3'
        if not callable(execute):raise ValueError('Upload executor required')
        if retry_delay_s<=0 or max_retry_delay_s<retry_delay_s:raise ValueError('Invalid upload retry delays')
        self.execute=execute;self.retry_delay_s=retry_delay_s;self.max_retry_delay_s=max_retry_delay_s
        self._stop=threading.Event();self._wake=threading.Event();self._thread=None
        self._worker_lock=DeviceLock('collection-upload-worker:'+str(self.path))
        with self._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS uploads (
              operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, state TEXT NOT NULL,
              manifest TEXT, prepared_root TEXT, receipt TEXT, attempts INTEGER NOT NULL DEFAULT 0,
              next_attempt_at REAL NOT NULL DEFAULT 0, error TEXT, alert INTEGER NOT NULL DEFAULT 0,
              created_at REAL NOT NULL, updated_at REAL NOT NULL)''')

    @contextmanager
    def _db(self):
        db=sqlite3.connect(self.path,timeout=30)
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA synchronous=FULL')
        try:
            with db:yield db
        finally:db.close()

    def enqueue(self,operation_id,payload):
        missing=[key for key in IDENTITY_FIELDS if key not in payload]
        if missing:raise Fault('UPLOAD_IDENTITY_REQUIRED','Missing upload identity: '+','.join(missing))
        if payload['upload_operation']!=operation_id:raise Fault('UPLOAD_IDENTITY_MISMATCH','Upload operation differs')
        value=_json(payload);now=time.time()
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT payload FROM uploads WHERE operation_id=?',(operation_id,)).fetchone()
            if old:
                if old['payload']!=value:raise Fault('UPLOAD_IDENTITY_CONFLICT','Upload operation has different immutable payload')
            else:
                db.execute('INSERT INTO uploads(operation_id,payload,state,created_at,updated_at) VALUES(?,?,?,?,?)',
                           (operation_id,value,'PENDING',now,now))
        self._wake.set()
        return self.get(operation_id)

    def bind_manifest(self,operation_id,manifest,*,prepared_root=None):
        if (not isinstance(manifest,dict) or not all(isinstance(manifest.get(k),str) and manifest[k] for k in ('dataset_id','version','content_hash'))
            or not re.fullmatch(r'sha256:[0-9a-f]{64}',manifest['content_hash'])):
            raise Fault('INVALID_MANIFEST','A finalized manifest identity is required')
        value=_json(manifest);root=str(Path(prepared_root).resolve()) if prepared_root is not None else None
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT manifest,prepared_root FROM uploads WHERE operation_id=?',(operation_id,)).fetchone()
            if old is None:raise Fault('NOT_FOUND','Upload operation not found')
            if old['manifest'] and old['manifest']!=value:raise Fault('UPLOAD_MANIFEST_CONFLICT','Upload manifest has different immutable content')
            if old['prepared_root'] and root and old['prepared_root']!=root:raise Fault('UPLOAD_MANIFEST_CONFLICT','Prepared root has different immutable identity')
            db.execute("UPDATE uploads SET manifest=?,prepared_root=COALESCE(prepared_root,?),state=CASE WHEN state='PREPARING' THEN 'UPLOADING' ELSE state END,updated_at=? WHERE operation_id=?",
                       (value,root,time.time(),operation_id))
        return self.get(operation_id)

    @staticmethod
    def _decode(row):
        value=dict(row)
        for field in ('payload','manifest','receipt'):
            value[field]=json.loads(value[field]) if value[field] is not None else None
        value['alert']=bool(value['alert'])
        return value

    def get(self,operation_id):
        with self._db() as db:row=db.execute('SELECT * FROM uploads WHERE operation_id=?',(operation_id,)).fetchone()
        if row is None:raise Fault('NOT_FOUND','Upload operation not found')
        return self._decode(row)

    def pending(self):
        with self._db() as db:rows=db.execute("SELECT * FROM uploads WHERE state!='READY' ORDER BY created_at,operation_id").fetchall()
        return [self._decode(row) for row in rows]

    def start(self):
        if self._thread and self._thread.is_alive():return
        self._worker_lock.acquire()
        self._stop.clear()
        try:
            with self._db() as db:
                db.execute("UPDATE uploads SET state='PENDING',next_attempt_at=0,updated_at=? WHERE state IN ('PREPARING','UPLOADING')",(time.time(),))
            self._thread=threading.Thread(target=self._run,name='CollectionUploadOutbox',daemon=True)
            self._thread.start()
        except BaseException:
            self._worker_lock.release();raise

    def _take(self):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute("SELECT * FROM uploads WHERE state IN ('PENDING','RETRY_WAIT','WAITING') AND next_attempt_at<=? ORDER BY created_at,operation_id LIMIT 1",(time.time(),)).fetchone()
            if row is None:return None
            state='UPLOADING' if row['manifest'] else 'PREPARING'
            db.execute('UPDATE uploads SET state=?,attempts=attempts+1,updated_at=? WHERE operation_id=?',(state,time.time(),row['operation_id']))
            value=self._decode(row);value['state']=state;value['attempts']+=1
            return value

    def _verified(self,operation_id,receipt):
        job=self.get(operation_id);manifest=job['manifest']
        if (not manifest or not isinstance(receipt,dict) or receipt.get('upload_verified') is not True
            or receipt.get('business_complete') is not True or receipt.get('content_hash')!=manifest['content_hash']
            or receipt.get('target_dataset_id')!=manifest['dataset_id'] or receipt.get('target_version')!=manifest['version']
            or not isinstance(receipt.get('storage_ref'),str) or not receipt['storage_ref']):
            raise Fault('UPLOAD_RECEIPT_UNVERIFIED','Receiver did not verify the bound manifest identity and registration')
        for field in ('session_id','episode_id','recording_generation','upload_operation'):
            if receipt.get(field)!=job['payload'][field] or type(receipt.get(field)) is not type(job['payload'][field]):
                raise Fault('UPLOAD_RECEIPT_IDENTITY_MISMATCH','Receiver receipt belongs to another recording identity')
        if receipt.get('owner')!=job['payload']['operator'] or receipt.get('project')!=job['payload']['project']:
            raise Fault('UPLOAD_RECEIPT_OWNER_MISMATCH','Receiver registration belongs to another owner/project')
        value=_json(receipt)
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT receipt FROM uploads WHERE operation_id=?',(operation_id,)).fetchone()
            if old['receipt'] and old['receipt']!=value:raise Fault('UPLOAD_RECEIPT_CONFLICT','Receiver receipt has different immutable content')
            db.execute("UPDATE uploads SET state='READY',receipt=?,error=NULL,alert=0,updated_at=? WHERE operation_id=?",(value,time.time(),operation_id))

    def _run(self):
        try:
            while not self._stop.is_set():
                job=self._take()
                if job is None:
                    self._wake.wait(min(self.retry_delay_s,1));self._wake.clear();continue
                try:self._verified(job['operation_id'],self.execute(job))
                except UploadPending:
                    with self._db() as db:
                        db.execute("UPDATE uploads SET state='WAITING',attempts=MAX(0,attempts-1),error=NULL,alert=0,next_attempt_at=?,updated_at=? WHERE operation_id=?",
                                   (time.time()+max(self.retry_delay_s,PENDING_POLL_S),time.time(),job['operation_id']))
                except UploadRejected as exc:
                    with self._db() as db:
                        db.execute("UPDATE uploads SET state='REJECTED',error=?,alert=1,updated_at=? WHERE operation_id=?",(str(exc)[:4000],time.time(),job['operation_id']))
                except Exception as exc:
                    delay=min(self.max_retry_delay_s,self.retry_delay_s*2**min(job['attempts']-1,20))
                    with self._db() as db:
                        db.execute("UPDATE uploads SET state='RETRY_WAIT',error=?,alert=?,next_attempt_at=?,updated_at=? WHERE operation_id=? AND state!='READY'",
                                   (str(exc)[:4000],int(job['attempts']>=3),time.time()+delay,time.time(),job['operation_id']))
        finally:self._worker_lock.release()

    def stop(self,timeout_s=5):
        self._stop.set();self._wake.set()
        if self._thread:self._thread.join(timeout_s)
        return not (self._thread and self._thread.is_alive())
