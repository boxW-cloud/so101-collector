"""Durable episode uploads and real native snapshots, without robot motion."""
import copy
import importlib
import importlib.util
import sqlite3
import threading
import time
from pathlib import Path

import pytest


def module(name):
    qualified='deployment_runtime.'+name
    assert importlib.util.find_spec(qualified) is not None, 'Missing '+qualified
    return importlib.import_module(qualified)


def until(predicate, timeout=10):
    end=time.monotonic()+timeout
    while not predicate():
        assert time.monotonic()<end, 'Background condition timed out'
        time.sleep(.01)


def payload():
    return dict(session_id='batch-a',project='lab',operator='student',robot_id='sim-a',
                target_type='simulation',mode='simulation_teleop',episode_id='episode-a',
                episode_index=0,recording_generation=2,manifest_version=1,
                upload_operation='upload-a',sink_id='center')


def manifest():
    return dict(dataset_id='batch-a',version='episode-a-g2',content_hash='sha256:'+'a'*64)


def receipt():
    return dict(upload_verified=True,business_complete=True,content_hash=manifest()['content_hash'],
                target_dataset_id='batch-a',target_version='episode-a-g2',storage_ref='center:batch-a/episode-a-g2',
                session_id='batch-a',episode_id='episode-a',recording_generation=2,upload_operation='upload-a',
                owner='student',project='lab')


def test_outbox_persists_payload_and_manifest_before_execution(tmp_path):
    uploads=module('collection_uploads')
    original=payload()
    queue=uploads.DurableUploads(tmp_path,lambda job:pytest.fail('enqueue must not upload'))
    queue.enqueue('upload-a',original)
    original['episode_id']='mutated'
    queue.bind_manifest('upload-a',manifest())
    queue.stop()
    reopened=uploads.DurableUploads(tmp_path,lambda job:pytest.fail('not started'))
    assert reopened.get('upload-a')['payload']==payload()
    assert reopened.get('upload-a')['manifest']==manifest()
    assert reopened.pending()[0]['operation_id']=='upload-a'
    with pytest.raises(Exception,match='different|conflict|immutable'):
        reopened.enqueue('upload-a',{**payload(),'recording_generation':3})
    with pytest.raises(Exception,match='different|conflict|immutable'):
        reopened.bind_manifest('upload-a',{**manifest(),'content_hash':'sha256:'+'b'*64})
    reopened.stop()


def test_worker_retries_original_manifest_and_records_verified_receipt(tmp_path):
    uploads=module('collection_uploads')
    attempts=[]
    def execute(job):
        attempts.append(copy.deepcopy(job))
        if len(attempts)<3:raise ConnectionError('receiver unavailable')
        return receipt()
    queue=uploads.DurableUploads(tmp_path,execute,retry_delay_s=.01,max_retry_delay_s=.02)
    queue.enqueue('upload-a',payload());queue.bind_manifest('upload-a',manifest());queue.start()
    try:
        until(lambda:queue.get('upload-a')['state']=='READY')
        job=queue.get('upload-a')
        assert job['attempts']==3 and job['receipt']==receipt()
        assert all(v['payload']==payload() and v['manifest']==manifest() for v in attempts)
    finally:queue.stop()


@pytest.mark.parametrize('bad',[{}, {'upload_verified':False}, {**receipt(),'content_hash':'sha256:'+'b'*64}])
def test_unverified_or_wrong_hash_receipt_never_means_ready(tmp_path,bad):
    uploads=module('collection_uploads')
    queue=uploads.DurableUploads(tmp_path,lambda job:bad,retry_delay_s=.01,max_retry_delay_s=.02)
    queue.enqueue('upload-a',payload());queue.bind_manifest('upload-a',manifest());queue.start()
    try:
        until(lambda:queue.get('upload-a')['alert'])
        job=queue.get('upload-a')
        assert job['state']!='READY' and job['error'] and job['alert']
        assert job['manifest']==manifest()
    finally:queue.stop()


def test_restart_automatically_requeues_interrupted_job(tmp_path):
    uploads=module('collection_uploads')
    queue=uploads.DurableUploads(tmp_path,lambda job:None)
    queue.enqueue('upload-a',payload());queue.bind_manifest('upload-a',manifest());queue.stop()
    with sqlite3.connect(tmp_path/'collection-uploads.sqlite3') as db:
        db.execute("UPDATE uploads SET state='UPLOADING' WHERE operation_id='upload-a'")
    resumed=uploads.DurableUploads(tmp_path,lambda job:receipt(),retry_delay_s=.01)
    resumed.start()
    try:until(lambda:resumed.get('upload-a')['state']=='READY')
    finally:resumed.stop()


def test_queue_does_not_block_collection_thread_on_network(tmp_path):
    uploads=module('collection_uploads')
    entered=threading.Event();release=threading.Event()
    def execute(job):
        entered.set();assert release.wait(10);return receipt()
    queue=uploads.DurableUploads(tmp_path,execute)
    queue.enqueue('upload-a',payload());queue.bind_manifest('upload-a',manifest());queue.start()
    try:
        assert entered.wait(3)
        next_payload={**payload(),'episode_id':'episode-b','episode_index':1,'upload_operation':'upload-b'}
        started=time.monotonic();queue.enqueue('upload-b',next_payload)
        assert time.monotonic()-started<1
        assert queue.get('upload-b')['state']=='PENDING'
    finally:release.set();queue.stop()


def raw_task(tmp_path):
    pytest.importorskip('lerobot')
    import numpy as np
    from lerobot_sdk import CollectionConfig,JOINT_NAMES
    from lerobot_sdk.testing import simulation_config
    from lerobot_sdk.data.filesystem import atomic_json,digest,sha_file
    config=simulation_config('sim-a',required_cameras=('front',))
    directory=tmp_path/'tasks'/'batch-a';directory.mkdir(parents=True)
    cfg=CollectionConfig('batch-a','snapshot integration',directory.parent,5,'move object',
                         config.robot_id,config.calibration_hash,config.units,{'front':(12,16,3)},min_free_bytes=1)
    commits=[]
    for ep in range(2):
        episode=directory/'episodes'/('raw-'+str(ep));episode.mkdir(parents=True)
        files=[]
        for index in range(3):
            frame={key:np.zeros(6,dtype=np.float32) for key in ('observation.state','action','action.requested','action.executed')}
            frame.update({'task':cfg.task,'observation.images.front':np.full((12,16,3),index+ep,dtype=np.uint8)})
            for key,value in [('sdk.source_sequence',ep*10+index),('sdk.captured_monotonic_ns',10**9+ep*10**9+index*200000000),
                              ('sdk.sent_monotonic_ns',10**9+ep*10**9+index*200000000+1),('sdk.epoch',2),
                              ('sdk.camera.front.captured_monotonic_ns',10**9+ep*10**9+index*200000000)]:
                frame[key]=np.array([value],dtype=np.int64)
            path=episode/(str(index)+'.npz');np.savez(path,**frame)
            files.append({'path':path.relative_to(directory).as_posix(),'sha256':sha_file(path)})
        entry={'episode_index':ep,'frames':3,'task':cfg.task,'files':files};entry['content_hash']=digest(entry);commits.append(entry)
    atomic_json(directory/'task.json',{'config':cfg.to_dict(),'commits':commits})
    return directory,commits


def identities(count):
    return [{**payload(),'episode_id':'episode-'+str(i),'episode_index':i,'upload_operation':'upload-'+str(i)} for i in range(count)]


def test_snapshots_publish_immutable_native_prefixes_with_episode_identity(tmp_path):
    snapshots=module('collection_snapshots')
    from lerobot_sdk import DatasetManager
    directory,commits=raw_task(tmp_path)
    original={p:p.read_bytes() for p in directory.rglob('*.npz')}
    first_payload={**identities(1)[-1],'collection_episodes':identities(1)}
    second_payload={**identities(2)[-1],'collection_episodes':identities(2)}
    first=snapshots.prepare_snapshot(directory,commits[:1],first_payload,tmp_path/'snapshots')
    second=snapshots.prepare_snapshot(directory,commits,second_payload,tmp_path/'snapshots')
    assert first['dataset_id']==second['dataset_id']=='batch-a'
    assert first['version']!=second['version']
    assert DatasetManager().inspect(first['dataset_root']).episodes==1
    assert DatasetManager().inspect(second['dataset_root']).episodes==2
    assert first['manifest']['collection_episodes']==identities(1)
    assert second['manifest']['collection_episodes']==identities(2)
    assert set(first['manifest']['file_metadata'])==set(first['manifest']['files'])
    assert all(v['relative_path']==k and v['time_basis'] for k,v in first['manifest']['file_metadata'].items())
    assert all(p.read_bytes()==data for p,data in original.items())
    again=snapshots.prepare_snapshot(directory,commits[:1],first_payload,tmp_path/'snapshots')
    assert again['content_hash']==first['content_hash']


@pytest.mark.parametrize('change',['corrupt','escape','identity'])
def test_snapshot_refuses_torn_commits_and_unsafe_identity(tmp_path,change):
    snapshots=module('collection_snapshots')
    directory,commits=raw_task(tmp_path)
    p={**identities(1)[-1],'collection_episodes':identities(1)}
    if change=='corrupt':(directory/commits[0]['files'][0]['path']).write_bytes(b'corrupt')
    if change=='escape':commits[0]['files'][0]['path']='../other.npz'
    if change=='identity':p['episode_id']='../../outside'
    with pytest.raises(Exception):snapshots.prepare_snapshot(directory,commits[:1],p,tmp_path/'snapshots')
    assert not list((tmp_path/'snapshots').glob('batch-a/*'))


@pytest.mark.parametrize('change',['count','last_episode','last_generation','last_operation','trusted_root','robot','version','manifest_version','episode_index'])
def test_snapshot_requires_exact_generation_and_trusted_journal(tmp_path,change):
    snapshots=module('collection_snapshots')
    directory,commits=raw_task(tmp_path)
    p={**identities(1)[-1],'collection_episodes':identities(1)}
    kwargs={'trusted_task_root':directory.parent}
    if change=='count':p['collection_episodes']=identities(2)
    if change=='last_episode':p['episode_id']='different'
    if change=='last_generation':p['recording_generation']=3
    if change=='last_operation':p['upload_operation']='different'
    if change=='trusted_root':kwargs['trusted_task_root']=tmp_path/'another'
    if change=='robot':p['robot_id']='another';p['collection_episodes'][0]['robot_id']='another'
    if change=='version':p['version']='arbitrary-version'
    if change=='manifest_version':
        p['manifest_version']=2;p['collection_episodes'][0]['manifest_version']=2
    if change=='episode_index':
        p['episode_index']=False;p['collection_episodes'][0]['episode_index']=False
    with pytest.raises(Exception):snapshots.prepare_snapshot(directory,commits[:1],p,tmp_path/'snapshots',**kwargs)


def test_worker_refuses_receipt_for_different_dataset_version(tmp_path):
    uploads=module('collection_uploads')
    queue=uploads.DurableUploads(tmp_path,lambda job:{**receipt(),'target_version':'old-generation'},retry_delay_s=.01)
    queue.enqueue('upload-a',payload());queue.bind_manifest('upload-a',manifest());queue.start()
    try:
        until(lambda:queue.get('upload-a')['alert'])
        assert queue.get('upload-a')['receipt'] is None
    finally:queue.stop()


@pytest.mark.parametrize('field',['session_id','episode_id','recording_generation','upload_operation','owner','project'])
def test_worker_refuses_late_receipt_from_other_recording_or_owner(tmp_path,field):
    uploads=module('collection_uploads')
    wrong={**receipt(),field:3 if field=='recording_generation' else 'another'}
    queue=uploads.DurableUploads(tmp_path,lambda job:wrong,retry_delay_s=.01,max_retry_delay_s=.02)
    queue.enqueue('upload-a',payload());queue.bind_manifest('upload-a',manifest());queue.start()
    try:
        until(lambda:queue.get('upload-a')['state']=='READY' or queue.get('upload-a')['alert'])
        assert queue.get('upload-a')['state']!='READY'
    finally:queue.stop()


def test_snapshot_cleanup_failure_releases_version_lock_and_preserves_publication(tmp_path):
    snapshots=module('collection_snapshots')
    from lerobot_sdk.adapters.seeed_dataset import SeeedWriter
    directory,commits=raw_task(tmp_path)
    p={**identities(1)[-1],'collection_episodes':identities(1)}
    class CleanupFails(SeeedWriter):
        def abort(self):raise OSError('Injected writer cleanup failure')
    with pytest.raises(OSError,match='cleanup'):
        snapshots.prepare_snapshot(directory,commits[:1],p,tmp_path/'snapshots',writer_factory=CleanupFails)
    # The immutable publication is complete, but the first invocation reported
    # its cleanup error. A retry must be able to validate and reuse that version.
    result=snapshots.prepare_snapshot(directory,commits[:1],p,tmp_path/'snapshots')
    assert result['manifest']['episodes']==1
