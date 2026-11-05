"""Actual SDK recording, native snapshots, HTTP receiver and background retries.

The device/probe seam is explicitly software-only. Encoding, hash verification,
HTTP transport, receiver publication and registry are their production classes.
"""
import asyncio
import hashlib
import json
import socket
import threading
import time
from pathlib import Path

import pytest

pytest.importorskip('lerobot',reason='Native collection dependencies required')


def until(predicate,timeout=180):
    end=time.monotonic()+timeout
    while not predicate():
        assert time.monotonic()<end, 'Native background condition timed out'
        time.sleep(.03)


class ReceiverHTTP:
    def __init__(self,platform,accounts):
        self.platform=platform;self.accounts=accounts;self.thread=None
        with socket.socket() as reserved:
            reserved.bind(('127.0.0.1',0));self.port=reserved.getsockname()[1]

    def start(self):
        ready=threading.Event();errors=[]
        def run():
            from aiohttp import web
            from deployment_runtime.http import make_app
            self.loop=asyncio.new_event_loop();asyncio.set_event_loop(self.loop)
            async def serve():
                self.runner=web.AppRunner(make_app(self.platform,self.accounts));await self.runner.setup()
                await web.TCPSite(self.runner,'127.0.0.1',self.port).start()
            try:self.loop.run_until_complete(serve())
            except BaseException as exc:errors.append(exc);ready.set();return
            ready.set();self.loop.run_forever();self.loop.close()
        self.thread=threading.Thread(target=run,name='ActualTrainingReceiver',daemon=True);self.thread.start()
        assert ready.wait(10)
        if errors:raise errors[0]

    def stop(self):
        if not self.thread or not self.thread.is_alive():return
        asyncio.run_coroutine_threadsafe(self.runner.cleanup(),self.loop).result(10)
        self.loop.call_soon_threadsafe(self.loop.stop);self.thread.join(10)
        assert not self.thread.is_alive()


def test_native_two_episodes_stop_preserves_third_and_uploads_after_receiver_returns(tmp_path):
    from deployment_runtime.collection_contract import CollectionContract
    from deployment_runtime.collection_uploads import DurableUploads
    from deployment_runtime.service import Platform
    from deployment_runtime.sessions import SessionRuntime
    from deployment_runtime.store import Principal
    from lerobot_sdk import DatasetManager
    from lerobot_sdk.testing import FakeRobot,simulation_config

    token='native-receiver-test-only'
    accounts=[{'name':'student','projects':['lab'],'roles':['operator','trainer','admin'],
               'token_sha256':hashlib.sha256(token.encode()).hexdigest()}]
    token_file=tmp_path/'receiver-token';token_file.write_text(token)
    receiver=Platform({'role':'training','root':str(tmp_path/'center')})
    http=ReceiverHTTP(receiver,accounts);http.start();http.stop()
    # HTTP app cleanup closes its Platform. A receiver restart reopens the same
    # durable registry/intake directories rather than reusing a closed connection.
    receiver=Platform({'role':'training','root':str(tmp_path/'center')});http.platform=receiver
    cfg=simulation_config('native-sim',required_cameras=('front',),max_observation_age_s=3)
    sdk={**cfg.__dict__,'release_pose':None}
    for field in ('units','limits','max_velocity','max_step','tolerance'):sdk[field]=dict(sdk[field])

    class SoftwareTeleop(FakeRobot):
        def leader(self,timeout_s):
            self.used_source_sequence=getattr(self,'used_source_sequence',0)+1
            return {'positions':dict(self.positions),'provenance':{'source_kind':'simulation','source':'test_fixture',
                'source_sequence':self.used_source_sequence,'native_positions':dict.fromkeys('123456',2000+self.used_source_sequence),
                'active_axes':['shoulder_lift','elbow_flex','wrist_flex'],'gains':{'elbow_flex':.15},
                'runtime_config_sha256':'sha256:'+'a'*64}}
        def sample(self,timeout_s):
            from dataclasses import replace
            value=super().sample(timeout_s)
            return replace(value,source_metadata={'clock_mapping':{'method':'fixture-local',
                'source_capture_ns':value.captured_monotonic_ns,'mapped_lower_bound_ns':value.captured_monotonic_ns,
                'exact_hardware_sync':False}})

    runtime=SessionRuntime(tmp_path/'collector/capture',lambda robot:SoftwareTeleop(str(tmp_path),cameras=('front',)))
    collector=Platform({'role':'collection','root':str(tmp_path/'collector'),'default_upload_sink':'training',
        'sinks':{'training':{'url':'http://127.0.0.1:'+str(http.port),'token_file':str(token_file),'principal':'student','project':'lab'}}},sessions=runtime)
    actor=Principal('student',('lab',),('operator','admin','trainer'))
    collector.register('robots',{'project':'lab','id':'native-sim','revision':'v1','body':{
        'robot_id':'native-sim','kind':'simulation','sdk_config':sdk,'camera_shapes':{'front':[12,16,3]},
        'scene_id':'desk','scene_revision':'v1','capabilities':['collection','teleop'],
        'ros':{'domain_id':117,'namespace':'/native_test'}}},actor)
    box=DurableUploads(collector.root/'collection-uploads',lambda job:contract._upload(job),retry_delay_s=.03,max_retry_delay_s=.1)
    contract=CollectionContract(collector,uploads=box,target_probe=lambda *args:{'peer':{'owner':None,'fault':None,'holding':True,'action_queue_empty':True}})
    collector.collection=contract
    first=None
    try:
        first=contract.start({'project':'lab','robot_id':'native-sim','robot_revision':'v1','mode':'simulation_teleop',
            'name':'native collection','fps':5,'calibration_revision':'baseline','scene_id':'desk','scene_revision':'v1'},'native-start',actor)
        key=first['session_id']
        until(lambda:runtime.status(key)['current_frames']>=2)
        second=contract.complete(key,first['episode_id'],{'project':'lab','recording_generation':1},'native-complete-a',actor)
        assert second['state']=='RUNNING' and second['episode_index']==1 and second['current_frames']>=1
        until(lambda:runtime.status(key)['current_frames']>=2)
        third=contract.complete(key,second['episode_id'],{'project':'lab','recording_generation':1},'native-complete-b',actor)
        assert third['state']=='RUNNING' and third['episode_index']==2 and third['current_frames']>=1
        raw_before={p:p.read_bytes() for p in (runtime.root/'tasks'/key).rglob('*.npz')}
        started=time.monotonic();stopped=contract.stop(key,{'project':'lab'},'native-stop',actor);elapsed=time.monotonic()-started
        assert elapsed<5, 'Stop waited for unrelated encoding/network work'
        assert stopped['state']=='IDLE' and stopped['preserved_current_frames']>=1
        assert contract.status({'project':'lab','robot_id':'native-sim','robot_revision':'v1'},actor)['state']=='IDLE'
        assert not collector.store.claims() and all(p.read_bytes()==data for p,data in raw_before.items())
        operations=[second['upload_operation'],third['upload_operation']]
        until(lambda:all(box.get(op)['manifest'] and box.get(op)['error'] for op in operations))
        assert not receiver.store.list('datasets',actor,project='lab')['items']
        assert all(box.get(op)['state']!='READY' for op in operations)
        http.start()
        until(lambda:all(box.get(op)['state']=='READY' for op in operations))
        published=receiver.store.list('datasets',actor,project='lab')['items']
        assert len(published)==2 and {entry['id'] for entry in published}=={key}
        latest=max(published,key=lambda entry:entry['body']['manifest']['episodes'])
        inspected=DatasetManager().inspect(latest['body']['root'])
        assert inspected.episodes==2 and inspected.native_readable
        manifest=latest['body']['manifest']
        assert [ep['episode_id'] for ep in manifest['collection_episodes']]==[first['episode_id'],second['episode_id']]
        assert [ep['episode_index'] for ep in manifest['collection_episodes']]==[0,1]
        assert manifest['content_hash']==box.get(operations[-1])['manifest']['content_hash']
        assert box.get(operations[-1])['receipt']['content_hash']==manifest['content_hash']
        assert box.get(operations[-1])['receipt']['owner']=='student'
        source=json.loads((Path(latest['body']['root'])/'meta/collection-source.json').read_text())
        assert source['source_metadata']['scene_revision']=='v1' and source['source_metadata']['ros_config']['domain_id']==117
        assert source['time_basis']['clock_reference']['clock_id']
        trace=manifest['source_provenance']
        assert trace['frames']==inspected.frames and trace['exact_hardware_sync'] is False
        assert trace['files'] and all(name in manifest['files'] for name in trace['files'])
        for name in trace['files']:
            entry=json.loads((Path(latest['body']['root'])/name).read_text())
            assert entry['collection_binding']['target_type']=='simulation'
            assert entry['provenance']['leader']['source_kind']=='simulation'
            assert entry['provenance']['leader']['source']=='test_fixture'
            assert entry['provenance']['leader']['source_sequence']>=1
            assert entry['provenance']['observation']['clock_mapping']['exact_hardware_sync'] is False
            assert hashlib.sha256((runtime.root/'tasks'/key/entry['raw_frame']['path']).read_bytes()).hexdigest()==entry['raw_frame']['sha256'].removeprefix('sha256:')
        assert all(p.read_bytes()==data for p,data in raw_before.items())
    finally:
        collector.close();http.stop();receiver.close()
