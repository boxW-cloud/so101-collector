"""Pinned V3 -> V2.1 tool on a private copy, then verify and atomically publish."""
from dataclasses import asdict
from pathlib import Path
import json,os,shutil,subprocess,sys,tempfile,uuid
from lerobot_sdk.data.filesystem import inventory,digest,sha_file,atomic_json,publish_directory
from lerobot_sdk.data.contracts import DatasetRef,DatasetInspection
from lerobot_sdk.data.sinks import LocalDirectorySink
from lerobot_sdk.data.transfer import upload
from .protocol import ApiError

PINNED_CONVERTER='sha256:29a8388e48759aa10a17bfcbfd961e038f569e8520392ab61b6014382aec8c0e'

def validate_v21(root,expected=None):
    import numpy as np
    import pyarrow.parquet as pq
    import av
    root=Path(root);info=json.loads((root/'meta/info.json').read_text())
    if info['codebase_version']!='v2.1':raise ValueError('Converter did not produce LeRobot v2.1')
    rows=[json.loads(line) for line in (root/'meta/episodes.jsonl').read_text().splitlines() if line]
    tasks=[json.loads(line) for line in (root/'meta/tasks.jsonl').read_text().splitlines() if line]
    if not tasks:raise ValueError('Missing task metadata')
    task_ids={v['task_index'] for v in tasks if isinstance(v.get('task'),str) and v['task'].strip()}
    if expected and 'joint_names' in expected:
        for key in ('observation.state','action'):
            feature=info['features'][key]
            if feature['dtype']!='float32' or feature['shape']!=[6] or feature['names']!=expected['joint_names']:raise ValueError('Converted joint names/order/schema changed')
    if expected and (len(rows)!=expected['episodes'] or info['total_frames']!=expected['frames']):raise ValueError('Episode/frame counts changed')
    cameras=[k for k,v in info['features'].items() if v['dtype']=='video'];total=0
    for index,episode in enumerate(rows):
        if episode['episode_index']!=index:raise ValueError('Non-contiguous episode indices')
        chunks=info.get('chunks_size',1000);chunk=index//chunks
        path=root/info['data_path'].format(episode_chunk=chunk,episode_index=index)
        table=pq.read_table(path);count=len(table);total+=count
        if count!=episode['length']:raise ValueError('Episode length mismatch')
        if not set(table['task_index'].to_pylist())<=task_ids or table['annotation.human.task_description'].to_pylist()!=table['task_index'].to_pylist():raise ValueError('Language annotation/task mapping mismatch')
        for key in ('observation.state','action'):
            a=np.asarray(table[key].to_pylist())
            if a.shape!=(count,6) or not np.isfinite(a).all():raise ValueError('Six-axis features invalid')
        for key in cameras:
            path=root/info['video_path'].format(episode_chunk=chunk,episode_index=index,video_key=key)
            with av.open(str(path)) as video:
                frames=sum(1 for frame in video.decode(video=0) if frame.width>0 and frame.height>0)
            if frames!=count:raise ValueError(f'Video frame count mismatch: {key}, {frames} != {count}')
    if total!=info['total_frames']:raise ValueError('Total frame count mismatch')
    modality=json.loads((root/'meta/modality.json').read_text())
    if not all(k in modality for k in ('state','action','video','annotation')):raise ValueError('GR00T modality mapping missing')
    return {'episodes':len(rows),'frames':total,'camera_keys':cameras,'codebase_version':'v2.1','schema':info['features']}

class ConvertedInspector:
    def inspect(self,ref):
        root=Path(ref.root if isinstance(ref,DatasetRef) else ref).resolve();m=json.loads((root/'manifest.json').read_text())
        if m['format']!='gr00t-v2.1' or m['content_hash']!=digest({k:v for k,v in m.items() if k!='content_hash'}):raise ValueError('Converted manifest invalid')
        files=inventory(root)
        if files!=m['files']:raise ValueError('Converted data files changed')
        stats=validate_v21(root,m)
        result=DatasetRef(root,m['dataset_id'],m['version'],m['content_hash'])
        if isinstance(ref,DatasetRef) and result!=ref:raise ValueError('Converted version binding mismatch')
        # Structural/full-decode validation is not a GR00T native-loader run.
        return DatasetInspection(result,stats['episodes'],stats['frames'],len(files),sum(v['bytes'] for v in files.values()),'gr00t-v2.1',tuple(stats['camera_keys']),False)
    def upload(self,ref,sink):return upload(self,ref,sink)

class ConversionOperations:
    converter_hash=PINNED_CONVERTER
    converter_python='/usr/local/bin/python'
    def converter_command(self,executable,program,scratch):return [executable,str(program),'--repo-id','local/source','--root',str(scratch)]
    def conversion_log(self,job):
        ident=job.get('result',{}).get('log_id')
        if not ident:
            message=str(job.get('error',{}).get('message',''))
            ident=message.rsplit('log_id=',1)[-1] if 'log_id=' in message else None
        if not ident or len(ident)!=32 or any(c not in '0123456789abcdef' for c in ident):return {'status':job['status'],'text':'Log not yet available'}
        path=self.root/'conversion-logs'/(ident+'.log')
        return {'status':job['status'],'log_id':ident,'text':path.read_text(errors='replace')[-32000:],'tail_limit_bytes':32000}

    def handle_conversion(self,p,principal):
        self.fields(p,['source_dataset_id','source_version','target_format','converter_version','output_owner','project'])
        source=self.owner('datasets',p['source_dataset_id'],principal);self.permission(principal,p['project'])
        if p['output_owner']!=principal or p['project']!=source['project']:raise ApiError('FORBIDDEN','Conversion output must remain with authorized owner/project')
        if source.get('format')=='gr00t-v2.1':raise ApiError('INVALID_FORMAT','This converter requires LeRobot v3.0')
        inspected=self.dm.inspect(Path(source['root']))
        if p['source_version']!=inspected.ref.version:raise ApiError('DATASET_VERSION_MISMATCH','Use the exact immutable source version')
        converter=next((c for c in self.config.get('converters',{}).values() if c['version']==p['converter_version'] and c['target_format']==p['target_format']),None)
        if not converter or converter['sha256']!=self.converter_hash:raise ApiError('CONVERTER_NOT_ALLOWED','Converter version/format is not registered')
        return self.submit('dataset.convert',principal,p['project'],lambda:self.convert_logged(inspected.ref,converter,p),transfer=True)

    def convert_logged(self,source,converter,p):
        ident=uuid.uuid4().hex
        try:return self.convert_data(source,converter,p,ident=ident)
        except Exception as exc:
            folder=self.root/'conversion-logs';folder.mkdir(exist_ok=True)
            with (folder/(ident+'.log')).open('a') as stream:stream.write('\nAdapter validation/publication failed: '+str(exc)+'\n')
            raise ApiError('CONVERSION_FAILED',f'{type(exc).__name__}: {exc}; log_id={ident}') from exc

    def convert_data(self,source,converter,p,*,ident=None):
        inspected=self.dm.inspect(source);program=Path(converter['path'])
        if sha_file(program)!=self.converter_hash:raise ApiError('CONVERTER_CHANGED','Frozen converter hash differs')
        ident=ident or uuid.uuid4().hex;root=self.root/'converted';root.mkdir(exist_ok=True);target=root/ident
        log_dir=self.root/'conversion-logs';log_dir.mkdir(exist_ok=True);log=log_dir/(ident+'.log')
        with tempfile.TemporaryDirectory(prefix='.convert-',dir=root) as scratch:
            scratch=Path(scratch);working=scratch/'local/source';working.parent.mkdir();shutil.copytree(source.root,working)
            env={**os.environ,'HF_HUB_OFFLINE':'1','TRANSFORMERS_OFFLINE':'1'}
            with log.open('xb') as stream:
                executable=converter.get('python','/usr/local/bin/python')
                if executable!=self.converter_python:raise ApiError('CONVERTER_RUNTIME_MISMATCH','Use the frozen converter Python runtime')
                env.pop('PYTHONHOME',None);env.pop('VIRTUAL_ENV',None);env['PYTHONPATH']=''
                env['PATH']='/usr/local/bin:'+env.get('PATH','/usr/bin:/bin')
                proc=subprocess.run(self.converter_command(executable,program,scratch),stdout=stream,stderr=subprocess.STDOUT,env=env,timeout=900)
            if proc.returncode:raise ApiError('CONVERSION_FAILED',f'Frozen converter failed; log_id={ident}')
            from .video_alignment import align_videos
            alignment=align_videos(source.root,working)
            import pyarrow.parquet as pq
            for path in sorted((working/'data').rglob('*.parquet')):
                table=pq.read_table(path);column='annotation.human.task_description'
                if column in table.column_names:raise ValueError('Unexpected existing annotation column')
                pq.write_table(table.append_column(column,table['task_index']),path)
            info=json.loads((working/'meta/info.json').read_text());info['features']['annotation.human.task_description']=dict(info['features']['task_index']);atomic_json(working/'meta/info.json',info)
            modality={'state':{'single_arm':{'start':0,'end':5,'original_key':'observation.state'},'gripper':{'start':5,'end':6,'original_key':'observation.state'}},
                'action':{'single_arm':{'start':0,'end':5,'original_key':'action'},'gripper':{'start':5,'end':6,'original_key':'action'}},
                'video':{key:{'original_key':'observation.images.'+key} for key in inspected.camera_keys},
                'annotation':{'human.task_description':{'original_key':'task_index'}}}
            atomic_json(working/'meta/modality.json',modality)
            source_manifest=json.loads((source.root/'manifest.json').read_text())
            expected={'episodes':inspected.episodes,'frames':inspected.frames,'joint_names':source_manifest['joint_names']};stats=validate_v21(working,expected)
            manifest={'format':'gr00t-v2.1','dataset_id':ident,'version':uuid.uuid4().hex,'owner':p['output_owner'],'project':p['project'],
                **expected,'source':{'dataset_id':source.dataset_id,'version':source.version,'content_hash':source.content_hash},
                'converter':{'version':converter['version'],'sha256':self.converter_hash,'runtime':converter.get('runtime_id','workshop-python311'),'adapter_sha256':sha_file(__file__),'ffmpeg_sha256':sha_file(__import__('shutil').which('ffmpeg') or '/usr/local/bin/ffmpeg')},'modality_mapping_version':'so101-six-axis-front-wrist-language-v2',
                'video_alignment':alignment,'joint_names':source_manifest['joint_names'],'units':source_manifest['units'],'camera_keys':list(inspected.camera_keys),'files':inventory(working)}
            manifest['content_hash']=digest(manifest);atomic_json(working/'manifest.json',manifest)
            self.dm.inspect(source)
            if sha_file(program)!=self.converter_hash:raise ApiError('CONVERTER_CHANGED','Converter changed during conversion')
            ConvertedInspector().inspect(working);publish_directory(working,target)
        result={'dataset_id':ident,'version':manifest['version'],'content_hash':manifest['content_hash'],'format':'gr00t-v2.1','root':str(target),
            'episodes':stats['episodes'],'frames':stats['frames'],'converter_version':converter['version'],'source_version':source.version,'log_id':ident,'status':'SUCCEEDED'}
        with self.lock:self.state['datasets'][ident]={**result,'principal':p['output_owner'],'project':p['project'],'local_state':'LOCAL_READY','upload_state':'NOT_REQUESTED'};self.persist()
        return result

    def converted_dataset_op(self,op,data,p):
        manager=ConvertedInspector();info=manager.inspect(Path(data['root']))
        if op=='dataset.inspect':return json.loads(json.dumps(asdict(info),default=str))
        if op=='dataset.export':target=Path(self.config['export_root'])/data['dataset_id']
        else:
            sink=self.config['sinks'][p['sink_id']]
            if sink['kind']!='mounted_directory':raise ApiError('UNSUPPORTED_SINK','Converted output needs registered mounted sink')
            target=Path(sink['root'])/data['dataset_id'];data['upload_state']='UPLOADING'
        operation=manager.upload(info.ref,LocalDirectorySink(target,inspector=manager))
        try:
            while operation.status().state.value not in ('SUCCEEDED','FAILED','CANCELLED'):
                import time
                progress=operation.progress()
                if progress:self.emit('upload.progress',data['principal'],{'dataset_id':data['dataset_id'],**asdict(progress)})
                time.sleep(.1)
            result=operation.wait(300)
            with self.lock:
                if op=='dataset.upload':data.update(upload_state='READY',storage_ref=result.storage_ref)
                self.persist()
            return asdict(result)
        except Exception:
            with self.lock:
                if op=='dataset.upload':data['upload_state']='PENDING_RETRY';self.persist()
            raise
