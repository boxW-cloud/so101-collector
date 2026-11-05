"""Native immutable committed prefixes, prepared by the background upload worker."""
import re
import shutil
import uuid
from pathlib import Path

from lerobot_sdk.adapters.seeed_dataset import SeeedWriter,action_semantics_of,joint_names_of,joint_units_of,robot_type_of
from lerobot_sdk.core.errors import ContractMismatch
from lerobot_sdk.core.locking import DeviceLock
from lerobot_sdk.core.types import JOINT_NAMES
from lerobot_sdk.data.collection import _raw_frame,_verify_commits
from lerobot_sdk.data.contracts import CollectionConfig,DATA_SCHEMA,DatasetRef,SEEED_COMMIT
from lerobot_sdk.data.filesystem import atomic_json,digest,inventory,publish_directory,read_json
from lerobot_sdk.data.manager import DatasetManager
from .collection_uploads import IDENTITY_FIELDS
from .session_fields import public_metadata
from lerobot_sdk.data.provenance import training_frame,export_frame_provenance


def _identifier(value,label):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}',value):
        raise ContractMismatch('Unsafe '+label)
    return value


def _file_timelines(root,files,timelines,fps):
    import pyarrow.parquet as pq
    measured={ep['episode_index']:ep for ep in timelines}
    native=[]
    for path in sorted((root/'meta/episodes').rglob('*.parquet')):
        native.extend(pq.read_table(path).to_pylist())
    output={}
    for name in files:
        entry={'relative_path':name,'time_basis':'dataset_metadata','time_range':None,
               'episode_indices':[ep['episode_index'] for ep in timelines]}
        path=root/name
        if name.startswith('data/') and path.suffix=='.parquet':
            rows=pq.read_table(path,columns=['episode_index','sdk.captured_monotonic_ns']).to_pylist()
            stamps=[int(row['sdk.captured_monotonic_ns']) for row in rows]
            entry.update(time_basis='host_monotonic_ns',time_range={'start':min(stamps),'end':max(stamps)},
                         episode_indices=sorted({row['episode_index'] for row in rows}))
        elif name.startswith('videos/') and path.suffix=='.mp4':
            match=re.fullmatch(r'videos/(.+)/chunk-(\d+)/file-(\d+)\.mp4',name)
            if not match:raise ContractMismatch('Unsupported native video path')
            camera,chunk,index=match.group(1),int(match.group(2)),int(match.group(3))
            ranges=[];indices=[]
            for row in native:
                prefix='videos/'+camera+'/'
                if row.get(prefix+'chunk_index')==chunk and row.get(prefix+'file_index')==index:
                    camera_key=camera.removeprefix('observation.images.')
                    ranges.append({'episode_index':row['episode_index'],'start':row[prefix+'from_timestamp'],
                                   'end':row[prefix+'to_timestamp'],'measured_time_range':
                                   measured[row['episode_index']]['camera_time_ranges'][camera_key]})
                    indices.append(row['episode_index'])
            if not ranges:raise ContractMismatch('Video is missing episode time ranges')
            entry.update(time_basis='video_seconds',time_range={'start':min(v['start'] for v in ranges),'end':max(v['end'] for v in ranges)},
                         episode_indices=indices,episode_time_ranges=ranges,fps=fps)
        output[name]=entry
    return output


def _result(ref,manifest):
    return {'dataset_root':str(ref.root),'dataset_id':ref.dataset_id,'version':ref.version,
            'content_hash':ref.content_hash,'manifest':manifest}


def prepare_snapshot(task_directory,commits,payload,destination_root,*,writer_factory=None,trusted_task_root=None):
    """The caller passes frozen service journal values, never browser paths.

    Production passes the configured trusted_task_root. An encoding interruption
    leaves raw records intact and cannot publish a partially sealed version.
    """
    directory=Path(task_directory).absolute()
    if directory.is_symlink():raise ContractMismatch('Linked collection task is forbidden')
    for part in [directory,*directory.parents]:
        if part.is_symlink():raise ContractMismatch('Linked collection parent is forbidden')
    directory=directory.resolve()
    trusted=Path(trusted_task_root).resolve() if trusted_task_root is not None else directory.parent
    if directory.parent!=trusted or directory==trusted:raise ContractMismatch('Task leaves trusted collection root')
    session_id=_identifier(payload['session_id'],'session id')
    dataset_id=_identifier(payload.get('dataset_id',session_id),'dataset id')
    episode_id=_identifier(payload['episode_id'],'episode id')
    generation=payload['recording_generation']
    if type(generation)!=int or generation<1:raise ContractMismatch('Positive recording generation required')
    if type(payload['manifest_version'])!=int or payload['manifest_version']!=1:
        raise ContractMismatch('Only manifest version 1 is supported')
    if type(payload['episode_index'])!=int or payload['episode_index']<0:
        raise ContractMismatch('Nonnegative episode index required')
    version=_identifier(payload.get('version',episode_id+'-g'+str(generation)),'snapshot version')
    if version!=episode_id+'-g'+str(generation):raise ContractMismatch('Snapshot version must identify the exact recording generation')
    if directory.name!=session_id or dataset_id!=session_id:raise ContractMismatch('Batch/task identity differs')
    episodes=payload.get('collection_episodes')
    if not commits or not isinstance(episodes,list) or len(episodes)!=len(commits):
        raise ContractMismatch('Committed prefix and episode identities differ')
    for index,identity in enumerate(episodes):
        if any(key not in identity for key in IDENTITY_FIELDS):raise ContractMismatch('Episode business identity is incomplete')
        if type(identity['episode_index'])!=int or identity['episode_index']!=index or identity['session_id']!=session_id:
            raise ContractMismatch('Episode identity does not match committed prefix')
        for key in ('project','operator','robot_id','target_type','mode','manifest_version'):
            if identity[key]!=payload[key]:raise ContractMismatch('Episode '+key+' differs from batch')
        _identifier(identity['episode_id'],'committed episode id')
        if type(identity['recording_generation'])!=int or identity['recording_generation']<1:
            raise ContractMismatch('Committed recording generation is invalid')
    if any(episodes[-1][key]!=payload[key] for key in IDENTITY_FIELDS):
        raise ContractMismatch('Snapshot operation differs from the final committed recording identity')
    _verify_commits(directory,commits)
    journal=read_json(directory/'task.json');config=CollectionConfig(**journal['config'])
    if config.name!=session_id or config.root!=trusted or config.robot_id!=payload['robot_id']:
        raise ContractMismatch('Journal task root/identity differs')
    dest=Path(destination_root).absolute()
    if dest.is_symlink():raise ContractMismatch('Linked snapshot destination is forbidden')
    dest=dest.resolve()
    if dest==directory or dest.is_relative_to(directory) or directory.is_relative_to(dest):
        raise ContractMismatch('Snapshot and raw task directories cannot overlap')
    target=dest/dataset_id/version
    for part in [dest,*dest.parents,target.parent,target]:
        if part.is_symlink():raise ContractMismatch('Linked snapshot destination parent is forbidden')
    target.parent.mkdir(parents=True,exist_ok=True)
    binding={key:payload[key] for key in IDENTITY_FIELDS}
    lock=DeviceLock('collection-prefix:'+str(target));lock.acquire()
    writer=None;staging=target.parent/('.'+version+'.staging-'+uuid.uuid4().hex)
    try:
        if target.exists():
            inspected=DatasetManager().inspect(target);manifest=read_json(target/'manifest.json')
            if manifest.get('collection_binding')!=binding or manifest.get('collection_episodes')!=episodes or manifest.get('source_episode_hashes')!=[c['content_hash'] for c in commits]:
                raise ContractMismatch('Existing snapshot has different immutable identity')
            return _result(inspected.ref,manifest)
        writer=(writer_factory or SeeedWriter)(staging,config)
        timelines=[]
        for commit in commits:
            stamps=[];camera_stamps={key:[] for key in config.camera_shapes}
            for item in commit['files']:
                frame=_raw_frame(directory/item['path']);writer.add(training_frame(frame))
                stamps.append(int(frame['sdk.captured_monotonic_ns'].item()))
                for key in camera_stamps:
                    camera_stamps[key].append(int(frame['sdk.camera.'+key+'.captured_monotonic_ns'].item()))
            writer.save()
            timelines.append({'episode_index':commit['episode_index'],'measured_time_range':{'start':min(stamps),'end':max(stamps)},
                              'camera_time_ranges':{k:{'start':min(v),'end':max(v)} for k,v in camera_stamps.items()}})
        writer.finalize()
        provenance=export_frame_provenance(directory,commits,staging,bindings=episodes,
                                          require_provenance=payload.get('require_runtime_provenance') is True)
        source={'contract':'collection.episode.v1','binding':binding,'collection_episodes':episodes,
                'source_metadata':public_metadata(payload.get('source_metadata',{})),
                'episode_timelines':timelines,'time_basis':{'sensor':'host_monotonic_ns','video':'nominal_fps_seconds',
                'clock_scope':'recording_host_and_session','clock_reference':payload.get('time_source','recording_host_monotonic')},
                'batch_relationship':'immutable cumulative prefix of one collection session','source_provenance':provenance}
        atomic_json(staging/'meta/collection-source.json',source)
        files=inventory(staging)
        metadata={'dataset_id':dataset_id,'version':version,'robot_type':robot_type_of(config),'robot_id':config.robot_id,
                  'fps':config.fps,'joint_names':joint_names_of(config),'units':joint_units_of(config),
                  'calibration_hash':config.calibration_hash,'camera_keys':list(config.camera_shapes),
                  'camera_shapes':dict(config.camera_shapes),'task_labels':[config.task],'description':config.description,
                  'episodes':len(commits),'episode_lengths':[ep['frames'] for ep in commits],
                  'action_semantics':action_semantics_of(config),
                  'collection_id':session_id,'source_episode_hashes':[ep['content_hash'] for ep in commits],
                  'collection_binding':binding,'collection_episodes':episodes,
                  'file_metadata':_file_timelines(staging,files,timelines,config.fps),'source_provenance':provenance}
        if config.dataset_schema and 'from_internal' in config.dataset_schema:   # 只写了 action 的标准 SO101：关节不换算
            metadata['joint_mapping']={'internal_names':list(JOINT_NAMES),'internal_units':[config.units[k] for k in JOINT_NAMES],
                                       'from_internal':config.dataset_schema['from_internal']}
        manifest={**metadata,'schema':DATA_SCHEMA,'format':'lerobot-dataset-v3','manifest_version':1,
                  'lerobot_commit':SEEED_COMMIT,'status':'LOCAL_READY','files':files}
        manifest['content_hash']=digest(manifest)
        atomic_json(staging/'manifest.json',manifest)
        ref=DatasetManager().inspect(staging).ref
        _verify_commits(directory,commits)
        publish_directory(staging,target)
        return _result(DatasetRef(target,ref.dataset_id,ref.version,ref.content_hash),manifest)
    finally:
        if writer is not None:writer.abort()
        if staging.exists():
            if staging.parent!=target.parent or staging.is_symlink():raise ContractMismatch('Unsafe staging cleanup')
            shutil.rmtree(staging)
        lock.release()
