"""Import the pinned package; never copy its writer, encoder or reader algorithms."""
from pathlib import Path
import importlib.metadata
import json
from functools import lru_cache
import numpy as np
from lerobot_sdk.data.contracts import SEEED_COMMIT
from lerobot_sdk.core.types import JOINT_NAMES
from lerobot_sdk.core.errors import ConfigurationError, ContractMismatch


@lru_cache(maxsize=1)
def upstream():
    try:
        distribution=importlib.metadata.distribution('lerobot')
        direct=json.loads(distribution.read_text('direct_url.json') or '{}')
        if distribution.version!='0.6.2' or direct.get('vcs_info',{}).get('commit_id')!=SEEED_COMMIT:
            raise ConfigurationError('Install the exact Seeed VCS commit from the collection extra')
        from lerobot.datasets.lerobot_dataset import LeRobotDataset
        from lerobot.datasets.video_utils import VideoEncodingManager
        from lerobot.configs import RGBEncoderConfig
        from lerobot.utils.feature_utils import build_dataset_frame
        return LeRobotDataset,VideoEncodingManager,RGBEncoderConfig,build_dataset_frame
    except (ImportError,importlib.metadata.PackageNotFoundError) as exc:
        raise ConfigurationError('Collection requires the pinned Seeed dataset extra') from exc


def custom_joints(config):
    """登记里有机器人自己的关节（Bobac3 等）时返回 schema；没有 dataset_schema，或只写了 action（标准 SO101）时返回 None。"""
    schema=getattr(config,'dataset_schema',None)
    return schema if schema and 'joint_names' in schema else None


def joint_names_of(config):
    schema=custom_joints(config)
    return list(schema['joint_names']) if schema else list(JOINT_NAMES)


def joint_units_of(config):
    schema=custom_joints(config)
    return list(schema['units']) if schema else [config.units[k] for k in JOINT_NAMES]


def robot_type_of(config):
    schema=custom_joints(config)
    return schema['robot_type'] if schema else 'so101_follower'


ACTION_SEMANTICS={
    'sent_target':'action == action.executed == actual sent target; observation.state is measured',
    'next_observation_state':('action == action.executed == action.requested == next frame measured observation.state '
                              '(source has no commanded joint target); last frame of each episode dropped; observation.state is measured'),
    'source_action':('action == action.executed == target the source reports as actually sent for this observation '
                     '(paired by observation id); action.requested == the source requested target (or the sent target); '
                     'frames without a paired action are skipped; observation.state is measured'),
    # 2026-11-05：SO101 遥操作，主臂指令按时间配对（主臂的 action 不是根据某个观测算出来的，没有编号可对）
    'leader_action':('action == action.executed == action.requested == leader arm command: the first leader action whose '
                     'timestamp is at or after the observation state sample time on the same host monotonic clock; '
                     'frames without one within the skew limit are skipped; observation.state is measured'),
}


def action_rule_of(config):
    """数据集 action 列的来源（contracts.ACTION_RULES）。没有 dataset_schema 或没写 action 时是 sent_target。"""
    schema=getattr(config,'dataset_schema',None)
    return (schema or {}).get('action','sent_target')


def action_semantics_of(config):
    """写进 manifest 的 action_semantics。"""
    return ACTION_SEMANTICS[action_rule_of(config)]


def dataset_vector(config,positions):
    """SDK 内部 6 个槽位的值 → 数据集里写的 6 个值（没有 dataset_schema 时原样）。"""
    schema=custom_joints(config)
    if not schema:return [positions[n] for n in JOINT_NAMES]
    return [scale*positions[n]+offset for n,(scale,offset) in zip(JOINT_NAMES,schema['from_internal'],strict=True)]


def features(config):
    vector={'dtype':'float32','shape':(6,),'names':joint_names_of(config)}
    result={key:dict(vector) for key in ('observation.state','action','action.requested','action.executed')}
    result.update({
        'sdk.source_sequence':{'dtype':'int64','shape':(1,),'names':None},
        'sdk.captured_monotonic_ns':{'dtype':'int64','shape':(1,),'names':None},
        'sdk.sent_monotonic_ns':{'dtype':'int64','shape':(1,),'names':None},
        'sdk.epoch':{'dtype':'int64','shape':(1,),'names':None},
    })
    for key,shape in config.camera_shapes.items():
        result['observation.images.'+key]={'dtype':'video','shape':tuple(shape),'names':['height','width','channels']}
        result['sdk.camera.'+key+'.captured_monotonic_ns']={'dtype':'int64','shape':(1,),'names':None}
    return result


def make_frame(config,observation,receipt):
    _,_,_,build_frame=upstream()
    spec=features(config)
    raw={**dict(zip(joint_names_of(config),dataset_vector(config,observation.arm.positions),strict=True)),
         **{k:v.rgb for k,v in observation.images.items()}}
    frame=build_frame({'observation.state':spec['observation.state'],
                       **{k:v for k,v in spec.items() if k.startswith('observation.images.')}},raw,'observation')
    for key,target in [('action',receipt.sent_target),('action.executed',receipt.sent_target),
                       ('action.requested',receipt.requested_target)]:
        frame[key]=np.array(dataset_vector(config,target.positions),dtype=np.float32)
    for key,value in [('sdk.source_sequence',observation.arm.sequence),
                      ('sdk.captured_monotonic_ns',observation.arm.captured_monotonic_ns),
                      ('sdk.sent_monotonic_ns',receipt.sent_monotonic_ns),('sdk.epoch',receipt.epoch)]:
        frame[key]=np.array([value],dtype=np.int64)
    for key in config.camera_shapes:
        frame['sdk.camera.'+key+'.captured_monotonic_ns']=np.array([observation.images[key].captured_monotonic_ns],dtype=np.int64)
    frame['task']=config.task
    return frame


def _stable_episode_stats(stats):
    """Normalise metadata representation, not native data or the statistics.

    Pinned Seeed uses NumPy integer min/max for one sample and floating running
    statistics for >=2 samples. Its buffered metadata Parquet schema is fixed
    by the first episode. Use float64 statistics consistently at this SDK
    boundary, retaining integer counts; raw int64 sensors/float32 actions stay
    in their declared native feature types and remain strictly validated.
    """
    result={}
    for feature,values in stats.items():
        result[feature]={}
        for name,value in values.items():
            array=np.asarray(value)
            if not np.issubdtype(array.dtype,np.number) or not np.isfinite(array).all():
                raise ContractMismatch('Native episode statistics must be finite numeric arrays')
            if name=='count':
                if not np.issubdtype(array.dtype,np.integer) or np.any(array<0):
                    raise ContractMismatch('Native episode statistic counts must remain nonnegative integers')
                result[feature][name]=array.astype(np.int64,copy=True)
            else:result[feature][name]=array.astype(np.float64,copy=True)
    return result


class SeeedWriter:
    def __init__(self,root,config):
        dataset,manager,rgb,_=upstream()
        self.dataset=dataset.create(repo_id='local/'+config.name,root=Path(root),fps=config.fps,
            features=features(config),robot_type=robot_type_of(config),use_videos=True,
            video_backend='pyav',batch_encoding_size=1,metadata_buffer_size=1,
            image_writer_processes=0,image_writer_threads=0,encoder_threads=1,
            rgb_encoder=rgb(vcodec='h264',pix_fmt='yuv420p',g=2,crf=18))
        save_metadata=self.dataset.meta.save_episode
        def save_episode_metadata(episode_index,episode_length,episode_tasks,episode_stats,episode_metadata):
            return save_metadata(episode_index,episode_length,episode_tasks,_stable_episode_stats(episode_stats),episode_metadata)
        # Instance-local adaptation: other users of the pinned package and
        # concurrent SDK writers do not share a monkey-patched global function.
        self.dataset.meta.save_episode=save_episode_metadata
        self._manager=manager(self.dataset)
        self._manager.__enter__()
        self._closed=False

    def add(self,frame):
        self.dataset.add_frame(dict(frame))

    def save(self):
        self.dataset.save_episode(parallel_encoding=False)

    def clear(self):
        self.dataset.clear_episode_buffer()

    def finalize(self):
        if not self._closed:
            self._manager.__exit__(None,None,None)
            self._closed=True

    def abort(self):
        if not self._closed:
            try:self._manager.__exit__(RuntimeError,RuntimeError('SDK recording interrupted'),None)
            finally:self._closed=True


def validate_native(root,metadata):
    """Validate every Parquet row and decode every video; then use the native reader."""
    import pyarrow.parquet as pq
    import pyarrow as pa
    import av
    dataset,_,_,_=upstream()
    root=Path(root)
    info=json.loads((root/'meta/info.json').read_text(encoding='utf-8'))
    for required in ('meta/info.json','meta/stats.json','meta/tasks.parquet'):
        if not (root/required).is_file():raise ContractMismatch('Missing '+required)
    if not str(info.get('codebase_version','')).startswith('v3'):
        raise ContractMismatch('Expected native LeRobot v3 data')
    if info['fps']!=metadata['fps']:raise ContractMismatch('Native fps differs from manifest')
    for key in ('observation.state','action','action.requested','action.executed'):
        if (info['features'][key]['shape']!=[6] or info['features'][key]['names']!=list(metadata['joint_names'])
            or info['features'][key]['dtype']!='float32'):
            raise ContractMismatch('Joint feature contract mismatch')
    for key,shape in metadata['camera_shapes'].items():
        if info['features']['observation.images.'+key]['shape']!=list(shape):
            raise ContractMismatch('Camera shape mismatch')
    rows=[]
    for path in sorted((root/'data').rglob('*.parquet')):
        table=pq.read_table(path)
        for key in ('observation.state','action','action.requested','action.executed'):
            dtype=table.schema.field(key).type
            if not (pa.types.is_list(dtype) or pa.types.is_fixed_size_list(dtype)) or dtype.value_type!=pa.float32():
                raise ContractMismatch('Parquet joint columns must contain float32 vectors')
        rows.extend(table.to_pylist())
    rows.sort(key=lambda row:row['index'])
    lengths=metadata['episode_lengths']
    if not rows or len(rows)!=sum(lengths) or info['total_frames']!=len(rows) or info['total_episodes']!=len(lengths):
        raise ContractMismatch('Episode/frame totals do not match')
    start=0
    for episode,length in enumerate(lengths):
        last_stamp=last_sequence=-1
        for frame_index,row in enumerate(rows[start:start+length]):
            if row['index']!=start+frame_index or row['episode_index']!=episode or row['frame_index']!=frame_index:
                raise ContractMismatch('Broken frame or episode indexing')
            if abs(row['timestamp']-frame_index/metadata['fps'])>1e-4:
                raise ContractMismatch('Broken nominal frame timeline')
            stamp=int(row['sdk.captured_monotonic_ns']);sequence=int(row['sdk.source_sequence'])
            if stamp<=last_stamp or sequence<=last_sequence or int(row['sdk.sent_monotonic_ns'])<stamp:
                raise ContractMismatch('Invalid measured timeline')
            last_stamp,last_sequence=stamp,sequence
            for name in ('observation.state','action','action.requested','action.executed'):
                values=np.asarray(row[name])
                if values.shape!=(6,) or not np.isfinite(values).all():raise ContractMismatch('Invalid six-axis data')
                # 按每一列声明的单位检查取值范围（SO101：夹爪 0~100，归一化身体 ±100，和以前一样）
                for unit,value in zip(metadata['units'],values,strict=True):
                    if unit=='normalized_0_100' and not 0<=value<=100:raise ContractMismatch('Invalid gripper unit domain')
                    if unit=='normalized_-100_100' and abs(value)>100:raise ContractMismatch('Invalid normalized body domain')
            if row['action']!=row['action.executed']:raise ContractMismatch('Training action is not sent target')
        start+=length
    for key,shape in metadata['camera_shapes'].items():
        count=0
        videos=sorted((root/'videos'/('observation.images.'+key)).rglob('*.mp4'))
        if not videos:raise ContractMismatch('Missing camera videos')
        for path in videos:
            with av.open(str(path)) as video:
                for frame in video.decode(video=0):
                    if (frame.height,frame.width,3)!=tuple(shape):raise ContractMismatch('Decoded video shape differs')
                    count+=1
        if count!=len(rows):raise ContractMismatch('Video frame count differs from Parquet')
    reader=dataset(repo_id='local/'+metadata['dataset_id'],root=root,video_backend='pyav')
    if len(reader)!=len(rows) or reader.num_episodes!=len(lengths):raise ContractMismatch('Native reader totals differ')
    offset=0
    for length in lengths:
        for index in {offset,offset+length-1}:
            sample=reader[index]
            if sample['task']!=metadata['task_labels'][0]:raise ContractMismatch('Task text mismatch')
            for key in metadata['camera_shapes']:
                if sample['observation.images.'+key].ndim!=3:raise ContractMismatch('Native image decode failed')
        offset+=length
    return {'episodes':len(lengths),'frames':len(rows),'native_readable':True}
