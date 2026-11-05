"""Hash-bound raw source evidence exported beside unchanged training features."""
import json
from contextlib import ExitStack
from pathlib import Path

from lerobot_sdk.core.errors import ContractMismatch
from lerobot_sdk.core.provenance import bounded_metadata
from .filesystem import atomic_json,sha_file

SOURCE_FIELD='sdk.source_provenance_json'


def training_frame(frame):
    return {key:value for key,value in frame.items() if key!=SOURCE_FIELD}


def frame_provenance(frame):
    if SOURCE_FIELD not in frame:return None
    try:
        text=frame[SOURCE_FIELD].item()
        if not isinstance(text,str):raise ValueError('Source evidence must be UTF-8 JSON text')
        return bounded_metadata(json.loads(text))
    except (ValueError,TypeError,AttributeError) as error:
        raise ContractMismatch('Raw frame source provenance is malformed') from error


def export_frame_provenance(directory,commits,output,*,bindings=None,require_provenance=False):
    from .collection import _raw_frame
    directory=Path(directory).resolve();output=Path(output).resolve()
    files=[];runtime_hashes=set();count=0
    if bindings is not None and len(bindings)!=len(commits):raise ContractMismatch('Source evidence episode bindings differ')
    index_stream=None;index_name='meta/frame-index.jsonl';index_rows=0
    with ExitStack() as resources:
        for episode,commit in enumerate(commits):
            for index,item in enumerate(commit['files']):
                path=directory/item['path']
                if path.is_symlink() or not path.resolve().is_relative_to(directory) or sha_file(path)!=item['sha256']:
                    raise ContractMismatch('Raw source frame path/hash differs')
                frame=_raw_frame(path);provenance=frame_provenance(frame)
                if provenance is None:
                    if require_provenance:raise ContractMismatch('Committed frame lacks required USED source evidence')
                    continue
                if require_provenance and not isinstance(provenance.get('leader'),dict):
                    raise ContractMismatch('Committed frame lacks required leader source evidence')
                binding=bindings[episode] if bindings is not None else {'episode_index':episode}
                value=bounded_metadata({'schema':'sdk.source.frame.v1','episode_index':episode,'frame_index':index,
                    'collection_binding':binding,'raw_frame':dict(item),
                    'sdk_source_sequence':int(frame['sdk.source_sequence'].item()),
                    'captured_monotonic_ns':int(frame['sdk.captured_monotonic_ns'].item()),
                    'sent_monotonic_ns':int(frame['sdk.sent_monotonic_ns'].item()),'provenance':provenance})
                name=f'meta/source-frames/episode-{episode:06d}/frame-{index:08d}.json'
                target=output/name
                if target.exists() or target.is_symlink():raise ContractMismatch('Source evidence publication must not overwrite an existing frame')
                target.parent.mkdir(parents=True,exist_ok=True);atomic_json(target,value)
                files.append(name);count+=1
                digest=(provenance.get('leader') or {}).get('runtime_config_sha256')
                if isinstance(digest,str):runtime_hashes.add(digest)
                image_frames=(provenance.get('observation') or {}).get('image_frames',{})
                for camera,record in image_frames.items():
                    if index_stream is None:
                        path=output/index_name;path.parent.mkdir(parents=True,exist_ok=True)
                        if path.exists() or path.is_symlink():raise ContractMismatch('Frame index must not overwrite an existing file')
                        index_stream=resources.enter_context(path.open('x',encoding='utf-8',newline='\n'))
                    # These are the exact recorded source times, not nominal video FPS times.
                    row={'episode_index':episode,'frame_index':index,'camera_key':camera,
                         'frame':record,'provenance_file':name}
                    index_stream.write(json.dumps(row,ensure_ascii=False,allow_nan=False,separators=(',',':'))+'\n')
                    index_rows+=1
    if index_stream is not None:files.append(index_name)
    return {'schema':'sdk.source.frames.v1','frames':count,'files':files,
            'runtime_config_sha256':sorted(runtime_hashes),'exact_hardware_sync':False,
            **({'frame_index_file':index_name,'frame_index_rows':index_rows} if index_stream is not None else {})}
