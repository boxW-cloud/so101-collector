"""Exact episode frame selection via installed Seeed decoders/encoders.

The frozen workshop converter stream-copies MP4 segments. GOP boundaries can
include an extra frame. Rebuild each output from source V3 timestamps instead
of guessing which output frame to drop. This is a versioned adapter step.
"""
import json,tempfile
from pathlib import Path
from lerobot_sdk.adapters.seeed_dataset import upstream
from lerobot_sdk.data.filesystem import digest,sha_file

def align_videos(source,target):
    upstream()  # Enforce the installed Seeed version and commit before importing.
    import pyarrow.parquet as pq
    from PIL import Image
    from lerobot.datasets.video_utils import decode_video_frames,encode_video_frames
    from lerobot.configs import RGBEncoderConfig
    src=json.loads((source/'meta/info.json').read_text());out=json.loads((target/'meta/info.json').read_text())
    episodes=[]
    for path in sorted((source/'meta/episodes').rglob('*.parquet')):episodes.extend(pq.read_table(path).to_pylist())
    records=[];fps=src['fps']
    for episode in sorted(episodes,key=lambda e:e['episode_index']):
        index=episode['episode_index'];length=episode['length']
        for key,feature in src['features'].items():
            if feature['dtype']!='video':continue
            start=float(episode[f'videos/{key}/from_timestamp']);end=float(episode[f'videos/{key}/to_timestamp'])
            times=[start+i/fps for i in range(length)]
            if not times or times[-1]>=end+1e-5:raise ValueError('Source episode video timestamps do not cover its rows')
            path=source/src['video_path'].format(video_key=key,chunk_index=episode[f'videos/{key}/chunk_index'],file_index=episode[f'videos/{key}/file_index'])
            output=target/out['video_path'].format(video_key=key,episode_chunk=index//out['chunks_size'],episode_index=index)
            with tempfile.TemporaryDirectory(prefix='.frames-',dir=target.parent) as temp:
                temp=Path(temp)
                for offset in range(0,length,16):
                    images=decode_video_frames(path,times[offset:offset+16],tolerance_s=.49/fps,backend='pyav',return_uint8=True)
                    for local,rgb in enumerate(images):Image.fromarray(rgb.permute(1,2,0).contiguous().numpy()).save(temp/f'frame-{offset+local:06d}.png')
                encode_video_frames(temp,output,fps,video_encoder=RGBEncoderConfig(vcodec='h264',pix_fmt='yuv420p',g=2,crf=18),encoder_threads=1,overwrite=True)
            records.append({'episode_index':index,'camera':key,'frames':length,'source_start_s':start,'source_end_s':end,'timestamps_hash':digest(times)})
    return {'version':'seeed-source-timestamps-framealign-v1','adapter_sha256':sha_file(__file__),'encoding':{'vcodec':'h264','pix_fmt':'yuv420p','g':2,'crf':18},'episodes':records}
