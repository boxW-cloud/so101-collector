"""Unified frame metadata; no device access or timestamp threshold gates."""
import json
import importlib
import time
import numpy as np
import pytest

def module():
    return importlib.import_module('deployment_runtime.observation_frames')

def test_same_source_frame_has_stable_identity_and_raw_times_are_not_rewritten():
    rgb=np.zeros((4,6,3),np.uint8)
    source={'kind':'redis','process_id':'p1','sample_id':'sample-001','sampled_at':12.3,
            'original_source_timestamp':1700000000.25,'time_basis':'receive_approx','image_key':'image:one'}
    first=module().frame_record('front',rgb,100,source=source,received_monotonic_ns=200)
    second=module().frame_record('front',rgb,100,source=source,received_monotonic_ns=300)
    assert first['schema']=='collection.frame.v1'
    assert first['frame_id']==second['frame_id']
    assert first['source']==source
    assert first['captured_monotonic_ns']==100 and first['received_monotonic_ns']==200
    assert (first['width'],first['height'],first['encoding'])==(6,4,'rgb8')
    assert 'png' not in first and 'rgb' not in first

def test_old_future_missing_and_different_source_clock_values_are_recorded_not_rejected():
    rgb=np.zeros((2,2,3),np.uint8)
    for stamp in (-99,0,10**22,None):
        result=module().frame_record('wrist',rgb,123,source={'original_source_timestamp':stamp,'time_basis':'source'})
        assert result['source']['original_source_timestamp']==stamp

def test_camera_process_and_source_sample_distinguish_frame_identity():
    rgb=np.zeros((2,2,3),np.uint8)
    frames=[module().frame_record(camera,rgb,123,source={'process_id':pid,'sample_id':sid})
            for camera,pid,sid in [('front','p1','s1'),('wrist','p1','s1'),('front','p2','s1'),('front','p1','s2')]]
    assert len({frame['frame_id'] for frame in frames})==4

def test_missing_source_identity_is_explicitly_derived_from_capture_time():
    result=module().frame_record('front',np.zeros((2,2,3),np.uint8),123)
    assert result['identity_basis']=='camera_and_capture_time'
    assert result['captured_monotonic_ns']==123

def test_redis_original_clock_anchor_and_each_camera_source_survive_adapter(tmp_path):
    from test_redis_capture import FakeRedis,Source,config
    from deployment_runtime import redis_capture as rc
    redis=FakeRedis();cfg=config(tmp_path);producer=Source(redis)
    message=producer.observation(push=False)
    message['clock_anchor']={'monotonic':100.,'unix':1700000000.}
    message['sources']['front'].update(sample_id='source-frame-9',original_source_timestamp=1700000000.25,time_basis='receive_approx')
    consumer=rc.Consumer(cfg,redis.client)
    obs=consumer._convert(redis.client(),message,time.monotonic_ns())
    assert obs.evidence['source_clock']['time_basis']=='process_monotonic'
    assert obs.evidence['source_clock']['clock_anchor']==message['clock_anchor']
    assert obs.evidence['frame_sources']['front']['sample_id']=='source-frame-9'
    assert obs.evidence['frame_sources']['front']['original_source_timestamp']==1700000000.25

def test_normalized_record_and_rgb_pixels_share_the_same_decoded_frame():
    import base64,io
    from PIL import Image
    rgb=np.full((4,6,3),37,np.uint8);stream=io.BytesIO();Image.fromarray(rgb).save(stream,format='PNG')
    frames,records=module().decode_sample_frames({'front':{'png':base64.b64encode(stream.getvalue()).decode(),
        'captured_monotonic_ns':123,'source_frame':{'kind':'isaac','sample_id':'s1'}}},{'front':[4,6,3]})
    assert np.array_equal(frames['front'].rgb,rgb)
    assert records['front']['captured_monotonic_ns']==frames['front'].captured_monotonic_ns
    assert records['front']['source']['sample_id']=='s1'

def test_frame_index_exports_original_timestamps_with_episode_and_frame_identity(tmp_path):
    from lerobot_sdk.data.provenance import SOURCE_FIELD,export_frame_provenance
    from lerobot_sdk.data.filesystem import sha_file
    src=tmp_path/'raw';src.mkdir();out=tmp_path/'export';out.mkdir()
    descriptor={'schema':'collection.frame.v1','camera_key':'front','frame_id':'source-1',
                'captured_monotonic_ns':111,'source':{'original_source_timestamp':-100,'time_basis':'source'}}
    provenance={'observation':{'image_frames':{'front':descriptor}}}
    np.savez(src/'frame.npz',**{SOURCE_FIELD:np.array(json.dumps(provenance)),
        'sdk.source_sequence':np.array(2),'sdk.captured_monotonic_ns':np.array(111),'sdk.sent_monotonic_ns':np.array(112)})
    commits=[{'files':[{'path':'frame.npz','sha256':sha_file(src/'frame.npz')}]}]
    result=export_frame_provenance(src,commits,out)
    rows=[json.loads(line) for line in (out/'meta/frame-index.jsonl').read_text().splitlines()]
    assert result['frame_index_file']=='meta/frame-index.jsonl'
    assert rows==[{'episode_index':0,'frame_index':0,'camera_key':'front','frame':descriptor,
                  'provenance_file':'meta/source-frames/episode-000000/frame-00000000.json'}]

