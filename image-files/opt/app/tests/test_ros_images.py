"""ROS image output preserves source pixels/times and leaves HTTP independent."""
import importlib.util
import json
import runpy
import time
from io import BytesIO
from pathlib import Path

import numpy as np
import pytest
from PIL import Image

from deployment_runtime.camera_service import FrameStore, FrameUnavailable


def publisher_module():
    assert importlib.util.find_spec('deployment_runtime.ros_images') is not None, 'ROS image publisher is missing'
    from deployment_runtime import ros_images
    return ros_images


def frame(sequence=1, unix_ns=1793900000123456789, mono_ns=123456789000):
    rgb = np.array([[[255, 0, 0], [0, 255, 0]], [[0, 0, 255], [1, 2, 3]]], dtype=np.uint8)
    return rgb, {'sequence': sequence, 'captured_unix_ns': unix_ns, 'captured_monotonic_ns': mono_ns,
                 'source': 'sim', 'generation': 'generation-a'}


def test_rgb_packet_keeps_pixels_and_exact_capture_times():
    m = publisher_module()
    rgb, meta = frame()
    packet = m.build_packet('front', rgb, meta)
    assert packet.data == rgb.tobytes() and packet.encoding == 'rgb8'
    assert (packet.height, packet.width, packet.step) == (2, 2, 6)
    assert (packet.stamp_sec, packet.stamp_nanosec) == divmod(meta['captured_unix_ns'], 10**9)
    assert packet.optical_frame == 'so101_front_optical_frame'
    info = packet.info
    assert info['frame']['captured_monotonic_ns'] == meta['captured_monotonic_ns']
    assert info['captured_unix_ns'] == meta['captured_unix_ns']
    assert info['frame']['source']['sequence'] == 1
    assert info['frame']['source']['generation'] == 'generation-a'
    assert info['image_topic'] == '/so101/camera/front/image_raw'
    assert np.array_equal(rgb[0, 0], [255, 0, 0])  # no RGB/BGR reversal or source mutation


@pytest.mark.parametrize('stamp', [0, 1, 1990000000123456789])
def test_packet_does_not_add_timestamp_age_or_future_validation(stamp):
    m = publisher_module()
    rgb, meta = frame(unix_ns=stamp)
    packet = m.build_packet('wrist', rgb, meta)
    assert packet.stamp_sec * 10**9 + packet.stamp_nanosec == stamp
    assert packet.info['captured_unix_ns'] == stamp


def test_dedupe_is_by_source_identity_not_timestamp_order():
    m = publisher_module()
    current = {'front': frame(), 'wrist': frame()}
    sent = []
    pump = m.ImagePump(lambda camera: current[camera], lambda camera, packet: sent.append((camera, packet)))
    pump.tick();pump.tick()
    assert [c for c, _ in sent] == ['front', 'wrist']
    # A changed source sequence with an older time must still be emitted; no new ordering gate.
    current['front'] = frame(sequence=2, unix_ns=1, mono_ns=10)
    pump.tick()
    assert len(sent) == 3 and sent[-1][0] == 'front'
    assert sent[-1][1].info['captured_unix_ns'] == 1
    old_id = sent[-1][1].info['frame']['frame_id']
    current['front'][1]['generation'] = 'generation-b'
    pump.tick()
    assert len(sent) == 4 and sent[-1][1].info['frame']['frame_id'] != old_id


def test_missing_one_camera_does_not_block_the_other_or_http():
    m = publisher_module()
    sent = []
    def read(camera):
        if camera == 'front': raise FrameUnavailable('temporarily missing')
        return frame()
    pump = m.ImagePump(read, lambda camera, packet: sent.append(camera))
    pump.tick()
    assert sent == ['wrist'] and 'front' in pump.errors and not pump.errors.get('wrist')


def test_real_source_reads_existing_store_without_reopening_camera(tmp_path):
    m = publisher_module()
    rgb, meta = frame(mono_ns=time.monotonic_ns())
    store = FrameStore(tmp_path/'frames');stream = BytesIO();Image.fromarray(rgb).save(stream, 'JPEG')
    store.write('front', rgb, stream.getvalue(), captured_monotonic_ns=meta['captured_monotonic_ns'],
                captured_unix_ns=meta['captured_unix_ns'], sequence=1)
    config=tmp_path/'video.json';config.write_text('{"source":"real"}')
    source=m.ImageSource(video_config=config, camera_store=tmp_path/'frames')
    got, source_meta=source.read('front')
    assert np.array_equal(got, rgb)
    assert source_meta['captured_unix_ns'] == meta['captured_unix_ns']
    assert store.read('front','jpeg')[0] == stream.getvalue()  # HTTP's existing JPEG remains untouched


def test_sim_source_reads_preview_and_handles_source_switch(tmp_path):
    m = publisher_module()
    from integration.simulation.passive_preview import publish_preview
    rgb, meta=frame(mono_ns=time.monotonic_ns(), unix_ns=time.time_ns())
    gen='1'*32;generation=tmp_path/'generation.json';generation.write_text(json.dumps({'generation':gen}))
    preview=tmp_path/'ipc'
    publish_preview(preview, {'front':rgb,'wrist':rgb}, generation=gen, sequence=7,
                    captured_monotonic_ns=meta['captured_monotonic_ns'],captured_unix_ns=meta['captured_unix_ns'])
    config=tmp_path/'video.json';config.write_text('{"source":"sim"}')
    source=m.ImageSource(video_config=config, preview_dir=preview,generation_file=generation,camera_store=tmp_path/'real')
    got, source_meta=source.read('wrist')
    assert np.array_equal(got,rgb) and source_meta['generation']==gen and source_meta['source']=='sim'
    config.write_text('{"source":"real"}')
    with pytest.raises(FrameUnavailable):source.read('wrist')


def test_image_process_has_isolated_domain_environment():
    path=Path(__file__).resolve().parents[2]/'sim-retest/simulation_retest_supervisor.py'
    scope=runpy.run_path(str(path))
    assert 'image_ros_environment' in scope, 'Supervisor does not launch a separate image ROS environment'
    env=scope['image_ros_environment']({'ROS_DOMAIN_ID':'173','ROS_LOCALHOST_ONLY':'1',
        'FASTRTPS_DEFAULT_PROFILES_FILE':'old.xml','FASTDDS_DEFAULT_PROFILES_FILE':'old.xml'})
    assert env['ROS_DOMAIN_ID']=='96' and env['ROS_LOCALHOST_ONLY']=='0'
    assert 'FASTRTPS_DEFAULT_PROFILES_FILE' not in env and 'FASTDDS_DEFAULT_PROFILES_FILE' not in env
