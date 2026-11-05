"""Read existing SO101 camera stores and publish RGB images; never control devices.

Image header stamps use the source's existing Unix nanoseconds. The companion
record preserves original monotonic time, sequence and generation. This module
adds no timestamp age, skew or ordering gates; existing source readers retain
their original checks, also used by the compatible HTTP camera endpoints.
"""
import argparse
import hashlib
import json
import os
import signal
import threading
import time
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

from .camera_service import DEFAULT_STORE, FrameStore, FrameUnavailable
from .observation_frames import frame_record
from .video_routes import CAMERAS, MAX_AGE_S, VIDEO_CONFIG, SimFrames, configured_source


@dataclass(frozen=True)
class ImagePacket:
    data: bytes
    height: int
    width: int
    step: int
    encoding: str
    stamp_sec: int
    stamp_nanosec: int
    optical_frame: str
    identity: tuple
    info: dict


def build_packet(camera, rgb, meta):
    """Convert an already acquired frame; preserve its supplied timestamps."""
    import numpy as np
    if camera not in CAMERAS or rgb.dtype != np.uint8 or rgb.ndim != 3 or rgb.shape[2] != 3:
        raise ValueError('Registered RGB uint8 camera frame required')
    height, width, _ = rgb.shape
    raw = rgb.tobytes(order='C')
    unix_ns = int(meta['captured_unix_ns'])
    mono_ns = int(meta['captured_monotonic_ns'])
    sec, nanos = divmod(unix_ns, 10**9)
    source = meta['source']
    generation = meta.get('generation', '')
    sequence = meta['sequence']
    optical = 'so101_' + camera + '_optical_frame'
    origin = {'kind': 'isaac_preview' if source == 'sim' else 'so101_camera',
              'process_id': source + ':' + str(generation), 'sequence': sequence,
              'generation': generation, 'captured_unix_ns': unix_ns,
              'time_basis': 'host_monotonic', 'original_metadata': dict(meta)}
    record = frame_record(camera, rgb, mono_ns, source=origin)
    info = {'schema': 'so101.ros.camera-frame.v1', 'camera_key': camera,
            'image_topic': '/so101/camera/' + camera + '/image_raw',
            'ros_header': {'frame_id': optical, 'stamp': {'sec': sec, 'nanosec': nanos}},
            'stamp_time_basis': 'unix', 'captured_unix_ns': unix_ns,
            'rgb_sha256': hashlib.sha256(raw).hexdigest(), 'frame': record}
    return ImagePacket(raw, height, width, width * 3, 'rgb8', sec, nanos, optical,
                       (source, generation, camera, sequence, mono_ns, unix_ns), info)


class ImageSource:
    """Use the same underlying stores as HTTP, never its Redis-derived preview."""
    def __init__(self, *, video_config=VIDEO_CONFIG, camera_store=DEFAULT_STORE,
                 preview_dir='/data/isaac-console/ipc', generation_file='/data/isaac-console/generation.json'):
        self.video_config = Path(video_config)
        self.real = FrameStore(camera_store)
        self.sim = SimFrames(preview_dir, generation_file)

    def read(self, camera):
        source = configured_source(self.video_config)
        if source == 'real':
            rgb, meta = self.real.read(camera, 'rgb', MAX_AGE_S)
            return rgb, {**meta, 'source': 'real'}
        from integration.simulation.passive_preview import PreviewUnavailable
        from PIL import Image
        import numpy as np
        try:
            value = self.sim._store().read(camera)
        except (PreviewUnavailable, OSError, ValueError, KeyError) as error:
            raise FrameUnavailable(str(error)) from None
        with Image.open(BytesIO(value['png'])) as image:
            rgb = np.array(image.convert('RGB'))
        return rgb, {**value['metadata'], 'source': 'sim'}


class ImagePump:
    def __init__(self, read, publish):
        self.read, self.publish = read, publish
        self.last = {}
        self.errors = {}
        self.counts = {camera: 0 for camera in CAMERAS}

    def tick(self):
        for camera in CAMERAS:
            try:
                rgb, meta = self.read(camera)
                packet = build_packet(camera, rgb, meta)
                if self.last.get(camera) != packet.identity:
                    self.publish(camera, packet)
                    self.last[camera] = packet.identity
                    self.counts[camera] += 1
                self.errors.pop(camera, None)
            except Exception as error:
                self.errors[camera] = str(error)


def main():
    import rclpy
    from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy, HistoryPolicy
    from sensor_msgs.msg import Image
    from std_msgs.msg import String
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--rate-hz', type=float, default=float(os.environ.get('SO101_CAMERA_ROS_RATE_HZ', '10')))
    p.add_argument('--video-config', default=os.environ.get('SO101_VIDEO_CONFIG', VIDEO_CONFIG))
    p.add_argument('--camera-store', default=os.environ.get('SO101_CAMERA_STORE', DEFAULT_STORE))
    p.add_argument('--preview-dir', default=os.environ.get('SO101_SIM_PREVIEW_DIR', '/data/isaac-console/ipc'))
    p.add_argument('--generation-file', default=os.environ.get('SO101_SIM_GENERATION_FILE', '/data/isaac-console/generation.json'))
    args = p.parse_args()
    if not 0 < args.rate_hz <= 30: p.error('rate-hz must be in (0, 30]')
    rclpy.init()
    node = rclpy.create_node('so101_camera_publisher')
    qos = QoSProfile(depth=2, reliability=ReliabilityPolicy.RELIABLE,
                     durability=DurabilityPolicy.VOLATILE, history=HistoryPolicy.KEEP_LAST)
    image_pubs = {c: node.create_publisher(Image, '/so101/camera/' + c + '/image_raw', qos) for c in CAMERAS}
    info_pubs = {c: node.create_publisher(String, '/so101/camera/' + c + '/frame_info', qos) for c in CAMERAS}
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT): signal.signal(sig, lambda *_: stop.set())

    def publish(camera, packet):
        message = Image()
        message.header.stamp.sec = packet.stamp_sec
        message.header.stamp.nanosec = packet.stamp_nanosec
        message.header.frame_id = packet.optical_frame
        message.height, message.width, message.encoding = packet.height, packet.width, packet.encoding
        message.is_bigendian, message.step = 0, packet.step
        message.data = packet.data
        info = String();info.data = json.dumps(packet.info, ensure_ascii=False, allow_nan=False)
        info_pubs[camera].publish(info)
        image_pubs[camera].publish(message)

    source = ImageSource(video_config=args.video_config, camera_store=args.camera_store,
                         preview_dir=args.preview_dir, generation_file=args.generation_file)
    pump = ImagePump(source.read, publish)
    status_path = Path(os.environ.get('SO101_CAMERA_ROS_STATUS', '/tmp/so101-ros-images.json'))
    last_errors = None;next_status = 0.0
    try:
        while not stop.is_set() and rclpy.ok():
            began = time.monotonic()
            pump.tick()
            if pump.errors != last_errors:
                print(json.dumps({'camera_ros_errors': pump.errors}, ensure_ascii=False), flush=True)
                last_errors = dict(pump.errors)
            if began >= next_status:
                status = {'node': node.get_name(), 'domain': os.environ.get('ROS_DOMAIN_ID'),
                          'counts': pump.counts, 'errors': pump.errors, 'updated_unix': time.time(),
                          'topics': {c: '/so101/camera/' + c + '/image_raw' for c in CAMERAS}}
                temporary = status_path.with_suffix('.tmp')
                temporary.write_text(json.dumps(status), encoding='utf-8');temporary.replace(status_path)
                next_status = began + 1
            rclpy.spin_once(node, timeout_sec=0)
            stop.wait(max(0.0, 1 / args.rate_hz - (time.monotonic() - began)))
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == '__main__': main()
