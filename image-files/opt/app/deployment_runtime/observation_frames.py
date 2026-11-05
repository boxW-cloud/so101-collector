"""Container-internal RGB frame records shared by SO101 and Redis sources.

Source timestamps are recorded as supplied. This layer adds no age, skew,
ordering or cross-machine clock validation. Existing SDK clock checks remain.
Pixels go into the existing durable NPZ/LeRobot writer, never into metadata JSON.
"""
import base64
import hashlib
import json
import time
from io import BytesIO

from lerobot_sdk.core.provenance import bounded_metadata
from .store import Fault


def frame_record(camera_key, rgb, captured_monotonic_ns, *, source=None, received_monotonic_ns=None):
    source = bounded_metadata(source or {})
    sample_id = source.get('sample_id') or source.get('source_sample_id') or source.get('image_key')
    if sample_id is not None:
        basis = 'source_sample_id'
        identity = [camera_key, source.get('process_id'), sample_id]
    else:
        basis = 'camera_and_capture_time'
        identity = [camera_key, source.get('process_id'), captured_monotonic_ns, source.get('sequence')]
    frame_id = 'frame-' + hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:32]
    height, width, _ = rgb.shape
    return bounded_metadata({'schema': 'collection.frame.v1', 'camera_key': camera_key,
        'frame_id': frame_id, 'identity_basis': basis,
        'width': width, 'height': height, 'encoding': 'rgb8', 'dtype': 'uint8',
        'captured_monotonic_ns': captured_monotonic_ns,
        'received_monotonic_ns': received_monotonic_ns if received_monotonic_ns is not None else time.monotonic_ns(),
        'source': source})


def decode_sample_frames(payload, camera_shapes):
    """Decode the existing peer PNG envelope and attach one record per image."""
    import numpy as np
    from PIL import Image
    from lerobot_sdk import ImageFrame
    images, records = {}, {}
    received = time.monotonic_ns()
    for camera, frame in payload.items():
        data = base64.b64decode(frame['png'], validate=True)
        if len(data) > 2 * 1024**2: raise Fault('IMAGE_LIMIT', 'Camera payload too large')
        with Image.open(BytesIO(data)) as image:
            expected = camera_shapes.get(camera)
            if not expected or image.size != (expected[1], expected[0]):
                raise Fault('CAMERA_SHAPE', 'Camera shape differs from registered contract')
            rgb = np.array(image.convert('RGB'))
        images[camera] = ImageFrame(rgb, frame['captured_monotonic_ns'], 0, camera)
        records[camera] = frame_record(camera, rgb, frame['captured_monotonic_ns'],
            source=frame.get('source_frame'), received_monotonic_ns=received)
    return images, records
