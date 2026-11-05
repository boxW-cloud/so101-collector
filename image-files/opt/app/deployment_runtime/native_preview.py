"""Passive preview from the registered Windows owner, independent of motion."""
import base64
import hashlib
import math
import time
from io import BytesIO

from .store import Fault


def preview_frame(adapter, camera):
    cameras = adapter.config.get('cameras', {})
    if camera not in cameras:
        raise Fault('CAMERA_NOT_ALLOWED', 'Camera is not registered for this target')
    # This verifies current device identity and fault/feedback state; no lease.
    state = adapter.state()
    value = adapter.owner.call('capture', {'camera_id': cameras[camera]})
    captured = value.get('captured_at')
    if (value.get('simulated') is not False or value.get('camera_id') != cameras[camera]
            or type(captured) not in (float, int) or not math.isfinite(captured)
            or not 0 <= time.time() - captured <= .5):
        raise Fault('VIDEO_UNAVAILABLE', 'Registered real camera frame is not fresh')
    if any(type(value.get(k)) is not int or not 0 < value[k] <= 4096
           for k in ('width', 'height')):
        raise Fault('VIDEO_UNAVAILABLE', 'Real camera dimensions are invalid')
    encoded = value.get('jpeg_base64')
    if not isinstance(encoded, str) or len(encoded) > 2 * 1024 ** 2:
        raise Fault('VIDEO_UNAVAILABLE', 'Real camera payload exceeds its bound')
    try:
        from PIL import Image
        raw = base64.b64decode(encoded, validate=True)
        with Image.open(BytesIO(raw)) as image:
            if image.format != 'JPEG' or image.size != (value['width'], value['height']):
                raise ValueError('Camera payload format/size differs')
            buffer = BytesIO()
            image.convert('RGB').save(buffer, format='PNG', compress_level=1)
        png = buffer.getvalue()
    except Exception as error:
        raise Fault('VIDEO_UNAVAILABLE', 'Real camera payload could not be decoded') from error
    if len(png) > 4 * 1024 ** 2 or time.time() - captured > .5:
        raise Fault('VIDEO_UNAVAILABLE', 'Real camera frame expired while encoding')
    return {'png': png, 'metadata': {
        'fresh': True, 'source': 'physical_usb_camera', 'camera_id': cameras[camera],
        'generation': state['session_id'], 'device_profile': state['profile_id'],
        'captured_unix_ns': int(captured * 1e9), 'age_s': time.time() - captured,
        'sha256': hashlib.sha256(png).hexdigest(), 'size': len(png),
        'shape': [value['height'], value['width'], 3],
    }}


def preview_status(adapter):
    result = {}
    for camera in adapter.config.get('cameras', {}):
        try:
            result[camera] = preview_frame(adapter, camera)['metadata']
        except Exception as error:
            result[camera] = {'fresh': False, 'source': 'physical_usb_camera',
                              'age_s': None, 'error': getattr(error, 'code', 'VIDEO_UNAVAILABLE')}
    return result
