"""Read-only Isaac preview; frame publication never claims robot control.

The harness publishes its rendered observation with capture time. This service
only reads that bounded cache and stops serving an image when rendering stops.
"""
import argparse
import hashlib
import ipaddress
import json
import time
from io import BytesIO
from pathlib import Path

CAMERAS = ('front', 'wrist')
MAX_PNG_BYTES = 4 * 1024 ** 2


class PreviewUnavailable(Exception):
    pass


def publish_preview(root, images, *, generation, sequence,
                    captured_monotonic_ns, captured_unix_ns):
    """Write both payloads, then atomically replace their shared manifest."""
    import numpy as np
    from PIL import Image
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    if set(images) != set(CAMERAS):
        raise ValueError('Two registered Isaac cameras required')
    metadata = {'generation': generation, 'sequence': sequence,
                'captured_monotonic_ns': captured_monotonic_ns,
                'captured_unix_ns': captured_unix_ns, 'cameras': {}}
    for camera in CAMERAS:
        rgb = images[camera]
        if rgb.dtype != np.uint8 or rgb.ndim != 3 or rgb.shape[-1] != 3:
            raise ValueError('RGB uint8 observation required')
        stream = BytesIO()
        Image.fromarray(rgb).save(stream, format='PNG', compress_level=1)
        png = stream.getvalue()
        if len(png) > MAX_PNG_BYTES:
            raise ValueError('Preview image exceeds bounded cache')
        # Two fixed slots bound disk use. A reader verifies the digest, so a
        # concurrent replacement can only make that read unavailable.
        filename = f'{camera}-preview-{sequence % 2}.png'
        path = root / filename
        temporary = path.with_suffix('.png.tmp')
        temporary.write_bytes(png)
        temporary.replace(path)
        metadata['cameras'][camera] = {'filename': filename,
            'sha256': hashlib.sha256(png).hexdigest(), 'size': len(png),
            'shape': list(rgb.shape)}
    path = root / 'camera-preview.json'
    temporary = root / 'camera-preview.json.tmp'
    temporary.write_text(json.dumps(metadata, allow_nan=False), encoding='utf-8')
    temporary.replace(path)


class PreviewStore:
    def __init__(self, root, *, generation, max_age_s=2):
        self.root = Path(root).resolve()
        self.generation = generation
        self.max_age_ns = int(max_age_s * 1e9)
        if not generation or not 0 < self.max_age_ns <= 2_000_000_000:
            raise ValueError('Explicit run generation and age <= 2 seconds required')

    def read(self, camera):
        if camera not in CAMERAS:
            raise PreviewUnavailable('Camera is not registered')
        try:
            manifest = self.root / 'camera-preview.json'
            if manifest.is_symlink() or manifest.stat().st_size > 16384:
                raise PreviewUnavailable('Invalid preview manifest')
            metadata = json.loads(manifest.read_text(encoding='utf-8'))
            mono_age = time.monotonic_ns() - metadata['captured_monotonic_ns']
            unix_age = time.time_ns() - metadata['captured_unix_ns']
            if metadata['generation'] != self.generation or not (
                0 <= mono_age < self.max_age_ns and 0 <= unix_age < self.max_age_ns):
                raise PreviewUnavailable('Renderer frame is stale or belongs to another run')
            item = metadata['cameras'][camera]
            if item['filename'] != f'{camera}-preview-{metadata["sequence"] % 2}.png':
                raise PreviewUnavailable('Invalid preview payload identity')
            path = self.root / item['filename']
            if path.is_symlink() or path.stat().st_size > MAX_PNG_BYTES:
                raise PreviewUnavailable('Invalid preview payload')
            png = path.read_bytes()
            if len(png) != item['size'] or hashlib.sha256(png).hexdigest() != item['sha256']:
                raise PreviewUnavailable('Concurrent or modified frame payload')
            if not png.startswith(b'\x89PNG\r\n\x1a\n'):
                raise PreviewUnavailable('Invalid PNG payload')
            return {'png': png, 'metadata': {**item, 'fresh': True,
                'source': 'isaac_render', 'age_s': mono_age / 1e9,
                'generation': self.generation, 'sequence': metadata['sequence'],
                'captured_monotonic_ns': metadata['captured_monotonic_ns'],
                'captured_unix_ns': metadata['captured_unix_ns']}}
        except PreviewUnavailable:
            raise
        except (OSError, ValueError, KeyError, TypeError, OverflowError) as error:
            raise PreviewUnavailable('No complete fresh camera frame') from error

    def status(self):
        result = {}
        for camera in CAMERAS:
            try:
                result[camera] = self.read(camera)['metadata']
            except PreviewUnavailable:
                result[camera] = {'fresh': False, 'source': 'isaac_render', 'age_s': None}
        return result


def make_app(store):
    from aiohttp import web
    @web.middleware
    async def guard(request, handler):
        if not request.remote or not ipaddress.ip_address(request.remote).is_loopback:
            return web.json_response({'ok': False}, status=403)
        try:
            response = await handler(request)
        except PreviewUnavailable as error:
            response = web.json_response({'ok': False, 'error': {
                'code': 'VIDEO_UNAVAILABLE', 'message': str(error)}}, status=409)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    async def status(request):
        return web.json_response({'ok': True, 'result': store.status()})

    async def camera(request):
        frame = store.read(request.match_info['camera'])
        metadata = frame['metadata']
        return web.Response(body=frame['png'], content_type='image/png', headers={
            'X-Captured-Unix-Ns': str(metadata['captured_unix_ns']),
            'X-Captured-Monotonic-Ns': str(metadata['captured_monotonic_ns']),
            'X-Frame-Sha256': metadata['sha256'],
            'X-Preview-Generation': metadata['generation']})

    app = web.Application(middlewares=[guard])
    app.add_routes([web.get('/status', status), web.get('/camera/{camera}.png', camera)])
    return app


def main():
    from aiohttp import web
    parser = argparse.ArgumentParser()
    parser.add_argument('--ipc-directory', type=Path, required=True)
    parser.add_argument('--generation', required=True)
    parser.add_argument('--bind', default='127.0.0.1', choices=['127.0.0.1', '::1'])
    parser.add_argument('--port', type=int, default=8922)
    args = parser.parse_args()
    store = PreviewStore(args.ipc_directory, generation=args.generation)
    web.run_app(make_app(store), host=args.bind, port=args.port, access_log=None)


if __name__ == '__main__':
    main()
