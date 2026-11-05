"""采集接口（8910）上的视频：免 token、可跨域，任何前端都能直接用。只读画面，不碰机器人。

前端永远只有两路画面：camera=front（前方）和 camera=wrist（腕部）。画面是真机还是仿真由部署配置决定，
前端不用选也选不了：.env 的 COLLECTION_VIDEO_SOURCE=real|sim → ./apply-env.py → config/video.json。
每次请求都重读这个文件，改完执行 ./apply-env.py 就生效，不用重启容器（已经连着的 WebSocket 重连后生效）。

GET /collection/video                 当前画面来源，以及 front、wrist 两路各自是否有新画面
GET /collection/video/<camera>.jpg    单帧 JPEG，camera 为 front 或 wrist
GET /collection/video/ws              WebSocket。参数 camera=front|wrist（默认 wrist）、fps=1~30（默认 10）
    连上后：先收到一条 JSON 文本 {"type":"hello",...}，之后每有新画面就收到一条二进制消息（一帧完整 JPEG）。
    画面中断/恢复时收到 JSON 文本 {"type":"status","fresh":false|true,...}；参数错误收到 {"type":"error",...} 后断开。

画面来源（config/video.json 的 source）：
  real  本机真实摄像头，由摄像头常驻进程（camera_service）写在共享目录里
  sim   Isaac 仿真渲染的画面（原有的两槽 PNG 预览目录），这里转成 JPEG
"""
import asyncio
import json
import os
import time
from io import BytesIO
from pathlib import Path

from .camera_service import DEFAULT_STORE, FrameStore, FrameUnavailable
from .store import Fault

CAMERAS = ('front', 'wrist')
SOURCES = ('real', 'sim')
VIDEO_CONFIG = '/config/video.json'
MAX_VIEWERS = 16                 # 双机位：一个页面通常开两路
MAX_AGE_S = 1.0


class SimFrames:
    """Isaac 仿真画面：读原有的两槽 PNG 预览目录，转成 JPEG（同一帧只转一次）。"""

    def __init__(self, ipc_dir, generation_file):
        self.ipc_dir, self.generation_file = Path(ipc_dir), Path(generation_file)
        self._cache = {}

    def _store(self):
        from integration.simulation.passive_preview import PreviewStore
        generation = json.loads(self.generation_file.read_text(encoding='utf-8'))['generation']
        return PreviewStore(self.ipc_dir, generation=generation)

    def read(self, camera):
        from integration.simulation.passive_preview import PreviewUnavailable
        try:
            value = self._store().read(camera)
        except (PreviewUnavailable, OSError, ValueError, KeyError) as error:
            raise FrameUnavailable(f'仿真画面 {camera} 暂时没有：{error}') from None
        meta = value['metadata']
        key = (camera, meta['sequence'], meta['captured_monotonic_ns'])
        if key not in self._cache:
            from PIL import Image
            with Image.open(BytesIO(value['png'])) as image:
                stream = BytesIO()
                image.convert('RGB').save(stream, format='JPEG', quality=80)
            self._cache = {k: v for k, v in self._cache.items() if k[0] != camera}
            self._cache[key] = (stream.getvalue(), image.size)
        jpeg, (width, height) = self._cache[key]
        return jpeg, {'sequence': meta['sequence'], 'captured_monotonic_ns': meta['captured_monotonic_ns'],
                      'captured_unix_ns': meta['captured_unix_ns'], 'shape': [height, width, 3]}

    def status(self):
        result = {}
        for camera in CAMERAS:
            try:
                _, meta = self.read(camera)
                result[camera] = {'fresh': True, 'age_s': round((time.monotonic_ns() - meta['captured_monotonic_ns']) / 1e9, 3),
                                  'width': meta['shape'][1], 'height': meta['shape'][0], 'sequence': meta['sequence']}
            except FrameUnavailable as error:
                result[camera] = {'fresh': False, 'age_s': None, 'message': str(error)}
        return result


class RealFrames:
    """本机真实摄像头：读摄像头常驻进程的共享目录。"""

    def __init__(self, root):
        self.store = FrameStore(root)

    def read(self, camera):
        return self.store.read(camera, 'jpeg', MAX_AGE_S)

    def status(self):
        return self.store.status(MAX_AGE_S)


def configured_source(path):
    """config/video.json 里的 source。没有这个文件时是 real（和以前不传 source 时一样）。"""
    try:
        value = json.loads(Path(path).read_text(encoding='utf-8'))
    except FileNotFoundError:
        return 'real'
    except (OSError, ValueError) as error:
        raise Fault('VIDEO_SOURCE_INVALID', f'视频来源配置 {path} 读不了：{error}', 503) from None
    source = value.get('source') if isinstance(value, dict) else None
    if source not in SOURCES:
        raise Fault('VIDEO_SOURCE_INVALID', '视频来源配置的 source 只能是 real 或 sim：请改 .env 的 '
                                            'COLLECTION_VIDEO_SOURCE 后执行 ./apply-env.py', 503)
    return source


def add_routes(app, *, real=None, sim=None, config_path=None, max_viewers=MAX_VIEWERS):
    frames = {'real': real or RealFrames(os.environ.get('SO101_CAMERA_STORE', DEFAULT_STORE)),
              'sim': sim or SimFrames(os.environ.get('SO101_SIM_PREVIEW_DIR', '/data/isaac-console/ipc'),
                                      os.environ.get('SO101_SIM_GENERATION_FILE', '/data/isaac-console/generation.json'))}
    config_path = config_path or os.environ.get('SO101_VIDEO_CONFIG', VIDEO_CONFIG)
    viewers = set()
    from aiohttp import web

    def current():
        name = configured_source(config_path)
        return name, frames[name]

    def camera_of(request, default='wrist'):
        camera = request.match_info.get('camera') or request.query.get('camera', default)
        if camera not in CAMERAS:
            raise Fault('INVALID_CAMERA', 'camera 只能是 front（前方）或 wrist（腕部）', 400)
        return camera

    async def listing(request):
        name, source = current()
        try:
            status, problem = await asyncio.to_thread(source.status), None
        except Exception as error:
            status, problem = {}, str(error)
        cameras = {}
        for camera in CAMERAS:                           # 永远是这两路；每路的地址前端照着连就行
            value = status.get(camera) or {'fresh': False, 'age_s': None,
                                           'message': problem or f'{"摄像头" if name == "real" else "仿真画面"} {camera} 没有画面'}
            cameras[camera] = {**value, 'jpg': f'/collection/video/{camera}.jpg', 'ws': f'/collection/video/ws?camera={camera}'}
        return web.json_response({'ok': True, 'result': {
            'source': name, 'cameras': cameras, 'websocket': '/collection/video/ws?camera=wrist&fps=10',
            'max_viewers': max_viewers, 'active_viewers': len(viewers)}})

    async def snapshot(request):
        camera = camera_of(request)
        name, source = current()
        try:
            jpeg, meta = await asyncio.to_thread(source.read, camera)
        except FrameUnavailable as error:
            raise Fault('VIDEO_UNAVAILABLE', str(error), 409) from None
        return web.Response(body=jpeg, content_type='image/jpeg', headers={
            'X-Frame-Sequence': str(meta['sequence']), 'X-Captured-Unix-Ns': str(meta['captured_unix_ns']),
            'X-Video-Source': name})

    async def websocket(request):
        ws = web.WebSocketResponse(heartbeat=15, max_msg_size=4096)
        await ws.prepare(request)

        async def fail(code, message, close_code):
            await ws.send_str(json.dumps({'type': 'error', 'code': code, 'message': message}, ensure_ascii=False))
            await ws.close(code=close_code)
            return ws
        try:
            camera = camera_of(request)
            fps = int(request.query.get('fps', '10'))
            name, source = current()                     # 连上时定下来源；改了配置，重连后生效
        except Fault as error:
            return await fail(error.code, str(error), 4503 if error.code == 'VIDEO_SOURCE_INVALID' else 4400)
        except ValueError:
            return await fail('INVALID_FPS', 'fps 必须是 1~30 的整数', 4400)
        if not 1 <= fps <= 30:
            return await fail('INVALID_FPS', 'fps 必须是 1~30 的整数', 4400)
        if len(viewers) >= max_viewers:
            return await fail('TOO_MANY_VIEWERS', f'同时最多 {max_viewers} 路视频，请先关闭不用的页面', 4429)
        viewers.add(ws)

        async def drain():
            async for _ in ws:      # 只为及时处理对方的关闭和心跳；客户端不需要发任何消息
                pass
        reader = asyncio.ensure_future(drain())
        try:
            try:
                _, meta = await asyncio.to_thread(source.read, camera)
                fresh, shape = True, meta['shape']
            except FrameUnavailable as error:
                fresh, shape, first_error = False, None, str(error)
            await ws.send_str(json.dumps({'type': 'hello', 'source': name, 'camera': camera, 'fps': fps, 'fresh': fresh,
                                          'width': shape[1] if shape else None, 'height': shape[0] if shape else None,
                                          'format': 'image/jpeg', **({} if fresh else {'message': first_error})}, ensure_ascii=False))
            last_sequence = None
            period = 1 / fps
            while not ws.closed and not reader.done():
                started = time.monotonic()
                try:
                    jpeg, meta = await asyncio.to_thread(source.read, camera)
                    if not fresh:
                        fresh = True
                        await ws.send_str(json.dumps({'type': 'status', 'fresh': True}))
                    if meta['sequence'] != last_sequence:
                        last_sequence = meta['sequence']
                        await ws.send_bytes(jpeg)
                except FrameUnavailable as error:
                    if fresh:
                        fresh = False
                        await ws.send_str(json.dumps({'type': 'status', 'fresh': False, 'message': str(error)}, ensure_ascii=False))
                except (ConnectionResetError, RuntimeError):
                    break
                await asyncio.sleep(max(0.0, period - (time.monotonic() - started)))
        finally:
            viewers.discard(ws)
            reader.cancel()
            if not ws.closed:
                await ws.close()
        return ws

    app.router.add_get('/collection/video', listing)
    app.router.add_get('/collection/video/ws', websocket)
    app.router.add_get('/collection/video/{camera}.jpg', snapshot)
    return frames
