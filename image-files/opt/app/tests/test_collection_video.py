"""v10r10：摄像头常驻进程 + 8910 视频（列表 / 单帧 JPEG / WebSocket 推流；只有 front、wrist 两路，来源由部署配置定）+ 工作台实时画面。摄像头用软件替身。"""
import asyncio
import hashlib
import json
import threading
import time
from io import BytesIO
from types import SimpleNamespace

import numpy as np
import pytest
from aiohttp import WSMsgType
from aiohttp.test_utils import TestClient, TestServer

from deployment_runtime.camera_service import CameraWorker, FrameStore, FrameUnavailable, SharedCamera


def jpeg_of(rgb):
    from PIL import Image
    stream = BytesIO()
    Image.fromarray(rgb).save(stream, format='JPEG', quality=80)
    return stream.getvalue()


def write_frame(store, name='wrist', value=100, sequence=1, shape=(48, 64, 3)):
    rgb = np.full(shape, value, dtype=np.uint8)
    store.write(name, rgb, jpeg_of(rgb), captured_monotonic_ns=time.monotonic_ns(), captured_unix_ns=time.time_ns(), sequence=sequence)
    return rgb


# ══════════ 1. 共享画面目录 ══════════

def test_frame_store_round_trip_status_and_staleness(tmp_path):
    store = FrameStore(tmp_path)
    rgb = write_frame(store, value=7, sequence=3)
    got, meta = store.read('wrist', 'rgb')
    assert np.array_equal(got, rgb) and meta['sequence'] == 3 and meta['slot'] == 1
    jpeg, _ = store.read('wrist', 'jpeg')
    assert jpeg.startswith(b'\xff\xd8')
    assert store.status()['wrist']['fresh'] is True and store.names() == ['wrist']
    meta_path = tmp_path / 'wrist.json'
    old = json.loads(meta_path.read_text())
    old['captured_monotonic_ns'] -= 5_000_000_000
    meta_path.write_text(json.dumps(old))
    with pytest.raises(FrameUnavailable, match='没有更新'):
        store.read('wrist', 'rgb', 1.0)
    assert store.status()['wrist']['fresh'] is False


def test_frame_overwritten_mid_read_is_never_returned(tmp_path):
    store = FrameStore(tmp_path)
    write_frame(store, value=7, sequence=1)
    (tmp_path / 'wrist-1.rgb').write_bytes(b'\x00' * (48 * 64 * 3))      # 下一帧已经覆盖了同一个槽
    with pytest.raises(FrameUnavailable, match='正在更新'):
        store.read('wrist', 'rgb')


def test_shared_camera_reads_the_store_like_the_old_camera_class(tmp_path):
    from deployment_runtime.store import Fault
    camera = SharedCamera('wrist', tmp_path)
    with pytest.raises(Fault) as error:
        camera.wait_ready(.1)
    assert error.value.code == 'CAMERA_STALE'
    rgb = write_frame(FrameStore(tmp_path), value=42)
    camera.wait_ready(1)
    frame, stamp = camera.latest(1.0)
    assert np.array_equal(frame, rgb) and 0 <= time.monotonic_ns() - stamp < 1e9


def test_camera_worker_publishes_frames_and_reopens_after_unplug(tmp_path):
    opened = []

    class Capture:
        def __init__(self, fail_after):
            self.fail_after, self.reads = fail_after, 0

        def read(self):
            self.reads += 1
            time.sleep(.005)
            if self.reads > self.fail_after:
                return False, None                     # 拔线：之后一直取不到帧
            return True, np.full((48, 64, 3), self.reads % 255, dtype=np.uint8)

        def release(self):
            pass

    def factory():
        if len(opened) == 0:
            opened.append('fail')
            raise OSError('找不到 /dev/video0')          # 第一次打开失败（还没插上）
        opened.append('ok')
        return Capture(fail_after=40 if len(opened) == 2 else 10**9)

    store = FrameStore(tmp_path)
    worker = CameraWorker('wrist', '/dev/null', store, width=64, height=48, fps=30, capture_factory=factory)
    worker.thread.start()
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and opened.count('ok') < 2:
            time.sleep(.05)
        assert opened[:3] == ['fail', 'ok', 'ok']         # 失败后重试；取不到帧 2 秒后重新打开
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not store.status().get('wrist', {}).get('fresh'):
            time.sleep(.05)
        assert store.status()['wrist']['fresh'] is True and worker.sequence > 1
    finally:
        worker.stop.set()
        worker.thread.join(5)


def test_camera_worker_crops_and_scales_a_camera_that_ignores_the_resolution(tmp_path):
    class Capture:                                     # 只会给 1280x720 的摄像头
        def read(self):
            time.sleep(.01)
            frame = np.zeros((720, 1280, 3), dtype=np.uint8)
            frame[:, 160:1120] = 200                       # 中间 4:3 的部分是亮的，两边会被裁掉
            return True, frame

        def release(self):
            pass

    store = FrameStore(tmp_path)
    worker = CameraWorker('front', '/dev/null', store, width=64, height=48, fps=30, capture_factory=Capture)
    worker.thread.start()
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not store.status().get('front', {}).get('fresh'):
            time.sleep(.05)
        rgb, meta = store.read('front', 'rgb')
        assert meta['shape'] == [48, 64, 3] and rgb.min() >= 190 and '1280x720' in worker.state
    finally:
        worker.stop.set()
        worker.thread.join(5)


# ══════════ 2. 8910 视频路由（经过真实的鉴权和跨域中间件） ══════════

def write_isaac_preview(ipc, generation, value=200, sequence=5):
    """按 Isaac 渲染器的格式写一帧两槽 PNG 预览。"""
    from PIL import Image
    ipc.mkdir(parents=True, exist_ok=True)
    cameras = {}
    for camera in ('front', 'wrist'):
        stream = BytesIO()
        Image.fromarray(np.full((48, 64, 3), value, dtype=np.uint8)).save(stream, format='PNG')
        png = stream.getvalue()
        name = f'{camera}-preview-{sequence % 2}.png'
        (ipc / name).write_bytes(png)
        cameras[camera] = {'filename': name, 'size': len(png), 'sha256': hashlib.sha256(png).hexdigest()}
    (ipc / 'camera-preview.json').write_text(json.dumps({'generation': generation, 'sequence': sequence,
        'captured_monotonic_ns': time.monotonic_ns(), 'captured_unix_ns': time.time_ns(), 'cameras': cameras}))


def collection_app(tmp_path, monkeypatch, source='real'):
    from deployment_runtime.http import make_app
    monkeypatch.setenv('SO101_CAMERA_STORE', str(tmp_path / 'camera'))
    monkeypatch.setenv('SO101_SIM_PREVIEW_DIR', str(tmp_path / 'ipc'))
    monkeypatch.setenv('SO101_SIM_GENERATION_FILE', str(tmp_path / 'generation.json'))
    monkeypatch.setenv('SO101_VIDEO_CONFIG', str(tmp_path / 'video.json'))
    (tmp_path / 'generation.json').write_text(json.dumps({'generation': 'g1'}))
    if source:
        set_source(tmp_path, source)
    nothing = SimpleNamespace()
    platform = SimpleNamespace(role='collection', config={'collection_rest_auth': 'none', 'collection_project': 'lab'},
                               get_collection=lambda: nothing, get_minimal_collection=lambda: nothing,
                               get_current_collection=lambda: nothing, close=lambda: None)
    account = {'name': 'operator', 'projects': ['lab'], 'roles': ['operator'], 'token_sha256': hashlib.sha256(b'x').hexdigest()}
    return make_app(platform, [account])


def set_source(tmp_path, source):
    """和宿主机 apply-env.py 生成的 config/video.json 一样。"""
    (tmp_path / 'video.json').write_text(json.dumps({'_说明': 'apply-env.py', 'source': source}))


def test_video_list_and_snapshot_need_no_token_and_allow_any_origin(tmp_path, monkeypatch):
    async def run():
        app = collection_app(tmp_path, monkeypatch, source='real')
        write_frame(FrameStore(tmp_path / 'camera'), value=90, sequence=2)
        write_frame(FrameStore(tmp_path / 'camera'), 'front', value=30, sequence=4)
        write_isaac_preview(tmp_path / 'ipc', 'g1', sequence=7)
        async with TestClient(TestServer(app)) as client:
            r = await client.get('/collection/video', headers={'Origin': 'http://localhost:5173'})
            body = (await r.json())['result']
            assert r.status == 200 and r.headers['Access-Control-Allow-Origin'] == '*'
            assert body['source'] == 'real' and list(body['cameras']) == ['front', 'wrist']        # 永远只有这两路
            assert body['cameras']['wrist']['fresh'] is True and body['cameras']['front']['sequence'] == 4
            assert body['cameras']['front']['ws'] == '/collection/video/ws?camera=front'
            assert body['cameras']['wrist']['jpg'] == '/collection/video/wrist.jpg'
            assert body['websocket'] == '/collection/video/ws?camera=wrist&fps=10' and 'sources' not in body
            r = await client.get('/collection/video/wrist.jpg')
            assert r.status == 200 and r.headers['Content-Type'] == 'image/jpeg' and (await r.read()).startswith(b'\xff\xd8')
            assert r.headers['Access-Control-Allow-Origin'] == '*' and r.headers['X-Frame-Sequence'] == '2'
            assert r.headers['X-Video-Source'] == 'real'
            r = await client.get('/collection/video/front.jpg?source=sim')        # 前端选不了来源：参数不起作用
            assert r.status == 200 and r.headers['X-Frame-Sequence'] == '4' and r.headers['X-Video-Source'] == 'real'
            r = await client.get('/collection/video/top.jpg')                     # 只有 front、wrist
            assert r.status == 400 and (await r.json())['error']['code'] == 'INVALID_CAMERA'
            r = await client.options('/collection/video', headers={'Origin': 'http://x', 'Access-Control-Request-Method': 'GET'})
            assert r.status == 204 and r.headers['Access-Control-Allow-Origin'] == '*'

            set_source(tmp_path, 'sim')                                           # 部署配置改成仿真：不用重启，下一次请求就是仿真画面
            body = (await (await client.get('/collection/video')).json())['result']
            assert body['source'] == 'sim' and list(body['cameras']) == ['front', 'wrist']
            assert body['cameras']['front']['sequence'] == 7 and body['cameras']['front']['ws'] == '/collection/video/ws?camera=front'
            r = await client.get('/collection/video/front.jpg?source=real')
            assert r.status == 200 and r.headers['X-Frame-Sequence'] == '7' and r.headers['X-Video-Source'] == 'sim'

            (tmp_path / 'video.json').write_text(json.dumps({'source': 'both'}))   # 配置写错：明确报错，不猜
            r = await client.get('/collection/video/front.jpg')
            assert r.status == 503 and (await r.json())['error']['code'] == 'VIDEO_SOURCE_INVALID'
            (tmp_path / 'video.json').unlink()                                    # 老部署没有这个文件：和以前一样是真机
            assert (await (await client.get('/collection/video')).json())['result']['source'] == 'real'
    asyncio.run(run())


def test_video_list_always_shows_front_and_wrist_even_without_frames(tmp_path, monkeypatch):
    async def run():
        app = collection_app(tmp_path, monkeypatch, source='real')
        write_frame(FrameStore(tmp_path / 'camera'), 'top', value=10)          # 不是 front/wrist 的画面不出现
        async with TestClient(TestServer(app)) as client:
            body = (await (await client.get('/collection/video')).json())['result']
            assert list(body['cameras']) == ['front', 'wrist']
            assert all(c['fresh'] is False and c['message'] for c in body['cameras'].values())
            r = await client.get('/collection/video/wrist.jpg')
            assert r.status == 409 and (await r.json())['error']['code'] == 'VIDEO_UNAVAILABLE'
    asyncio.run(run())


def test_video_websocket_streams_jpeg_frames_and_reports_gaps(tmp_path, monkeypatch):
    async def run():
        app = collection_app(tmp_path, monkeypatch, source='real')
        store = FrameStore(tmp_path / 'camera')
        write_frame(store, sequence=1)
        stop = threading.Event()

        def camera():
            sequence = 1
            while not stop.is_set():
                sequence += 1
                write_frame(store, value=sequence % 255, sequence=sequence)
                time.sleep(.03)
        writer = threading.Thread(target=camera, daemon=True)
        writer.start()
        try:
            async with TestClient(TestServer(app)) as client:
                ws = await client.ws_connect('/collection/video/ws?camera=wrist&fps=20',
                                             headers={'Origin': 'http://localhost:5173'})
                hello = json.loads((await ws.receive(timeout=5)).data)
                assert hello == {**hello, 'type': 'hello', 'source': 'real', 'camera': 'wrist', 'fps': 20,
                                 'fresh': True, 'width': 64, 'height': 48, 'format': 'image/jpeg'}
                frames = []
                while len(frames) < 5:
                    message = await ws.receive(timeout=5)
                    assert message.type == WSMsgType.BINARY
                    frames.append(message.data)
                assert all(f.startswith(b'\xff\xd8') for f in frames) and len(set(frames)) > 1
                stop.set()                                   # 摄像头停了：1 秒后收到“画面中断”
                while True:
                    message = await ws.receive(timeout=5)
                    if message.type == WSMsgType.TEXT:
                        break
                assert json.loads(message.data)['type'] == 'status' and json.loads(message.data)['fresh'] is False
                await ws.close()
                for query, code in (('fps=99', 'INVALID_FPS'), ('camera=top', 'INVALID_CAMERA')):
                    bad = await client.ws_connect('/collection/video/ws?' + query)
                    error = await bad.receive(timeout=5)
                    assert json.loads(error.data)['code'] == code
                    closed = await bad.receive(timeout=5)
                    assert closed.type in (WSMsgType.CLOSE, WSMsgType.CLOSED) and bad.close_code == 4400
                set_source(tmp_path, 'sim')                  # 换成仿真后新连接拿到的是仿真；source 参数不起作用
                sim = await client.ws_connect('/collection/video/ws?source=real&camera=front')
                hello = json.loads((await sim.receive(timeout=5)).data)
                assert hello['source'] == 'sim' and hello['fresh'] is False and '仿真画面' in hello['message']
                await sim.close()
                write_isaac_preview(tmp_path / 'ipc', 'g1', sequence=9)
                sim = await client.ws_connect('/collection/video/ws?camera=wrist&fps=5')
                hello = json.loads((await sim.receive(timeout=5)).data)
                assert hello['source'] == 'sim' and hello['fresh'] is True and hello['width'] == 64
                frame = await sim.receive(timeout=5)
                assert frame.type == WSMsgType.BINARY and frame.data.startswith(b'\xff\xd8')
                await sim.close()
        finally:
            stop.set()
            writer.join(2)
    asyncio.run(run())


# ══════════ 3. 8920 工作台：真实从臂的画面直接取实时摄像头 ══════════

def test_console_live_video_uses_the_8910_camera_without_a_session(tmp_path):
    async def run():
        from PIL import Image
        from collection_console.server import Console
        rgb = np.full((48, 64, 3), 77, dtype=np.uint8)

        class Backend:
            def __init__(self):
                self.paths = []

            async def get(self, path):
                self.paths.append(path)
                if path.startswith('/v2/robots?'):
                    return {'items': [{'id': 'real', 'revision': 'r1', 'created': 1, 'body': {
                        'robot_id': 'real', 'kind': 'physical', 'provenance': 'physical_direct_follower', 'commissioned': True,
                        'camera_shapes': {'wrist': [48, 64, 3]}, 'capabilities': ['collection', 'teleop']}}], 'next_offset': None}
                if path == '/collection/video':
                    return {'sources': {'real': {'cameras': {'wrist': {'fresh': True, 'age_s': .05, 'width': 64, 'height': 48}}}}}
                if path.startswith('/collection/status'):
                    return {'state': 'IDLE'}
                return {'items': [], 'next_offset': None}

            async def request(self, method, path, body=None, raw=False):
                self.paths.append(path)
                assert raw and path == '/collection/video/wrist.jpg?source=real'
                return jpeg_of(rgb)

        backend = Backend()
        console = Console({'project': 'lab', 'state_file': str(tmp_path / 'c.sqlite'), 'physical_enabled': True,
                           'robot_ids': ['real'], 'preferred_revisions': {'real': 'r1'}, 'live_video': {'real@r1': 'real'}}, backend)
        state = await console.state('real@r1', 'physical_teleop')
        assert state['media']['cameras']['wrist']['fresh'] is True
        png = await console.live_png('real@r1', 'wrist')
        with Image.open(BytesIO(png)) as image:
            assert image.format == 'PNG' and image.size == (64, 48)
        assert not any('/media' in p for p in backend.paths)        # 不再依赖采集会话里的画面缓存
        console.close()
    asyncio.run(run())
