"""LAN WebRTC camera relay and authenticated simulation-only leader mailbox."""
import argparse
import asyncio
from fractions import Fraction
import hmac
import ipaddress
import json
from pathlib import Path
import time
from aiohttp import web
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCConfiguration, VideoStreamTrack
from av import VideoFrame
from PIL import Image, ImageDraw
from preview_input_health import input_health

p = argparse.ArgumentParser()
p.add_argument('--root', required=True, type=Path)
p.add_argument('--token-file', required=True, type=Path)
p.add_argument('--port', default=8787, type=int)
a = p.parse_args()
root = a.root
token = a.token_file.read_text().strip()
profile = json.loads((root/'profile.json').read_text())
pcs = set()
cameras = {'overview': 'overview-latest.jpg', 'front': 'rgb_external_D455-latest.jpg', 'wrist': 'rgb_ego-latest.jpg'}
client_id = None
sequence = -1
last_received = 0.


def publish(name, obj):
    path = root/(name+'.relay-tmp')
    path.write_text(json.dumps(obj), encoding='utf-8')
    path.replace(root/name)


def read(name, default):
    try:
        return json.loads((root/name).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return default


class Frames(VideoStreamTrack):
    def __init__(self, camera):
        super().__init__()
        self.path = root/cameras[camera]
        self.start = time.monotonic()
        self.counter = 0

    async def recv(self):
        await asyncio.sleep(max(0, self.start+self.counter/10-time.monotonic()))
        self.counter += 1
        if self.path.exists() and time.time()-self.path.stat().st_mtime < 5:
            with Image.open(self.path) as image:
                picture = image.convert('RGB')
        else:
            picture = Image.new('RGB', (960, 720), (20, 29, 36))
            ImageDraw.Draw(picture).text((35, 330), 'Waiting for live Isaac camera', fill='white')
        frame = VideoFrame.from_image(picture)
        frame.pts = int((time.monotonic()-self.start)*90000)
        frame.time_base = Fraction(1, 90000)
        return frame


@web.middleware
async def local_only(request, handler):
    if not request.remote or not ipaddress.ip_address(request.remote).is_private:
        raise web.HTTPForbidden()
    origins = {None, f'http://192.168.100.100:{a.port}', f'http://127.0.0.1:{a.port}'}
    if request.method == 'POST' and request.headers.get('Origin') not in origins:
        raise web.HTTPForbidden()
    response = await handler(request)
    response.headers['Cache-Control'] = 'no-store'
    return response


async def index(request):
    return web.FileResponse(Path(__file__).with_name('leader_preview.html'))


async def status(request):
    s = read('viewer-status.json', {'state': 'STARTING'})
    s.update(input_health(read('leader-input.json', None), time.monotonic()))
    s.update(webrtc_peers=len(pcs), received_sequence=sequence,
             received_age_s=None if not last_received else round(time.monotonic()-last_received, 3),
             camera_age_s={k:round(time.time()-(root/v).stat().st_mtime, 2) if (root/v).exists() else None for k,v in cameras.items()})
    return web.json_response(s)


async def leader_input(request):
    global client_id, sequence, last_received
    if not hmac.compare_digest(request.headers.get('Authorization', ''), 'Bearer '+token):
        raise web.HTTPUnauthorized()
    data = await request.json()
    if data.get('profile_hash') != profile['profile_hash']:
        raise web.HTTPConflict(text='profile mismatch')
    positions = data.get('native_positions', {})
    if set(positions) != set(map(str, range(1,7))) or any(type(v) is not int or not 0<=v<=4095 for v in positions.values()):
        raise web.HTTPBadRequest(text='six native encoder integers required')
    if type(data.get('sequence')) is not int or not isinstance(data.get('client_id'), str):
        raise web.HTTPBadRequest()
    now = time.monotonic()
    if client_id != data['client_id']:
        if client_id is not None and now-last_received < 2:
            raise web.HTTPConflict(text='input already owned')
        client_id, sequence = data['client_id'], -1
    if data['sequence'] <= sequence:
        raise web.HTTPConflict(text='old sequence')
    sequence = data['sequence']
    last_received = now
    data['received_monotonic'] = now
    publish('leader-input.json', data)
    return web.json_response({'accepted_sequence':sequence})


async def control(request):
    data = await request.json()
    if data.get('action') not in ('pause', 'align'):
        raise web.HTTPBadRequest()
    c = read('control.json', {'epoch':0})
    c = {'epoch':c['epoch']+1, 'paused':data['action']=='pause'}
    publish('control.json', c)
    return web.json_response(c)


async def offer(request):
    if len(pcs)>=4:
        raise web.HTTPServiceUnavailable()
    data = await request.json()
    camera = data.get('camera', 'overview')
    if camera not in cameras:
        raise web.HTTPBadRequest()
    pc = RTCPeerConnection(RTCConfiguration(iceServers=[]))
    pcs.add(pc)
    @pc.on('connectionstatechange')
    async def change():
        if pc.connectionState in ('failed', 'closed', 'disconnected'):
            await pc.close()
            pcs.discard(pc)
    try:
        await pc.setRemoteDescription(RTCSessionDescription(sdp=data['sdp'], type=data['type']))
        pc.addTrack(Frames(camera))
        await pc.setLocalDescription(await pc.createAnswer())
        return web.json_response({'sdp':pc.localDescription.sdp, 'type':pc.localDescription.type})
    except Exception:
        await pc.close()
        pcs.discard(pc)
        raise


async def shutdown(app):
    await asyncio.gather(*(pc.close() for pc in list(pcs)))


app = web.Application(middlewares=[local_only], client_max_size=128*1024)
app.router.add_get('/', index)
app.router.add_get('/status', status)
app.router.add_post('/offer', offer)
app.router.add_post('/leader-input', leader_input)
app.router.add_post('/control', control)
app.on_shutdown.append(shutdown)
web.run_app(app, host='192.168.100.100', port=a.port, access_log=None)
