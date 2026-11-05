"""Bounded preview cache; only validated observed frames, no device I/O on GET."""
import threading,time
from .store import Fault
CACHE={};LOCK=threading.RLock()
def publish(robot_id,frames):
    with LOCK:
        CACHE[robot_id]={k:{'rgb':v.rgb.copy(),'stamp':v.captured_monotonic_ns} for k,v in frames.items()}
def read(robot_id,camera):
    with LOCK:
        value=CACHE.get(robot_id,{}).get(camera)
        if value is None or not 0<=time.monotonic_ns()-value['stamp']<2_000_000_000:raise Fault('VIDEO_UNAVAILABLE','No fresh validated camera frame',409)
        return value['rgb'].copy(),value['stamp']
def state(robot_id):
    with LOCK:return {k:{'age_s':(time.monotonic_ns()-v['stamp'])/1e9,'fresh':0<=time.monotonic_ns()-v['stamp']<2_000_000_000} for k,v in CACHE.get(robot_id,{}).items()}

def add_routes(app,platform):
    from aiohttp import web
    import json
    peers=set()
    def authorized(r):
        platform.store.get('robots',r.match_info['id'],r.match_info['revision'],r['principal'])
        return r.match_info['id']
    async def status(r):return web.json_response({'ok':True,'result':state(authorized(r))})
    async def png(r):
        from io import BytesIO
        from PIL import Image
        rgb,stamp=read(authorized(r),r.match_info['camera']);stream=BytesIO();Image.fromarray(rgb).save(stream,format='PNG')
        return web.Response(body=stream.getvalue(),content_type='image/png',headers={'X-Captured-Monotonic-Ns':str(stamp)})
    async def offer(r):
        from aiortc import RTCPeerConnection,RTCSessionDescription,RTCConfiguration,VideoStreamTrack
        from av import VideoFrame
        from fractions import Fraction
        import asyncio,numpy as np
        robot=authorized(r);v=await r.json();camera=v['camera'];read(robot,camera)
        if set(v)!={'camera','type','sdp'} or v['type']!='offer' or len(v['sdp'])>64000:raise Fault('INVALID_OFFER','Bounded WebRTC offer required')
        if len(peers)>=8:raise Fault('PEER_LIMIT','Close inactive preview connections',429)
        class Frames(VideoStreamTrack):
            def __init__(self):super().__init__();self.start=time.monotonic()
            async def recv(self):
                await asyncio.sleep(.1)
                try:rgb,_=read(robot,camera)
                except Fault:rgb=np.zeros((480,640,3),dtype=np.uint8)
                frame=VideoFrame.from_ndarray(rgb,format='rgb24');frame.pts=int((time.monotonic()-self.start)*90000);frame.time_base=Fraction(1,90000);return frame
        pc=RTCPeerConnection(RTCConfiguration(iceServers=platform.config.get('ice_servers',[])));peers.add(pc)
        @pc.on('connectionstatechange')
        async def changed():
            if pc.connectionState in ('closed','failed','disconnected'):await pc.close();peers.discard(pc)
        await pc.setRemoteDescription(RTCSessionDescription(sdp=v['sdp'],type=v['type']));pc.addTrack(Frames())
        await pc.setLocalDescription(await pc.createAnswer());return web.json_response({'type':pc.localDescription.type,'sdp':pc.localDescription.sdp})
    app.router.add_get('/v2/robots/{id}/{revision}/media',status)
    app.router.add_get('/v2/robots/{id}/{revision}/media/{camera}.png',png)
    app.router.add_post('/v2/robots/{id}/{revision}/media/offer',offer)
    async def close(app):
        for peer in list(peers):await peer.close()
    app.on_cleanup.append(close)
