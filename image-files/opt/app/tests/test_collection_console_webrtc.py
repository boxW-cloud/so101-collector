"""8920 public read-only video; collection control remains session/CSRF protected."""
import asyncio
from io import BytesIO

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from PIL import Image
import pytest

from collection_console.server import Console, ConsoleError, make_app
from test_collection_console import Backend


class MediaBackend(Backend):
    def __init__(self,events):
        super().__init__();self.events=events;self.commissioned=True
        image=BytesIO();Image.new('RGB',(16,12),(3,5,7)).save(image,'PNG');self.png=image.getvalue()
    async def get(self,path):
        value=await super().get(path)
        if path.startswith('/v2/robots?'):
            value['items'].append({'id':'physical','revision':'v1','created':1,
                'body':{'robot_id':'physical','kind':'physical','commissioned':self.commissioned,
                    'camera_shapes':{'front':[12,16,3]},'capabilities':['collection']}})
        return value
    async def request(self,method,path,body=None,raw=False):
        self.calls.append((method,path,body))
        assert method=='GET' and path=='/v2/robots/sim/v2/media/front.png' and raw is True
        return self.png
    async def close(self):self.events.append('backend-close')


class Preview:
    def __init__(self,png,events):self.png=png;self.paths=[];self.events=events
    async def request(self,path,raw=False):
        self.paths.append((path,raw))
        return self.png if raw else {'front':{'fresh':True}}
    async def close(self):self.events.append('preview-close')


class VideoSeam:
    """Exercise real callback/session/WS wiring, without pretending to encode RTC."""
    def __init__(self,events,**callbacks):
        self.events=events;self.callbacks=callbacks;self.sockets=set();self.closed=False
        self.authenticated=[];self.frames=[]
    @property
    def active_count(self):return len(self.sockets)
    async def handler(self,request):
        ws=web.WebSocketResponse(timeout=.2);await ws.prepare(request);self.sockets.add(ws)
        try:
            value=await ws.receive_json(timeout=1)
            try:
                assert await self.callbacks['auth_first'](request,value.get('csrf_token')) is True
                self.authenticated.append((value['robot_key'],value['camera']))
                shape=await self.callbacks['validate_target'](value['robot_key'],value['camera'])
                frame=await self.callbacks['read_png'](value['robot_key'],value['camera'])
                self.frames.append((tuple(shape),frame))
                await ws.send_json({'type':'test-ready','camera':value['camera']})
            except Exception:
                await ws.send_json({'type':'error'});await ws.close();return ws
            await ws.receive()
        finally:
            self.sockets.discard(ws)
            if not ws.closed:await ws.close()
        return ws
    async def close(self):
        self.closed=True;self.events.append('video-close')
        await asyncio.gather(*(ws.close() for ws in tuple(self.sockets)))


def setup(tmp_path,*,preview=True,physical_enabled=False,allowed_hosts=None):
    events=[];backend=MediaBackend(events)
    config={'allowed_hosts':['127.0.0.1','localhost'] if allowed_hosts is None else allowed_hosts,'project':'lab',
            'state_file':str(tmp_path/'console.sqlite'),'physical_enabled':physical_enabled}
    console=Console(config,backend)
    if preview:console.previews['sim@v2']=Preview(backend.png,events)
    services=[]
    def factory(**kwargs):
        services.append(VideoSeam(events,**kwargs));return services[-1]
    app=make_app(config,console,rtc_factory=factory)
    return console,backend,services[0],events,TestClient(TestServer(app))


async def login(client):
    response=await client.get('/api/state?robot_key=sim@v2')
    assert response.status==200
    value=(await response.json())['result']
    return value['csrf_token'],str(client.make_url('/')).rstrip('/'),value


async def offer(client,origin=None,token=None,**values):
    ws=await client.ws_connect('/api/video/ws',headers={'Origin':origin} if origin is not None else {})
    payload={'type':'offer','sdp':'v=0','robot_key':'sim@v2','camera':'front',**values}
    if token is not None:payload['csrf_token']=token
    await ws.send_json(payload)
    return ws,await ws.receive_json(timeout=2)


async def released(service):
    for _ in range(100):
        if service.active_count==0:return
        await asyncio.sleep(.01)
    assert service.active_count==0


@pytest.mark.parametrize('origin',[None,'http://external-frontend.invalid:5173','null'])
def test_read_only_ws_accepts_external_origin_without_cookie_or_state_login(tmp_path,origin):
    async def run():
        console,backend,service,events,client=setup(tmp_path)
        async with client:
            assert not client.session.cookie_jar.filter_cookies(client.make_url('/'))
            ws,response=await offer(client,origin)
            assert response=={'type':'test-ready','camera':'front'}
            assert service.frames==[((12,16,3),backend.png)]
            assert not client.session.cookie_jar.filter_cookies(client.make_url('/'))
            await ws.close();await released(service)
            assert console.remembered('sim@v2')=={}
        assert service.closed and not any(method=='POST' for method,_,_ in backend.calls)
    asyncio.run(run())


def test_video_accepts_listener_ip_without_relaxing_other_hosts(tmp_path):
    async def run():
        _,_,service,_,client=setup(tmp_path,allowed_hosts=[])
        async with client:
            ws,response=await offer(client,'http://external-frontend.invalid:5173')
            assert response=={'type':'test-ready','camera':'front'}
            await ws.close();await released(service)
            response=await client.get('/api/video/ws',headers={'Host':'192.168.0.215'})
            assert response.status==403
            response=await client.get('/api/state?robot_key=sim@v2')
            assert response.status==403
    asyncio.run(run())


def test_legacy_token_message_is_accepted_without_browser_cookie(tmp_path):
    async def run():
        console,backend,service,events,client=setup(tmp_path)
        async with client:
            ws,response=await offer(client,'http://other-frontend.invalid','expired-legacy-token')
            assert response=={'type':'test-ready','camera':'front'}
            assert service.frames==[((12,16,3),backend.png)]
            await ws.close();await released(service)
        assert not any(method=='POST' for method,_,_ in backend.calls)
    asyncio.run(run())


def test_public_video_does_not_relax_collection_command_authentication(tmp_path):
    async def run():
        console,backend,service,events,client=setup(tmp_path)
        async with client:
            external='http://external-frontend.invalid:5173'
            ws,response=await offer(client,external)
            assert response['type']=='test-ready'
            body={'action':'start','request_id':'read-only-does-not-grant-control',
                  'robot_key':'sim@v2','mode':'simulation_teleop','name':'demo'}
            same=str(client.make_url('/')).rstrip('/')
            assert (await client.post('/api/command',json=body,headers={'Origin':same})).status==403
            token,same,_=await login(client)
            assert (await client.post('/api/command',json=body,headers={'Origin':external,'X-CSRF-Token':token})).status==403
            assert (await client.post('/api/command',json=body,headers={'Origin':same})).status==403
            await ws.close();await released(service)
        assert not any(method=='POST' for method,_,_ in backend.calls)
    asyncio.run(run())


@pytest.mark.parametrize('preview',[True,False])
def test_video_reads_only_registered_png_even_with_unknown_collection_status(tmp_path,preview):
    async def run():
        console,backend,service,events,client=setup(tmp_path,preview=preview)
        backend.failure=ConsoleError('COLLECTION_STATE_UNKNOWN','Status unknown',503)
        async with client:
            token,origin,snapshot=await login(client)
            assert snapshot['status'] is None and snapshot['status_error']['code']=='COLLECTION_STATE_UNKNOWN'
            ws,response=await offer(client,origin,token)
            assert response=={'type':'test-ready','camera':'front'}
            assert service.frames==[((12,16,3),backend.png)]
            if preview:
                assert console.previews['sim@v2'].paths[-1]==('/camera/front.png',True)
                assert not any(path.endswith('.png') for method,path,_ in backend.calls)
            else:assert ('GET','/v2/robots/sim/v2/media/front.png',None) in backend.calls
            await ws.close();await released(service)
        assert events.index('video-close')<events.index('backend-close')
        assert not any(method=='POST' for method,_,_ in backend.calls)
    asyncio.run(run())


@pytest.mark.parametrize('values',[{'robot_key':'other@v9'},{'camera':'missing'},{'camera':'../front'}])
def test_unregistered_target_or_camera_never_reaches_frame_source(tmp_path,values):
    async def run():
        console,backend,service,events,client=setup(tmp_path)
        async with client:
            token,origin,_=await login(client)
            ws,response=await offer(client,'http://external-frontend.invalid',**values)
            assert response['type']=='error';await ws.close();await released(service)
            assert not service.frames and console.previews['sim@v2'].paths==[('/status',False)]
            assert not any(path.endswith('.png') for method,path,_ in backend.calls)
        assert not any(method=='POST' for method,_,_ in backend.calls)
    asyncio.run(run())


@pytest.mark.parametrize('enabled,commissioned',[(False,True),(True,False)])
def test_physical_disabled_or_uncommissioned_cannot_bypass_target_gate(tmp_path,enabled,commissioned):
    async def run():
        console,backend,service,events,client=setup(tmp_path,physical_enabled=enabled)
        backend.commissioned=commissioned
        async with client:
            token,origin,_=await login(client)
            ws,response=await offer(client,'http://external-frontend.invalid',robot_key='physical@v1')
            assert response['type']=='error';await ws.close();await released(service)
            assert not service.frames and not any(path.endswith('.png') for method,path,_ in backend.calls)
        assert not any(method=='POST' for method,_,_ in backend.calls)
    asyncio.run(run())


def test_app_shutdown_closes_video_before_preview_backend_and_database(tmp_path):
    async def run():
        console,backend,service,events,client=setup(tmp_path)
        async with client:
            token,origin,_=await login(client)
            ws,response=await offer(client,origin,token)
            assert response['type']=='test-ready' and service.active_count==1
        assert service.closed and service.active_count==0 and console.closed
        assert events.index('video-close')<events.index('preview-close')
        assert events.index('video-close')<events.index('backend-close')
        assert not any(method=='POST' for method,_,_ in backend.calls)
    asyncio.run(run())
