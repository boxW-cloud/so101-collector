"""Read-only camera WebRTC media, with bounded non-trickle WS signaling.

The caller guards the network entry before upgrade. Authenticated instances also
call auth_first(request, csrf_token); read-only direct instances omit session auth.
Both validate the registered camera and read only fresh preview PNGs. This module
never connects, controls or closes a robot session.
"""
import asyncio
from dataclasses import dataclass
from fractions import Fraction
from io import BytesIO
import json
import math
from types import SimpleNamespace

from aiohttp import web, WSMsgType
from PIL import Image


MAX_MESSAGE_BYTES=70_000
FIRST_MESSAGE_TIMEOUT_S=10
NEGOTIATION_TIMEOUT_S=15
SOURCE_TIMEOUT_S=3


def _load_rtc():
    from aiortc import RTCPeerConnection,RTCConfiguration,RTCSessionDescription,RTCIceServer,VideoStreamTrack
    from aiortc.mediastreams import MediaStreamError
    from av import VideoFrame
    return SimpleNamespace(RTCPeerConnection=RTCPeerConnection,RTCConfiguration=RTCConfiguration,
        RTCSessionDescription=RTCSessionDescription,RTCIceServer=RTCIceServer,
        VideoStreamTrack=VideoStreamTrack,MediaStreamError=MediaStreamError,VideoFrame=VideoFrame)


class _VideoError(Exception):
    def __init__(self,code,message):self.code=code;self.message=message


@dataclass(eq=False)
class _Connection:
    ws: object
    pc: object=None
    track: object=None
    negotiation: object=None
    receiver: object=None
    disposal: object=None
    closing: bool=False
    failed: bool=False


def _json_message(message):
    if message.type!=WSMsgType.TEXT:raise _VideoError('PROTOCOL_ERROR','仅支持 JSON 视频信令。')
    if len(message.data.encode('utf-8'))>MAX_MESSAGE_BYTES:raise _VideoError('PROTOCOL_ERROR','视频信令过大。')
    try:value=json.loads(message.data)
    except (ValueError,TypeError):raise _VideoError('PROTOCOL_ERROR','视频信令格式错误。') from None
    if not isinstance(value,dict):raise _VideoError('PROTOCOL_ERROR','视频信令格式错误。')
    return value


def _offer(value,*,require_csrf=True,bound_target=False):
    required={'type','camera','sdp'}
    if not bound_target:required.add('robot_key')
    if require_csrf:required.add('csrf_token')
    if not required<=value.keys() or value.keys()-required-{'csrf_token','robot_key'} or value.get('type')!='offer':
        raise _VideoError('INVALID_OFFER','首条信令必须是完整视频 offer。')
    fields=[('camera',128),('sdp',MAX_MESSAGE_BYTES)]
    if 'robot_key' in value:fields.append(('robot_key',256))
    if 'csrf_token' in value:fields.append(('csrf_token',128))
    for name,bound in fields:
        if not isinstance(value[name],str) or not 0<len(value[name])<=bound:
            raise _VideoError('INVALID_OFFER','视频 offer 字段不完整。')
    lines=value['sdp'].replace('\r','').split('\n')
    media=[line for line in lines if line.startswith('m=')]
    if not lines or lines[0]!='v=0' or len(media)!=1 or not media[0].startswith('m=video '):
        raise _VideoError('INVALID_OFFER','每个连接只支持一路视频。')
    return value


def _image(png,shape):
    if not isinstance(png,bytes) or not 0<len(png)<=16*1024*1024:raise ValueError('Invalid PNG size')
    with Image.open(BytesIO(png)) as image:
        if image.format!='PNG' or image.size!=(shape[1],shape[0]):raise ValueError('Preview shape differs')
        image.load()
        return image.convert('RGB')


class VideoSocketService:
    def __init__(self,*,read_png,validate_target,auth_first,max_peers=4,fps=5,ice_servers=None,require_csrf=True,resolve_target=None):
        if type(max_peers)!=int or not 1<=max_peers<=4:raise ValueError('max_peers must be 1..4')
        if type(fps) not in (int,float) or not math.isfinite(fps) or not 1<=fps<=10:raise ValueError('fps must be 1..10')
        if not all(callable(v) for v in (read_png,validate_target,auth_first)):raise ValueError('Async callbacks required')
        if type(require_csrf)!=bool:raise ValueError('require_csrf must be boolean')
        if resolve_target is not None and not callable(resolve_target):raise ValueError('resolve_target must be an async callback')
        self.read_png=read_png;self.validate_target=validate_target;self.auth_first=auth_first;self.require_csrf=require_csrf
        self.resolve_target=resolve_target
        self.max_peers=max_peers;self.fps=fps;self.ice_servers=list(ice_servers or [])
        self._connections=set();self._admission=asyncio.Lock();self._closed=False

    @property
    def active_count(self):
        """Pending sockets plus negotiated peers; no private signaling data."""
        return len(self._connections)

    async def handler(self,request):
        async with self._admission:
            if self._closed or self.active_count>=self.max_peers:raise web.HTTPServiceUnavailable(text='Video peer limit reached')
            connection=_Connection(web.WebSocketResponse(max_msg_size=MAX_MESSAGE_BYTES,timeout=1,heartbeat=20))
            self._connections.add(connection)
        stage='offer'
        try:
            await connection.ws.prepare(request)
            first=await connection.ws.receive(timeout=FIRST_MESSAGE_TIMEOUT_S)
            if first.type in (WSMsgType.CLOSE,WSMsgType.CLOSED,WSMsgType.CLOSING):return connection.ws
            try:value=_json_message(first)
            except _VideoError:raise _VideoError('INVALID_OFFER','首条信令必须是完整视频 offer。') from None
            if value=={'type':'close'}:return connection.ws
            value=_offer(value,require_csrf=self.require_csrf,bound_target=self.resolve_target is not None);stage='negotiation'
            connection.negotiation=asyncio.create_task(self._negotiate(connection,request,value))
            connection.receiver=asyncio.create_task(connection.ws.receive())
            done,_=await asyncio.wait((connection.negotiation,connection.receiver),
                timeout=NEGOTIATION_TIMEOUT_S,return_when=asyncio.FIRST_COMPLETED)
            if not done:raise _VideoError('NEGOTIATION_TIMEOUT','视频协商超时，请重新连接。')
            if connection.receiver in done:
                message=connection.receiver.result()
                if self._client_close(message):return connection.ws
                raise _VideoError('PROTOCOL_ERROR','协商期间不支持额外信令。')
            answer=connection.negotiation.result()
            if connection.closing or connection.ws.closed:return connection.ws
            await connection.ws.send_json(answer)
            stage='stream'
            while not connection.closing:
                message=await connection.receiver
                if self._client_close(message):break
                raise _VideoError('PROTOCOL_ERROR','本连接不支持 ICE 重启或追加信令。')
        except asyncio.TimeoutError:
            await self._error(connection,'OFFER_TIMEOUT' if stage=='offer' else 'NEGOTIATION_TIMEOUT','视频连接超时，请重新连接。')
        except _VideoError as error:await self._error(connection,error.code,error.message)
        except asyncio.CancelledError:raise
        except Exception:await self._error(connection,'VIDEO_UNAVAILABLE','视频服务暂不可用，请重试。')
        finally:await self._dispose(connection)
        return connection.ws

    @staticmethod
    def _client_close(message):
        if message.type in (WSMsgType.CLOSE,WSMsgType.CLOSED,WSMsgType.CLOSING):return True
        return _json_message(message)=={'type':'close'}

    async def _negotiate(self,connection,request,offer):
        if self.require_csrf:
            try:
                if await self.auth_first(request,offer['csrf_token']) is not True:raise web.HTTPForbidden()
            except Exception:raise _VideoError('FORBIDDEN','请刷新页面后重新连接视频。') from None
        legacy_key='robot_key' in offer
        if self.resolve_target is not None:
            try:
                key=await self.resolve_target(offer['camera'])
                if not isinstance(key,str) or not 0<len(key)<=256:raise ValueError('Invalid bound key')
            except Exception as error:
                messages={'VIDEO_TARGET_NOT_CONFIGURED':'容器尚未绑定视频目标。',
                          'VIDEO_TARGET_AMBIGUOUS':'容器的视频目标绑定不唯一。',
                          'VIDEO_TARGET_INVALID':'容器的视频目标配置无效。'}
                code=getattr(error,'code',None)
                if code in messages:raise _VideoError(code,messages[code]) from None
                raise _VideoError('INVALID_TARGET','容器绑定的机器人或相机不可用。') from None
            if legacy_key and offer['robot_key']!=key:
                raise _VideoError('INVALID_TARGET','请求目标与容器视频绑定不一致。')
            # Freeze one internal source for the entire peer, including every frame.
            offer={**offer,'robot_key':key}
        try:
            shape=tuple(await self.validate_target(offer['robot_key'],offer['camera']))
            if (len(shape)!=3 or shape[2]!=3 or any(type(v)!=int or v<=0 for v in shape)
                or shape[0]*shape[1]>4096*2160):raise ValueError('Invalid camera shape')
        except Exception:raise _VideoError('INVALID_TARGET','指定的机器人或相机不可用。') from None
        try:_image(await asyncio.wait_for(self.read_png(offer['robot_key'],offer['camera']),SOURCE_TIMEOUT_S),shape)
        except Exception:raise _VideoError('SOURCE_UNAVAILABLE','相机画面已过期或不可用，请恢复画面后重连。') from None
        rtc=_load_rtc()
        servers=[rtc.RTCIceServer(**v) if isinstance(v,dict) else v for v in self.ice_servers]
        pc=connection.pc=rtc.RTCPeerConnection(configuration=rtc.RTCConfiguration(iceServers=servers))
        gathering=asyncio.Event()
        @pc.on('icegatheringstatechange')
        async def gathered():
            if pc.iceGatheringState=='complete':gathering.set()
        @pc.on('connectionstatechange')
        async def changed():
            if pc.connectionState in ('failed','closed') and not connection.closing:
                asyncio.create_task(self._fail(connection,'CONNECTION_FAILED','视频连接已中断，请重新连接。'))
        connection.track=self._track(rtc,connection,offer,shape)
        await pc.setRemoteDescription(rtc.RTCSessionDescription(sdp=offer['sdp'],type='offer'))
        pc.addTrack(connection.track)
        await pc.setLocalDescription(await pc.createAnswer())
        if pc.iceGatheringState!='complete':await gathering.wait()
        answer={'type':'answer','sdp':pc.localDescription.sdp,'camera':offer['camera']}
        if legacy_key:answer['robot_key']=offer['robot_key']
        return answer

    def _track(self,rtc,connection,offer,shape):
        service=self
        class PreviewTrack(rtc.VideoStreamTrack):
            def __init__(self):
                super().__init__();self.started=asyncio.get_running_loop().time();self.next_frame=self.started;self.last_pts=-1
            async def recv(self):
                if self.readyState!='live':raise rtc.MediaStreamError()
                try:
                    clock=asyncio.get_running_loop().time
                    await asyncio.sleep(max(0,self.next_frame-clock()))
                    if self.readyState!='live':raise rtc.MediaStreamError()
                    png=await asyncio.wait_for(service.read_png(offer['robot_key'],offer['camera']),SOURCE_TIMEOUT_S)
                    frame=rtc.VideoFrame.from_image(_image(png,shape))
                    frame.pts=max(self.last_pts+1,int((clock()-self.started)*90000))
                    self.last_pts=frame.pts;frame.time_base=Fraction(1,90000);self.next_frame=clock()+1/service.fps
                    return frame
                except asyncio.CancelledError:raise
                except Exception:
                    self.stop()
                    asyncio.create_task(service._fail(connection,'SOURCE_UNAVAILABLE','相机画面已过期或不可用，请恢复画面后重连。'))
                    raise rtc.MediaStreamError() from None
        return PreviewTrack()

    async def _error(self,connection,code,message):
        if connection.closing or connection.failed or connection.ws.closed:return
        connection.failed=True
        try:await connection.ws.send_json({'type':'error','code':code,'message':message})
        except Exception:pass

    async def _fail(self,connection,code,message):
        await self._error(connection,code,message)
        await self._dispose(connection)

    async def _dispose(self,connection):
        if connection.disposal is None:
            connection.closing=True
            connection.disposal=asyncio.create_task(self._cleanup(connection))
        await asyncio.shield(connection.disposal)

    async def _cleanup(self,connection):
        try:
            pending=[v for v in (connection.negotiation,connection.receiver) if v is not None and not v.done()]
            for task in pending:task.cancel()
            if pending:await asyncio.gather(*pending,return_exceptions=True)
            if connection.track is not None:connection.track.stop()
            if connection.pc is not None:await asyncio.wait_for(connection.pc.close(),5)
        finally:
            try:
                if connection.ws.prepared and not connection.ws.closed:await connection.ws.close()
            finally:self._connections.discard(connection)

    async def close(self):
        """Reject new peers and close only this service's media/socket resources."""
        self._closed=True
        await asyncio.gather(*(self._dispose(v) for v in tuple(self._connections)))
