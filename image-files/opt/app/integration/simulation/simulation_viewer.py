"""Private-LAN simulation view and stop signal; no arbitrary robot control route."""
import argparse,json,time,ipaddress
import asyncio
from fractions import Fraction
from pathlib import Path
from aiohttp import web
from aiortc import RTCPeerConnection,RTCSessionDescription,RTCConfiguration,VideoStreamTrack
from av import VideoFrame
from PIL import Image

def main():
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--port',type=int,default=8788);p.add_argument('--mode',choices=['inference','collection'],default='inference');p.add_argument('--max-peers',type=int,default=4);a=p.parse_args()
 if not 2<=a.max_peers<=8:p.error('max-peers must be 2..8')
 root=a.root
 peers=set()
 class Frames(VideoStreamTrack):
  def __init__(self,key):super().__init__();self.key=key;self.start=time.monotonic()
  async def recv(self):
   await asyncio.sleep(.1);path=root/(self.key+'-latest.jpg')
   if path.exists():
    with Image.open(path) as picture:frame=VideoFrame.from_image(picture.convert('RGB'))
   else:frame=VideoFrame.from_image(Image.new('RGB',(640,480),(16,26,34)))
   frame.pts=int((time.monotonic()-self.start)*90000);frame.time_base=Fraction(1,90000);return frame
 html='''<!doctype html><meta charset="utf-8"><title>SO101 ROS2 仿真验收</title><style>body{background:#101a22;color:#e5f1f4;font:18px sans-serif;margin:32px}button{padding:14px;background:#d43;color:white;border:0;border-radius:8px}img{width:46%;margin:1%;background:#20303b}pre{white-space:pre-wrap}a{color:#8ad}</style><h1>SO101 · ROS2 仿真闭环</h1><p>仅仿真；真实模型输出，不连接真机。训练样本不代表已学会抓取。</p><button onclick="fetch('/stop',{method:'POST'}).then(()=>document.getElementById('notice').textContent='停止请求已提交；以HOLD反馈为准')">停止当前仿真推理</button><span id="notice"></span><div><img id="front"><img id="wrist"></div><pre id="state"></pre><div id="videos"></div><script>async function tick(){let s=await fetch('/status').then(r=>r.json());document.getElementById('state').textContent=JSON.stringify(s,null,2);for(let k of ['front','wrist'])document.getElementById(k).src='/camera/'+k+'?t='+Date.now();if(s.completed)document.getElementById('videos').innerHTML='<p>验收已结束，下方是录制回放，不是实时控制。</p><video controls width="46%" src="/video/front"></video><video controls width="46%" src="/video/wrist"></video>';}setInterval(tick,1000);tick();</script>'''
 if a.mode=='collection':
  html=html.replace('SO101 · ROS2 仿真闭环','真实主臂 → ROS2 → 虚拟从臂')
  html=html.replace('仅仿真；真实模型输出，不连接真机。训练样本不代表已学会抓取。','COM16主臂只读输入；从臂和两路相机来自Isaac。下方显示输入在线状态、采集状态和画面帧龄。')
  html=html.replace('停止当前仿真推理','停止当前验证／采集（录制时保存）')
 @web.middleware
 async def guard(request,handler):
  if not request.remote or not ipaddress.ip_address(request.remote).is_private:raise web.HTTPForbidden()
  if request.method=='POST' and request.headers.get('Origin') not in (None,f'http://192.168.100.100:{a.port}',f'http://127.0.0.1:{a.port}'):raise web.HTTPForbidden()
  return await handler(request)
 async def index(request):return web.Response(text=html,content_type='text/html')
 async def status(request):
  try:value=json.loads((root/'status.json').read_text())
  except (FileNotFoundError,ValueError):value={'phase':'waiting'}
  value['camera_age_s']={k:round(time.time()-(root/(k+'-latest.jpg')).stat().st_mtime,2) if (root/(k+'-latest.jpg')).exists() else None for k in ('front','wrist')}
  value['completed']=(root/'complete.json').exists();value['video_connections']=[pc.connectionState for pc in peers];return web.json_response(value)
 async def stop(request):
  if (root/'complete.json').exists():return web.json_response({'requested':False,'reason':'run finished'})
  (root/'stop.request').write_text('user stopped simulation');return web.json_response({'requested':True,'holding_confirmed':False})
 async def offer(request):
  data=await request.json();key=data.get('camera')
  if key not in ('front','wrist'):raise web.HTTPBadRequest()
  if len(peers)>=a.max_peers:return web.json_response({'error':'Too many video connections; close duplicate preview tabs and refresh'},status=429)
  pc=RTCPeerConnection(RTCConfiguration(iceServers=[]));peers.add(pc)
  @pc.on('connectionstatechange')
  async def changed():
   if pc.connectionState in ('failed','closed','disconnected'):await pc.close();peers.discard(pc)
  await pc.setRemoteDescription(RTCSessionDescription(sdp=data['sdp'],type=data['type']));pc.addTrack(Frames(key))
  await pc.setLocalDescription(await pc.createAnswer());return web.json_response({'sdp':pc.localDescription.sdp,'type':pc.localDescription.type})
 async def file(request):
  key=request.match_info['key']
  if key not in ('front','wrist'):raise web.HTTPNotFound()
  name=key+'-latest.jpg' if request.path.startswith('/camera/') else key+'.mp4'
  if not (root/name).is_file():raise web.HTTPNotFound()
  return web.FileResponse(root/name)
 html=html.replace('<img id="front"><img id="wrist">','<video id="front" autoplay muted playsinline style="width:46%"></video><video id="wrist" autoplay muted playsinline style="width:46%"></video>')
 html=html.replace("for(let k of ['front','wrist'])document.getElementById(k).src='/camera/'+k+'?t='+Date.now();",'')
 html=html.replace("if(s.completed)document.getElementById('videos').innerHTML=","if(s.completed && !document.getElementById('videos').children.length)document.getElementById('videos').innerHTML=")
 html=html.replace('</script>',"async function connect(k){let p=new RTCPeerConnection({iceServers:[]});p.addTransceiver('video',{direction:'recvonly'});p.ontrack=e=>document.getElementById(k).srcObject=e.streams[0];await p.setLocalDescription(await p.createOffer());if(p.iceGatheringState!=='complete')await new Promise(ok=>p.addEventListener('icegatheringstatechange',()=>{if(p.iceGatheringState==='complete')ok()}));let answer=await fetch('/offer',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({...p.localDescription.toJSON(),camera:k})}).then(r=>r.json());await p.setRemoteDescription(answer)}connect('front');connect('wrist');</script>")
 app=web.Application(middlewares=[guard]);app.add_routes([web.get('/',index),web.get('/status',status),web.post('/stop',stop),web.post('/offer',offer),web.get('/camera/{key}',file),web.get('/video/{key}',file)])
 web.run_app(app,host='192.168.100.100',port=a.port,access_log=None)
if __name__=='__main__':main()
