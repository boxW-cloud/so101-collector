"""Authenticated loopback Windows owner bridge with conservative clock mapping.

ROS stays on the collection host. An SSH reverse tunnel carries the signed
request to a Windows loopback agent; native controller retains serial ownership.
"""
import copy,json,time,threading,urllib.request
from pathlib import Path
from .ros_wire import sign,verify,Peer,FixtureAdapter,clock_id
from .store import Fault

def translate(reply,packet,started_ns,received_ns,received_unix):
    value=copy.deepcopy(reply);result=value.get('result',{})
    if not value.get('ok'):return value
    source_now=value.get('response_monotonic_ns')
    if packet['operation']=='inspect':
        result['origin_clock_id']=result.get('clock_id');result['clock_id']=clock_id();result['clock_bridge']='conservative_request_lower_bound'
    if packet['operation']=='sample':
        if type(source_now)!=int:raise Fault('CLOCK_METADATA_MISSING','Device response capture clock required')
        # Include full request age + allowed sender/receiver wall-clock skew.
        # A retried or cached response can only get OLDER, never be restamped fresh.
        request_age=max(0,received_unix-packet['issued_at'])+1.0
        metadata=[]
        for item in [result,*result.get('images',{}).values()]:
            raw=item['captured_monotonic_ns'];age=source_now-raw
            if type(raw)!=int or not 0<=age<=5_000_000_000:raise Fault('CLOCK_INVALID','Device timestamp outside bounded response age')
            mapped=started_ns-age-int(request_age*1e9)
            metadata.append({'source_capture_ns':raw,'mapped_lower_bound_ns':mapped,'response_transit_bound_ns':received_ns-started_ns})
            item['captured_monotonic_ns']=mapped
        result['clock_mapping']={'method':'conservative_request_lower_bound','observations':metadata,'exact_hardware_sync':False}
    return value

class ForwardPeer:
    def __init__(self,url,key):
        # The tunnel URL is deployment configuration, never an HTTP caller field.
        if not url.startswith(('http://127.0.0.1:','http://host.docker.internal:')):raise ValueError('Use the configured local/host SSH tunnel endpoint')
        self.url=url.rstrip('/')+'/rpc';self.key=key;self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    def call(self,packet):
        verify(packet,self.key);started=time.monotonic_ns()
        req=urllib.request.Request(self.url,data=json.dumps(packet).encode(),headers={'Content-Type':'application/json'})
        with self.opener.open(req,timeout=10) as r:value=verify(json.load(r),self.key)
        mapped=translate(value,packet,started,time.monotonic_ns(),time.time());return sign(mapped,self.key)
    def tick(self):pass  # The Windows peer has its own independent watchdog.

def configured_adapter(config):
    """Choose only a deployment-trusted mode, never a request-supplied mode."""
    mode=config.get('adapter_mode','native_owner')
    if mode=='native_owner':
        from .native_adapter import NativeOwnerAdapter
        return NativeOwnerAdapter(config['adapter'])
    if mode=='native_relative_teleop':
        from .native_teleop_adapter import NativeRelativeTeleopAdapter
        return NativeRelativeTeleopAdapter(config['adapter'])
    raise ValueError('Unknown trusted physical adapter mode')

def preview_response(adapter,path):
    """Private read-only preview; camera identifiers come from trusted config."""
    from .native_preview import preview_status,preview_frame
    if path=='/status':
        value={'ok':True,'result':preview_status(adapter)}
        return 200,{'Content-Type':'application/json'},json.dumps(value).encode()
    if path.startswith('/camera/') and path.endswith('.png'):
        camera=path[len('/camera/'):-len('.png')]
        if camera not in adapter.config.get('cameras',{}):raise Fault('CAMERA_NOT_ALLOWED','Camera is not registered',404)
        frame=preview_frame(adapter,camera);metadata=frame['metadata']
        return 200,{'Content-Type':'image/png','X-Captured-Unix-Ns':str(metadata['captured_unix_ns']),
                    'X-Frame-Sha256':metadata['sha256'],'X-Preview-Generation':metadata['generation']},frame['png']
    raise Fault('NOT_FOUND','Preview route is not available',404)

def serve(config):
    from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
    adapter=configured_adapter(config)
    key=Path(config['key_file']).read_text().strip();peer=Peer(adapter,key);exit=threading.Event()
    def watchdog():
        while not exit.wait(.1):peer.tick()
    threading.Thread(target=watchdog,daemon=True).start()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*a):pass
        def end_headers(self):
            self.send_header('Cache-Control','no-store')
            super().end_headers()
        def do_GET(self):
            if config.get('preview_enabled') is not True:
                self.send_error(404);return
            try:status,headers,raw=preview_response(adapter,self.path)
            except Exception as error:
                status=404 if getattr(error,'status',None)==404 or getattr(error,'code',None) in ('CAMERA_NOT_ALLOWED','NOT_FOUND') else 409
                headers={'Content-Type':'application/json'}
                raw=json.dumps({'ok':False,'error':{'code':getattr(error,'code','PREVIEW_UNAVAILABLE'),'message':str(error)}}).encode()
            self.send_response(status)
            for name,value in headers.items():self.send_header(name,value)
            self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
        def do_POST(self):
            if self.path!='/rpc' or self.headers.get('Transfer-Encoding'):
                self.send_error(400);return
            self.connection.settimeout(3);length=int(self.headers.get('Content-Length','0'))
            if not 0<length<=2*1024**2:self.send_error(400);return
            try:packet=json.loads(self.rfile.read(length));value=verify(peer.call(packet),key);value['response_monotonic_ns']=time.monotonic_ns();raw=json.dumps(sign(value,key)).encode()
            except Exception:self.send_error(400);return
            self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
    server=ThreadingHTTPServer(('127.0.0.1',config.get('port',8872)),Handler)
    try:server.serve_forever()
    finally:
        exit.set()
        peer.shutdown()
        server.server_close()

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);a=p.parse_args();serve(json.loads(Path(a.config).read_text()))
