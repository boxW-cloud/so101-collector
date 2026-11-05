"""Loopback-only Python3.12 service behind the public ROS2 bridge."""
import argparse,json,signal,subprocess,threading,time,os,uuid
from pathlib import Path
from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
from .service import Service,encode
from integration.ros_sim_contract import validate_contract

def make_server(service,port):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_POST(self):
            try:
                if self.path!='/dispatch' or self.headers.get('Transfer-Encoding'):raise ValueError('Unsupported request')
                size=int(self.headers.get('Content-Length','0'))
                if not 0<size<=65536:raise ValueError('Request too large')
                self.connection.settimeout(3)
                value=json.loads(self.rfile.read(size),parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite JSON')))
                result=service.dispatch(value)
                raw=json.dumps(result,default=encode).encode();self.send_response(200)
            except Exception:raw=b'{"ok":false,"error":{"code":"INVALID_REQUEST"}}';self.send_response(400)
            self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
    return ThreadingHTTPServer(('127.0.0.1',port),Handler)

def main():
    p=argparse.ArgumentParser();p.add_argument('--config',type=Path,required=True);p.add_argument('--secrets',type=Path,required=True);a=p.parse_args()
    config=json.loads(a.config.read_text());keys=json.loads(a.secrets.read_text())
    validate_contract(config['ros'])
    root=Path(config['data_root']);root.mkdir(parents=True,exist_ok=True)
    ipc=Path(config['ipc_root'])
    if ipc!=root/'ipc':raise ValueError('IPC must be the private data-root/ipc directory')
    if ipc.exists():ipc.rename(root/('ipc-archive-'+uuid.uuid4().hex))
    ipc.mkdir()
    (root/'ros-contract.json').write_text(json.dumps(config['ros']))
    service=Service(config,keys);server=make_server(service,config['internal_port'])
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    children=[]
    def spawn(name,args):
        stream=(root/(name+'.log')).open('ab');child=subprocess.Popen(args,stdout=stream,stderr=stream,start_new_session=True);children.append((name,child,stream))
    ros=['/opt/edge/collection_service/ros_entry.sh',config['dds_profile'],str(config['ros']['domain_id'])]
    spawn('api-ros',ros+['/opt/edge/collection_service/ros_api.py','--config',str(a.config)])
    spawn('gateway',ros+['/opt/edge/integration/ros_sim_gateway.py','--root',config['ipc_root'],'--contract',str(root/'ros-contract.json'),'--token',config['robot_secret']])
    stop=threading.Event()
    for signum in (signal.SIGINT,signal.SIGTERM):signal.signal(signum,lambda *_:stop.set())
    (root/'service-ready.json').write_text(json.dumps({'ready':True,'pid':os.getpid(),'api_namespace':config['api_namespace'],'domain_id':config['ros']['domain_id']}))
    try:
        while not stop.wait(.5):
            service.monitor()
            for name,child,_ in children:
                if child.poll() is not None:
                    # API remains usable for uploads when no simulator is connected.
                    if name=='api-ros':raise RuntimeError('Public ROS API process exited')
    finally:
        results=service.shutdown();server.shutdown()
        (Path(config['ipc_root'])/'shutdown.request').write_text('container shutdown')
        for _,child,stream in children:
            if child.poll() is None:
                child.terminate()
                try:child.wait(10)
                except subprocess.TimeoutExpired:child.kill();child.wait()
            stream.close()
        (root/'shutdown.json').write_text(json.dumps({'done':True,'time':time.time(),'sessions':results,
            'all_active_holds_confirmed':all(v['hold_confirmed'] for v in results)}))

if __name__=='__main__':main()
