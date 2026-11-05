"""Outbound SSH reverse tunnel. The physical HTTP adapter remains loopback-only."""
import argparse,os,socket,select,threading,time
from pathlib import Path
import paramiko
def main():
    p=argparse.ArgumentParser();p.add_argument('--local-port',type=int,default=8871);p.add_argument('--remote-port',type=int,default=18871);p.add_argument('--status-file',type=Path,required=True);a=p.parse_args()
    client=paramiko.SSHClient();client.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'));client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect('192.168.100.100',username='nvidia',password=os.environ['SPARK_REPRO_PASSWORD'],look_for_keys=False,allow_agent=False,timeout=12)
    transport=client.get_transport();transport.set_keepalive(10);transport.request_port_forward('127.0.0.1',a.remote_port)
    a.status_file.write_text(__import__('json').dumps({'connected':True,'remote_port':a.remote_port,'local_port':a.local_port,'at':time.time()}))
    def relay(channel):
        sock=socket.create_connection(('127.0.0.1',a.local_port),timeout=3)
        try:
            while True:
                ready,_,_=select.select([sock,channel],[],[],1)
                for source in ready:
                    data=source.recv(65536)
                    if not data:return
                    (channel if source is sock else sock).sendall(data)
        finally:sock.close();channel.close()
    try:
        while transport.is_active():
            channel=transport.accept(1)
            if channel:threading.Thread(target=relay,args=(channel,),daemon=True).start()
    finally:
        a.status_file.write_text(__import__('json').dumps({'connected':False,'at':time.time()}));client.close()
if __name__=='__main__':main()
