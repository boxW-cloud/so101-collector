"""COM15 native passive reads to dedicated simulation mailbox. No motor writes."""
import argparse,json,os,time,uuid,urllib.request
from pathlib import Path
import serial
from serial.tools import list_ports
from native_leader_snapshot import query_frame,parse_telemetry
p=argparse.ArgumentParser()
p.add_argument('--profile',required=True,type=Path)
p.add_argument('--reference',required=True,type=Path)
p.add_argument('--runtime',required=True,type=Path)
p.add_argument('--url',default='http://192.168.100.100:8787')
p.add_argument('--seconds',default=3600,type=int)
a=p.parse_args();profile=json.loads(a.profile.read_text(encoding='utf-8'));ref=json.loads(a.reference.read_text(encoding='utf-8'))
token=(a.runtime/'token').read_text().strip();client=uuid.uuid4().hex
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
state={'pid':os.getpid(),'port':'COM15','control_writes_sent':0,'state':'STARTING','frames':0,'client_id':client,'duration_s':a.seconds,'changed_frames':0}
def save():
 state['updated_unix']=time.time();tmp=a.runtime/'bridge-status.tmp';tmp.write_text(json.dumps(state),encoding='utf-8');tmp.replace(a.runtime/'bridge-status.json')
try:
 dev=next((x for x in list_ports.comports() if x.device=='COM15'),None)
 if not dev or dev.hwid!=ref['usb_hwid']:raise ValueError('COM15 identity mismatch')
 with serial.Serial('COM15',1000000,timeout=.02,write_timeout=.2) as bus:
  def query(i,kind):
   bus.reset_input_buffer();bus.write(query_frame(i,kind));bus.flush();result=bytearray();end=time.monotonic()+.35
   while time.monotonic()<end and len(result)<16000:
    result.extend(bus.read(bus.in_waiting or 1))
    if b'!' in result:return bytes(result[:result.index(b'!')+1]).decode('ascii')
   raise TimeoutError(f'ID{i} timeout')
  for i in range(1,7):
   raw=query(i,'PRALL');prefix=f'#{i:03d}PRALL:'
   if not raw.startswith(prefix) or json.loads(raw[len(prefix):-1])!=ref['motors'][str(i)]['parameters']:raise ValueError(f'ID{i} parameters changed')
  start=time.monotonic();errors=0;state['started_unix']=time.time();state['deadline_unix']=time.time()+a.seconds
  trace=(a.runtime/('trace-'+client+'.jsonl')).open('x',encoding='utf-8')
  while time.monotonic()-start<a.seconds and not (a.runtime/'stop.request').exists():
   tick=time.monotonic();values={str(i):parse_telemetry(i,query(i,'PRTV'))['native_encoder'] for i in range(1,7)}
   if any(not 0<=v<=4095 for v in values.values()) or time.monotonic()-tick>.4:raise ValueError('Invalid/stale six-axis sample')
   state['frames']+=1
   if state.get('native_positions') is not None and values!=state['native_positions']:state['changed_frames']+=1
   state['remaining_s']=max(0,a.seconds-(time.monotonic()-start))
   data={'client_id':client,'sequence':state['frames'],'profile_hash':profile['profile_hash'],'native_positions':values,'source_remaining_s':state['remaining_s']}
   req=urllib.request.Request(a.url+'/leader-input',data=json.dumps(data).encode(),headers={'Content-Type':'application/json','Authorization':'Bearer '+token})
   try:
    with opener.open(req,timeout=.5) as response:accepted=json.load(response)
    errors=0;state.pop('error',None);state.update(state='STREAMING',last_accepted=accepted['accepted_sequence'],native_positions=values)
   except Exception as exc:
    errors+=1;state.update(state='NETWORK_WAIT',error=str(exc))
    if errors>=10:raise RuntimeError('Simulation unavailable; reader stopping') from exc
   trace.write(json.dumps({'time':time.time(),'sequence':state['frames'],'positions':values,'state':state['state'],'accepted':state.get('last_accepted')})+'\n');trace.flush()
   save();time.sleep(max(0,.1-(time.monotonic()-tick)))
  state['state']='STOPPED';state['stop_reason']='DURATION_ELAPSED' if time.monotonic()-start>=a.seconds else 'STOP_REQUEST';trace.close()
except BaseException as exc:
 state.update(state='FAILED',error=repr(exc));raise
finally:save()
