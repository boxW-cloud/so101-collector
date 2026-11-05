"""Real ROS2 roundtrip timings before model loading; no physical robot."""
import json,time,argparse
from pathlib import Path
from lerobot_sdk.testing.file_sim import FileSimulationBackend
p=argparse.ArgumentParser();p.add_argument('--ipc',type=Path,required=True);p.add_argument('--contract',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
c=json.loads(a.contract.read_text());b=FileSimulationBackend(a.ipc,calibration_hash=c['calibration_hash']);report={}
try:
 start=time.monotonic();b.connect(10);report['connect_s']=time.monotonic()-start;report['samples']=[]
 for i in range(100):
  start=time.monotonic();s=b.sample(10);report['samples'].append({'elapsed_s':time.monotonic()-start,'stamp':s.captured_monotonic_ns,'cameras':{k:list(v.rgb.shape) for k,v in s.images.items()}})
 report['passed']=True
finally:
 try:b.close(hold=True,timeout_s=10)
 finally:a.output.write_text(json.dumps(report,indent=2))
