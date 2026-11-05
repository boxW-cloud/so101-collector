python3 - <<'PY'
from pathlib import Path
import subprocess,json,time
root=Path('/home/nvidia/Sim-to-Real-SO-101-Workshop/outputs/sdk-stage1')
root.joinpath('ipc').mkdir(exist_ok=True)
inside='/workspace/Sim-to-Real-SO-101-Workshop/outputs/sdk-stage1'
script='source /root/sim_env.sh\nunset DISPLAY\nexec timeout --signal=INT --kill-after=15s 900s "$ISAAC_SIM/python.sh" "$@"'
cmd=['docker','exec','-i','-e','PYTHONUNBUFFERED=1','-e','OMP_NUM_THREADS=4',
     '-e','HTTPS_PROXY=http://127.0.0.1:23138','-e','HTTP_PROXY=http://127.0.0.1:23138',
     '-e','NO_PROXY=127.0.0.1,localhost','teleop','bash','-c',script,'sdk-isaac',
     inside+'/isaac_harness.py','--ipc-directory',inside+'/ipc','--contract-file',inside+'/contract.json',
     '--headless','--enable_cameras','--device','cpu','--livestream','0','--kit_args=--/app/extensions/registryEnabled=false']
running=subprocess.check_output(['docker','top','teleop','-eo','pid,args'],text=True)
assert 'sdk-stage1/isaac_harness.py' not in running,'An SDK harness already runs'
with (root/'harness.log').open('ab') as f:p=subprocess.Popen(cmd,stdout=f,stderr=f,start_new_session=True)
info={'pid':p.pid,'command':cmd,'started_unix':time.time(),'scope':'isolated SDK simulation; serial blocked'}
(root/'launch.json').write_text(json.dumps(info,indent=2));print(json.dumps({'pid':p.pid,'log':str(root/'harness.log')}))
PY
