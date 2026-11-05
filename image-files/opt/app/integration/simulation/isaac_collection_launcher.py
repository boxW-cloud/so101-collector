"""Generation-aware launcher. Only owns its exact run IPC, never other jobs."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import time
import uuid
from replay_support import prepare_ipc


def running(run_id):
    output=subprocess.check_output(['docker','top','teleop','-eo','pid,args'],text=True)
    return any(f'/sdk-collection-{run_id}/ipc' in line for line in output.splitlines()[1:])


def main():
    p=argparse.ArgumentParser()
    p.add_argument('run_id');p.add_argument('--shutdown',action='store_true')
    a=p.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,80}',a.run_id):p.error('Invalid run ID')
    root=Path('/home/nvidia/Sim-to-Real-SO-101-Workshop/outputs')/('sdk-collection-'+a.run_id)
    inside='/workspace/Sim-to-Real-SO-101-Workshop/outputs/'+root.name
    ipc=root/'ipc'
    if a.shutdown:
        generation=json.loads((ipc/'generation.json').read_text())['generation']
        (ipc/'shutdown.request').write_text('owned run shutdown')
        end=time.monotonic()+90
        while time.monotonic()<end:
            try:
                s=json.loads((ipc/'shutdown.json').read_text())
                if s.get('generation')==generation and not running(a.run_id):
                    print(json.dumps({'shutdown_confirmed':True,'generation':generation}));return
            except (FileNotFoundError,json.JSONDecodeError):pass
            time.sleep(.5)
        raise RuntimeError('Owned harness did not confirm shutdown and exit')
    root.mkdir(parents=True,exist_ok=True)
    import fcntl
    with (root/'launch.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if running(a.run_id):raise RuntimeError('This run already has a live harness')
        generation=uuid.uuid4().hex
        archive=prepare_ipc(root,generation)
        for name in ['isaac_harness.py','contract.json']:
            if not (root/name).exists():raise FileNotFoundError(root/name)
        shell='source /root/sim_env.sh\nunset DISPLAY\nexec timeout --signal=INT --kill-after=15s 2400s "$ISAAC_SIM/python.sh" "$@"'
        cmd=['docker','exec','-i','-e','PYTHONUNBUFFERED=1','-e','OMP_NUM_THREADS=4','teleop','bash','-c',shell,'sdk-isaac',
             inside+'/isaac_harness.py','--ipc-directory',inside+'/ipc','--contract-file',inside+'/contract.json',
             '--generation',generation,'--headless','--enable_cameras','--device','cpu','--livestream','0',
             '--kit_args=--/app/extensions/registryEnabled=false']
        started=time.time_ns()
        logfile=root/f'harness-{generation}.log'
        with logfile.open('xb') as f:
            child=subprocess.Popen(cmd,stdout=f,stderr=f,start_new_session=True)
        launch={'generation':generation,'pid':child.pid,'started_unix_ns':started,'log':str(logfile),'archived_ipc':str(archive)}
        (root/'launch-current.json').write_text(json.dumps(launch,indent=2))
        end=time.monotonic()+600
        while time.monotonic()<end:
            if child.poll() is not None:raise RuntimeError(f'Harness exited {child.returncode}; see {logfile}')
            if (ipc/'failure.json').exists():raise RuntimeError((ipc/'failure.json').read_text())
            try:
                ready=json.loads((ipc/'ready.json').read_text())
                if ready.get('generation')==generation and ready.get('ready_unix_ns',0)>=started and running(a.run_id):
                    print(json.dumps({'ready':ready,'launch':launch}));return
            except (FileNotFoundError,json.JSONDecodeError):pass
            time.sleep(.5)
        (ipc/'shutdown.request').write_text('startup deadline exceeded')
        raise TimeoutError(f'Harness did not become ready: {logfile}')


if __name__=='__main__':main()
