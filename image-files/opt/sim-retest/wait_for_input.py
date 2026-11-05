"""Wait for the external Isaac run before opening mapped leader input."""
import json,os,sys,time
from pathlib import Path
args=sys.argv[1:]
anchor=Path(args[args.index('--anchor-ready')+1])
generation=args[args.index('--anchor-generation')+1]
announced=False
while True:
    try:
        ready=json.loads(anchor.read_text())
        if ready.get('generation')==generation:break
    except (OSError,ValueError):pass
    if not announced:
        print('Waiting for external Isaac ready metadata; leader input is not open.',flush=True)
        announced=True
    time.sleep(.5)
os.execv(sys.executable,[sys.executable,*args])
