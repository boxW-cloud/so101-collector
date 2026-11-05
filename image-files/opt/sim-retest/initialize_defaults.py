"""Initialize missing defaults only; retain existing data and credentials."""
import json,uuid
from pathlib import Path

def atomic(path,payload):
    temporary=path.with_name(path.name+'.init-'+uuid.uuid4().hex)
    temporary.write_bytes(payload);temporary.chmod(0o600);temporary.replace(path)

def prepare_defaults():
    data=Path('/data');shared=Path('/shared');defaults=Path('/opt/collection-defaults')
    data.mkdir(parents=True,exist_ok=True);shared.mkdir(parents=True,exist_ok=True)
    for item in defaults.rglob('*'):
        if not item.is_file() or item.name=='registrations.json':continue
        target=data/item.relative_to(defaults);target.parent.mkdir(parents=True,exist_ok=True)
        if not target.exists():atomic(target,item.read_bytes())
    run=data/'isaac-console';(run/'ipc').mkdir(exist_ok=True)
    generation=run/'generation.json'
    if not generation.exists():atomic(generation,json.dumps({'generation':uuid.uuid4().hex}).encode())
    # v10r15：不再生成主臂输入令牌（采集容器不读主臂了）
    registry=data/'registry.sqlite'
    if not registry.exists():
        from deployment_runtime.store import Store,Principal
        temporary=registry.with_name('.initial-'+uuid.uuid4().hex+'.sqlite')
        store=Store(temporary)
        try:
            for item in json.loads((defaults/'registrations.json').read_text()):
                actor=Principal(item['owner'],(item['project'],),('admin','operator'))
                store.register(item['kind'],item['id'],item['revision'],item['project'],actor,item['body'])
        finally:store.close()
        temporary.replace(registry)
    print('Runtime defaults ready; no recording or motion started.',flush=True)

if __name__=='__main__':prepare_defaults()
