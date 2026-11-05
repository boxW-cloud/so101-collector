"""Publish a frozen dataset to a mounted directory inside the collector process."""
from pathlib import Path

from .store import Fault, ident


def publish_directory_destination(platform, packet, actor, operation_id, destination):
    from lerobot_sdk import DatasetManager
    from lerobot_sdk.data.sinks import LocalDirectorySink

    actor.require(packet['project'],'operator')
    if (destination.get('authentication')!='none' or destination.get('principal')!=actor.name
            or destination.get('project')!=packet['project']):
        raise Fault('PRINCIPAL_MAPPING_REQUIRED','Frozen directory owner/project differs')
    root=Path(destination.get('directory',''))
    if not root.is_absolute():raise Fault('SINK_NOT_CONFIGURED','Absolute directory destination required')
    # Reject links in every existing component, including deployment ancestors.
    for path in (root,*root.parents):
        if path.is_symlink():raise Fault('INVALID_PATH','Linked directory destination is forbidden')
    components=[ident(packet['project']),ident(actor.name),ident(packet['dataset_id']),ident(packet['version'])]
    target=root.joinpath(*components)
    for path in (target,*target.parents):
        if path.is_symlink():raise Fault('INVALID_PATH','Linked directory destination is forbidden')
    if not target.resolve().is_relative_to(root.resolve()):
        raise Fault('INVALID_PATH','Directory destination escaped its deployment root')
    source=platform.store.get('datasets',packet['dataset_id'],packet['version'],actor)
    if source['owner']!=actor.name:
        raise Fault('DATASET_OWNER_MISMATCH','Directory delivery must retain the source owner')
    if source['project']!=packet['project'] or source['body']['content_hash']!=packet['content_hash']:
        raise Fault('DATASET_VERSION_MISMATCH','Directory source project/hash differs')

    def progress(value):
        platform.store.event(operation_id,'upload_progress',{
            'delivery_kind':'directory','completed_bytes':value.completed_bytes,
            'total_bytes':value.total_bytes,'phase':value.phase})

    result=LocalDirectorySink(target).upload(source['body']['root'],progress)
    published=DatasetManager().inspect(target)
    if (result.content_hash!=packet['content_hash'] or published.ref.content_hash!=packet['content_hash']
            or published.ref.dataset_id!=packet['dataset_id'] or published.ref.version!=packet['version']):
        raise Fault('HASH_MISMATCH','Published directory differs from frozen source')
    return {'upload_state':'READY','business_complete':True,'upload_verified':True,
        'delivery_kind':'directory','content_hash':published.ref.content_hash,
        'target_dataset_id':published.ref.dataset_id,'target_version':published.ref.version,
        'storage_ref':str(target),'target_directory':str(target)}
