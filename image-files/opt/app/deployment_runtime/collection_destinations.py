"""Frozen v10 batch destination and existing v2 receiver protocol.

Credential references stay in the private journal, never in dataset manifests.
An unregistered caller URL is anonymous: default/inbound tokens never follow it.
"""
import copy
import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from .store import Fault


def normalize_url(value):
    if not isinstance(value, str) or not value or len(value) > 2048 or any(c.isspace() or ord(c) < 32 for c in value):
        raise Fault('INVALID_UPLOAD_URL', 'Use a complete HTTP(S) receiver base URL', 400)
    try:
        parts = urllib.parse.urlsplit(value)
        port = parts.port
        if (parts.scheme not in ('http', 'https') or not parts.hostname or parts.username is not None
                or parts.password is not None or parts.query or parts.fragment or '?' in value or '#' in value
                or '\\' in value or (port is not None and not 1 <= port <= 65535)):
            raise ValueError()
    except ValueError:
        raise Fault('INVALID_UPLOAD_URL', 'URL must use HTTP(S), without credentials, query or fragment', 400) from None
    path = parts.path.rstrip('/')
    if path.endswith(('/v2/transfers', '/v2/identity')):
        raise Fault('INVALID_UPLOAD_URL', 'Supply the receiver base URL; /v2 endpoints are appended by the sender', 400)
    return urllib.parse.urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, '', ''))


def _registered(sink, sink_id, actor, project):
    sink = copy.deepcopy(sink)
    mapping = sink.get('principals', {}).get(actor.name)
    if mapping: sink = {**sink, **mapping, 'principal': actor.name}
    if sink.get('principal') != actor.name or sink.get('project', project) != project:
        raise Fault('PRINCIPAL_MAPPING_REQUIRED', 'Configure the destination for this owner/project')
    if sink.get('kind') == 'directory':
        directory=sink.get('directory')
        if not isinstance(directory,str) or not directory or not Path(directory).is_absolute():
            raise Fault('SINK_NOT_CONFIGURED','Directory destination requires an absolute deployment path')
        return {'protocol':'so101.directory.v1','directory':directory,'sink_id':sink_id,
            'authentication':'none','principal':actor.name,'project':project}
    url = normalize_url(sink.get('url'))
    token_file = sink.get('token_file')
    if not isinstance(token_file, str) or not token_file:
        raise Fault('SINK_NOT_CONFIGURED', 'Registered destination credential reference is missing')
    return {'protocol': 'so101.transfer.v2', 'url': url, 'sink_id': sink_id,
        'authentication': 'bearer_file', 'token_file': token_file, 'principal': actor.name, 'project': project}


def resolve_destination(platform, upload_url, actor, project):
    """No network I/O. Freeze URL and same-principal credential reference once."""
    actor.require(project, 'operator')
    sinks = platform.config.get('sinks', {})
    if upload_url is None:
        sink_id = platform.config.get('default_upload_sink', 'training')
        sink = sinks.get(sink_id)
        if not sink:
            # Ending and local save must remain possible without a receiver.
            return {'protocol': 'so101.transfer.v2', 'sink_id': sink_id,
                'authentication': 'none', 'configuration_error': 'SINK_NOT_CONFIGURED',
                'principal': actor.name, 'project': project}
        return _registered(sink, sink_id, actor, project)
    url = normalize_url(upload_url)
    for sink_id, sink in sinks.items():
        mapping = sink.get('principals', {}).get(actor.name, {})
        candidate = {**sink, **mapping}
        try: same = normalize_url(candidate.get('url')) == url
        except Fault: same = False
        if same: return _registered(sink, sink_id, actor, project)
    return {'protocol': 'so101.transfer.v2', 'url': url, 'authentication': 'none',
        'principal': actor.name, 'project': project}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise Fault('UPLOAD_REDIRECT_REJECTED', 'Receiver redirects are not followed')


def push_destination(platform, p, actor, operation_id, destination):
    """Send only the frozen local version to the frozen compatible receiver."""
    if destination.get('configuration_error'):
        raise Fault(destination['configuration_error'], 'Batch has no configured destination; local files retained')
    if destination.get('principal') != actor.name or destination.get('project') != p['project']:
        raise Fault('PRINCIPAL_MAPPING_REQUIRED', 'Frozen destination owner/project differs')
    if destination.get('protocol') == 'so101.directory.v1':
        from .collection_local_delivery import publish_directory_destination
        return publish_directory_destination(platform,p,actor,operation_id,destination)
    if destination.get('protocol') != 'so101.transfer.v2':
        raise Fault('UPLOAD_PROTOCOL_UNSUPPORTED', 'Unsupported batch receiver protocol')
    base_url = normalize_url(destination.get('url'))
    headers = {}
    if destination.get('authentication') == 'bearer_file':
        headers['Authorization'] = 'Bearer ' + Path(destination['token_file']).read_text().strip()
    elif destination.get('authentication') != 'none':
        raise Fault('SINK_NOT_CONFIGURED', 'Unsupported destination authentication')
    source = platform.store.get('datasets', p['dataset_id'], p['version'], actor)
    if source['project'] != p['project'] or source['body']['content_hash'] != p['content_hash']:
        raise Fault('DATASET_VERSION_MISMATCH', 'Upload source project/hash differs')
    manifest = source['body']['manifest']
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())

    def call(method, path, value=None, raw=None):
        request_headers = {**headers, 'Content-Type': 'application/json' if value is not None else 'application/octet-stream'}
        req = urllib.request.Request(base_url + path, method=method,
            data=json.dumps(value).encode() if value is not None else raw, headers=request_headers)
        with opener.open(req, timeout=120) as response: result = json.load(response)
        if not result.get('ok'): raise Fault('UPLOAD_FAILED', 'Destination rejected transfer')
        return result['result']

    identity = call('GET', '/v2/identity')
    if identity.get('principal') != actor.name or p['project'] not in identity.get('projects', []):
        raise Fault('SINK_IDENTITY_MISMATCH', 'Destination identity/project differs; no data uploaded')
    transfer = call('POST', '/v2/transfers', {'project': p['project'], 'manifest': manifest})
    key = transfer['transfer_id']
    if transfer['state'] != 'READY':
        for name in manifest['files']:
            path = platform.datasets.file(p['dataset_id'], p['version'], name, actor)
            offset = transfer['offsets'][name]
            with path.open('rb') as stream:
                stream.seek(offset)
                while raw := stream.read(1024 ** 2):
                    state = call('PUT', f'/v2/transfers/{urllib.parse.quote(key, safe="")}/files?' +
                        urllib.parse.urlencode({'path': name, 'offset': offset}), raw=raw)
                    offset += len(raw)
                    platform.store.event(operation_id, 'upload_progress', {'transfer_id': key,
                        'completed_bytes': state['completed_bytes'], 'total_bytes': state['total_bytes']})
    ready = call('POST', f'/v2/transfers/{urllib.parse.quote(key, safe="")}/finish', {})
    if ready['body']['content_hash'] != p['content_hash'] or ready['owner'] != actor.name or ready['project'] != p['project']:
        raise Fault('HASH_MISMATCH', 'Remote published hash/ownership differs')
    return {'upload_state': 'READY', 'business_complete': True, 'upload_verified': True,
        'content_hash': p['content_hash'], 'target_dataset_id': ready['id'], 'target_version': ready['revision'],
        'storage_ref': base_url + ':' + ready['id'] + '/' + ready['revision']}
