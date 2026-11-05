"""Describe v10 current-session commands without changing legacy contracts.

The five POST routes accept a v10 shape without a caller-supplied request ID.
Legacy v8/v9 request and response shapes remain documented on shared paths.
No new polling or push channel is required by the v10 frontend contract.
"""
from copy import deepcopy


def _ref(name):
    return {'$ref': '#/components/schemas/' + name}


def _object(properties, required=()):
    return {'type': 'object', 'properties': properties,
            'required': list(required), 'additionalProperties': False}


def _response(schema, description):
    return {'description': description,
            'content': {'application/json': {'schema': schema}}}


def current_schemas():
    """Return fresh schemas for the single-current-session v10 interface."""
    ack = _object({
        'accepted': {'type': 'boolean', 'const': True},
        'message': {'type': 'string', 'const': '已接收，后台执行中'},
    }, ('accepted', 'message'))
    return {
        'CollectionCurrentStartRequest': _object({
            'id': {'type': 'string', 'pattern': r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$',
                   'description': 'Required upstream-generated batch ID. It becomes the session, dataset and sync task ID '
                                  'and the folder name on the sync server, <dir>/<id>. Each id can start only one collection; '
                                  'reuse returns 409 ID_ALREADY_USED.'},
            'name': {'type': 'string', 'minLength': 1, 'pattern': r'\S',
                     'description': 'Required demonstration task text supplied by the caller.'},
            'description': {'type': 'string'},
            'fps': {'type': 'integer', 'minimum': 1, 'maximum': 30},
            'ros': _ref('RosConfig'),
        }, ('id', 'name')),
        'CollectionCurrentEmptyRequest': _object({}),
        'CollectionCurrentFinishRequest': _object({
            'upload_url': {'type': ['string', 'null'], 'maxLength': 1024,
                           'description': 'Optional. Omit it (or send null / "") to use the default directory '
                                          '<deploy_root>/action-composer/data/artifacts/data_set, where deploy_root is '
                                          'COLLECTION_SYNC_DEPLOY_ROOT in the container .env (e.g. /home/lxk/dev2). '
                                          'If given, it must be an absolute directory on the sync server and replaces the default. '
                                          'The sync server, account and password come from the container config, not from this request. '
                                          'The batch is pushed to <dir>/<id>/, verified file by file, and only then '
                                          '<dir>/<id>/.ready.json is written last inside the batch folder; that marker means the folder is complete. '
                                          'No platform callback is made; failures, including a missing deploy_root, '
                                          'are retried by the container until they succeed. '
                                          'Each path segment may contain letters, digits, _ + . - and must not start with . or -.'},
        }),
        'CollectionCurrentAck': ack,
        'CollectionCurrentAckResponse': _object({
            'ok': {'type': 'boolean', 'const': True},
            'result': _ref('CollectionCurrentAck'),
        }, ('ok', 'result')),
    }


def add_current_paths(document):
    """Merge v10 POST shapes into a copy; preserve legacy paths and schemas."""
    result = deepcopy(document)
    result.setdefault('components', {}).setdefault('schemas', {}).update(current_schemas())
    paths = result.setdefault('paths', {})
    actions = {
        'start': ('CollectionCurrentStartRequest', 'Start a new current collection using backend-bound configuration'),
        'redo': ('CollectionCurrentEmptyRequest', 'Discard the current uncommitted attempt and record the same logical episode again'),
        'complete': ('CollectionCurrentEmptyRequest', 'Commit the current episode locally, then start the next episode; defer delivery until finish'),
        'finish': ('CollectionCurrentFinishRequest', 'Stop, save a nonempty current episode and deliver the whole batch to one frozen destination'),
        'abandon': ('CollectionCurrentEmptyRequest', 'Abandon the current batch: cancel pending commands, stop device sessions with the follower holding, mark unfinished batches ABANDONED (not uploaded), release claims; reset to idle'),
    }
    for action, (schema_name, summary) in actions.items():
        route = paths.setdefault('/collection/' + action, {})
        old_post = route.get('post', {})
        post = deepcopy(old_post)
        post['summary'] = summary + ' (v10; v8/v9 shapes remain compatible)'
        post['description'] = (
            'Without request_id or project, this is the v10 single-current-session command. '
            'With request_id, the v9 contract remains in effect; align with project uses v8. '
            'V10 start takes one upstream id (the batch ID); no other session/episode/generation/operation IDs are exchanged. '
            'A 202 response means durably accepted, not completed. '
            'The container records final results and failures in its internal logs; '
            'v10 does not require status/operation polling or a new WebSocket. '
            'Externally repeated commands are not guaranteed to be deduplicated without a caller request ID.'
        )
        request_schema = _ref(schema_name)
        old_schema = (old_post.get('requestBody', {}).get('content', {})
                      .get('application/json', {}).get('schema'))
        if old_schema is not None:
            branches = (deepcopy(old_schema['oneOf']) if set(old_schema) == {'oneOf'}
                        else [deepcopy(old_schema)])
            request_schema = {'oneOf': branches + [request_schema]}
        post['requestBody'] = {
            'required': action == 'start',
            'description': ('A JSON object containing id and name is required.' if action == 'start' else
                            'V10 accepts an empty body or an object; upload_url is optional.' if action == 'finish' else
                            'V10 accepts an empty body or an object. Legacy shapes still require their documented fields.'),
            'content': {'application/json': {'schema': request_schema}},
        }
        responses = post.setdefault('responses', {})
        ack_schema = _ref('CollectionCurrentAckResponse')
        old_ack = (responses.get('202', {}).get('content', {})
                   .get('application/json', {}).get('schema'))
        if old_ack is not None:
            ack_schema = {'anyOf': [deepcopy(old_ack), ack_schema]}
        responses['202'] = _response(
            ack_schema, 'V10: durable acceptance only, with accepted/message and no IDs. Legacy v9 returns its original ACK.')
        for code, description in (
            (400, 'Invalid request fields or destination URL'),
            (401, 'Bearer credential required'),
            (403, 'Owner/project denied'),
            (404, 'Resource not found'),
            (409, 'No usable current session, conflicting command or unavailable target'),
            (500, 'Invalid deployment configuration'),
            (503, 'Service or target unavailable'),
        ):
            responses.setdefault(str(code), _response(_ref('ErrorResponse'), description))
        route['post'] = post
    return result
