"""Add the minimal asynchronous collection routes to the existing OpenAPI.

This module describes the HTTP facade only. It does not resolve projects,
acquire a robot, replace the v8 contract or change the core SDK types.
``add_minimal_paths`` returns a deep copy; its caller's document is untouched.
"""
from copy import deepcopy


_ID = {'type': 'string', 'pattern': r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'}
_TEXT = {'type': 'string', 'minLength': 1, 'pattern': r'\S'}


def _object(properties, required=(), *, additional=False, **extra):
    return {'type': 'object', 'properties': properties, 'required': list(required),
            'additionalProperties': additional, **extra}


def _ref(name):
    return {'$ref': '#/components/schemas/' + name}


def _envelope(schema):
    return _object({'ok': {'type': 'boolean', 'const': True}, 'result': schema}, ('ok', 'result'))


def _response(schema, description):
    return {'description': description,
            'content': {'application/json': {'schema': schema}}}


def _errors():
    return {str(status): _response(_ref('ErrorResponse'), description)
            for status, description in ((400, 'Invalid request fields'), (401, 'Bearer credential required'),
                                        (403, 'Owner/project denied'), (404, 'Resource not found'),
                                        (409, 'Conflict, stale attempt or target unavailable'),
                                        (500, 'Invalid deployment target configuration'),
                                        (503, 'Target/operation outcome unavailable'))}


def minimal_schemas():
    """Fresh schema dictionaries for the additive minimal HTTP facade."""
    common = {'request_id': _ID}
    session = {'session_id': _ID, **common}
    episode = {**session, 'episode_id': _ID}
    return deepcopy({
        'CollectionMinimalStartRequest': _object({
            **common, 'name': _TEXT,
            'description': {'type': 'string'},
            'fps': {'type': 'integer', 'minimum': 1, 'maximum': 30},
            'ros': _ref('RosConfig')}, ('name', 'request_id')),
        'CollectionMinimalEpisodeRequest': _object(episode, ('session_id', 'episode_id', 'request_id')),
        'CollectionMinimalFinishRequest': _object({**session, 'operation_id': _ID}, ('request_id',),
            oneOf=[{'required': ['session_id'], 'not': {'required': ['operation_id']}},
                   {'required': ['operation_id'], 'not': {'required': ['session_id']}}]),
        'CollectionMinimalAck': _object({
            'accepted': {'type': 'boolean', 'const': True},
            'request_id': _ID, 'operation_id': _ID}, ('accepted', 'request_id', 'operation_id')),
        'CollectionMinimalOperation': _object({
            'operation_id': _ID, 'request_id': _ID,
            'action': {'type': 'string', 'enum': ['start', 'redo', 'complete', 'finish']},
            'state': {'type': 'string', 'enum': ['QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'INTERRUPTED']},
            'result': {'type': ['object', 'null'], 'additionalProperties': True},
            'error': {'anyOf': [_ref('Error'), {'type': 'null'}]}},
            ('operation_id', 'request_id', 'action', 'state', 'result', 'error')),
        'CollectionMinimalStatus': _object({'state': {'type': 'string',
            'enum': ['IDLE', 'RUNNING', 'PROCESSING', 'FINISHING', 'START_FAILED']}}, ('state',), additional=True),
    })


def minimal_paths():
    """Minimal-only paths; use add_minimal_paths for shared v8 paths."""
    paths = {}
    request_names = {
        'start': 'CollectionMinimalStartRequest',
        'redo': 'CollectionMinimalEpisodeRequest',
        'complete': 'CollectionMinimalEpisodeRequest',
        'finish': 'CollectionMinimalFinishRequest',
    }
    descriptions = {
        'start': 'Queue recording of the first durable frame on the one robot this container collects',
        'redo': 'Queue replacement of the current unsubmitted attempt with a new episode_id at the same index',
        'complete': 'Queue submission and background upload of the current episode, then record the next',
        'finish': 'Queue one-click stop, save of a nonempty current episode and background delivery',
    }
    for action, schema_name in request_names.items():
        paths['/collection/' + action] = {'post': {
            'summary': descriptions[action],
            'requestBody': {'required': True, 'content': {'application/json': {'schema': _ref(schema_name)}}},
            'responses': {'202': _response(_envelope(_ref('CollectionMinimalAck')),
                'Durable request accepted; poll /collection/operations/{operation_id}. Acceptance is not completion.'), **_errors()}}}
    paths['/collection/operations/{id}'] = {'get': {
        'summary': 'Read this operator-owned asynchronous collection request; it is not an upload outbox lookup',
        'parameters': [{'in': 'path', 'name': 'id', 'required': True, 'schema': _ID}],
        'responses': {'200': _response(_envelope(_ref('CollectionMinimalOperation')), 'Current operation snapshot'), **_errors()}}}
    paths['/collection/status'] = {'get': {
        'summary': 'Minimal status without project; session_id selects its original target kind',
        'parameters': [{'in': 'query', 'name': 'session_id', 'required': False, 'schema': _ID}],
        'responses': {'200': _response(_envelope(_ref('CollectionMinimalStatus')), 'Current target/session state'), **_errors()}}}
    return deepcopy(paths)


def add_minimal_paths(document):
    """Return a merged copy, preserving v8 status/align through disjoint shapes."""
    result = deepcopy(document)
    schemas = result.setdefault('components', {}).setdefault('schemas', {})
    schemas.update(minimal_schemas())
    paths = result.setdefault('paths', {})
    additions = minimal_paths()

    paths.pop('/collection/align', None)       # v10r15：没有对齐接口了

    old_status = paths.get('/collection/status', {}).get('get')
    if old_status is not None:
        minimal_status = additions['/collection/status']['get']
        existing = deepcopy(old_status.get('parameters', []))
        for parameter in existing:
            if parameter.get('name') == 'project':
                parameter['required'] = False
                parameter['description'] = 'Present selects the v8 project-bound query; absent selects the minimal query.'
        minimal_status['parameters'] = existing + minimal_status['parameters']
        minimal_status['summary'] = 'With project: v8 query. Without project: minimal configured target or original session query.'
        old_response = old_status['responses']['200']['content']['application/json']['schema']
        minimal_status['responses']['200']['content']['application/json']['schema'] = {
            'anyOf': [deepcopy(old_response), _envelope(_ref('CollectionMinimalStatus'))]}
        minimal_status['description'] = (
            'Use either legacy project/robot_id/robot_revision/mode parameters or the minimal optional session_id; '
            'do not mix the shapes. Without session_id the minimal query reports the one robot this container collects.')

    paths.update(additions)
    return result
