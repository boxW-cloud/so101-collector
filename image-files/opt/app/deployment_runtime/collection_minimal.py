"""Durable minimal.v9 commands over the unchanged registered device contract.

v10r12（2026-10-29）：一台采集容器只采一台机器人（.env 的 COLLECTION_ROBOT），采集目标就是部署配置里唯一绑定的那个。
请求和查询里不再有 is_sim；老前端还传的话忽略（不起作用、不报错），返回结果里也不再带 is_sim。
v10r15（2026-11-05）：没有 align 了，开始采集也不再要对齐凭据。对齐由操作者自己负责，平台不管。
"""
import copy
import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid

from .store import Fault, Principal, canonical, digest, ident


API_CONTRACT = 'minimal.v9'
_PRIVATE = {'project', 'mode', 'recording_generation', 'config_revision', 'calibration_revision',
            'scene_id', 'scene_revision', 'ros', 'ros_config', 'runtime_provenance', 'runtime_provenance_history',
            'time_source', 'api_contract', 'managed'}


def public(value):
    if isinstance(value, list): return [public(item) for item in value]
    if not isinstance(value, dict): return value
    return {key: public(item) for key, item in value.items() if key not in _PRIVATE}


def without_is_sim(value):
    """老前端还在传的 is_sim：忽略。"""
    return {key: item for key, item in value.items() if key != 'is_sim'} if isinstance(value, dict) else value


class MinimalCollectionAPI:
    api_contract = API_CONTRACT
    database_name = 'collection-minimal.sqlite'

    def __init__(self, platform):
        self.platform = platform; self.contract = platform.get_collection()
        self.lock = threading.RLock(); self.exit = threading.Event(); self.closed = False
        self.boot = uuid.uuid4().hex; self.workers = {}
        self.abandoned = set()   # 被“放弃异常批次”取消的命令：它们的工作线程不再改写命令状态
        with platform.lock:
            if not hasattr(platform, 'collection_admission_lock'): platform.collection_admission_lock = threading.RLock()
            if not hasattr(platform, 'collection_command_apis'): platform.collection_command_apis = []
            platform.collection_command_apis.append(self)
        self.db = sqlite3.connect(platform.root / self.database_name, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL'); self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
CREATE TABLE IF NOT EXISTS commands(id TEXT PRIMARY KEY,owner TEXT,request_id TEXT,fingerprint TEXT,
action TEXT,project TEXT,resource TEXT,frozen TEXT,state TEXT,result TEXT,error TEXT,created REAL,updated REAL,
UNIQUE(owner,request_id));
CREATE TABLE IF NOT EXISTS end_intents(start_operation_id TEXT PRIMARY KEY,finish_operation_id TEXT);
''')
        with self.lock:
            self.db.execute("UPDATE commands SET state='INTERRUPTED',error=?,updated=? WHERE state IN ('QUEUED','RUNNING') AND action!='finish'",
                (canonical({'code': 'RESTARTED', 'message': 'Motion is not replayed after restart'}), time.time()))
            restore = self.db.execute("SELECT id FROM commands WHERE action='finish' AND state IN ('QUEUED','RUNNING')").fetchall()
        for row in restore: self._kick(row['id'])

    def _project(self, actor):
        configured = self.platform.config.get('collection_project')
        environment = os.environ.get('SO101_COLLECTION_PROJECT') or None
        if configured and environment and configured != environment:
            raise Fault('COLLECTION_PROJECT_CONFIG_CONFLICT', 'Conflicting deployment project bindings', 500)
        project = configured or environment
        if project is None:
            targets = self.platform.config.get('collection_targets', {})
            choices = set(actor.projects).intersection(targets if isinstance(targets, dict) else ())
            if len(choices) != 1: raise Fault('COLLECTION_PROJECT_AMBIGUOUS', 'A unique authorized deployment project is required')
            project = choices.pop()
        ident(project); actor.require(project, 'operator'); return project

    @staticmethod
    def _shape(action, body):
        fields = {
            'start': (('request_id', 'name'), ('description', 'fps', 'ros')),
            'redo': (('request_id', 'session_id', 'episode_id'), ()),
            'complete': (('request_id', 'session_id', 'episode_id'), ()),
            'finish': (('request_id',), ('session_id', 'operation_id')),
        }
        if action not in fields: raise Fault('INVALID_ACTION', 'Unsupported minimal collection action', 400)
        required, optional = fields[action]
        if not isinstance(body, dict) or set(body) - set(required) - set(optional) or set(required) - set(body):
            raise Fault('INVALID_FIELDS', 'Required fields: ' + ', '.join(required), 400)
        ident(body['request_id'])
        for key in ('session_id', 'episode_id', 'operation_id'):
            if key in body: ident(body[key])
        if action == 'finish' and ('session_id' in body) == ('operation_id' in body):
            raise Fault('INVALID_FIELDS', 'Exactly one of session_id and operation_id is required', 400)
        if action == 'start' and (not isinstance(body['name'], str) or not body['name'].strip()):
            raise Fault('INVALID_NAME', 'Nonempty task name required', 400)
        if 'description' in body and not isinstance(body['description'], str): raise Fault('INVALID_FIELDS', 'description must be text', 400)

    def _entry(self, key, actor=None):
        ident(key)
        with self.lock: row = self.db.execute('SELECT * FROM commands WHERE id=?', (key,)).fetchone()
        if row is None: raise Fault('NOT_FOUND', 'Collection operation not found', 404)
        value = dict(row)
        if actor:
            actor.require(value['project'], 'operator')
            if actor.name != value['owner']: raise Fault('FORBIDDEN', 'Operation belongs to another operator', 403)
        for key in ('frozen', 'result', 'error'): value[key] = json.loads(value[key]) if value[key] else None
        return value

    def _session(self, key, actor):
        row = self.contract._read(key, actor)
        if row.get('api_contract') != self.api_contract: raise Fault('API_VERSION_MISMATCH', 'This session belongs to another API contract')
        return row

    def _mode(self, actor):
        """这台容器采的那台机器人的采集模式：部署配置里唯一绑定的目标（.env 的 COLLECTION_ROBOT 决定）。"""
        from .collection_targets import configured_mode
        return configured_mode(self.platform, self._project(actor))

    def _target(self, body, actor):
        from .collection_targets import resolve_target
        from .field_bindings import bind_robot_fields
        from .api_contract import validate
        project = self._project(actor)
        payload, robot = resolve_target(self.platform, {'project': project, 'mode': self._mode(actor)}, actor)
        payload.update(calibration_revision=robot.get('calibration_revision', 'baseline'), config_revision=robot['revision'])
        if robot['kind'] == 'simulation': payload.update(scene_id=robot['scene_id'], scene_revision=robot['scene_revision'])
        for key in ('name', 'description', 'fps', 'ros'):
            if key in body: payload[key] = body[key]
        payload.setdefault('fps', self.platform.config.get('collection_fps', robot.get('fps', 5)))
        validate('collections.start', payload); bind_robot_fields(robot, payload)
        registered = self.platform.store.get('robots', robot['robot_id'], robot['revision'], actor)
        from .direct_follower import is_direct
        return {'payload': payload, 'target_hash': registered['hash'], 'direct': is_direct(robot)}

    def _from_session(self, row, body, actor):
        if 'episode_id' in body and row['episode_id'] != body['episode_id']: raise Fault('STALE_EPISODE', 'Current episode_id required')
        registered = self.platform.store.get('robots', row['robot_id'], row['robot_revision'], actor)
        return {'session_id': row['session_id'], 'episode_id': row['episode_id'],
            'recording_generation': row['recording_generation'], 'target_hash': registered['hash'],
            'payload': {key: row[key] for key in
                ('project', 'robot_id', 'robot_revision', 'mode', 'calibration_revision', 'config_revision')},
            'scene': {key: row[key] for key in ('scene_id', 'scene_revision') if key in row}}

    def submit(self, action, body, actor):
        with self.platform.collection_admission_lock:
            return self._submit_locked(action, without_is_sim(body), actor)

    def _assert_shared_admission(self):
        if getattr(self.platform, 'collection_abandoning', False):
            raise Fault('BUSY', 'Abandoning the current batch (放弃异常批次进行中), retry in a few seconds')
        if getattr(self.platform,'collection_legacy_inflight',0):
            raise Fault('BUSY','A legacy collection start or alignment is in progress')
        # Both public contracts participate even before a robot job/claim exists.
        for api in self.platform.collection_command_apis:
            if api is self or api.closed: continue
            with api.lock:
                if api.db.execute("SELECT 1 FROM commands WHERE state IN ('QUEUED','RUNNING') LIMIT 1").fetchone():
                    raise Fault('BUSY', 'Another collection API has an operation in flight')
        if self.api_contract != 'minimal.v10':
            with self.contract.lock:
                rows = [json.loads(row['body']) for row in self.contract.db.execute('SELECT body FROM sessions').fetchall()]
            if any(row.get('api_contract') == 'minimal.v10' and row.get('finish', {}).get('state') not in ('EMPTY', 'QUEUED_UPLOAD', 'ABANDONED') for row in rows):
                raise Fault('BUSY', 'The container current collection must be ended through its own API')

    def _prepare_frozen(self, action, body, frozen, actor):
        return frozen

    def _contract_start(self, payload, inner, actor, *, frozen=None):
        return self.contract.minimal_start(payload, inner, actor)

    def _contract_episode(self, action, frozen, payload, inner, actor):
        method = self.contract.minimal_redo if action == 'redo' else self.contract.minimal_complete
        return method(frozen['session_id'], frozen['episode_id'], payload, inner, actor)

    def _contract_finish(self, sid, frozen, actor):
        return self.contract.minimal_finish(sid, {'project': frozen['payload']['project']}, frozen['inner_request_id'], actor)

    def _submit_locked(self, action, body, actor):
        self._shape(action, body); body = copy.deepcopy(body)
        try: fingerprint = digest([action, body])
        except (ValueError, TypeError) as error: raise Fault('INVALID_FIELDS', 'Finite JSON fields required', 400) from error
        with self.lock:
            if self.closed or self.exit.is_set(): raise Fault('SHUTTING_DOWN', 'Collection API is closing', 503)
            previous = self.db.execute('SELECT * FROM commands WHERE owner=? AND request_id=?', (actor.name, body['request_id'])).fetchone()
            if previous:
                actor.require(previous['project'], 'operator')
                if previous['fingerprint'] != fingerprint: raise Fault('IDEMPOTENCY_CONFLICT', 'request_id conflict')
                return {'accepted': True, 'request_id': body['request_id'], 'operation_id': previous['id']}
            self._assert_shared_admission()
            if action == 'start':
                frozen = self._target(body, actor)
                # 本机的数据源（Redis 采集队列）走这套开始流程；其他真机目标仍然拒绝。
                if frozen['payload']['mode'] == 'physical_teleop' and not frozen.get('direct'):
                    raise Fault('PHYSICAL_READINESS_REQUIRED', 'Trusted physical takeover evidence is required')
            elif action == 'finish' and 'operation_id' in body:
                parent = self._entry(body['operation_id'], actor)
                if parent['action'] != 'start' or parent['state'] not in ('QUEUED', 'RUNNING'):
                    raise Fault('INVALID_OPERATION', 'Only an unfinished start operation can be ended')
                frozen = copy.deepcopy(parent['frozen']); frozen['start_operation_id'] = parent['id']
            else:
                row = self._session(body['session_id'], actor)
                frozen = self._from_session(row, body, actor)
            frozen = self._prepare_frozen(action, body, frozen, actor)
            project = frozen['payload']['project']; resource = 'robot:' + frozen['payload']['robot_id']
            if action == 'finish' and self.db.execute("SELECT 1 FROM commands WHERE resource=? AND action='finish' AND state IN ('QUEUED','RUNNING')", (resource,)).fetchone():
                raise Fault('BUSY', 'An end request for this target is already in flight')
            if action != 'finish':
                if self.db.execute("SELECT 1 FROM commands WHERE resource=? AND state IN ('QUEUED','RUNNING')", (resource,)).fetchone():
                    raise Fault('BUSY', 'One target mutation is already in flight')
                if action == 'start' and any(row['resource'] == resource for row in self.platform.store.claims()):
                    raise Fault('BUSY', 'Target is already claimed')
            key = uuid.uuid4().hex
            frozen['inner_request_id'] = 'minimal-' + action + '-' + key
            now = time.time()
            self.db.execute('BEGIN IMMEDIATE')
            try:
                self.db.execute('INSERT INTO commands VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (key, actor.name, body['request_id'], fingerprint, action, project, resource, canonical(frozen), 'QUEUED', None, None, now, now))
                if action == 'finish' and frozen.get('start_operation_id'):
                    self.db.execute('INSERT OR REPLACE INTO end_intents VALUES(?,?)', (frozen['start_operation_id'], key))
                self.db.execute('COMMIT')
            except BaseException: self.db.execute('ROLLBACK'); raise
            self._kick(key)
            return {'accepted': True, 'request_id': body['request_id'], 'operation_id': key}

    def _kick(self, key):
        with self.lock:
            if key in self.workers and self.workers[key].is_alive(): return
            worker = threading.Thread(target=self._work, args=(key,), name='MinimalCollection-' + key[:8], daemon=True)
            self.workers[key] = worker; worker.start()

    def _update(self, key, state, *, result=None, error=None):
        with self.lock:
            if key in self.abandoned and state != 'INTERRUPTED': return   # 已被放弃，保持 INTERRUPTED
            self.db.execute('UPDATE commands SET state=?,result=?,error=?,updated=? WHERE id=?',
                (state, canonical(result) if result is not None else None, canonical(error) if error else None, time.time(), key))

    def _verify(self, frozen, actor):
        payload = frozen['payload']
        registered = self.platform.store.get('robots', payload['robot_id'], payload['robot_revision'], actor)
        if registered['hash'] != frozen['target_hash']: raise Fault('TARGET_CONFIG_CHANGED', 'Frozen target registration changed')
        if frozen.get('session_id'):
            row = self._session(frozen['session_id'], actor)
            if any(row[key] != payload[key] for key in ('project', 'robot_id', 'robot_revision', 'mode', 'calibration_revision', 'config_revision')):
                raise Fault('SESSION_BINDING_CHANGED', 'Frozen session identity changed')
            return row

    def _context(self, operation):
        with self.contract.lock:
            pending = self.contract.db.execute('SELECT result FROM requests WHERE owner=? AND request_id=?',
                (operation['owner'], operation['frozen']['inner_request_id'])).fetchone()
        if pending and pending['result']:
            return json.loads(pending['result'])
        return {}

    def _start_session(self, operation):
        return self._context(operation).get('session_id')

    def _work(self, key):
        command = self._entry(key); actor = Principal(command['owner'], (command['project'],), ('operator',))
        frozen = command['frozen']; action = command['action']
        try:
            with self.lock:
                if self.exit.is_set() or self._entry(key)['state'] not in ('QUEUED', 'RUNNING'): return
                self._update(key, 'RUNNING')
            if action == 'finish':
                self._finish(key, frozen, actor); return
            self._verify(frozen, actor)
            if self.exit.is_set():
                self._update(key, 'INTERRUPTED', error={'code': 'SHUTTING_DOWN', 'message': 'Command did not acquire the target'})
                return
            inner = frozen['inner_request_id']; payload = copy.deepcopy(frozen['payload'])
            if action == 'start':
                with self.lock: ending = self.db.execute('SELECT 1 FROM end_intents WHERE start_operation_id=?', (key,)).fetchone()
                if ending: raise Fault('START_CANCELLED', 'Start was ended before device acquisition')
                result = self._contract_start(payload, inner, actor, frozen=frozen)
            else:
                row = self._session(frozen['session_id'], actor)
                if row['episode_id'] != frozen['episode_id'] or row['recording_generation'] != frozen['recording_generation']:
                    raise Fault('STALE_EPISODE', 'Frozen episode is no longer current')
                result = self._contract_episode(action, frozen,
                    {'project': command['project'], 'recording_generation': frozen['recording_generation']}, inner, actor)
            self._update(key, 'SUCCEEDED', result=public(result))
        except BaseException as error:
            details = getattr(error, 'details', None)
            self._update(key, 'FAILED', error=public({'code': getattr(error, 'code', type(error).__name__),
                'message': str(error), **({'details': details} if details else {})}))

    def _finish(self, key, frozen, actor):
        sid = frozen.get('session_id')
        while sid is None and not self.exit.is_set() and key not in self.abandoned:
            parent = self._entry(frozen['start_operation_id'], actor)
            sid = self._start_session(parent)
            if sid: break
            if parent['state'] in ('QUEUED', 'FAILED', 'INTERRUPTED'):
                # No inner session/journal means no acquisition by this start.
                with self.platform.store.lock:
                    job = self.platform.store.db.execute('SELECT id FROM jobs WHERE owner=? AND request_id=?',
                        (actor.name, parent['frozen']['inner_request_id'])).fetchone()
                if job is not None:
                    self.exit.wait(.05); continue
                if parent['state'] == 'QUEUED': self._update(parent['id'], 'FAILED', error={'code': 'START_CANCELLED', 'message': 'Ended before start'})
                self._update(key, 'SUCCEEDED', result={'cancelled_before_start': True, 'recording': False})
                return
            self.exit.wait(.05)
        if self.exit.is_set() or key in self.abandoned: return  # Durable finish is the only replay-safe restored command.
        self._session(sid, actor)
        cancel = getattr(self.contract.runtime, 'cancel_start', None)
        if callable(cancel): cancel(sid)  # Immediate cancellation flag; no new motion/enable.
        self._contract_finish(sid, frozen, actor)
        while not self.exit.is_set() and key not in self.abandoned:
            row = self._session(sid, actor)
            if row.get('finish', {}).get('state') in ('QUEUED_UPLOAD', 'EMPTY'):
                stop = row.get('stop_result') or {}
                if stop.get('holding') is not True or stop.get('action_queue_empty') is not True:
                    raise Fault('STOP_UNCONFIRMED', 'Finish did not prove HOLD and empty queue')
                self._update(key, 'SUCCEEDED', result=public({**stop, 'session_id': sid,
                    'finish_state': row['finish']['state'], 'finished': True, 'automatic_upload': True,
                    'upload_operation': row.get('last_upload_operation')})); return
            self.exit.wait(.05)

    def operation(self, key, actor):
        command = self._entry(key, actor); result = command['result']
        if command['action'] == 'start' and command['state'] in ('RUNNING', 'INTERRUPTED', 'FAILED') and result is None:
            sid = self._start_session(command)
            if sid: result = {'session_id': sid}
        return {'operation_id': key, 'request_id': command['request_id'], 'action': command['action'],
                'state': command['state'], 'result': public(result), 'error': public(command['error'])}

    def status(self, query, actor):
        query = without_is_sim(query)
        if not isinstance(query, dict) or set(query) - {'session_id'}:
            raise Fault('INVALID_FIELDS', 'Status accepts an optional session_id only', 400)
        if query.get('session_id'):
            row = self._session(query['session_id'], actor)
            # Querying historical A must never be redirected to a later B on
            # the same target. A's stop result is history, not B's current HOLD.
            if row['internal_state'] == 'RUNNING' and not row.get('finish'):
                return public({**self.contract._result(row), 'active': True})
            result = {key: row[key] for key in ('session_id', 'dataset_id', 'robot_id', 'robot_revision',
                                               'target_type', 'mode', 'episode_id', 'episode_index')}
            finish = row.get('finish', {})
            if row['internal_state'] == 'NEEDS_RECONCILE':
                result.update(state='NEEDS_RECONCILE', active=True, error=row.get('error'))
            elif row['internal_state'] in ('STOPPED', 'START_FAILED'):
                result.update(state='IDLE' if row['internal_state'] == 'STOPPED' else 'START_FAILED', active=False)
            else: result.update(state='FINISHING' if finish else 'PROCESSING', active=True)
            if finish:
                result.update(finish_state=finish['state'], finished=finish['state'] in ('QUEUED_UPLOAD', 'EMPTY'),
                              automatic_upload=True, upload_operation=row.get('last_upload_operation'))
            return public(result)
        else:
            project = self._project(actor)
            from .collection_targets import resolve_target
            payload, _ = resolve_target(self.platform, {'project': project, 'mode': self._mode(actor)}, actor)
            payload.pop('mode')
        with self.lock:
            pending = self.db.execute("SELECT id FROM commands WHERE project=? AND resource=? AND state IN ('QUEUED','RUNNING') ORDER BY created DESC",
                (payload['project'], 'robot:' + payload['robot_id'])).fetchall()
        if pending:
            operation = self.operation(pending[0]['id'], actor)
            return {'state': 'FINISHING' if operation['action'] == 'finish' else 'PROCESSING',
                    'operation_id': operation['operation_id'],
                    **({'session_id': operation['result']['session_id']} if operation['result'] and operation['result'].get('session_id') else {})}
        result = self.contract.status(payload, actor)
        if result.get('session_id'):
            row = self.contract._read(result['session_id'], actor)
            if row.get('api_contract') != self.api_contract and result.get('state') == 'IDLE':
                # A v8 batch may be the last completed batch after an upgrade.
                # Keep current target status without exposing an unusable v9 SID.
                result = {key: value for key, value in result.items() if key in
                    ('state', 'robot_id', 'robot_revision', 'target_type', 'mode', 'holding', 'action_queue_empty')}
            else: self._session(result['session_id'], actor)
        return public(result)

    def close(self):
        with self.lock:
            if self.closed: return
            self.exit.set(); workers = list(self.workers.items())
        deadline = time.monotonic() + self.platform.config.get('collection_shutdown_timeout_s', 30)
        while any(worker.is_alive() for _, worker in workers) and time.monotonic() < deadline:
            for key, worker in workers:
                if worker.is_alive():
                    command = self._entry(key)
                    if command['action'] == 'start':
                        context = self._context(command)
                        target = context.get('session_id')
                        if not target:
                            inner = command['frozen']['inner_request_id']
                            with self.platform.store.lock:
                                job = self.platform.store.db.execute('SELECT id FROM jobs WHERE owner=? AND request_id=?', (command['owner'], inner)).fetchone()
                            if job is not None: target = job['id']
                        if target and hasattr(self.contract.runtime, 'cancel_start'): self.contract.runtime.cancel_start(target)
                    if worker is not threading.current_thread(): worker.join(min(.05, max(0, deadline - time.monotonic())))
        if any(worker.is_alive() for _, worker in workers): raise Fault('MINIMAL_DRAIN_PENDING', 'Owned command workers have not exited; journals remain open', 503)
        with self.lock:
            self.closed = True; self.db.close()
