"""minimal.v10: one container-owned collection, driven by an upstream-issued id.

Accepted commands use the durable v9 worker machinery. Device correlation,
attempt generations and end intents stay internal; public retries are not
deduplicated after an operation has completed.

v10r9 对外参数（前端只发最小参数即可）：
  start    必填 id、name；可选 description、fps、ros
           id 由上游生成后下发，作为本批次的会话 ID、数据集 ID 和同步任务 ID，只能用一次。
           v10r12 起一台容器只采一台机器人（.env 的 COLLECTION_ROBOT），目标就是部署配置里唯一绑定的那个；
           老前端还传 is_sim 的话忽略（不起作用、不报错）。
           v10r15 起没有 align：采集容器只采集，不读主臂、不控制从臂；对齐由操作者自己负责，平台不管。
           开始采集时打开供数开关（Redis collection_enabled），结束时关掉。
  redo / complete   空对象
  finish   空对象即可。数据推到配置文件里那台服务器的 <部署根目录>/action-composer/data/artifacts/data_set/<id>/，
           部署根目录来自 .env 的 COLLECTION_SYNC_DEPLOY_ROOT（如 /home/lxk/dev2）。
           可选 upload_url = 服务器上的绝对目录，带了就用它代替默认目录（null 或空字符串视为没带）。
           逐文件校验后最后写 <目录>/<id>/.ready.json（完成标记）。不通知平台；推送失败由容器自己重试，直到成功。
  abandon  放弃异常批次（POST /collection/abandon，无参数）：取消当前批次和所有未完成的命令，停掉设备会话
           ，未结束的批次标为 ABANDONED（不上传，本地数据保留），释放占用，回到可以开始新批次的初始状态。已经结束、在上传队列里的批次不受影响。
"""
import copy
import json
import logging
import threading
import time
import uuid
from pathlib import Path

from .collection_minimal import MinimalCollectionAPI, without_is_sim
from .collection_sync import SYNC_PROTOCOL, normalize_remote_dir
from .store import Fault, canonical, ident


log = logging.getLogger(__name__)
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
    log.addHandler(handler)
log.setLevel(logging.INFO)
log.propagate = False

REQUIRED = {'start': {'id', 'name'}, 'redo': set(), 'complete': set(), 'finish': set()}
OPTIONAL = {'start': {'description', 'fps', 'ros'}, 'redo': set(), 'complete': set(), 'finish': {'upload_url'}}
PUBLIC_ONLY = ('id', 'upload_url')   # 只属于 v10 对外接口的字段，不进入内部 v9 形状检查


class CurrentCollectionAPI(MinimalCollectionAPI):
    api_contract = 'minimal.v10'
    database_name = 'collection-current.sqlite'

    @staticmethod
    def _external_shape(action, body):
        if action not in REQUIRED:
            raise Fault('INVALID_ACTION', 'Unsupported current collection action', 400)
        if not isinstance(body, dict) or set(body) - REQUIRED[action] - OPTIONAL[action]:
            raise Fault('INVALID_FIELDS', 'Unexpected frontend collection parameters', 400)
        missing = REQUIRED[action] - set(body)
        if missing:
            raise Fault('INVALID_FIELDS', 'Required fields: ' + ', '.join(sorted(missing)), 400)
        if action == 'start':
            try:
                ident(body['id'])
            except Fault:
                raise Fault('INVALID_ID', 'id must be 1-128 ASCII letters, digits, "_", "." or "-", starting with a letter or digit', 400) from None
            if not isinstance(body['name'], str) or not body['name'].strip():
                raise Fault('INVALID_NAME', 'Nonempty task name required', 400)
        if 'description' in body and not isinstance(body['description'], str):
            raise Fault('INVALID_FIELDS', 'description must be text', 400)
        if action == 'finish' and body.get('upload_url') not in (None, ''):
            try:
                normalize_remote_dir(body['upload_url'])
            except ValueError as error:
                raise Fault('INVALID_UPLOAD_URL', str(error), 400) from None

    @staticmethod
    def _shape(action, body):
        # This is the internally generated v9-shaped command, not user input.
        MinimalCollectionAPI._shape(action, {key: value for key, value in body.items() if key not in PUBLIC_ONLY})

    def _rows(self):
        with self.contract.lock:
            values = [json.loads(row['body']) for row in self.contract.db.execute('SELECT body FROM sessions').fetchall()]
        result = []
        for row in values:
            if row.get('finish', {}).get('state') in ('EMPTY', 'QUEUED_UPLOAD', 'ABANDONED'): continue
            if row.get('api_contract') == self.api_contract:
                # A stopped v10 batch remains current until explicitly finished.
                result.append(row)
            elif row['internal_state'] not in ('STOPPED', 'START_FAILED') or row.get('finish'):
                result.append(row)
            # A stopped legacy batch is historical, even if its preserved
            # files remain. It is never adopted, deleted or resumed by v10.
        return result

    def _current(self, actor):
        rows = self._rows()
        if not rows: raise Fault('NO_CURRENT_COLLECTION', 'There is no current collection', 409)
        if len(rows) != 1: raise Fault('COLLECTION_STATE_AMBIGUOUS', 'Multiple unfinished collections require operator reconciliation', 409)
        row = rows[0]
        actor.require(row['project'], 'operator')
        if row['operator'] != actor.name: raise Fault('FORBIDDEN', 'The current collection belongs to another operator', 403)
        return self._session(row['session_id'], actor)

    def submit(self, action, body, actor):
        body = without_is_sim(body)
        self._external_shape(action, body)
        body = copy.deepcopy(body)
        with self.platform.collection_admission_lock:
            self._assert_shared_admission()
            with self.lock:
                if self.closed or self.exit.is_set(): raise Fault('SHUTTING_DOWN', 'Collection API is closing', 503)
                pending = [dict(row) for row in self.db.execute("SELECT * FROM commands WHERE state IN ('QUEUED','RUNNING') ORDER BY created").fetchall()]
            internal = {**body, 'request_id': 'current-' + uuid.uuid4().hex}
            if action == 'start':
                if pending or self._rows() or self.platform.store.claims():
                    raise Fault('BUSY', 'End the current collection before starting another')
                if self.contract.id_in_use(body['id']):
                    raise Fault('ID_ALREADY_USED', 'This id has already been used by an earlier collection', 409)
            elif action == 'finish':
                if any(row['action'] == 'finish' for row in pending): raise Fault('BUSY', 'The current collection is already finishing')
                starts = [row for row in pending if row['action'] == 'start']
                if starts:
                    if len(starts) != 1: raise Fault('COLLECTION_STATE_AMBIGUOUS', 'Multiple starts require reconciliation')
                    self._entry(starts[0]['id'], actor)
                    internal['operation_id'] = starts[0]['id']
                else:
                    internal['session_id'] = self._current(actor)['session_id']
            else:
                if pending: raise Fault('BUSY', 'A collection operation is already in flight')
                row = self._current(actor)
                if row['internal_state'] != 'RUNNING' or row.get('finish'):
                    raise Fault('NOT_RECORDING', 'The current collection is not recording')
                internal.update(session_id=row['session_id'], episode_id=row['episode_id'])
            super().submit(action, internal, actor)
            return {'accepted': True, 'message': '已接收，后台执行中'}

    def _prepare_frozen(self, action, body, frozen, actor):
        if action == 'start':
            frozen['session_key'] = body['id']
        elif action == 'finish':
            # 本批次的去向在结束时冻结：同步服务器来自配置文件；请求带了 upload_url 就冻结它，
            # 没带就记 None，推送时才按配置拼 <部署根目录>/action-composer/data/artifacts/data_set。
            upload_url = body.get('upload_url')
            frozen['destination'] = {'protocol': SYNC_PROTOCOL,
                                     'upload_url': normalize_remote_dir(upload_url) if upload_url else None}
        return frozen

    def _contract_start(self, payload, inner, actor, *, frozen=None):
        return self.contract.current_start(payload, inner, actor, session_key=(frozen or {}).get('session_key'))

    def _contract_episode(self, action, frozen, payload, inner, actor):
        method = self.contract.current_redo if action == 'redo' else self.contract.current_complete
        return method(frozen['session_id'], frozen['episode_id'], payload, inner, actor)

    def _contract_finish(self, sid, frozen, actor):
        return self.contract.current_finish(sid, {'project': frozen['payload']['project']},
            frozen['inner_request_id'], actor, destination=frozen['destination'])

    def _update(self, key, state, *, result=None, error=None):
        super()._update(key, state, result=result, error=error)
        log.info('collection current operation=%s state=%s error_code=%s', key, state,
            error.get('code') if isinstance(error, dict) else None)

    # ══════════ 放弃异常批次 ══════════

    def abandon(self, body, actor):
        """POST /collection/abandon：立即回复 202，后台把采集状态全部初始化（见模块说明）。没有异常时调用也无害。"""
        if not isinstance(body, dict) or body:
            raise Fault('INVALID_FIELDS', 'abandon takes no parameters', 400)
        actor.require(self._project(actor), 'operator')
        with self.platform.collection_admission_lock:
            if getattr(self.platform, 'collection_abandoning', False):
                return {'accepted': True, 'message': '已接收，后台执行中'}   # 上一次放弃还在进行
            self.platform.collection_abandoning = True
            try:
                cancelled = self._cancel_all_commands()
            except BaseException:
                self.platform.collection_abandoning = False
                raise
        threading.Thread(target=self._abandon_work, args=(cancelled,), name='CollectionAbandon', daemon=True).start()
        return {'accepted': True, 'message': '已接收，后台执行中'}

    def _cancel_all_commands(self):
        """新版和旧版接口里所有排队/执行中的命令都标为 INTERRUPTED（ABANDONED），它们的工作线程不再改写状态。"""
        error = canonical({'code': 'ABANDONED', 'message': 'Cancelled by abandon (放弃异常批次)'})
        cancelled = []
        for api in list(getattr(self.platform, 'collection_command_apis', [])):
            if api.closed:
                continue
            with api.lock:
                rows = api.db.execute("SELECT id, action FROM commands WHERE state IN ('QUEUED','RUNNING')").fetchall()
                for row in rows:
                    api.abandoned.add(row['id'])
                    api.db.execute("UPDATE commands SET state='INTERRUPTED',error=?,updated=? WHERE id=?",
                                   (error, time.time(), row['id']))
                    cancelled.append({'api': api.api_contract, 'operation_id': row['id'], 'action': row['action']})
        return cancelled

    def _abandon_work(self, cancelled):
        report = {'started_at': time.time(), 'cancelled_commands': cancelled, 'stopped_sessions': [],
                  'forced_sessions': [], 'abandoned_batches': [], 'released_claims': [], 'errors': []}
        runtime = self.contract.runtime
        live = getattr(runtime, 'live', {})
        cancel = getattr(runtime, 'cancel_start', None)
        try:
            # 1. 叫停所有正在开始/对齐/录制的设备会话
            keys = set(live) | {row['session_id'] for row in self._rows()}
            if callable(cancel):
                for key in keys:
                    cancel(key)
            # 2. 停会话：录制会话 suspend（从臂原地保持、关闭设备连接），对齐会话 stop；启动中的会话等它取消完
            failures = {}
            deadline = time.monotonic() + 30
            while live and time.monotonic() < deadline:
                for key in list(live):
                    value = live.get(key)
                    if value is None:
                        continue
                    try:
                        if value.get('verification') and not value.get('starting'):
                            runtime.command(key, 'stop', {})
                        else:
                            runtime.suspend_capture(key)
                        report['stopped_sessions'].append(key)
                    except Exception as error:
                        failures[key] = str(error)
                if live:
                    time.sleep(.5)
            for key in list(live):                 # 30 秒还停不下来：直接让从臂停住并关闭连接
                value = live.pop(key, None) or {}
                robot = value.get('robot')
                for step in ('stop', 'close'):
                    try:
                        if robot is not None:
                            getattr(robot, step)(*(['abandon'] if step == 'stop' else []))
                    except Exception as error:
                        report['errors'].append(f'{key} {step}: {error}')
                report['forced_sessions'].append({'session_id': key, 'last_error': failures.get(key)})
            # 3. 等被取消的命令线程退出（它们会自己清理，不会再改写命令状态）
            deadline = time.monotonic() + 30
            for api in list(getattr(self.platform, 'collection_command_apis', [])):
                for key, worker in list(api.workers.items()):
                    if key in api.abandoned and worker.is_alive():
                        worker.join(max(0.0, deadline - time.monotonic()))
            # 4. 未结束的批次标为 ABANDONED：不再上传，不再是“当前批次”，本地数据保留
            forced = {item['session_id'] for item in report['forced_sessions']}
            for row in self._rows():
                key = row['session_id']
                with self.contract._serial(key):
                    with self.contract.lock:
                        record = self.contract.db.execute('SELECT body FROM sessions WHERE id=?', (key,)).fetchone()
                    latest = json.loads(record['body'])
                    if latest.get('finish', {}).get('state') in ('EMPTY', 'QUEUED_UPLOAD', 'ABANDONED'):
                        continue
                    latest['finish'] = {**latest.get('finish', {}), 'state': 'ABANDONED', 'abandoned_at': time.time()}
                    if latest['internal_state'] not in ('STOPPED', 'START_FAILED'):
                        latest.update(internal_state='STOPPED', holding_confirmed=key not in forced, stop_result={
                            'state': 'IDLE', 'session_id': key, 'dataset_id': key, 'holding': key not in forced,
                            'action_queue_empty': True, 'committed_episodes': len(latest.get('collection_episodes', [])),
                            'abandoned': True})
                    self.contract._write(latest)
                    report['abandoned_batches'].append(key)
            for key in report['abandoned_batches']:          # 结束流程看到 ABANDONED 会自己退出
                worker = self.contract.finish_workers.get(key)
                if worker is not None and worker.is_alive():
                    worker.join(10)
            # 5. 还没结束的设备作业标为中断；释放所有占用；清掉所有对齐凭据
            with self.platform.store.lock:
                self.platform.store.db.execute(
                    "UPDATE jobs SET state='INTERRUPTED',error=?,updated=? WHERE state IN ('QUEUED','RUNNING','CANCELLING') "
                    "AND (kind LIKE 'collections.%' OR kind LIKE 'verifications.%')",
                    (canonical({'code': 'ABANDONED', 'message': 'Cancelled by abandon (放弃异常批次)'}), time.time()))
            for claim in self.platform.store.claims():
                self.platform.store.release(claim['resource'], claim['job_id'], holding=True)
                report['released_claims'].append(claim['resource'])
        except Exception as error:
            report['errors'].append(f'{type(error).__name__}: {error}')
            log.exception('collection abandon failed')
        finally:
            report['finished_at'] = time.time()
            try:
                path = Path(self.platform.root) / 'collection-abandon-last.json'
                path.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding='utf-8')
            except Exception:
                pass
            log.info('collection abandon: cancelled=%d stopped=%s forced=%s batches=%s claims=%s errors=%s',
                     len(report['cancelled_commands']), report['stopped_sessions'], report['forced_sessions'],
                     report['abandoned_batches'], report['released_claims'], report['errors'])
            self.platform.collection_abandoning = False
