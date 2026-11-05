"""Same-origin console. Credentials stay server-side; GET never starts a task."""
import argparse,asyncio,copy,hashlib,ipaddress,json,os,re,secrets,sqlite3,time
from pathlib import Path
from urllib.parse import urlencode,quote,urlsplit
from aiohttp import web,ClientSession,ClientTimeout,ClientError

class ConsoleError(Exception):
    def __init__(self,code,message,status=409,details=None):
        super().__init__(message);self.code=code;self.status=status;self.details=details

MESSAGES={'UPSTREAM_OUTCOME_UNKNOWN':'服务暂未返回操作结果，请重试原操作。',
 'TARGET_STATUS_UNAVAILABLE':'目标状态暂不可确认，请检查仿真或设备连接。',
 'COLLECTION_STATE_UNKNOWN':'采集状态尚未确认，请先停止并核查同一会话。',
 'SOURCE_STALE':'遥操输入已断开，请检查输入设备连接。',
 'STALE_GENERATION':'本条录制代数已变化，请刷新状态。',
 'STALE_EPISODE':'当前条目已变化，请刷新状态。',
 'NEXT_EPISODE_FAILED':'本条已提交，但下一条未启动。请保留会话并核查目标。',
 'STOP_UNCONFIRMED':'停止尚未得到确认，请保留现场状态并继续核查。'}
def error_public(e):
    return {'code':e.code,'message':MESSAGES.get(e.code,str(e)),
            **({'details':e.details} if e.details else {})}
def encode(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)

class Backend:
    def __init__(self,url,token_file):
        parsed=urlsplit(url)
        if parsed.scheme not in ('http','https') or parsed.username or parsed.password or parsed.query or parsed.fragment:raise ValueError('Fixed service URL required')
        self.url=url.rstrip('/');self.token=Path(token_file).read_text().strip();self.session=None
        if not self.token:raise ValueError('Empty service credential')
    async def open(self):
        if self.session is None:self.session=ClientSession(timeout=ClientTimeout(total=180),trust_env=False)
    async def request(self,method,path,body=None,raw=False):
        await self.open()
        try:
            async with self.session.request(method,self.url+path,json=body,headers={'Authorization':'Bearer '+self.token},timeout=ClientTimeout(total=180 if method=='POST' else 12)) as r:
                if raw:
                    data=await r.read()
                    if r.status!=200:raise ConsoleError('VIDEO_UNAVAILABLE','当前没有可用的实时画面。',r.status)
                    if len(data)>4*1024**2:raise ConsoleError('VIDEO_LIMIT','图像超过限制。',413)
                    return data
                value=await r.json()
                if r.status>=400 or value.get('ok') is not True:
                    e=value.get('error',{});raise ConsoleError(e.get('code','UPSTREAM_ERROR'),e.get('message','服务拒绝操作。'),r.status,e.get('details'))
                return value['result']
        except ConsoleError:raise
        except (asyncio.TimeoutError,OSError,ValueError,ClientError) as e:
            raise ConsoleError('UPSTREAM_OUTCOME_UNKNOWN' if method=='POST' else 'UPSTREAM_UNAVAILABLE','采集服务连接失败。',503) from e
    async def get(self,path):return await self.request('GET',path)
    async def post(self,path,body):return await self.request('POST',path,body)
    async def close(self):
        if self.session:await self.session.close()

class PassivePreview:
    """A deployment-pinned loopback service, never a browser-supplied URL."""
    def __init__(self,url):
        parsed=urlsplit(url)
        if parsed.scheme!='http' or parsed.hostname not in ('127.0.0.1','localhost','::1') or parsed.username or parsed.password or parsed.path not in ('','/') or parsed.query or parsed.fragment:
            raise ValueError('Fixed loopback preview URL required')
        self.url=url.rstrip('/');self.session=None
    async def request(self,path,raw=False):
        if self.session is None:self.session=ClientSession(timeout=ClientTimeout(total=3),trust_env=False)
        try:
            async with self.session.get(self.url+path,allow_redirects=False) as response:
                if response.status!=200:raise ConsoleError('VIDEO_UNAVAILABLE','当前没有新鲜的仿真画面。',409)
                if raw:
                    data=await response.read()
                    capture=int(response.headers.get('X-Captured-Unix-Ns','0'))
                    if not 0<=time.time_ns()-capture<2_000_000_000 or len(data)>4*1024**2 or not data.startswith(b'\x89PNG\r\n\x1a\n') or hashlib.sha256(data).hexdigest()!=response.headers.get('X-Frame-Sha256'):
                        raise ConsoleError('VIDEO_UNAVAILABLE','画面已过期或校验失败。',409)
                    return data
                value=await response.json()
                if value.get('ok') is not True:raise ValueError('Preview rejected')
                return value['result']
        except ConsoleError:raise
        except (asyncio.TimeoutError,OSError,ValueError,ClientError) as error:
            raise ConsoleError('VIDEO_UNAVAILABLE','仿真画面暂不可用。',409) from error
    async def close(self):
        if self.session:await self.session.close()

class Console:
    def __init__(self,config,backend):
        self.config=config;self.backend=backend;self.project=config.get('project','lab');self.robots={};self.catalog_at=0
        self.previews={key:PassivePreview(url) for key,url in config.get('trusted_previews',{}).items()}
        # 实时画面来源：机器人 → 8910 的 /collection/video 来源（real=本机真实摄像头），不用等采集会话
        self.live_video={key:source for key,source in config.get('live_video',{}).items() if source in ('real','sim')}
        self.lock=asyncio.Lock();self.version=0;self.closed=False
        p=Path(config.get('state_file','console.sqlite'));p.parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(p);self.db.row_factory=sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('CREATE TABLE IF NOT EXISTS ui_sessions(robot_key TEXT PRIMARY KEY,body TEXT); CREATE TABLE IF NOT EXISTS ui_requests(request_id TEXT PRIMARY KEY,fingerprint TEXT,client TEXT,path TEXT,payload TEXT,state TEXT,result TEXT,error TEXT);')
        self.db.execute("UPDATE ui_requests SET state='UNKNOWN' WHERE state='PENDING'");self.db.commit()
    async def directory(self,kind,robot=None):
        items=[];offset=0
        for _ in range(100):
            query={'project':self.project,'offset':offset,'limit':200}
            if robot:query['robot_id']=robot
            value=await self.backend.get('/v2/'+kind+'?'+urlencode(query));items.extend(value.get('items',[]))
            if value.get('next_offset') is None:return items
            offset=value['next_offset']
        raise ConsoleError('DIRECTORY_LIMIT','列表过大，请缩小项目范围。',413)
    async def catalog(self):
        if self.robots and time.monotonic()-self.catalog_at<10:return
        rows=await self.directory('robots');preferred=self.config.get('preferred_revisions',{});chosen={}
        for row in sorted(rows,key=lambda v:v.get('created',0)):
            if 'robot_ids' in self.config and row['id'] not in self.config['robot_ids']:continue
            if row['id'] in preferred and row['revision']!=preferred[row['id']]:continue
            chosen[row['id']]=row
        self.robots={row['id']+'@'+row['revision']:row for row in chosen.values()};self.catalog_at=time.monotonic()
    def view_robot(self,key,row):
        b=row['body'];kind=b['kind'];enabled=kind=='simulation' or self.config.get('physical_enabled') is True and b.get('commissioned') is True
        enabled=enabled and 'collection' in b.get('capabilities',[])
        # v10r15：工作台没有对齐了（对齐由操作者自己负责，平台不管）
        return {'key':key,'id':row['id'],'revision':row['revision'],'label':self.config.get('aliases',{}).get(row['id'],row['id']),
            'kind':kind,'source':b.get('provenance',kind),'cameras':list(b.get('camera_shapes',{})),
            'available':bool(enabled),'reason':None if enabled else '该目标尚未完成采集链路配置或验收。'}
    def remembered(self,key):
        r=self.db.execute('SELECT body FROM ui_sessions WHERE robot_key=?',(key,)).fetchone()
        value=json.loads(r['body']) if r else {}
        if value.get('stopped') and 'preserved_current_frames' not in value:
            for row in self.db.execute("SELECT result FROM ui_requests WHERE state='DONE' AND result IS NOT NULL ORDER BY rowid DESC"):
                result=json.loads(row['result'])
                if result.get('session_id')==value.get('session_id') and type(result.get('preserved_current_frames')) is int:
                    value['preserved_current_frames']=result['preserved_current_frames'];break
        return value
    def remember(self,key,value):
        self.db.execute('INSERT OR REPLACE INTO ui_sessions VALUES(?,?)',(key,encode(value)));self.db.commit()
    def pending(self,key):
        if self.remembered(key).get('finish_requested'):return None
        for row in self.db.execute("SELECT client,state FROM ui_requests WHERE state IN ('PENDING','UNKNOWN') ORDER BY rowid DESC"):
            value=json.loads(row['client'])
            if value['robot_key']==key:return {**value,'retryable':row['state']=='UNKNOWN','client_payload':value}
        return None
    async def state(self,key=None,mode='simulation_teleop'):
        version=self.version
        try:await self.catalog()
        except ConsoleError as e:return {'project':self.project,'robots':[],'selection':{'robot_key':key,'mode':mode},'status':None,'status_error':error_public(e),'connection':{'available':False,'reason':str(e)},'datasets':[],'media':{'cameras':{}},'pending':None}
        if mode not in ('simulation_teleop','physical_teleop'):raise ConsoleError('INVALID_MODE','请选择遥操采集模式。',400)
        expected=mode.split('_')[0]
        if key not in self.robots or self.robots[key]['body']['kind']!=expected:
            key=next((k for k,r in self.robots.items() if r['body']['kind']==expected),None)
        public=[self.view_robot(k,r) for k,r in self.robots.items()]
        if key is None:return {'project':self.project,'robots':public,'selection':{'robot_key':None,'mode':mode},'status':None,'status_error':{'code':'NO_TARGET','message':'当前没有可用的采集目标。'},'connection':{'available':True},'datasets':[],'media':{'cameras':{}},'pending':None}
        row=self.robots[key];known=self.remembered(key)
        query=urlencode({'project':self.project,'robot_id':row['id'],'robot_revision':row['revision']})
        async def read_status():
            try:return await self.backend.get('/collection/status?'+query),None
            except ConsoleError as e:return None,error_public(e)
        async def read_datasets():
            try:return await self.directory('datasets',None if self.config.get('dataset_scope')=='project' else row['id']),None
            except ConsoleError as e:return [],error_public(e)
        async def read_media():
            if key in self.live_video:
                try:return (await self.backend.get('/collection/video'))['sources'][self.live_video[key]]['cameras']
                except (ConsoleError,KeyError,TypeError):return {}
            if key in self.previews:
                try:return await self.previews[key].request('/status')
                except ConsoleError:return {}
            try:return await self.backend.get('/v2/robots/'+quote(row['id'],safe='')+'/'+quote(row['revision'],safe='')+'/media')
            except ConsoleError:return {}
        (status,status_error),(datasets,dataset_error),media=await asyncio.gather(read_status(),read_datasets(),read_media())
        if version!=self.version:
            status=None;status_error={'code':'STATE_REFRESH_REQUIRED','message':'操作结果已更新，正在重新读取状态。'}
        if status is not None and status.get('state') not in ('RUNNING','IDLE','FINISHING'):
            status=None;status_error={'code':'INVALID_STATUS','message':'采集状态尚未确认。'}
        if status and status['state']=='RUNNING' and version==self.version:
            self.remember(key,{**known,**status,'stopped':False})
        elif status and status.get('finish_state') and version==self.version:
            self.remember(key,{**known,**status,'finish_requested':True,'stopped':status['state']=='IDLE','preserved_current_frames':0})
        if version==self.version and status_error and (status_error.get('details') or {}).get('session_id'):
            self.remember(key,{**known,**status_error['details']})
        known=self.remembered(key)
        formatted=[]
        for data in datasets:
            b=data.get('body',{});m=b.get('manifest',{});binding=m.get('collection_binding',{})
            formatted.append({'dataset_id':data['id'],'version':data['revision'],'name':(m.get('task_labels') or ['采集数据'])[0],
                'episodes':m.get('episodes',0),'frames':sum(m.get('episode_lengths',[])),'upload_state':data.get('upload_state','UNKNOWN'),
                'delivery_kind':data.get('delivery_kind'),
                'upload_alert':data.get('upload_alert',False),'upload_error':data.get('upload_error'),'content_hash':b.get('content_hash'),
                'created':data.get('created',0),'episode_id':binding.get('episode_id')})
            formatted[-1].update(robot_id=b.get('robot_id'),target_type=binding.get('target_type') or m.get('target_type'),
                mode=binding.get('mode'))
        batches={}
        for data in formatted:
            prior=batches.get(data['dataset_id'])
            if prior is None or (data['episodes'],data['created'])>(prior['episodes'],prior['created']):batches[data['dataset_id']]=data
        formatted=sorted(batches.values(),key=lambda v:v['created'],reverse=True)
        return {'project':self.project,'robots':public,'selection':{'robot_key':key,'mode':mode},'status':status,
            'status_error':status_error,'dataset_error':dataset_error,'connection':{'available':True,'reason':None},
            'datasets':formatted,'media':{'cameras':media},'pending':self.pending(key),'task_name':known.get('name'),
            'known_session':{k:known[k] for k in ('session_id','episode_id','recording_generation') if k in known},
            'resume_session':{'session_id':known['session_id'],'name':known.get('name'),
                'preserved_current_frames':known.get('preserved_current_frames'),
                'episode_id':known.get('episode_id'),'episode_index':known.get('episode_index'),
                'recording_generation':known.get('recording_generation')} if known.get('stopped') and known.get('session_id') and known.get('preserved_current_frames',0)>0 else None,
            'notice':'软件仿真测试目标。示教前请手工连接已登记的遥操输入。' if row['body'].get('provenance')=='test_fixture' else '仿真遥操前请手工连接输入。' if expected=='simulation' else None}
    async def command(self,p):
        if not isinstance(p,dict):raise ConsoleError('INVALID_BODY','操作参数无效。',400)
        required={'action','request_id','robot_key','mode'};optional={'name','description','session_id','episode_id','recording_generation','workspace_confirmed'}
        if not required<=p.keys() or p.keys()-required-optional:raise ConsoleError('INVALID_BODY','操作参数不完整或包含未知字段。',400)
        if p['action'] not in ('start','redo','complete','stop','resume') or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',str(p['request_id'])):raise ConsoleError('INVALID_ACTION','无效操作。',400)
        fingerprint=hashlib.sha256(encode(p).encode()).hexdigest();old=self.db.execute('SELECT * FROM ui_requests WHERE request_id=?',(p['request_id'],)).fetchone()
        if old:
            if old['fingerprint']!=fingerprint:raise ConsoleError('IDEMPOTENCY_CONFLICT','原请求内容不能改变。')
            if old['state']=='DONE':return json.loads(old['result'])
            if old['state']=='FAILED':
                e=json.loads(old['error']);raise ConsoleError(e['code'],e['message'],e['status'],e.get('details'))
            if old['state']=='PENDING':raise ConsoleError('COMMAND_BUSY','操作正在进行，请等待结果。')
        if self.lock.locked():raise ConsoleError('COMMAND_BUSY','另一个操作尚未完成。')
        async with self.lock:
            await self.catalog();key=p['robot_key'];action=p['action']
            if key not in self.robots:raise ConsoleError('INVALID_TARGET','目标不在登记列表中。')
            row=self.robots[key];view=self.view_robot(key,row);b=row['body']
            if p['mode'] not in ('simulation_teleop','physical_teleop') or p['mode'].split('_')[0]!=b['kind']:raise ConsoleError('INVALID_MODE','采集模式与目标不匹配。')
            if not view['available']:raise ConsoleError('TARGET_UNAVAILABLE',view['reason'])
            frozen=old is not None
            if frozen:path=old['path'];payload=json.loads(old['payload'])
            else:
                pending=self.pending(key)
                if pending and action!='stop':raise ConsoleError('OUTCOME_UNKNOWN','请先重试原操作或确认其结果。')
                payload={'project':self.project,'request_id':p['request_id']}
                if action=='start':
                    if not isinstance(p.get('name'),str) or not p['name'].strip() or len(p['name'])>120:raise ConsoleError('NAME_REQUIRED','请填写采集任务名称。',400)
                    state=await self.state(key,p['mode'])
                    if not state['status'] or state['status']['state']!='IDLE':raise ConsoleError('NOT_IDLE','目标状态未确认空闲，不能开始。')
                    payload.update(robot_id=row['id'],robot_revision=row['revision'],mode=p['mode'],name=p['name'].strip(),description=p.get('description',''),
                        fps=self.config.get('fps',5),calibration_revision=b.get('calibration_revision','baseline'))
                    if b['kind']=='simulation':payload.update(scene_id=b['scene_id'],scene_revision=b['scene_revision'])
                    else:
                        if p.get('workspace_confirmed') is not True:raise ConsoleError('WORKSPACE_CONFIRMATION_REQUIRED','请确认现场工作区。')
                        payload['workspace_confirmed']=True
                    path='/collection/sessions'
                else:
                    known=self.remembered(key);sid=p.get('session_id')
                    if not sid or sid!=known.get('session_id'):raise ConsoleError('INVALID_SESSION','当前会话身份不匹配，请刷新。')
                    path='/collection/sessions/'+quote(sid,safe='')+('/finish' if action=='stop' else '/'+action)
                    if action in ('redo','complete'):
                        if not p.get('episode_id') or type(p.get('recording_generation'))!=int:raise ConsoleError('EPISODE_REQUIRED','请刷新当前条目身份。',400)
                        if p['episode_id']!=known.get('episode_id') or p['recording_generation']!=known.get('recording_generation'):raise ConsoleError('STALE_EPISODE','当前条目已变化，请刷新。')
                        state=await self.state(key,p['mode'])
                        stopped_submit=action=='complete' and known.get('stopped') and known.get('preserved_current_frames',0)>0 and state['status'] and state['status']['state']=='IDLE'
                        if not stopped_submit and (not state['status'] or state['status']['state']!='RUNNING'):raise ConsoleError('NOT_RECORDING','录制或停止状态未确认。')
                        payload['recording_generation']=p['recording_generation']
                        path='/collection/sessions/'+quote(sid,safe='')+'/episodes/'+quote(p['episode_id'],safe='')+'/'+action
                    if action=='resume':
                        if p.get('workspace_confirmed') is not True:raise ConsoleError('WORKSPACE_CONFIRMATION_REQUIRED','请确认现场后恢复。')
                        payload['workspace_confirmed']=True
                self.db.execute('INSERT INTO ui_requests VALUES(?,?,?,?,?,?,?,?)',(p['request_id'],fingerprint,encode(p),path,encode(payload),'PENDING',None,None));self.db.commit()
            self.version+=1
            self.db.execute("UPDATE ui_requests SET state='PENDING' WHERE request_id=?",(p['request_id'],));self.db.commit()
            try:
                result=await self.backend.post(path,payload)
                remembered=self.remembered(key)
                remembered.update({k:v for k,v in result.items() if k in ('session_id','episode_id','recording_generation','episode_index','current_frames','committed_episodes','preserved_current_frames')})
                if action=='start':
                    remembered['name']=p['name'];remembered.pop('finish_requested',None);remembered.pop('finish_state',None)
                if action=='stop':
                    remembered.update(finish_requested=True,finish_state=result.get('finish_state'),preserved_current_frames=0)
                remembered['stopped']=result.get('state')=='IDLE';self.remember(key,remembered)
                self.db.execute("UPDATE ui_requests SET state='DONE',result=? WHERE request_id=?",(encode(result),p['request_id']));self.db.commit();return result
            except ConsoleError as e:
                if e.details and e.details.get('session_id'):self.remember(key,{**self.remembered(key),**{k:e.details[k] for k in ('session_id','episode_id','recording_generation') if k in e.details}})
                status='UNKNOWN' if e.code in ('UPSTREAM_OUTCOME_UNKNOWN','REQUEST_IN_PROGRESS') else 'FAILED'
                self.db.execute('UPDATE ui_requests SET state=?,error=? WHERE request_id=?',(status,encode({'code':e.code,'message':str(e),'status':e.status,'details':e.details}),p['request_id']));self.db.commit();raise
            finally:self.version+=1
    async def live_png(self,key,camera):
        """实时摄像头的一帧（8910 给的是 JPEG，工作台的视频通道要 PNG，这里转一下）。"""
        data=await self.backend.request('GET','/collection/video/'+quote(camera,safe='')+'.jpg?source='+self.live_video[key],raw=True)
        def convert():
            from io import BytesIO
            from PIL import Image
            with Image.open(BytesIO(data)) as image:
                stream=BytesIO();image.convert('RGB').save(stream,format='PNG',compress_level=1);return stream.getvalue()
        return await asyncio.to_thread(convert)
    def close(self):
        if not self.closed:self.db.close();self.closed=True

def _bound_video_key(config):
    """Resolve deployment configuration only; the catalog never selects a target."""
    def valid(key):
        if (not isinstance(key,str) or not 0<len(key)<=256 or key.count('@')!=1
            or any(not part or any(ch.isspace() for ch in part) for part in key.split('@'))):
            raise ConsoleError('VIDEO_TARGET_INVALID','容器的视频目标配置无效。',503)
        return key
    if 'SO101_VIDEO_ROBOT_KEY' in os.environ:return valid(os.environ['SO101_VIDEO_ROBOT_KEY'])
    if 'video_robot_key' in config:return valid(config['video_robot_key'])
    candidates=set()
    ids=config.get('robot_ids',[]);preferred=config.get('preferred_revisions',{})
    if isinstance(ids,list) and isinstance(preferred,dict) and ids and all(isinstance(v,str) and v in preferred for v in ids):
        for robot_id in ids:candidates.add(valid(robot_id+'@'+str(preferred[robot_id])))
    previews=config.get('trusted_previews',{})
    if isinstance(previews,dict):
        for key in previews:candidates.add(valid(key))
    if not candidates:raise ConsoleError('VIDEO_TARGET_NOT_CONFIGURED','容器尚未绑定视频目标。',503)
    if len(candidates)!=1:raise ConsoleError('VIDEO_TARGET_AMBIGUOUS','容器的视频目标绑定不唯一。',503)
    return next(iter(candidates))


def _matches_listener_ip(request):
    address=request.transport.get_extra_info('sockname') if request.transport else None
    if not address:return False
    try:
        host=urlsplit('//'+request.host)
        if host.path or host.query or host.fragment or host.username or host.password:return False
        return ipaddress.ip_address(host.hostname)==ipaddress.ip_address(address[0])
    except (TypeError,ValueError):
        return False


def make_app(config,console,*,rtc_factory=None):
    sessions={};assets=Path(__file__).parent/'static';allowed=config.get('allowed_hosts',['127.0.0.1','localhost'])
    if rtc_factory is None:
        from .webrtc_ws import VideoSocketService
        rtc_factory=VideoSocketService
    async def video_auth(r,token):
        # This endpoint serves registered read-only cameras to private-network clients.
        # Collection/control POSTs retain their independent session and CSRF guard.
        return True
    async def video_binding(camera):
        return _bound_video_key(config)
    async def video_target(key,camera):
        await console.catalog();row=console.robots.get(key)
        if not row or not console.view_robot(key,row)['available'] or camera not in row['body'].get('camera_shapes',{}):
            raise ConsoleError('VIDEO_UNAVAILABLE','机器人或相机未登记为可用视频目标。',404)
        return tuple(row['body']['camera_shapes'][camera])
    async def video_png(key,camera):
        await video_target(key,camera)
        if key in console.live_video:return await console.live_png(key,camera)
        row=console.robots[key];preview=console.previews.get(key)
        if preview:
            for attempt in range(3):
                try:return await preview.request('/camera/'+quote(camera,safe='')+'.png',raw=True)
                except ConsoleError as error:
                    if error.code!='VIDEO_UNAVAILABLE' or attempt==2:raise
                    await asyncio.sleep(.02)
        path='/v2/robots/'+quote(row['id'],safe='')+'/'+quote(row['revision'],safe='')+'/media/'+quote(camera,safe='')+'.png'
        return await console.backend.request('GET',path,raw=True)
    video=rtc_factory(read_png=video_png,validate_target=video_target,auth_first=video_auth,
                      max_peers=config.get('video_max_peers',4),fps=config.get('video_fps',5),ice_servers=config.get('ice_servers',[]),
                      require_csrf=False,resolve_target=video_binding)
    @web.middleware
    async def guard(r,handler):
        try:
            if not r.remote or not ipaddress.ip_address(r.remote).is_private:raise ConsoleError('FORBIDDEN','仅允许本机或私有网络。',403)
            direct_video=r.method=='GET' and r.path=='/api/video/ws'
            if r.host.split(':')[0] not in allowed and not (direct_video and _matches_listener_ip(r)):
                raise ConsoleError('FORBIDDEN','入口主机未配置。',403)
            expected=r.scheme+'://'+r.host;origin=r.headers.get('Origin')
            if origin and origin!=expected and not direct_video:raise ConsoleError('FORBIDDEN','只允许同源页面请求。',403)
            if r.method=='POST':
                session=sessions.get(r.cookies.get('console_session'));csrf=r.headers.get('X-CSRF-Token','')
                if origin!=expected or not session or session['expires']<time.monotonic() or not secrets.compare_digest(csrf,session['csrf']):raise ConsoleError('FORBIDDEN','请刷新当前页面后操作。',403)
                if r.content_type!='application/json':raise ConsoleError('INVALID_BODY','需要JSON请求。',415)
            response=await handler(r)
        except ConsoleError as e:response=web.json_response({'ok':False,'error':error_public(e)},status=e.status)
        except (ValueError,TypeError,KeyError):response=web.json_response({'ok':False,'error':{'code':'INVALID_BODY','message':'请求参数无效。'}},status=400)
        response.headers.update({'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Referrer-Policy':'no-referrer'})
        return response
    app=web.Application(middlewares=[guard],client_max_size=70000)
    async def asset(r):
        name=r.match_info.get('name','index.html');path=(assets/name).resolve()
        if not path.is_relative_to(assets.resolve()) or not path.is_file() or path.is_symlink():raise web.HTTPNotFound()
        return web.FileResponse(path)
    async def state(r):
        result=await console.state(r.query.get('robot_key'),r.query.get('mode',config.get('default_mode','simulation_teleop')))
        now=time.monotonic()
        for key in list(sessions):
            if sessions[key]['expires']<now:sessions.pop(key)
        sid=r.cookies.get('console_session')
        if sid not in sessions:
            if len(sessions)>=64:raise ConsoleError('SESSION_LIMIT','请关闭多余页面。',429)
            sid=secrets.token_urlsafe(32);sessions[sid]={'csrf':secrets.token_urlsafe(32),'expires':now+8*3600}
        result['csrf_token']=sessions[sid]['csrf'];response=web.json_response({'ok':True,'result':result})
        response.set_cookie('console_session',sid,httponly=True,samesite='Strict',max_age=8*3600);return response
    async def command(r):return web.json_response({'ok':True,'result':await console.command(await r.json())})
    async def camera(r):
        await console.catalog();row=console.robots.get(r.match_info['key']);cam=r.match_info['camera']
        if not row or cam not in row['body'].get('camera_shapes',{}):raise ConsoleError('VIDEO_UNAVAILABLE','相机未登记。',404)
        if r.match_info['key'] in console.live_video:
            return web.Response(body=await console.live_png(r.match_info['key'],cam),content_type='image/png')
        preview=console.previews.get(r.match_info['key'])
        if preview:
            data=await preview.request('/camera/'+quote(cam,safe='')+'.png',raw=True)
            return web.Response(body=data,content_type='image/png')
        path='/v2/robots/'+quote(row['id'],safe='')+'/'+quote(row['revision'],safe='')+'/media/'+quote(cam,safe='')+'.png'
        data=await console.backend.request('GET',path,raw=True);return web.Response(body=data,content_type='image/png')
    async def offer(r):
        value=await r.json();await console.catalog();row=console.robots.get(value.get('robot_key'))
        if not row or value.get('camera') not in row['body'].get('camera_shapes',{}):raise ConsoleError('VIDEO_UNAVAILABLE','相机未登记。',404)
        if set(value)!={'robot_key','camera','type','sdp'}:raise ConsoleError('INVALID_BODY','无效视频连接参数。',400)
        path='/v2/robots/'+quote(row['id'],safe='')+'/'+quote(row['revision'],safe='')+'/media/offer'
        return web.json_response(await console.backend.post(path,{k:value[k] for k in ('camera','type','sdp')}))
    app.add_routes([web.get('/',asset),web.get('/static/{name:.*}',asset),web.get('/api/state',state),web.post('/api/command',command),
                    web.get('/api/camera/{key}/{camera}.png',camera),web.post('/api/offer',offer),
                    web.get('/api/video/ws',video.handler)])
    async def shutdown_video(app):await video.close()
    app.on_shutdown.append(shutdown_video)
    async def cleanup(app):
        # Closing the browser/UI process does not stop collection.
        close=getattr(console.backend,'close',None)
        if close:await close()
        for preview in console.previews.values():await preview.close()
        console.close()
    app.on_cleanup.append(cleanup);return app

def main():
    p=argparse.ArgumentParser();p.add_argument('--config',type=Path,required=True);a=p.parse_args();config=json.loads(a.config.read_text())
    console=Console(config,Backend(config['platform_url'],config['token_file']))
    web.run_app(make_app(config,console),host=config.get('bind','127.0.0.1'),port=config.get('port',8920),access_log=None)
if __name__=='__main__':main()
