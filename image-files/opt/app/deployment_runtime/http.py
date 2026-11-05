"""Authenticated front-end contract for the two delivery services."""
import asyncio,hashlib,hmac,json,os,time,logging
from pathlib import Path
from aiohttp import web
from .store import Principal,Fault
from .field_bindings import resource_fields

def make_app(platform,accounts):
    from .collection_access import CollectionAccess
    access=CollectionAccess(platform,accounts)
    if platform.role=='collection':
        platform.get_collection()  # Restart resumes uploads without a browser request.
        platform.get_minimal_collection()  # Reconcile accepted v9 commands without replaying motion.
        platform.get_current_collection()  # Restore only the accepted v10 end/send intent.
    @web.middleware
    async def auth(request,handler):
        try:
            if request.path=='/healthz':return web.json_response({'role':platform.role,'api':'v2','ready':True,'hardware_accepted':False})
            if access.methods(request.path):request['principal']=access.principal
            else:
                header=request.headers.get('Authorization','');token=header[7:] if header.startswith('Bearer ') else ''
                digest=hashlib.sha256(token.encode()).hexdigest();account=next((v for v in accounts if token and hmac.compare_digest(v['token_sha256'],digest)),None)
                if account is None:raise Fault('UNAUTHORIZED','Valid platform credential required',401)
                request['principal']=Principal(account['name'],tuple(account['projects']),tuple(account['roles']))
            response=await handler(request)
            if not response.prepared:response.headers['Cache-Control']='no-store'   # WebSocket 已经握手，不能再改头
            return response
        except Fault as e:return web.json_response({'ok':False,'error':{'code':e.code,'message':str(e),**({'details':e.details} if getattr(e,'details',None) and not request.get('collection_current') else {})}},status=e.status)
        except (ValueError,TypeError,KeyError) as e:return web.json_response({'ok':False,'error':{'code':'INVALID_REQUEST','message':str(e)}},status=400)
    @web.middleware
    async def collection_cors(request,handler):
        methods=access.methods(request.path)
        if not methods:return await handler(request)
        if request.method=='OPTIONS':
            method=request.headers.get('Access-Control-Request-Method','').upper()
            headers={v.strip().lower() for v in request.headers.get('Access-Control-Request-Headers','').split(',') if v.strip()}
            if method and method not in methods:
                response=web.json_response({'ok':False,'error':{'code':'METHOD_NOT_ALLOWED','message':'Unsupported collection method'}},status=405)
            elif headers-{'content-type','authorization','accept'}:
                response=web.json_response({'ok':False,'error':{'code':'CORS_HEADERS_NOT_ALLOWED','message':'Unsupported collection request headers'}},status=400)
            else:response=web.Response(status=204)
        else:
            try:response=await handler(request)
            except web.HTTPException as error:
                response=web.json_response({'ok':False,'error':{'code':'HTTP_'+str(error.status),'message':error.reason}},status=error.status)
            except Exception:
                logging.getLogger(__name__).exception('Collection REST request failed')
                response=web.json_response({'ok':False,'error':{'code':'INTERNAL_ERROR','message':'Collection service error; see service log'}},status=500)
        if response.prepared:return response   # WebSocket 推流：握手时已回复，跨域头对 WebSocket 不适用
        response.headers.update({'Access-Control-Allow-Origin':'*',
            'Access-Control-Allow-Methods':', '.join((*methods,'OPTIONS')),
            'Access-Control-Allow-Headers':'Content-Type, Authorization, Accept','Cache-Control':'no-store'})
        if request.method=='OPTIONS' and request.headers.get('Access-Control-Request-Private-Network')=='true':
            response.headers['Access-Control-Allow-Private-Network']='true'
        return response
    app=web.Application(middlewares=[collection_cors,auth],client_max_size=5*1024**2)
    def reply(v,status=200):return web.json_response({'ok':True,'result':v},status=status,dumps=lambda v:json.dumps(v,default=str,allow_nan=False))
    async def body(request,*,allow_empty=False):
        raw=await request.text()
        if allow_empty and not raw.strip():return {}
        v=json.loads(raw,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite JSON')))
        if not isinstance(v,dict):raise ValueError('JSON object required')
        return v
    async def capabilities(r):return reply(platform.capabilities(r['principal']))
    async def identity(r):
        a=r['principal'];return reply({'principal':a.name,'projects':list(a.projects),'roles':list(a.roles)})
    async def schema(r):
        from .api_contract import openapi
        return web.json_response(access.openapi(openapi()))
    async def directory(r):
        kind=r.match_info['kind'];a=r['principal']
        if kind not in ('robots','tasks','agents','datasets','models','sessions','calibrations'):raise Fault('NOT_FOUND','Unknown directory',404)
        if r.method=='POST':return reply(resource_fields(await asyncio.to_thread(platform.register,kind,await body(r),a)),201)
        result=platform.store.list(kind,a,project=r.query['project'],offset=int(r.query.get('offset',0)),limit=int(r.query.get('limit',50)),robot_id=r.query.get('robot_id'))
        values=[resource_fields(v) for v in result['items']]
        if kind=='datasets' and platform.role=='collection':values=[platform.get_collection().dataset_fields(v) for v in values]
        return reply({**result,'items':values})
    async def version(r):
        value=resource_fields(platform.store.get(r.match_info['kind'],r.match_info['id'],r.match_info['revision'],r['principal']))
        if r.match_info['kind']=='datasets' and platform.role=='collection':value=platform.get_collection().dataset_fields(value)
        return reply(value)
    async def robot_status(r):return reply(await asyncio.to_thread(platform.probe,r.match_info['id'],r.match_info['revision'],r['principal']))
    async def command(r):
        p=await body(r)
        if set(p)!={'request_id','operation','payload'}:raise ValueError('request_id/operation/payload required')
        if p['operation'] in ('collections.start','verifications.start'):
            return reply(await asyncio.to_thread(platform.run_legacy_collection,platform.submit,
                p['operation'],p['payload'],p['request_id'],r['principal']),202)
        return reply(await asyncio.to_thread(platform.submit,p['operation'],p['payload'],p['request_id'],r['principal']),202)
    async def operation(r):
        key=r.match_info['id'];a=r['principal'];part=r.match_info.get('part')
        if part=='cancel':return reply(await asyncio.to_thread(platform.cancel,key,a),202)
        if part=='events':return reply(platform.store.events(key,a,int(r.query.get('after',0))))
        if part=='metrics':return reply(platform.metrics(key,a,int(r.query.get('after_step',0))))
        return reply(platform.operation(key,a))
    async def operation_log(r):
        return reply(await asyncio.to_thread(platform.operation_log,r.match_info['id'],r['principal'],conversion=r.match_info['log_type']=='conversion-log'))
    async def session(r):return reply(await asyncio.to_thread(platform.session,r.match_info['id'],r['principal']))
    async def collection_status(r):
        query=dict(r.query)
        if 'project' in query:service=platform.get_collection()
        else:service=platform.get_current_collection()
        try:result=await asyncio.to_thread(service.status,query,r['principal'])
        except Fault as error:
            # A v9 caller can still query its existing batch; ownership and
            # all other failures must be returned, never hidden by fallback.
            if 'project' in query or error.code!='API_VERSION_MISMATCH':raise
            result=await asyncio.to_thread(platform.get_minimal_collection().status,query,r['principal'])
        return reply(result)
    async def minimal_action(r):
        action=r.match_info['action'];p=await body(r,allow_empty=action!='start')
        if 'request_id' in p:service=platform.get_minimal_collection()
        else:
            r['collection_current']=True;service=platform.get_current_collection()
        return reply(await asyncio.to_thread(service.submit,action,p,r['principal']),202)
    async def collection_abandon(r):
        p=await body(r,allow_empty=True)   # 放弃异常批次：无参数
        r['collection_current']=True
        return reply(await asyncio.to_thread(platform.get_current_collection().abandon,p,r['principal']),202)
    async def minimal_operation(r):
        return reply(await asyncio.to_thread(platform.get_minimal_collection().operation,r.match_info['id'],r['principal']))
    async def collection_action(r):
        # v10r15：没有 /collection/align 了，对齐由操作者自己负责，平台不管
        p=await body(r)
        if 'request_id' not in p:raise Fault('INVALID_FIELDS','request_id required',400)
        request_id=p.pop('request_id');service=platform.get_collection();a=r['principal']
        if r.path=='/collection/sessions':
            return reply(await asyncio.to_thread(platform.run_legacy_collection,service.start,p,request_id,a),201)
        action=r.match_info['action'];key=r.match_info['id']
        # v8 mutations cannot bypass the v9 recording-attempt identity protocol.
        # Safe stop/finish remain available to the owner during recovery.
        version=service._read(key,a).get('api_contract')
        if (action in ('redo','complete','resume') and version=='minimal.v9') or (version=='minimal.v10' and action!='stop'):
            raise Fault('API_VERSION_MISMATCH','Use the minimal collection API for this session',409)
        if action in ('redo','complete'):
            result=await asyncio.to_thread(getattr(service,action),key,r.match_info['episode_id'],p,request_id,a)
        else:result=await asyncio.to_thread(getattr(service,action),key,p,request_id,a)
        return reply(result)
    async def stream(r):
        key=r.match_info['id'];a=r['principal'];platform.store.job(key,a)
        cursor=int(r.headers.get('Last-Event-ID',r.query.get('after','0')))
        if cursor<0:raise ValueError('Nonnegative event cursor required')
        response=web.StreamResponse(headers={'Content-Type':'text/event-stream','Cache-Control':'no-store','X-Accel-Buffering':'no'})
        await response.prepare(r);end=time.monotonic()+30
        try:
            while time.monotonic()<end:
                events=platform.store.events(key,a,cursor)
                for event in events:
                    cursor=event['sequence'];data=json.dumps(event,ensure_ascii=False,allow_nan=False)
                    await response.write(f'id: {cursor}\nevent: operation\ndata: {data}\n\n'.encode())
                if not events:await response.write(b': keepalive\n\n')
                await asyncio.sleep(.5)
        except (ConnectionResetError,asyncio.CancelledError):pass
        return response
    async def begin_transfer(r):
        p=await body(r);r['principal'].require(p['project'],'operator' if platform.role=='collection' else 'trainer')
        return reply(await asyncio.to_thread(platform.datasets.begin,p['manifest'],r['principal'],p['project']),201)
    async def transfer(r):
        key=r.match_info['id'];part=r.match_info.get('part');a=r['principal']
        if part=='finish':return reply(await asyncio.to_thread(platform.datasets.finish,key,a))
        if part=='files':return reply(await asyncio.to_thread(platform.datasets.write,key,r.query['path'],int(r.query['offset']),await r.read(),a))
        return reply(platform.datasets.status(key,a))
    async def import_data(r):
        p=await body(r);a=r['principal'];a.require(p['project'],'operator' if platform.role=='collection' else 'trainer')
        roots=platform.config.get('import_roots',[]);source=platform.config.get('import_sources',{}).get(p['source_id'])
        if not source:raise Fault('NOT_FOUND','Source ID is not registered',404)
        return reply(await asyncio.to_thread(platform.datasets.register_local,source,p['content_hash'],a,p['project'],roots))
    async def file(r):
        path=await asyncio.to_thread(platform.datasets.file,r.match_info['id'],r.match_info['revision'],r.match_info['name'],r['principal'])
        return web.FileResponse(path,headers={'ETag':'"'+__import__('deployment_runtime.datasets',fromlist=['file_hash']).file_hash(path)+'"'})
    async def job_file(r):
        platform.store.job(r.match_info['id'],r['principal']);root=(platform.root/'jobs'/r.match_info['id']).resolve();name=r.match_info['name'];path=(root/name).resolve()
        if not path.is_relative_to(root) or path.is_symlink() or not path.is_file():raise Fault('NOT_FOUND','Job artifact not found',404)
        return web.FileResponse(path)
    app.router.add_get('/v2/capabilities',capabilities)
    app.router.add_get('/collection/status',collection_status)
    app.router.add_post('/collection/{action:start|redo|complete|finish}',minimal_action)
    app.router.add_get('/collection/operations/{id}',minimal_operation)
    app.router.add_post('/collection/abandon',collection_abandon)
    app.router.add_post('/collection/sessions',collection_action)
    app.router.add_post('/collection/sessions/{id}/episodes/{episode_id}/{action:redo|complete}',collection_action)
    app.router.add_post('/collection/sessions/{id}/{action:stop|resume|finish}',collection_action)
    app.router.add_get('/v2/identity',identity)
    app.router.add_get('/openapi.json',schema)
    app.router.add_post('/v2/commands',command)
    app.router.add_get('/v2/operations/{id}',operation)
    app.router.add_get('/v2/operations/{id}/{log_type:logs|conversion-log}',operation_log)
    app.router.add_get('/v2/operations/{id}/stream',stream)
    for part in ('events','metrics'):app.router.add_get('/v2/operations/{id}/{part:'+part+'}',operation)
    app.router.add_post('/v2/operations/{id}/{part:cancel}',operation)
    app.router.add_get('/v2/operations/{id}/files/{name:.*}',job_file)
    app.router.add_get('/v2/sessions/{id}/status',session)
    app.router.add_get('/v2/robots/{id}/{revision}/status',robot_status)
    app.router.add_post('/v2/transfers',begin_transfer)
    app.router.add_get('/v2/transfers/{id}',transfer)
    app.router.add_put('/v2/transfers/{id}/{part:files}',transfer)
    app.router.add_post('/v2/transfers/{id}/{part:finish}',transfer)
    app.router.add_post('/v2/datasets/import',import_data)
    app.router.add_get('/v2/datasets/{id}/{revision}/files/{name:.*}',file)
    from .media import add_routes
    add_routes(app,platform)
    if platform.role=='collection':
        from .video_routes import add_routes as add_video_routes
        add_video_routes(app)   # /collection/video、/collection/video/<camera>.jpg、/collection/video/ws
    app.router.add_get('/v2/{kind}',directory);app.router.add_post('/v2/{kind}',directory)
    app.router.add_get('/v2/{kind}/{id}/{revision}',version)
    async def health(r):return web.json_response({'ready':True})
    app.router.add_get('/healthz',health)
    async def close(app):await asyncio.to_thread(platform.close)
    app.on_cleanup.append(close);return app

def main():
    import argparse
    from .service import Platform
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--accounts',required=True);a=p.parse_args()
    cfg=json.loads(Path(a.config).read_text());accounts=json.loads(Path(a.accounts).read_text());platform=Platform(cfg)
    if cfg.get('bootstrap'):
        admin=Principal('bootstrap',tuple({p['project'] for p in cfg['bootstrap']}),('admin','operator','trainer'))
        for value in cfg['bootstrap']:platform.register(value['kind'],value,admin)
    web.run_app(make_app(platform,accounts),host=cfg.get('bind','0.0.0.0'),port=cfg.get('port',8080),access_log=None)
if __name__=='__main__':main()
