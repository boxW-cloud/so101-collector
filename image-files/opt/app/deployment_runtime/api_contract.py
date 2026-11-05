"""Shared request shapes and OpenAPI; execution and authority remain in services."""
from copy import deepcopy
import math
import re
from .store import Fault

CONTRACT_VERSION = 'so101.platform.v2.1'
TARGET = 'project robot_id robot_revision'
CONFIG_FIELDS = 'config_revision scene_id scene_revision ros'
FIELDS = {
 'collections.start': (TARGET+' mode name calibration_revision', 'description fps task_id task_revision workspace_confirmed '+CONFIG_FIELDS),
 'verifications.start': (TARGET+' mode calibration_revision', 'name fps duration_s workspace_confirmed '+CONFIG_FIELDS),
 'collections.save': ('project session_id episode_id', ''),
 'collections.redo': ('project session_id episode_id confirm', ''),
 'collections.next': ('project session_id', ''),
 'collections.stop': ('project session_id', 'save_current sink_id'),
 'verifications.stop': ('project session_id', ''),
 'scene.reset': (TARGET+' scene_id scene_revision', 'config_revision ros'),
 'robots.reconcile': (TARGET, 'workspace_confirmed'),
 'calibration.start': (TARGET+' role', 'config_revision'),
 'calibration.sample': ('project parent_operation_id calibration_id point', 'motor_id'),
 'calibration.finish': ('project parent_operation_id calibration_id', ''),
 'calibration.cancel': ('project parent_operation_id calibration_id', ''),
 'datasets.convert': ('project dataset_id version content_hash converter_version target_format', 'output_owner output_project'),
 'datasets.upload': ('project dataset_id version content_hash sink_id', ''),
 'datasets.grant': ('project dataset_id version subject', ''),
 'training.start': ('project image_id agent_id agent_revision datasets', 'parameters resources base_model resume_operation_id'),
 'inference.start': (TARGET+' scene_id scene_revision model task', 'parameters device config_revision ros'),
}

def _ref(name): return {'$ref': '#/components/schemas/'+name}
def _object(properties, required=(), *, additional=False, **extra):
    return {'type':'object','properties':properties,'required':list(required),'additionalProperties':additional,**extra}
def _array(items, **extra): return {'type':'array','items':items,**extra}
def _nullable(schema): return {'anyOf':[schema,{'type':'null'}]}

TEXT = {'type':'string','minLength':1,'pattern':r'\S'}
SCENE_REFERENCE = {**TEXT,'maxLength':128}
IDENTIFIER = {'type':'string','pattern':r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'}
HASH = {'type':'string','pattern':r'^sha256:[a-f0-9]{64}$'}
BOOL = {'type':'boolean'}
COUNT = {'type':'integer','minimum':0}
POSITIVE = {'type':'number','exclusiveMinimum':0}
JSON_OBJECT = {'type':'object','additionalProperties':True}
MODES = ['physical_teleop','physical_task','simulation_teleop','simulation_task']

def _schemas():
    schemas = {
        'DatasetRef':_object({'dataset_id':IDENTIFIER,'version':IDENTIFIER,'content_hash':HASH},('dataset_id','version','content_hash')),
        'ModelRef':_object({'model_id':IDENTIFIER,'version':IDENTIFIER,'content_hash':HASH},('model_id','version','content_hash')),
        'ResourceRequest':_object({'cpu_threads':{'type':'integer','minimum':1},
            'memory_mb':{'type':'integer','minimum':128},'gpu':BOOL},
            description='Requested ceilings; acceptance does not imply per-job memory or GPU isolation.'),
        'TrainingParameters':_object({
            'steps':{'type':'integer','minimum':1,'maximum':1000000},
            'batch_size':{'type':'integer','minimum':1,'maximum':32},
            'seed':{'type':'integer','minimum':0,'maximum':2147483647},
            'save_freq':{'type':'integer','minimum':1,'maximum':1000000},
            'policy_params':{**JSON_OBJECT,'description':'Finite native parameters validated by the pinned Seeed adapter.'},
            'scheduler_params':_nullable(JSON_OBJECT)}),
        'InferenceParameters':_object({
            'mode':{'type':'string','enum':['step','chunk']},'fps':_nullable(POSITIVE),
            'max_steps':{'type':'integer','minimum':1},'max_duration_s':POSITIVE,
            'max_observation_age_s':POSITIVE,'inference_timeout_s':POSITIVE,
            'queue_capacity':{'type':'integer','minimum':1},'low_watermark':COUNT,
            'stop_timeout_s':POSITIVE,'metrics_capacity':{'type':'integer','minimum':1}}),
        'RosQos':_object({
            'reliability':{'type':'string','enum':['reliable','best_effort']},
            'durability':{'type':'string','enum':['volatile','transient_local']},
            'history':{'type':'string','enum':['keep_last','keep_all']},
            'depth':{'type':'integer','minimum':1}},('reliability','durability','history','depth')),
        'RosTopic':_object({'name':TEXT,'type':TEXT,'qos':_ref('RosQos')},('name','type','qos')),
    }
    schemas['RosConfig'] = _object({
        'domain_id':{'type':'integer','minimum':0,'maximum':232},
        'namespace':{'type':'string','pattern':r'^/[a-z][a-z0-9_/]*$'},
        'rmw_implementation':TEXT,'contract_version':TEXT,'clock_domain':TEXT,
        'state_topic_units':TEXT,'dds_config_uri':TEXT,'dds_config_sha256':HASH,
        **{key:_ref('RosTopic') for key in ('command_topic','result_topic','state_topic','observation_topic','clock_topic')},
        'camera_topics':{'type':'object','propertyNames':{'pattern':r'^[a-z][a-z0-9_]*$'},
                         'additionalProperties':_ref('RosTopic')}},
        description='Sanitized public ROS fields. Requests may match a subset; Topics are complete name/type/QoS references and cannot override configuration or supply credentials.')
    schemas['TimestampStatus'] = _object({
        'clock_domain':_nullable(TEXT),'clock_domain_verified':_nullable(BOOL),
        'last_state_captured_monotonic_ns':_nullable(COUNT),
        'last_camera_captured_monotonic_ns':{'type':'object','additionalProperties':_nullable(COUNT)},
        'age_ms':_nullable({'type':'number'}),
        'camera_age_ms':{'type':'object','additionalProperties':_nullable({'type':'number'})},
        'sample_within_configured_age':_nullable(BOOL),'monotonicity_checked':{'type':'boolean','const':False}},
        ('monotonicity_checked',),description='Snapshot diagnostics only; source timestamp monotonicity is not verified.')
    schemas['RosStatus'] = _object({
        'connection_state':{'type':'string','enum':['CONNECTED','CLOSED','UNVERIFIED']},
        'checked_at_monotonic_ns':_nullable(COUNT),
        'nodes':_array(_object({'name':TEXT,'namespace':TEXT},('name','namespace'))),
        'topics':_array(_object({'name':TEXT,'type':TEXT,'qos':_ref('RosQos'),'direction':TEXT,'endpoint_count':COUNT},additional=True)),
        'topics_verified':COUNT,'configuration_verified':BOOL,'timestamp_status':_ref('TimestampStatus')},
        ('connection_state','timestamp_status'))
    schemas['Error'] = _object({'code':TEXT,'message':{'type':'string'},'log_id':TEXT,'log_ref':TEXT,'details':JSON_OBJECT},
                               ('code','message'),additional=True)
    schemas['ErrorResponse'] = _object({'ok':{'type':'boolean','const':False},'error':_ref('Error')},('ok','error'))
    binding = {'robot_id':IDENTIFIER,'robot_revision':IDENTIFIER,'config_revision':IDENTIFIER,
               'calibration_revision':_nullable(IDENTIFIER),
               'scene_id':_nullable(SCENE_REFERENCE),'scene_revision':_nullable(SCENE_REFERENCE),
               'ros_config':_ref('RosConfig'),'ros_status':_ref('RosStatus')}
    schemas['SessionStatus'] = _object({
        **binding,'session_id':IDENTIFIER,'dataset_id':_nullable(IDENTIFIER),'state':TEXT,
        'mode':{'type':'string','enum':MODES},'source':TEXT,'episode_id':_nullable(IDENTIFIER),
        'committed_episodes':COUNT,'current_frames':COUNT,'task_id':_nullable(IDENTIFIER),
        'error':{'anyOf':[{'type':'string'},_ref('Error'),{'type':'null'}]},
        'task_sequence_exhausted':BOOL,'task_success_verified':BOOL},
        ('session_id','dataset_id','state','calibration_revision'),additional=True)
    schemas['CaptureStopResult'] = _object({
        **binding,'session_id':IDENTIFIER,'dataset_id':_nullable(IDENTIFIER),'state':TEXT,
        'committed_episodes':COUNT,'saved_episodes':COUNT,'manifest_hash':_nullable(HASH),
        'seal_state':{'type':'string','enum':['LOCAL_READY','CLOSED','NOT_APPLICABLE']},
        'content_hash':_nullable(HASH),'version':_nullable(IDENTIFIER),'holding':BOOL,
        'action_queue_empty':BOOL,'business_complete':BOOL,'upload_state':TEXT,'upload_verified':BOOL,
        'storage_ref':_nullable(TEXT),'dataset':_nullable(JSON_OBJECT)},
        ('session_id','dataset_id','committed_episodes','saved_episodes','manifest_hash','seal_state','calibration_revision'),additional=True)
    schemas['ResourceGuarantees'] = _object({
        'cpu_threads_bounded':BOOL,'per_job_memory':{'type':'boolean','const':False},
        'gpu_isolation':{'type':'boolean','const':False},'parallel_heavy_jobs':{'type':'boolean','const':False},
        'container_memory_limit_mb':_nullable(COUNT),'requested_memory_enforced':{'type':'boolean','const':False}},
        description='CPU thread bounds and one heavy job; container aggregate memory is not a per-job guarantee.')
    schemas['TrainingResult'] = _object({
        'training_run_id':IDENTIFIER,'job_id':IDENTIFIER,'state':TEXT,'step':COUNT,
        'checkpoint_dir':_nullable(TEXT),'output_dir':TEXT,'model_dir':TEXT,'dataset_hash':HASH,
        'datasets':_array(_ref('DatasetRef'),minItems=1),
        'agent':_object({'agent_id':IDENTIFIER,'agent_revision':IDENTIFIER,'policy_type':TEXT},('agent_id','agent_revision','policy_type')),
        'base_model':_nullable(_ref('ModelRef')),'image_id':TEXT,'parameters':_ref('TrainingParameters'),
        'resources_requested':_ref('ResourceRequest'),'resource_guarantees':_ref('ResourceGuarantees'),
        'resource_enforcement':TEXT,'metrics_ref':TEXT,'logs_ref':TEXT,'model_version':_nullable(IDENTIFIER),
        'model':_nullable(JSON_OBJECT),'resumed_from':_nullable(TEXT)},('training_run_id',),additional=True)
    schemas['ConversionResult'] = _object({
        'dataset_id':IDENTIFIER,'version':IDENTIFIER,'content_hash':HASH,
        'output_owner':TEXT,'output_project':TEXT,'source_ref':_ref('DatasetRef'),
        'converter_version':TEXT,'log_id':TEXT,'log_ref':TEXT},
        ('source_ref','converter_version','output_owner','output_project','log_id','log_ref'),additional=True)
    schemas['Operation'] = _object({
        'operation_id':IDENTIFIER,'request_id':IDENTIFIER,'kind':TEXT,'owner':TEXT,
        'project':TEXT,'state':TEXT,'created':{'type':'number'},'updated':{'type':'number'},
        'training_run_id':IDENTIFIER,'session_id':IDENTIFIER,'dataset_id':_nullable(IDENTIFIER),
        'payload':JSON_OBJECT,
        'result':{'anyOf':[_ref('CaptureStopResult'),_ref('TrainingResult'),_ref('SessionStatus'),
                          _ref('ConversionResult'),JSON_OBJECT,{'type':'null'}]},
        'error':_nullable(_ref('Error'))},('operation_id','state'),additional=True)
    for name in ('Operation','SessionStatus','CaptureStopResult','TrainingResult','ConversionResult'):
        schemas[name+'Response'] = _object({'ok':{'type':'boolean','const':True},'result':_ref(name)},('ok','result'))
    schemas['Event'] = _object({'sequence':COUNT,'job_id':IDENTIFIER,'kind':TEXT,
                                'body':JSON_OBJECT,'created':{'type':'number'}},('sequence','kind','body'))
    schemas['ConversionLog'] = _object({'operation_id':IDENTIFIER,'log_id':_nullable(TEXT),
        'text':{'type':'string'},'state':TEXT,'available':BOOL,'truncated':BOOL,
        'limit_bytes':{'type':'integer','const':32000},'bytes_total':COUNT},
        ('operation_id','log_id','text','state','available','truncated','limit_bytes'))
    schemas['TrainingLog'] = deepcopy(schemas['ConversionLog'])
    schemas['WebRtcAnswer'] = _object({'type':{'type':'string','const':'answer'},'sdp':TEXT},('type','sdp'))
    return schemas

def _payload_schema(op):
    required, optional = FIELDS[op]
    props = {key:IDENTIFIER for key in (required+' '+optional).split()}
    for key in ('name','description','task','device','image_id','target_format','project','output_owner','output_project'):
        if key in props: props[key] = {'type':'string'} if key=='description' else TEXT
    for key in ('confirm','save_current','workspace_confirmed'):
        if key in props: props[key] = BOOL
    modes = {'collections.start':MODES,
             'verifications.start':['physical_teleop','simulation_teleop']}.get(op,[])
    overrides = {
        'fps':{'type':'integer','minimum':1,'maximum':30},
        'duration_s':{'type':'number','minimum':1,'maximum':1800},
        'motor_id':{'type':'integer','minimum':1,'maximum':6},
        'scene_id':SCENE_REFERENCE,'scene_revision':SCENE_REFERENCE,
        'role':{'type':'string','enum':['leader','follower']},
        'point':{'type':'string','enum':['minimum','center','maximum']},
        'mode':{'type':'string','enum':modes},
        'content_hash':HASH,'resources':_ref('ResourceRequest'),'base_model':_ref('ModelRef'),
        'model':_ref('ModelRef'),'ros':_ref('RosConfig'),
        'datasets':_array(_ref('DatasetRef'),minItems=1,
            description='One or more immutable versions. Execution currently returns MULTI_DATASET_UNSUPPORTED for multiple versions; no implicit merge.'),
        'parameters':_ref('TrainingParameters' if op=='training.start' else 'InferenceParameters')}
    props.update({key:value for key,value in overrides.items() if key in props})
    schema = _object(props,required.split())
    if op in ('collections.start','verifications.start'):
        schema['allOf'] = [{'if':{'properties':{'mode':{'enum':['simulation_teleop','simulation_task']}},'required':['mode']},
                            'then':{'required':['scene_id','scene_revision']}}]
    if op=='collections.start':
        schema['allOf'].append({'if':{'properties':{'mode':{'enum':['physical_task','simulation_task']}},'required':['mode']},
                                'then':{'required':['task_id','task_revision']}})
    return schema

def _finite_json(value, path):
    if type(value) in (str,bool,int) or value is None: return
    if type(value) is float:
        if math.isfinite(value): return
    elif type(value) is list:
        for index,child in enumerate(value): _finite_json(child,f'{path}[{index}]')
        return
    elif type(value) is dict:
        for key,child in value.items():
            if type(key) is not str: raise Fault('INVALID_FIELDS',path+' requires string object keys',400)
            _finite_json(child,path+'.'+key)
        return
    raise Fault('INVALID_FIELDS',path+' must contain finite JSON values',400)

def _check(value, schema, schemas, path='payload'):
    """Validate only the JSON Schema subset used by our public request shapes."""
    if '$ref' in schema: return _check(value,schemas[schema['$ref'].rsplit('/',1)[1]],schemas,path)
    if 'anyOf' in schema:
        for alternative in schema['anyOf']:
            try:
                _check(value,alternative,schemas,path)
                return
            except Fault: pass
        raise Fault('INVALID_FIELDS',path+' has an invalid type',400)
    type_name = schema.get('type')
    valid = {'object':type(value) is dict,'array':type(value) is list,'integer':type(value) is int,
             'number':type(value) in (int,float),'boolean':type(value) is bool,
             'string':type(value) is str,'null':value is None}
    if type_name and not valid[type_name]: raise Fault('INVALID_FIELDS',path+' must be '+type_name,400)
    if 'enum' in schema and value not in schema['enum']: raise Fault('INVALID_FIELDS',path+' is outside supported values',400)
    if 'const' in schema and value!=schema['const']: raise Fault('INVALID_FIELDS',path+' differs from its required value',400)
    if type(value) is str:
        if (len(value)<schema.get('minLength',0) or 'maxLength' in schema and len(value)>schema['maxLength']
                or 'pattern' in schema and not re.search(schema['pattern'],value)):
            raise Fault('INVALID_FIELDS',path+' has an invalid string value',400)
    if type(value) in (int,float):
        if ('minimum' in schema and value<schema['minimum'] or 'maximum' in schema and value>schema['maximum']
                or 'exclusiveMinimum' in schema and value<=schema['exclusiveMinimum']):
            code = 'INVALID_FPS' if path=='payload.fps' else 'INVALID_DURATION' if path=='payload.duration_s' else 'INVALID_FIELDS'
            raise Fault(code,path+' is outside allowed bounds',400)
    if type(value) is list:
        if len(value)<schema.get('minItems',0): raise Fault('INVALID_FIELDS',path+' requires at least one item',400)
        for index,child in enumerate(value): _check(child,schema.get('items',{}),schemas,f'{path}[{index}]')
    if type(value) is dict:
        missing = set(schema.get('required',()))-value.keys()
        if missing: raise Fault('INVALID_FIELDS',path+' missing '+', '.join(sorted(missing)),400)
        props = schema.get('properties',{}); additional = schema.get('additionalProperties',True)
        for key,child in value.items():
            if 'propertyNames' in schema: _check(key,schema['propertyNames'],schemas,path+' key')
            if key in props: _check(child,props[key],schemas,path+'.'+key)
            elif additional is False: raise Fault('INVALID_FIELDS',path+' contains unknown field '+key,400)
            elif isinstance(additional,dict): _check(child,additional,schemas,path+'.'+key)
    for conditional in schema.get('allOf',()):
        try: _check(value,conditional['if'],schemas,path)
        except Fault: continue
        _check(value,conditional['then'],schemas,path)

def validate(op, payload):
    if not isinstance(op,str) or op not in FIELDS: raise Fault('UNSUPPORTED_OPERATION','Unknown operation',422)
    _finite_json(payload,'payload')
    _check(payload,_payload_schema(op),_schemas())
    return payload

def _response(schema, description='Authorized result', *, media='application/json'):
    return {'description':description,'content':{media:{'schema':schema}}}
def _envelope(schema):
    return _object({'ok':{'type':'boolean','const':True},'result':schema},('ok','result'))

def openapi():
    alternatives = [_object({'request_id':IDENTIFIER,'operation':{'type':'string','const':op},
                              'payload':_payload_schema(op)},('request_id','operation','payload')) for op in FIELDS]
    errors = {str(code):_response(_ref('ErrorResponse'),description) for code,description in
              ((400,'Invalid fields'),(401,'Authentication required'),(403,'Owner/project denied'),
               (404,'Resource not found'),(409,'Conflict, version mismatch or unavailable capability'),(422,'Unsupported operation'))}
    paths = {'/v2/commands':{'post':{
        'summary':'Submit idempotent operation; acceptance is not completion',
        'requestBody':{'required':True,'content':{'application/json':{'schema':{'oneOf':alternatives}}}},
        'responses':{'202':_response(_ref('OperationResponse'),'Accepted operation and stable aliases'),**errors}}}}
    routes = {
        '/v2/capabilities':['get'],'/v2/identity':['get'],'/v2/{kind}':['get','post'],
        '/v2/{kind}/{id}/{revision}':['get'],'/v2/robots/{id}/{revision}/status':['get'],
        '/v2/operations/{id}':['get'],'/v2/operations/{id}/events':['get'],
        '/v2/operations/{id}/metrics':['get'],'/v2/operations/{id}/logs':['get'],
        '/v2/operations/{id}/conversion-log':['get'],'/v2/operations/{id}/stream':['get'],
        '/v2/operations/{id}/cancel':['post'],'/v2/operations/{id}/files/{name}':['get'],
        '/v2/sessions/{id}/status':['get'],'/v2/transfers':['post'],
        '/v2/transfers/{id}':['get'],'/v2/transfers/{id}/files':['put'],
        '/v2/transfers/{id}/finish':['post'],'/v2/datasets/import':['post'],
        '/v2/datasets/{id}/{revision}/files/{name}':['get'],
        '/v2/robots/{id}/{revision}/media':['get'],'/v2/robots/{id}/{revision}/media/{camera}.png':['get'],
        '/v2/robots/{id}/{revision}/media/offer':['post']}
    for path,methods in routes.items():
        paths[path] = {}
        for method in methods:
            success = _envelope(JSON_OBJECT); media = 'application/json'
            if path=='/v2/operations/{id}' or path.endswith('/cancel'): success = _ref('OperationResponse')
            elif path=='/v2/sessions/{id}/status': success = _ref('SessionStatusResponse')
            elif path.endswith('/events'): success = _envelope(_array(_ref('Event')))
            elif path.endswith('/conversion-log'): success = _envelope(_ref('ConversionLog'))
            elif path.endswith('/logs'): success = _envelope(_ref('TrainingLog'))
            elif path.endswith('/media/offer'): success = _ref('WebRtcAnswer')
            if path.endswith('/stream'):
                success = {'type':'string','description':'SSE operation events; Last-Event-ID or after resumes the cursor.'}
                media = 'text/event-stream'
            elif path.endswith('.png') or '/files/{name}' in path:
                success = {'type':'string','format':'binary'}
                media = 'image/png' if path.endswith('.png') else 'application/octet-stream'
            status = '202' if path.endswith('/cancel') else '201' if method=='post' and path in ('/v2/transfers','/v2/{kind}') else '200'
            paths[path][method] = {
                'parameters':[{'name':name,'in':'path','required':True,'schema':{'type':'string'}} for name in re.findall(r'{(\w+)}',path)],
                'responses':{status:_response(success,media=media),**errors}}
    paths['/healthz'] = {'get':{'security':[],'responses':{'200':_response(JSON_OBJECT,'Service readiness, not hardware acceptance')}}}
    paths['/openapi.json'] = {'get':{'responses':{'200':_response(JSON_OBJECT,'This public contract'),**errors}}}
    capture_status=_envelope(_object({'state':{'type':'string','enum':['RUNNING','IDLE','FINISHING']},
        'session_id':IDENTIFIER,'dataset_id':IDENTIFIER,'episode_id':IDENTIFIER,'episode_index':COUNT,
        'recording_generation':{'type':'integer','minimum':1},'current_frames':COUNT,'committed_episodes':COUNT},('state',),additional=True))
    capture_body=_object({'project':IDENTIFIER,'request_id':IDENTIFIER,
        'recording_generation':{'type':'integer','minimum':1}},('project','request_id','recording_generation'))
    for action in ('redo','complete'):
        path='/collection/sessions/{id}/episodes/{episode_id}/'+action
        paths[path]={'post':{'summary':'Redo current generation' if action=='redo' else 'Commit current episode, persist upload intent and automatically record next',
            'parameters':[{'in':'path','name':n,'required':True,'schema':IDENTIFIER} for n in ('id','episode_id')],
            'requestBody':{'required':True,'content':{'application/json':{'schema':capture_body}}},
            'responses':{'200':_response(capture_status),**errors,'503':_response(_ref('ErrorResponse'),'Unknown state or next recording failed; committed details retained')}}}
    start_schema=_payload_schema('collections.start')
    start_schema['properties']['request_id']=IDENTIFIER;start_schema['required'].append('request_id')
    start_schema['required']=[name for name in start_schema['required'] if name not in ('robot_id','robot_revision')]
    start_schema['dependentRequired']={'robot_id':['robot_revision'],'robot_revision':['robot_id']}
    for name in ('robot_id','robot_revision'):
        start_schema['properties'][name]={**start_schema['properties'][name],
            'description':'Optional legacy explicit identity; otherwise fixed project/mode binding is used.'}
    start_schema['properties']['mode']={'type':'string','enum':['physical_teleop','simulation_teleop']}
    paths['/collection/sessions']={'post':{'summary':'Start first episode; returns only after durable first frame',
        'requestBody':{'required':True,'content':{'application/json':{'schema':start_schema}}},
        'responses':{'201':_response(capture_status,'Actual first frame confirmed'),**errors}}}
    for action in ('stop','resume'):
        body=_object({'project':IDENTIFIER,'request_id':IDENTIFIER,'workspace_confirmed':BOOL},
            ('project','request_id','workspace_confirmed') if action=='resume' else ('project','request_id'))
        paths['/collection/sessions/{id}/'+action]={'post':{'summary':'Preserve unsubmitted data and stop independently of uploads' if action=='stop' else 'Explicitly resume the same preserved session after onsite confirmation',
            'parameters':[{'in':'path','name':'id','required':True,'schema':IDENTIFIER}],
            'requestBody':{'required':True,'content':{'application/json':{'schema':body}}},
            'responses':{'200':_response(capture_status),**errors,'503':_response(_ref('ErrorResponse'),'Stop or recovery not confirmed')}}}
    paths['/collection/sessions/{id}/finish']={'post':{'summary':'One-click end: stop, commit nonempty current episode and send in background with durable retry; never starts next episode',
        'parameters':[{'in':'path','name':'id','required':True,'schema':IDENTIFIER}],
        'requestBody':{'required':True,'content':{'application/json':{'schema':_object({'project':IDENTIFIER,'request_id':IDENTIFIER},('project','request_id'))}}},
        'responses':{'200':_response(capture_status,'Durable end request accepted; stop/seal/upload progress follows asynchronously'),**errors}}}
    paths['/collection/status']={'get':{'summary':'Project status: unique active session target, otherwise fixed project default; never chooses the newest robot revision',
        'parameters':[{'in':'query','name':n,'required':n=='project','schema':IDENTIFIER} for n in ('project','robot_id','robot_revision')]
            +[{'in':'query','name':'mode','required':False,'schema':{'type':'string','enum':['simulation_teleop','physical_teleop']}}],
        'responses':{'200':_response(capture_status),**errors,'503':_response(_ref('ErrorResponse'),'Query failed or target state unknown; never IDLE')}}}
    # v10r15：没有 /collection/align（对齐由操作者自己负责，平台不管）
    # Documentation transforms must not mutate shared request definitions.
    from .collection_minimal_schema import add_minimal_paths
    from .collection_current_schema import add_current_paths
    return add_current_paths(add_minimal_paths({'openapi':'3.1.0','info':{'title':'SO101 collection/training delivery','version':CONTRACT_VERSION},
        'security':[{'Bearer':[]}],'components':{'securitySchemes':{'Bearer':{'type':'http','scheme':'bearer'}},
        'schemas':_schemas()},'paths':paths}))
