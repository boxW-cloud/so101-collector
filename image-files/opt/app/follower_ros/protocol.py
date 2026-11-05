import hashlib,hmac,json,math,re,time,os
from integration.action_primitives import BackendFailure

SCHEMA='so101.follower.v1'
ROBOT=os.environ.get('SO101_ROBOT_ID','so101-follower-com15')
if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}',ROBOT):raise ValueError('Invalid configured ROS robot ID')
ACTION_TOPIC='/so101_follower/actions/request'
CONTROL_TOPIC='/so101_follower/control/request'
RESULT_TOPIC='/so101_follower/actions/result'
CONTROL_RESULT_TOPIC='/so101_follower/control/result'
STATUS_TOPIC='/so101_follower/actions/status'
FEEDBACK_TOPIC='/so101_follower/feedback'
CATALOG_TOPIC='/so101_follower/catalog'

class Error(BackendFailure):
    def __init__(self,code,message):super().__init__(code,message)
def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sign(v,key):return {**v,'signature':hmac.new(key.encode(),canonical(v),hashlib.sha256).hexdigest()}
def verify(v,key,*,now=None):
    if not isinstance(v,dict) or set(v)!={'schema','request_id','robot_id','principal','operation','payload','issued_at','expires_at','signature'}:raise Error('INVALID_ENVELOPE','Exact request envelope required')
    unsigned={k:x for k,x in v.items() if k!='signature'}
    if not isinstance(v['signature'],str) or not hmac.compare_digest(sign(unsigned,key)['signature'],v['signature']):raise Error('UNAUTHORIZED','Signature mismatch')
    if v['schema']!=SCHEMA or v['robot_id']!=ROBOT or v['principal']!='operator':raise Error('WRONG_TARGET','Unknown schema, physical robot or principal')
    if not isinstance(v['request_id'],str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}',v['request_id']):raise Error('INVALID_ID','ASCII request_id required')
    if not isinstance(v['payload'],dict):raise Error('INVALID_PAYLOAD','Object payload required')
    a,b=v['issued_at'],v['expires_at'];now=time.time() if now is None else now
    if any(type(x) not in (int,float) or not math.isfinite(x) for x in (a,b)) or not 0<b-a<=10 or not a-2<=now<b:raise Error('EXPIRED','Request TTL must be at most10 seconds and currently valid')
    return unsigned
def envelope(operation,payload,request_id,key):
    now=time.time();return sign({'schema':SCHEMA,'request_id':request_id,'robot_id':ROBOT,'principal':'operator','operation':operation,'payload':payload,'issued_at':now,'expires_at':now+5},key)
