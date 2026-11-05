import hashlib,hmac,json,re,time

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def signed(value,key):return {**value,'signature':hmac.new(key.encode(),canonical(value),hashlib.sha256).hexdigest()}
def verified(value,key):
    payload={k:v for k,v in value.items() if k!='signature'}
    if not isinstance(value.get('signature'),str) or not hmac.compare_digest(value['signature'],signed(payload,key)['signature']):raise ValueError('Invalid signature')
    return payload
def identifier(value):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}',value):raise ValueError('Invalid identifier')
    return value

class ApiError(Exception):
    def __init__(self,code,message,*,details=None):super().__init__(message);self.code=code;self.details=details

def request_payload(value,keys,*,now=None):
    if not isinstance(value,dict) or set(value)!= {'request_id','principal','operation','payload','issued_at','signature'}:raise ApiError('INVALID_REQUEST','Exact request envelope required')
    principal=identifier(value['principal'])
    if principal not in keys:raise ApiError('FORBIDDEN','Unknown principal')
    try:payload=verified(value,keys[principal])
    except ValueError as exc:raise ApiError('FORBIDDEN',str(exc)) from exc
    identifier(payload['request_id'])
    if type(payload['issued_at']) not in (int,float) or abs((time.time() if now is None else now)-payload['issued_at'])>30:raise ApiError('EXPIRED','Request timestamp outside30s budget')
    if not isinstance(payload['payload'],dict):raise ApiError('INVALID_REQUEST','Payload object required')
    return payload
