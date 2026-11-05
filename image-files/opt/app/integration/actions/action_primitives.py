"""Standalone section 17.1 facade. Stdlib only; no SDK/device/ROS imports.

Inject an ActionBackend to connect existing services. This module validates and
routes requests; it does not implement kinematics, transport, robot permissions,
physical grip detection, named poses or a persistent platform job scheduler.
"""
from contextlib import AbstractContextManager
from copy import deepcopy
from dataclasses import asdict, dataclass, field
import hashlib
import json
import math
import threading
from typing import Mapping, Protocol
import uuid


def _side(default='right',both=True):
    return {'semantic':'enum','options':['left','right','both'] if both else ['left','right'],'default':default}


_CATALOG={
    'ARM_MOVETO':{'name':'移动手臂到目标位置','definition':'将指定手臂移动到给定的目标位置和姿态','part_id':[1,2],
        'parameters':{'target_position':{'semantic':'pose_6d','unit':'meter_radian',
            'default':dict.fromkeys(('x','y','z','roll','pitch','yaw'),0.0)},
            'speed':{'semantic':'velocity','unit':'m/s','default':.5,'range':[.1,2.0],'optional':True}},
        'failure_message':'移动手臂到目标位置失败，请稍后重试'},
    'ARM_MOVE':{'name':'相对移动手臂','definition':'将指定手臂的末端执行器沿给定方向移动指定距离','part_id':[1,2],
        'parameters':{'distance':{'semantic':'length','unit':'meter','default':.1,'range':[.01,1.0]},
            'direction':{'semantic':'enum','options':['forward','backward','left','right','up','down'],'default':'forward'},
            'side':_side(both=False)},'failure_message':'移动手臂失败，请稍后重试'},
    'ARM_RELEASE':{'name':'释放手臂','definition':'将指定手臂从当前姿态安全放下；不会打开夹爪','part_id':[1,2,3],
        'parameters':{'side':_side('both')},'failure_message':'释放手臂失败，请稍后重试'},
    'LOOSEN':{'name':'打开夹爪','definition':'打开指定手臂对应的夹爪','part_id':[1,2],
        'parameters':{'side':_side()},'failure_message':'打开夹爪失败，请稍后重试'},
    'GRASP':{'name':'闭合夹爪','definition':'闭合指定手臂对应的夹爪以抓取物体','part_id':[1,2],
        'parameters':{'side':_side()},'failure_message':'闭合夹爪失败，请稍后重试'},
    'HOLD_ARM':{'name':'胸前保持手臂','definition':'将指定手臂抬到机器人胸前预设位置并持续保持','part_id':[1,2,3],
        'parameters':{'side':_side()},'failure_message':'手臂移动到胸前保持位置失败，请稍后重试'},
    'HOLD_GRASP':{'name':'胸前保持抓取','definition':'闭合指定夹爪抓取物体，然后将对应手臂抬到胸前预设位置并持续保持','part_id':[1,2,3],
        'parameters':{'side':_side()},'failure_message':'抓取物体并保持到胸前失败，请稍后重试'},
}
for _spec in _CATALOG.values():
    _spec.update(actionType='ExecutionAction',actionCategory='BuildinAction')


def action_catalog():
    """Independent copy; part_id lists are supported IDs, not assumed side mappings."""
    return deepcopy(_CATALOG)


@dataclass(frozen=True)
class ActionContext:
    robot_id: str
    epoch: int
    control_token: str=field(repr=False)
    request_id: str

    def __post_init__(self):
        if any(not isinstance(v,str) or not v.strip() for v in (self.robot_id,self.control_token,self.request_id)):
            raise ValueError('robot_id, control_token and request_id must be nonempty strings')
        if type(self.epoch) is not int or self.epoch<0:raise ValueError('epoch must be a nonnegative integer')


@dataclass(frozen=True)
class BackendResult:
    success: bool
    completed_parts: tuple[int,...]
    error_code: str|None=None
    detail: str=''


@dataclass(frozen=True)
class PrimitiveResult:
    operation_id: str
    request_id: str
    action_id: str
    state: str
    success: bool
    completed_commands: tuple[str,...]=()
    error_code: str|None=None
    message: str=''
    detail: str=''

    def to_dict(self):return asdict(self)


class ActionBackend(Protocol):
    def control_scope(self,context:ActionContext,parts:tuple[int,...])->AbstractContextManager:
        """Authorize robot/token/epoch and exclusively claim ALL targets through exit.

        Integrate the existing controller's ownership mechanism; do not create a
        second control channel. Deny conflicts with teleop/collection/inference.
        A scope exit releases the claim, not support torque or gripper state.
        """
        ...
    def validate_control(self,context:ActionContext)->None:
        """Reject invalid/revoked permission, token or epoch, including during composites."""
        ...
    def preflight(self,plan,*,part_ids:tuple[int,...],context:ActionContext)->None:
        """Validate every planned command/target BEFORE any movement.

        Check pose/linear speed support and reachable orientation, reference frame,
        configured calibration-bound release/chest poses, gripper semantics and
        all target capabilities. Raise NotImplementedError for missing support.
        """
        ...
    def invoke(self,command:str,*,part_ids:tuple[int,...],parameters:Mapping,context:ActionContext)->BackendResult:
        """Return only after the real operation finishes; accepted/queued is NOT success.

        move_to retains all six pose fields and m/s, move retains meters/direction;
        release never opens grippers or automatically disables support torque;
        hold_arm reaches the configured chest pose and retains holding. A plain
        gripper close proves closure, not object detection or task success.
        """
        ...


class _Rejected(Exception):
    def __init__(self,code,detail):self.code=code;self.detail=detail

class BackendFailure(Exception):
    """Typed adapter rejection; preserves safety/unsupported codes without hiding them."""
    def __init__(self,code,detail):super().__init__(detail);self.code=code;self.detail=detail


def _number(value):
    if type(value) not in (int,float) or not math.isfinite(value):
        raise _Rejected('INVALID_ARGUMENT','Expected a finite number, not bool/string/NaN/Infinity')
    return float(value)


def _parameters(action_id,parameters):
    spec=_CATALOG.get(action_id)
    if spec is None:raise _Rejected('UNKNOWN_ACTION','Unknown action ID')
    if parameters is None:parameters={}
    if not isinstance(parameters,Mapping) or set(parameters)-set(spec['parameters']):
        raise _Rejected('INVALID_ARGUMENT','Unknown parameters or non-object input')
    result={}
    for name,field_spec in spec['parameters'].items():
        value=deepcopy(parameters.get(name,field_spec['default']))
        if field_spec['semantic']=='pose_6d':
            if not isinstance(value,Mapping) or set(value)!=set(field_spec['default']):
                raise _Rejected('INVALID_ARGUMENT','pose_6d requires exactly x/y/z/roll/pitch/yaw')
            value={k:_number(value[k]) for k in field_spec['default']}
        elif 'options' in field_spec:
            if not isinstance(value,str) or value not in field_spec['options']:
                raise _Rejected('INVALID_ARGUMENT',f'Invalid {name}')
        else:
            value=_number(value);lo,hi=field_spec['range']
            if not lo<=value<=hi:raise _Rejected('INVALID_ARGUMENT',f'{name} must be in [{lo}, {hi}]')
        result[name]=value
    return result


class ActionWrapper:
    def __init__(self,backend:ActionBackend|None=None,*,part_ids_by_side,cache_capacity=128):
        if (not isinstance(part_ids_by_side,Mapping) or set(part_ids_by_side)-{'left','right','both'}):
            raise ValueError('Explicit left/right/both target mappings required')
        self._parts={}
        for side,parts in part_ids_by_side.items():
            if (not isinstance(parts,(tuple,list)) or not parts or any(type(p) is not int or p not in (1,2,3) for p in parts)
                or len(set(parts))!=len(parts)):
                raise ValueError('Part mapping requires unique supported integer IDs')
            self._parts[side]=tuple(parts)
        if type(cache_capacity) is not int or cache_capacity<1:raise ValueError('Positive cache_capacity required')
        self.backend=backend;self.cache_capacity=cache_capacity
        self._lock=threading.RLock();self._active=False;self._requests={}

    def _resolve(self,action_id,params,part_id):
        if part_id is not None and type(part_id) is not int:raise _Rejected('INVALID_ARGUMENT','part_id must be an integer')
        if action_id=='ARM_MOVETO':
            known={p for parts in self._parts.values() for p in parts}
            if part_id not in known:raise _Rejected('TARGET_NOT_CONFIGURED','ARM_MOVETO needs an explicit configured part_id')
            parts=(part_id,)
        else:
            parts=self._parts.get(params['side'])
            if not parts:raise _Rejected('TARGET_NOT_CONFIGURED','Requested side has no configured target; no implicit fallback')
            if part_id is not None and parts!=(part_id,):raise _Rejected('INVALID_ARGUMENT','part_id conflicts with side mapping')
        if any(p not in _CATALOG[action_id]['part_id'] for p in parts):
            raise _Rejected('INVALID_ARGUMENT','Action does not support this part_id')
        return parts

    def execute(self,action_id,parameters=None,*,context:ActionContext,part_id=None):
        """Blocking execution; another thread may poll status or repeat the same request.

        Idempotency is process-local and bounded: cache-full rejects new requests,
        rather than silently evicting IDs and risking duplicate execution. Production
        durable job IDs, cancellation and restart recovery belong to the outer service.
        """
        if not isinstance(context,ActionContext):raise TypeError('ActionContext required')
        operation_id=uuid.uuid4().hex;completed=[];details=[]
        def failure(code,detail):
            return PrimitiveResult(operation_id,context.request_id,action_id,'FAILED',False,tuple(completed),code,
                _CATALOG.get(action_id,{}).get('failure_message','未知动作'),detail.replace(context.control_token,'[redacted]'))
        if not isinstance(action_id,str):
            action_id=''
            return failure('INVALID_ARGUMENT','action_id must be a string')
        try:
            params=_parameters(action_id,parameters);parts=self._resolve(action_id,params,part_id)
        except _Rejected as exc:return failure(exc.code,exc.detail)
        fingerprint=hashlib.sha256(json.dumps([action_id,params,parts,context.robot_id,context.epoch,context.control_token],sort_keys=True).encode()).hexdigest()
        key=(context.robot_id,context.request_id)
        with self._lock:
            if key in self._requests:
                old_fingerprint,result=self._requests[key]
                return result if old_fingerprint==fingerprint else failure('REQUEST_CONFLICT','request_id already used for another payload/lease')
            if self._active:return failure('BUSY','Wrapper is executing another action')
            if len(self._requests)>=self.cache_capacity:return failure('REQUEST_CACHE_FULL','Use an outer durable idempotency store; cache does not evict')
            running=PrimitiveResult(operation_id,context.request_id,action_id,'RUNNING',False)
            self._requests[key]=(fingerprint,running);self._active=True
        command={'ARM_MOVETO':'move_to','ARM_MOVE':'move','ARM_RELEASE':'release','LOOSEN':'open_gripper',
                 'GRASP':'close_gripper','HOLD_ARM':'hold_arm','HOLD_GRASP':'close_gripper'}[action_id]
        forwarded={k:v for k,v in params.items() if k!='side'}
        plan=[(command,forwarded)]
        if action_id=='HOLD_GRASP':plan.append(('hold_arm',{}))
        try:
            if self.backend is None:raise _Rejected('BACKEND_NOT_CONFIGURED','Inject the actual service adapter before motion testing')
            with self.backend.control_scope(context,parts):
                self.backend.validate_control(context)
                self.backend.preflight(deepcopy(plan),part_ids=parts,context=context)
                for command,payload in plan:
                    self.backend.validate_control(context)
                    outcome=self.backend.invoke(command,part_ids=parts,parameters=deepcopy(payload),context=context)
                    if (not isinstance(outcome,BackendResult) or type(outcome.success) is not bool or
                        not isinstance(outcome.detail,str) or
                        (outcome.error_code is not None and not isinstance(outcome.error_code,str)) or
                        not isinstance(outcome.completed_parts,tuple) or
                        any(type(p) is not int for p in outcome.completed_parts) or
                        len(set(outcome.completed_parts))!=len(outcome.completed_parts) or
                        not set(outcome.completed_parts)<=set(parts)):
                        raise _Rejected('BACKEND_CONTRACT','Adapter must return a typed completion result')
                    if not outcome.success:
                        raise _Rejected(outcome.error_code or 'BACKEND_FAILED',outcome.detail or 'Adapter reported failure; no following command executed')
                    if set(outcome.completed_parts)!=set(parts):raise _Rejected('BACKEND_CONTRACT','Not all selected parts completed')
                    completed.append(command)
                    if outcome.detail:details.append(command+': '+outcome.detail)
                self.backend.validate_control(context)
            result=PrimitiveResult(operation_id,context.request_id,action_id,'SUCCEEDED',True,tuple(completed),detail='; '.join(details))
        except (_Rejected,BackendFailure) as exc:result=failure(exc.code,exc.detail)
        except PermissionError:result=failure('CONTROL_DENIED','Control permission/token/epoch validation failed')
        except NotImplementedError:result=failure('UNSUPPORTED_CAPABILITY','Adapter does not implement a required capability or configured pose')
        except Exception as exc:result=failure('BACKEND_ERROR',f'Adapter raised {type(exc).__name__}; inspect adapter logs')
        finally:
            with self._lock:self._active=False
        with self._lock:self._requests[key]=(fingerprint,result)
        return result

    def status(self,*,context:ActionContext):
        """Latest local result. External services must enforce their query authorization."""
        with self._lock:
            item=self._requests.get((context.robot_id,context.request_id))
            return item[1] if item else None
