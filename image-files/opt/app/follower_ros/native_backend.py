"""Adapt validated primitives to the existing loopback controller; never opens COM."""
from contextlib import contextmanager
from pathlib import Path
import json,time,urllib.request,urllib.error
from integration.action_primitives import BackendResult
from .protocol import Error

class NativeClient:
    def __init__(self,base='http://127.0.0.1:8765'):
        from urllib.parse import urlparse
        parsed=urlparse(base)
        if parsed.scheme!='http' or parsed.hostname!='127.0.0.1' or not parsed.port or parsed.path not in ('','/') or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError('Controller URL must be an explicit IPv4 loopback HTTP port')
        self.base=base.rstrip('/');self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}));self.token=None
    def authenticate(self):
        request=urllib.request.Request(self.base+'/api/bootstrap',headers={'X-Arm-Console':'1','Referer':self.base+'/','Origin':self.base})
        with self.opener.open(request,timeout=3) as r:self.token=json.load(r)['token']
    def call(self,command,args,_refresh=True):
        if self.token is None:self.authenticate()
        req=urllib.request.Request(self.base+'/api/command',data=json.dumps({'command':command,'args':args}).encode(),headers={'Content-Type':'application/json','Authorization':'Bearer '+self.token})
        try:r=self.opener.open(req,timeout=10)
        except urllib.error.HTTPError as exc:r=exc
        with r:
            status=r.status;value=json.load(r)
        if status==401 and _refresh and command in ('state','devices','native_inspect','model','preview_model','plan_cartesian','operation'):
            self.token=None;self.authenticate();return self.call(command,args,_refresh=False)
        if not value.get('ok'):
            error=value.get('error',{})
            if not isinstance(error,dict):raise Error('CONTROLLER_AUTH' if status==401 else 'CONTROLLER_ERROR',str(error))
            raise Error(error.get('code','CONTROLLER_ERROR'),error.get('message','Controller rejected request'))
        return value['result']

class NativeBackend:
    def __init__(self,service):self.service=service;self.last_device_result=None;self.possibly_issued=False
    @contextmanager
    def control_scope(self,context,parts):
        self.validate_control(context)
        yield
    def validate_control(self,context):
        self.service.check_lease(context.epoch)
        if self.service.cancel.is_set():raise Error('STOPPED','Action cancelled')
        state=self.service.native.call('state',{})
        self.service.check_identity(state)
        if state.get('fault'):raise Error('FAULT_LATCHED',state['fault']['code']+': '+state['fault'].get('message',''))
        if not self.service.workspace_confirmed:raise Error('WORKSPACE_NOT_CONFIRMED','This ROS binding has not received operator workspace confirmation')
        if not state.get('consent_active'):raise Error('WORKSPACE_NOT_CONFIRMED','Operator must confirm a fixed base and clear workspace')
        if not 0<=time.time()-state['sampled_at']<=1.5:raise Error('STALE_FEEDBACK','Controller feedback is not fresh')
        op=state.get('operation') or {}
        if op.get('status')=='running':raise Error('BUSY','Another native operation is active')
        if self.last_device_result and op.get('id')!=self.last_device_result.get('id'):raise Error('CONTROL_CHANGED','Native operation changed between primitive segments; no next segment sent')
        return state
    def preflight(self,plan,*,part_ids,context):
        # Entire composite capability is checked before any gripper closure.
        for command,parameters in plan:
            if command=='move_to':raise Error('UNSUPPORTED_POSE6D','Owner supports XYZ only; full orientation and calibrated m/s execution are not commissioned')
            if command=='move' and not self.service.config.get('enable_relative_xyz',False):raise Error('UNCOMMISSIONED_XYZ','Physical model zero/TCP is not commissioned; relative Cartesian execution remains disabled')
            if command in ('release','hold_arm') and not self.service.config.get(('park' if command=='release' else 'chest')+'_position_m'):
                raise Error('POSE_NOT_CONFIGURED','No commissioned safe-park/chest preset for this physical arm; no substitute pose used')
        state=self.validate_control(context)
        # Fresh six-axis parameter/coordinate check through the owner's queue.
        self.service.native.call('native_inspect',{'session_id':state['session_id'],'motor_ids':[1,2,3,4,5,6]})
        self.validate_control(context)
        for command,parameters in plan:
            if command in ('release','hold_arm'):
                if not state.get('cartesian_available'):raise Error('CARTESIAN_NOT_READY','Cannot execute configured preset')
                target=self.service.config['park_position_m' if command=='release' else 'chest_position_m']
                self.service.native.call('plan_cartesian',{'session_id':state['session_id'],'mapping_id':state['mapping_id'],'position_m':target})
    def issue(self,command,args,context):
        with self.service.issue_lock:
            self.service.check_lease(context.epoch)
            if self.service.cancel.is_set():raise Error('STOPPED','No action is sent after a latched stop')
            self.possibly_issued=True
            return self.service.native.call(command,args)
    def wait_device(self,operation,context):
        deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            self.service.check_lease(context.epoch)
            if self.service.cancel.is_set():raise Error('STOPPED','Stop requested')
            result=self.service.native.call('operation',{'session_id':self.service.bound_session,'operation_id':operation['id']})
            state=self.service.native.call('state',{});self.service.check_identity(state)
            if state.get('fault'):raise Error('FAULT_LATCHED',state['fault']['code'])
            if (state.get('operation') or {}).get('id')!=operation['id']:raise Error('CONTROL_CHANGED','Native operation replaced during execution')
            if result['status']!='running':
                self.last_device_result=result;return result
            time.sleep(.15)
        raise Error('DEVICE_TIMEOUT','Timed out; no motion replay')
    def invoke(self,command,*,part_ids,parameters,context):
        if command=='move':
            state=self.validate_control(context)
            if not state.get('cartesian_available'):raise Error('CARTESIAN_NOT_READY','Native controller rejects Cartesian motion')
            if parameters['distance']>self.service.config.get('max_relative_distance_m',.03):raise Error('CAPABILITY_LIMIT','Bench relative move is limited to3cm; request is not clamped')
            pose=self.service.native.call('preview_model',{'session_id':state['session_id'],'mapping_id':state['mapping_id'],'angles':{'wrist_roll':state['model_angles']['wrist_roll']}})
            target=list(pose['tool']['position_m']);axis,sign={'forward':(0,1),'backward':(0,-1),'left':(1,1),'right':(1,-1),'up':(2,1),'down':(2,-1)}[parameters['direction']]
            target[axis]+=sign*parameters['distance']
            self.service.native.call('plan_cartesian',{'session_id':state['session_id'],'mapping_id':state['mapping_id'],'position_m':target})
            state=self.validate_control(context)
            operation=self.issue('move_cartesian',{'session_id':state['session_id'],'revision':state['revision'],'mapping_id':state['mapping_id'],'position_m':target,'idempotency_key':context.request_id+'-xyz','speed':100,'acceleration':5},context)
            result=self.wait_device(operation,context)
            if result['status'] not in ('reached','settled_short') or not result.get('hold_confirmed'):
                raise Error('TARGET_NOT_REACHED','Cartesian endpoint not confirmed; no precision retry')
            return BackendResult(True,part_ids,detail=f"base_link endpoint by encoder FK (status={result['status']}, residual={result.get('endpoint_error_mm')}mm reported); physical TCP accuracy is not independently calibrated")
        if command in ('move_to','release','hold_arm'):
            state=self.validate_control(context)
            if not state.get('cartesian_available'):raise Error('CARTESIAN_NOT_READY','Native controller rejects Cartesian motion')
            if command=='move_to':
                p=parameters['target_position']
                target=[p['x'],p['y'],p['z']]
                counts=max(50,min(600,round(50+(parameters.get('speed',.5)-.1)/1.9*550)))
                label=('ARM_MOVETO position-only: roll/pitch/yaw are NOT actuated (wrist locked); '
                       'speed mapped m/s->servo counts (bench mapping, uncalibrated)')
            else:
                key='park_position_m' if command=='release' else 'chest_position_m'
                configured=self.service.config.get(key)
                if not configured:raise Error('POSE_NOT_CONFIGURED',f'{key} is not configured for this physical arm')
                target=list(configured);counts=80 if command=='release' else 100
                label=('ARM_RELEASE to configured park pose; gripper state untouched' if command=='release'
                       else 'HOLD_ARM to configured chest pose; holding retained')
            self.service.native.call('plan_cartesian',{'session_id':state['session_id'],'mapping_id':state['mapping_id'],'position_m':target})
            state=self.validate_control(context)
            operation=self.issue('move_cartesian',{'session_id':state['session_id'],'revision':state['revision'],'mapping_id':state['mapping_id'],'position_m':target,'idempotency_key':context.request_id+'-'+command,'speed':counts,'acceleration':5},context)
            result=self.wait_device(operation,context)
            if result['status'] not in ('reached','settled_short') or not result.get('hold_confirmed'):
                raise Error('TARGET_NOT_REACHED','Cartesian endpoint not confirmed; no precision retry')
            return BackendResult(True,part_ids,detail=label+f"; encoder FK status={result['status']} residual={result.get('endpoint_error_mm')}mm (reported, not hidden)")
        if command not in ('open_gripper','close_gripper'):raise Error('UNSUPPORTED_CAPABILITY','No commissioned executor for this primitive')
        state=self.validate_control(context);joint=next(j for j in state['joints'] if j['id']==6)
        # Calibrated new gripper: opening increases logical register56 position.
        target=joint['maximum']-20 if command=='open_gripper' else joint['minimum']+20
        for index in range(32):
            state=self.validate_control(context);joint=next(j for j in state['joints'] if j['id']==6);delta=target-joint['position']
            # This jaw parks 21-28 ticks off any goal on this hardware (measured;
            # native PDB/NZS config). Inside that zone the firmware's own control
            # law stops responding, so a stable park within 30 ticks IS the
            # reachable endpoint. The residual is reported, never hidden.
            if abs(delta)<=30:
                return BackendResult(True,part_ids,detail=f'Configured jaw endpoint reached within hardware dead zone (residual {delta} ticks); object grasp is not inferred')
            step=max(-100,min(100,delta))
            if abs(step)<5:raise Error('TARGET_NOT_REACHED','Remaining error is below controller step minimum')
            # Controller validates session/revision, drift, temperature, limits,
            # firmware and foreign torque on every bounded native operation.
            operation=self.issue('gripper_move',{'session_id':state['session_id'],'revision':state['revision'],
                'delta_ticks':step,'idempotency_key':context.request_id+'-grip-'+str(index)},context)
            result=self.wait_device(operation,context)
            if not result.get('hold_confirmed'):raise Error('STOP_UNCONFIRMED','Gripper holding was not confirmed')
            if result['status']!='reached':
                # A stalled jaw may mean contact OR a failure: don't invent success.
                raise Error('TARGET_NOT_REACHED','Gripper settled short; endpoint not reached and grasp not verified')
        raise Error('ACTION_BUDGET','Bounded gripper action budget exhausted')
