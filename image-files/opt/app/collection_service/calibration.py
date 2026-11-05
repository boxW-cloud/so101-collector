"""Operator-guided native leader software mapping; never writes motor registers."""
import copy,json,time,uuid
from pathlib import Path
from .protocol import ApiError,identifier
from .contracts import digest

class CalibrationOperations:
    def initialize_calibration(self):
        self.calibration_active=None
        self.state.setdefault('calibrations',{});self.state.setdefault('calibration_revisions',{})
        for value in self.state['calibrations'].values():
            if value['state']=='WAITING_SAMPLES':value['state']='INTERRUPTED'
        self.calibration_root=self.root/'calibrations';self.calibration_root.mkdir(exist_ok=True)
    def calibration_catalog(self,principal=None):
        return [{'revision':'baseline','method':'registered_existing_mapping'}]+[
            {'revision':key,'content_hash':v['content_hash'],'robot_id':v['robot_id'],'method':'native_three_point_software_mapping'}
            for key,v in self.state['calibration_revisions'].items() if principal is None or v['principal']==principal]
    def calibration_access(self,principal,project,revision):
        if revision=='baseline':return
        value=self.calibration_record(revision)
        if value['principal']!=principal or value['project']!=project:raise ApiError('FORBIDDEN','Calibration revision belongs to another owner/project')
    def calibration_record(self,revision):
        ref=self.state['calibration_revisions'].get(identifier(revision))
        if not ref:raise ApiError('CALIBRATION_NOT_FOUND','Unknown calibration revision')
        record=json.loads((self.calibration_root/(revision+'.json')).read_text())
        if record['content_hash']!=digest({k:v for k,v in record.items() if k!='content_hash'}) or record['content_hash']!=ref['content_hash']:raise ApiError('CALIBRATION_CORRUPT','Calibration revision content changed')
        return record
    def handle_calibration(self,op,p,principal):
        if op=='calibration.list':
            self.fields(p,[]);return {'revisions':self.calibration_catalog(principal)}
        if op=='calibration.start':
            self.fields(p,['robot_id','project','role','device_config_revision','config_revision','epoch','control_token'])
            claim=self.context(p,principal);self.permission(principal,p['project'])
            if 'calibrator' not in self.config['principals'][principal].get('roles',[]):raise ApiError('FORBIDDEN','Calibration role required')
            if p['role']=='follower':raise ApiError('HARDWARE_NOT_READY','Follower calibration is deferred')
            target=self.config.get('leader_robot',{})
            if p['role']!='leader' or p['robot_id']!=target.get('robot_id'):raise ApiError('WRONG_TARGET','Claim the independently registered leader target')
            if claim['project']!=p['project']:raise ApiError('FORBIDDEN','Claim project differs')
            if p['device_config_revision']!=target['device_config_revision']:raise ApiError('DEVICE_CONFIG_MISMATCH','Leader device revision differs')
            if self.busy():raise ApiError('BUSY','Target is in use')
            source=target['source'];cfg=self.config['sources'][source];profile=json.loads(Path(cfg['profile']).read_text())
            ident=uuid.uuid4().hex;opid=uuid.uuid4().hex
            record={'calibration_id':ident,'operation_id':opid,'robot_id':p['robot_id'],'principal':principal,'project':p['project'],
                'role':'leader','device_config_revision':p['device_config_revision'],'config_revision':p['config_revision'],
                'epoch':claim['epoch'],'source':source,'parent_profile':profile,'points':{},'state':'WAITING_SAMPLES','method':'native_three_point_software_mapping'}
            self.state['calibrations'][ident]=record;self.calibration_active=ident
            self.state['jobs'][opid]={'operation_id':opid,'operation':op,'principal':principal,'project':p['project'],'status':'RUNNING','created_at':time.time(),'calibration_id':ident}
            return {'operation_id':opid,'calibration_id':ident,'status':'WAITING_SAMPLES','required_points':{str(i):['endpoint_a','center','endpoint_b'] for i in range(1,6)}|{'6':['closed','open']},'motor_writes':False}
        if op=='calibration.status':
            self.fields(p,['calibration_id']);record=self.owner('calibrations',p['calibration_id'],principal)
            return {k:v for k,v in record.items() if k!='parent_profile'}
        required=['calibration_id','robot_id','config_revision','epoch','control_token']
        self.fields(p,required+(['motor_id','point'] if op=='calibration.sample' else []))
        claim=self.context(p,principal);record=self.owner('calibrations',p['calibration_id'],principal)
        if record['state']!='WAITING_SAMPLES' or self.calibration_active!=record['calibration_id']:raise ApiError('INVALID_STATE','Calibration no longer accepts samples')
        if record['robot_id']!=p['robot_id'] or record['epoch']!=claim['epoch']:raise ApiError('STALE_SESSION','Calibration target/epoch differs')
        if record['device_config_revision']!=self.config['leader_robot']['device_config_revision']:raise ApiError('DEVICE_CONFIG_MISMATCH','Device configuration changed during calibration')
        job=self.state['jobs'][record['operation_id']]
        if op=='calibration.cancel':
            record['state']='CANCELLED';job.update(status='CANCELLED',finished_at=time.time());self.calibration_active=None
            return {'status':'CANCELLED','calibration_id':record['calibration_id'],'motor_writes':False}
        if op=='calibration.sample':
            motor=p['motor_id'];point=p['point']
            if type(motor) is not int or not 1<=motor<=6 or point not in (['closed','open'] if motor==6 else ['endpoint_a','center','endpoint_b']):raise ApiError('INVALID_POINT','Select a valid motor and reference point')
            cfg=self.config['sources'][record['source']];packet=json.loads((Path(cfg['root'])/'input.json').read_text())
            if packet['profile_hash']!=record['parent_profile']['profile_hash'] or not packet['received_monotonic']<=time.monotonic()<packet['expires_monotonic']:raise ApiError('SOURCE_STALE','Leader input missing, stale or from another profile')
            samples=record['points'].setdefault(str(motor),{})
            if point in samples:raise ApiError('POINT_ALREADY_RECORDED','Use same request_id for retry or cancel and start a new calibration')
            samples[point]={'encoder':packet['native_positions'][str(motor)],'sequence':packet['sequence'],'captured_monotonic':packet['received_monotonic']}
            return {'status':'WAITING_SAMPLES','calibration_id':record['calibration_id'],'motor_id':motor,'point':point,**samples[point]}
        if op!='calibration.finish':raise ApiError('UNSUPPORTED','Unsupported calibration operation')
        profile=copy.deepcopy(record['parent_profile'])
        for name,axis in profile['axes'].items():
            motor=axis['motor_id'];points=record['points'].get(str(motor),{})
            needed=['closed','open'] if motor==6 else ['endpoint_a','center','endpoint_b']
            if set(points)!=set(needed):raise ApiError('CALIBRATION_INCOMPLETE','All reference points must be explicitly sampled')
            if motor==6:
                closed=points['closed']['encoder'];opened=points['open']['encoder']
                # Preserve this commissioned device's known direction; don't infer inversion.
                if closed-opened<40:raise ApiError('CALIBRATION_DIRECTION','Expected closed encoder above open encoder for this registered device')
                low,high=opened,closed;axis['reference_native']=closed
            else:
                a,b=points['endpoint_a']['encoder'],points['endpoint_b']['encoder'];center=points['center']['encoder']
                low,high=min(a,b),max(a,b)
                if not low+20<center<high-20:raise ApiError('CALIBRATION_RANGE','Center must be separated from both endpoints')
                axis['native_center']=center;axis['reference_native']=center
            axis.update(native_min=low,native_max=high)
            axis['spark_normalization_calibration'].update(range_min=4095-high,range_max=4095-low)
        revision='cal-'+uuid.uuid4().hex
        profile.update(parent_profile_hash=record['parent_profile']['profile_hash'],calibration_revision=revision,
            status='operator_sampled_software_mapping',physical_angle_accuracy_verified=False,must_not_write_to_motor_calibration=True)
        profile.pop('profile_hash',None);profile['profile_hash']=digest(profile)
        output={'revision':revision,'robot_id':record['robot_id'],'role':'leader','device_config_revision':record['device_config_revision'],
            'source':record['source'],'principal':principal,'project':record['project'],'profile':profile,'points':record['points'],'method':record['method'],'created_at':time.time()}
        output['content_hash']=digest(output)
        with (self.calibration_root/(revision+'.json')).open('x') as f:json.dump(output,f,indent=2)
        (self.calibration_root/(revision+'.json')).chmod(0o444)
        self.state['calibration_revisions'][revision]={k:output[k] for k in ('robot_id','source','content_hash','project','principal')}
        result={'calibration_revision':revision,'content_hash':output['content_hash'],'summary':{'method':record['method'],'motors':6,'motor_writes':False},'status':'SUCCEEDED'}
        record.update(state='SUCCEEDED',result=result);job.update(status='SUCCEEDED',result=result,finished_at=time.time());self.calibration_active=None
        return result
