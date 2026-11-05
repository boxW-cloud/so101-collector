import hashlib,json,threading,time
import numpy as np
from lerobot_sdk.core.types import RobotConfig,JointTarget,JOINT_NAMES,DEGREE_UNITS,ImageFrame
from lerobot_sdk.control.backend import BackendSnapshot
from lerobot_sdk.core.errors import DeviceDisconnected

def simulation_config(robot_id='sim-test',*,initial=None,required_cameras=('front',),**kwargs):
    """Explicit TEST-only values; never a safe physical SO101 calibration."""
    limits=dict(zip(JOINT_NAMES,[(-110,110),(-100,100),(-100,90),(-95,95),(-160,160),(0,100)],strict=True))
    digest='sha256:'+hashlib.sha256(json.dumps({'kind':'simulation-only','robot_id':robot_id,'limits':limits},sort_keys=True).encode()).hexdigest()
    initial=initial or dict(zip(JOINT_NAMES,[0,0,0,0,0,50],strict=True))
    values=dict(robot_id=robot_id,calibration_hash=digest,units=DEGREE_UNITS,limits=limits,
                max_velocity=dict.fromkeys(JOINT_NAMES,20),max_step=dict.fromkeys(JOINT_NAMES,2),
                tolerance=dict.fromkeys(JOINT_NAMES,.25),gripper_open=70,gripper_closed=30,
                release_pose=JointTarget(initial,DEGREE_UNITS,digest),fps=100,stable_frames=3,
                action_timeout_s=6,io_timeout_s=.1,max_observation_age_s=.2,max_sensor_skew_s=.1,
                required_cameras=required_cameras)
    values.update(kwargs);return RobotConfig(**values)

class FakeRobot:
    kind='simulation'
    def __init__(self,resource_id='fake:one',*,initial=None,speed=180,cameras=('front',)):
        self.resource_id=resource_id;self.positions=dict(initial or zip(JOINT_NAMES,[0,0,0,0,0,50],strict=True))
        self.target=dict(self.positions);self.speed=speed;self.cameras=cameras;self.connected=False
        self.calls=[];self.sent=[];self.holds=[];self.delay_s=0;self.fail_samples=False
        self.stale_ns=0;self.image_stale_ns=0;self._last=time.monotonic();self._count=0
    def _call(self,name,timeout_s):
        self.calls.append((name,threading.get_ident()))
        if self.delay_s:
            time.sleep(min(self.delay_s,timeout_s))
            if self.delay_s>=timeout_s:raise TimeoutError('Fake backend honored IO deadline')
    def connect(self,timeout_s):
        self._call('connect',timeout_s);self.connected=True;self._last=time.monotonic()
    def sample(self,timeout_s):
        self._call('sample',timeout_s)
        if not self.connected or self.fail_samples:raise DeviceDisconnected('Fake sensor disconnected')
        now=time.monotonic();dt=now-self._last;self._last=now
        for k,v in self.target.items():
            delta=v-self.positions[k];self.positions[k]+=min(abs(delta),self.speed*dt)*(1 if delta>=0 else -1)
        self._count+=1;stamp=time.monotonic_ns()-self.stale_ns
        pixel=np.full((12,16,3),self._count%256,dtype=np.uint8)
        frames={k:ImageFrame(pixel,stamp-self.image_stale_ns,0,k) for k in self.cameras}
        return BackendSnapshot(stamp,dict(self.positions),frames)
    def send(self,positions,timeout_s):
        self._call('send',timeout_s);self.target=dict(positions)
        self.sent.append((time.monotonic(),dict(positions)));return dict(positions)
    def hold(self,positions,timeout_s):
        self._call('hold',timeout_s);self.target=dict(positions);self.holds.append(dict(positions))
    def close(self,*,hold,timeout_s):
        self._call('close',timeout_s);assert hold;self.connected=False
