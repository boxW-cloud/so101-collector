"""Passive scripted source for simulation tests; never opens or drives a leader."""
import math
import time
from lerobot_sdk.core.types import JOINT_NAMES, JointTarget


class ScriptedLeader:
    is_connected=True

    def __init__(self,config,positions,resource_id='scripted-leader'):
        self.config=config
        self.positions=dict(positions)
        self.resource_id=resource_id
        self.calls=0

    def get_action(self,*,timeout_s):
        self.calls+=1
        return JointTarget(self.positions,self.config.units,self.config.calibration_hash)


class TrajectoryLeader:
    """Bounded scripted trajectory source for automated simulation collection.

    Produces smooth per-episode JointTarget trajectories clipped to the robot
    contract limits; never opens hardware or touches serial. Patterns are
    functional virtual-arm motion (agent_scripted_simulation), not human
    teleoperation or task-success demonstrations.
    """

    is_connected=True

    def __init__(self,config,base,*,patterns,resource_id='trajectory-leader'):
        # config: RobotConfig; base: six named base positions within limits;
        # patterns: list of episodes, each a list of (joint, amplitude, frequency_hz, phase)
        self.config=config
        self.resource_id=resource_id
        self._base={k:float(base[k]) for k in JOINT_NAMES}
        config.validate_target(JointTarget(self._base,config.units,config.calibration_hash))
        if not patterns:raise ValueError('At least one episode pattern is required')
        for pattern in patterns:
            for joint,amp,freq,phase in pattern:
                if joint not in JOINT_NAMES:raise ValueError(f'Unknown joint {joint}')
                if not (abs(amp)>0 and freq>0):raise ValueError('Amplitude/frequency must be nonzero')
        self._origin_base=dict(self._base)
        self._patterns=list(patterns)
        self._episode=0
        self._started=None
        self.calls=0

    def begin_episode(self,index,*,base=None):
        if not 0<=index<len(self._patterns):raise ValueError('Episode pattern index out of range')
        if base is not None:
            target=JointTarget(base,self.config.units,self.config.calibration_hash)
            self.config.validate_target(target)
            self._base=dict(target.positions)
        self._episode=index
        self._started=None

    def get_action(self,*,timeout_s):
        self.calls+=1
        now=time.monotonic()
        if self._started is None:self._started=now
        t=now-self._started
        values=dict(self._base)
        for joint,amp,freq,phase in self._patterns[self._episode]:
            lo,hi=self.config.limits[joint]
            a=min(abs(amp),(hi-lo)/2)
            if joint=='gripper.pos':
                # Start at the measured opening, with zero initial velocity.
                direction=1 if self._base[joint] <= (lo+hi)/2 else -1
                available=(hi-self._base[joint]) if direction==1 else (self._base[joint]-lo)
                values[joint]=self._base[joint]+direction*min(a,available/2)*(1-math.cos(2*math.pi*freq*t))
            else:
                home=self._origin_base[joint]
                margin=min(2.0,(hi-lo)/10)
                a=min(a,max(0,min(home-lo-margin,hi-margin-home))/2)
                goal=home+a*(math.sin(2*math.pi*freq*t+phase)-math.sin(phase))
                # Smoothly join each measured start to the same home-based family,
                # so episodic offsets never accumulate at a mechanical/model limit.
                blend=1-math.exp(-max(0,t))
                values[joint]=self._base[joint]+blend*(goal-self._base[joint])
        for joint in JOINT_NAMES:
            lo,hi=self.config.limits[joint]
            values[joint]=min(max(values[joint],lo),hi)
        return JointTarget(values,self.config.units,self.config.calibration_hash)
