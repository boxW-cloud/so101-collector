"""Keep accepted absolute calibration; require explicit consent for startup catch-up."""
from .protocol import ApiError

def preflight_alignment(robot,leader,*,explicit=False):
    first=leader.get_action(timeout_s=.25);current=dict(robot.get_arm_data().positions)
    thresholds={name:3.0 for name in current}
    errors={name:abs(first.positions[name]-current[name]) for name in current}
    required=any(errors[name]>thresholds[name] for name in current)
    report={'required':required,'completed':not required,'phase':'ALIGNING' if required else 'ALIGNED',
        'initial_error_by_joint':errors,'current_positions':current,'leader_target':dict(first.positions),'thresholds':thresholds,
        'mapping':'accepted_absolute_three_point; no automatic relative remapping'}
    if required and not explicit:
        raise ApiError('ALIGNMENT_REQUIRED','Initial leader and simulator poses differ; explicitly align the virtual arm before following',details=report)
    return first,report
