"""Isaac gripper radians/degree observation to the SDK bounded opening unit."""
import math

def normalized_gripper_feedback(degrees):
    if not math.isfinite(degrees):raise ValueError('Non-finite gripper feedback')
    value=(degrees+10)/110*100
    # Bounded float32 endpoint-roundoff budget, not a joint-limit relaxation.
    if not -1e-4 <= value <= 100+1e-4:
        raise ValueError('Gripper feedback outside numerical endpoint budget')
    bounded=max(0.0,min(100.0,value))
    return bounded, bounded != value
