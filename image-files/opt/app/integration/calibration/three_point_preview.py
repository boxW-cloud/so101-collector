"""Pure per-axis three-position interpolation for the simulation preview."""
import math


def mapped_degrees(native, axis):
    if type(native) not in (int, float) or not math.isfinite(native):
        raise ValueError('Invalid native position')
    low, center, high = axis['native_min'], axis['native_center'], axis['native_max']
    if not low < center < high:
        raise ValueError('Center must lie strictly between measured endpoints')
    targets = axis['three_point_sim_deg']
    if len(targets) != 3 or not all(math.isfinite(v) for v in targets):
        raise ValueError('Three finite virtual targets required')
    clipped = min(high, max(low, native))
    if clipped <= center:
        value = targets[0]+(clipped-low)/(center-low)*(targets[1]-targets[0])
    else:
        value = targets[1]+(clipped-center)/(high-center)*(targets[2]-targets[1])
    return value, clipped != native
