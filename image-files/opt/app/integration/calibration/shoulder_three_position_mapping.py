"""ID2 three-position control coordinates, using only restart-02 anchors.

The endpoint angles are coordinate definitions, not measured physical angles.
No hardware imports, motor configuration, or active simulator modifications.
"""
import math

FRONT = 885
CENTER = 2065
END_A = 3272


def normalized_position(native_encoder: float) -> float:
    """Front +100, user center 0, endpoint A -100; reject unmeasured range."""
    if type(native_encoder) not in (int, float) or not math.isfinite(native_encoder):
        raise ValueError('A finite native encoder count is required')
    if not FRONT <= native_encoder <= END_A:
        raise ValueError('Outside recorded ID2 endpoint range [885, 3272]')
    span = CENTER-FRONT if native_encoder <= CENTER else END_A-CENTER
    return (CENTER-native_encoder)*100/span


def control_angle_deg(native_encoder: float) -> float:
    """Defined coordinates: endpoint A 0, center 90, front 180 degrees."""
    return 90 + .9*normalized_position(native_encoder)


def encoder_from_normalized(value: float) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or not -100 <= value <= 100:
        raise ValueError('Normalized control position must be in [-100, 100]')
    span = CENTER-FRONT if value >= 0 else END_A-CENTER
    return CENTER-value*span/100
