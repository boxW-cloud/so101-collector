"""Pure planning for conservative Isaac hard limits; no simulation or I/O.

Call after reading the actual articulation and action-term joint names. This
helper never transforms observations or weakens the SDK feedback contract.
Only the calling harness may apply the returned radians through Isaac's API.
"""
from collections.abc import Mapping
import math
from numbers import Real

import numpy as np


SDK_NAMES = ('shoulder_pan.pos', 'shoulder_lift.pos', 'elbow_flex.pos',
             'wrist_flex.pos', 'wrist_roll.pos', 'gripper.pos')
ACTION_NAMES = ('Rotation', 'Pitch', 'Elbow', 'Wrist_Pitch', 'Wrist_Roll', 'Jaw')


def _names(value, expected, label, *, ordered):
    if isinstance(value, (str, bytes)):
        raise ValueError(label + ' must be six distinct joint names')
    try:
        result = tuple(value)
    except TypeError as error:
        raise ValueError(label + ' must be six distinct joint names') from error
    if (len(result) != 6 or any(not isinstance(v, str) for v in result)
            or len(set(result)) != 6 or set(result) != set(expected)):
        raise ValueError(label + ' has missing, duplicate or unknown joint names')
    if ordered and result != expected:
        raise ValueError(label + ' order differs from the named six-axis action contract')
    return result


def _number(value, label):
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, Real):
        raise ValueError(label + ' must contain finite real numbers')
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(label + ' must contain finite real numbers')
    return value


def compute_joint_bounds(*, sdk_names, action_names, asset_names,
                         original_limits_rad, sdk_limits, sdk_units):
    """Return ``(new_limits_rad, audit)`` in the actual asset joint order.

    ``original_limits_rad`` must be [6, 2] or [N, 6, 2]. Its shape is retained.
    The SDK uses degrees for the first five axes and normalized_0_100 for Jaw.
    The fixed Jaw calibration maps 0..100 to -10..100 degrees. Intersect the
    original physical limits with these SDK bounds before reserving 0.25
    degrees for body joints or 0.5 normalized units (0.55 degrees) for Jaw.
    Invalid identities, units, numbers and empty/reserve-collapsed ranges fail
    the entire plan. Input tensors, configuration and sensor data are unmodified.
    """
    sdk = _names(sdk_names, SDK_NAMES, 'sdk_names', ordered=True)
    actions = _names(action_names, ACTION_NAMES, 'action_names', ordered=True)
    assets = _names(asset_names, ACTION_NAMES, 'asset_names', ordered=False)
    for value, label in ((sdk_limits, 'sdk_limits'), (sdk_units, 'sdk_units')):
        if not isinstance(value, Mapping) or set(value) != set(sdk):
            raise ValueError(label + ' must describe exactly the six SDK joints')
    expected_units = ('deg',) * 5 + ('normalized_0_100',)
    if tuple(sdk_units[name] for name in sdk) != expected_units:
        raise ValueError('Unsupported joint units: body requires deg and Jaw normalized_0_100')

    try:
        source = np.asarray(original_limits_rad, dtype=object)
    except (TypeError, ValueError) as error:
        raise ValueError('Physical limits must have shape [6, 2] or [N, 6, 2]') from error
    if not (source.shape == (6, 2) or
            source.ndim == 3 and source.shape[0] > 0 and source.shape[1:] == (6, 2)):
        raise ValueError('Physical limits must have shape [6, 2] or [N, 6, 2]')
    original = np.array([_number(v, 'Physical limits') for v in source.flat],
                        dtype=np.float64).reshape(source.shape)
    original = original[None, ...] if original.ndim == 2 else original
    if np.any(original[..., 0] >= original[..., 1]):
        raise ValueError('Physical joint limits must be strictly ordered')

    physical_deg = np.rad2deg(original)
    result_deg = physical_deg.copy()
    rows = []
    for action_index, (name, sim_name) in enumerate(zip(sdk, actions, strict=True)):
        pair = sdk_limits[name]
        if isinstance(pair, (str, bytes)):
            raise ValueError('SDK limits must contain two ordered numbers: ' + name)
        try:
            if len(pair) != 2:
                raise ValueError('SDK limits must contain two ordered numbers: ' + name)
            lo, hi = (_number(v, 'SDK limits') for v in pair)
        except TypeError as error:
            raise ValueError('SDK limits must contain two ordered numbers: ' + name) from error
        if lo >= hi:
            raise ValueError('SDK limits must be strictly ordered: ' + name)
        is_jaw = action_index == 5
        if is_jaw and not 0 <= lo < hi <= 100:
            raise ValueError('Jaw SDK limits must remain within normalized 0..100')
        sdk_deg = np.array([lo, hi], dtype=np.float64)
        if is_jaw:
            sdk_deg = sdk_deg * 1.1 - 10.0
        asset_index = assets.index(sim_name)
        intersection = np.column_stack((
            np.maximum(physical_deg[:, asset_index, 0], sdk_deg[0]),
            np.minimum(physical_deg[:, asset_index, 1], sdk_deg[1])))
        margin_deg = .55 if is_jaw else .25
        narrowed = intersection + np.array([margin_deg, -margin_deg])
        if np.any(narrowed[:, 1] - narrowed[:, 0] <= 1e-12):
            raise ValueError('Joint range has no room for the conservative reserve: ' + name)
        result_deg[:, asset_index] = narrowed
        rows.append({'sdk_name': name, 'sim_name': sim_name, 'asset_index': asset_index,
            'action_index': action_index, 'sdk_unit': sdk_units[name], 'sdk_limits': [lo, hi],
            'sdk_limits_deg': sdk_deg.tolist(),
            'original_limits_rad': original[:, asset_index].tolist(),
            'intersection_deg': intersection.tolist(), 'new_limits_deg': narrowed.tolist(),
            'margin': {'value': .5 if is_jaw else .25,
                       'unit': 'normalized_0_100' if is_jaw else 'deg', 'degrees': margin_deg}})
    result = np.deg2rad(result_deg)
    if source.ndim == 2:
        result = result[0]
    audit = {'schema': 'so101.isaac.conservative_joint_bounds.v1', 'units': 'rad',
             'environment_count': original.shape[0], 'sdk_names': list(sdk),
             'action_names': list(actions), 'asset_names': list(assets), 'mapping': rows}
    return result, audit
