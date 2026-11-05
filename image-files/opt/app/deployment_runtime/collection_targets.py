"""Resolve only explicit identities or deployment-pinned project/mode bindings.

No registry listing, latest-revision choice or metadata inference. Callers keep
their raw request for idempotency and validate the resolved explicit core body.
"""
import copy

from .store import Fault,ident


COLLECTION_MODES=('simulation_teleop','physical_teleop')


def _fixed_binding(platform,project,mode):
    targets=platform.config.get('collection_targets')
    if not isinstance(targets,dict) or project not in targets:
        raise Fault('COLLECTION_TARGET_NOT_CONFIGURED','Project has no fixed collection target binding')
    configured=targets[project]
    if not isinstance(configured,dict):raise Fault('COLLECTION_TARGET_CONFIG_INVALID','Project collection binding must be an object',500)
    if mode is None:
        mode=configured.get('default_mode')
        if mode is None:raise Fault('COLLECTION_TARGET_NOT_CONFIGURED','Project default_mode is not configured')
        if mode not in COLLECTION_MODES:raise Fault('COLLECTION_TARGET_CONFIG_INVALID','Configured default_mode is not a supported teleoperation mode',500)
    if mode not in configured:raise Fault('COLLECTION_TARGET_NOT_CONFIGURED','Project has no fixed binding for '+mode)
    binding=configured[mode]
    if not isinstance(binding,dict) or set(binding)!={'robot_id','robot_revision'}:
        raise Fault('COLLECTION_TARGET_CONFIG_INVALID','Fixed target requires robot_id and robot_revision only',500)
    try:ident(binding['robot_id']);ident(binding['robot_revision'])
    except Fault as error:raise Fault('COLLECTION_TARGET_CONFIG_INVALID','Configured target identity is invalid',500) from error
    return mode,copy.deepcopy(binding)


def configured_mode(platform,project):
    """v10r12：一台采集容器只采一台机器人，部署配置里只绑定这一个目标；返回它的采集模式（不再由请求选）。
    写了 default_mode 就用它；没写而且只绑定了一个模式，就是那一个。"""
    targets=platform.config.get('collection_targets')
    configured=targets.get(project) if isinstance(targets,dict) else None
    if isinstance(configured,dict) and 'default_mode' not in configured:
        bound=[mode for mode in COLLECTION_MODES if mode in configured]
        if len(bound)==1:return bound[0]
    return _fixed_binding(platform,project,None)[0]


def resolve_target(platform,p,a,*,mode=None):
    """Return a copied explicit payload and its registered, authorized robot.

    ``mode`` constrains/defaults an operation such as simulation alignment. Only
    robot identity and mode may be filled; scene/calibration/FPS remain caller
    assertions. Explicit paired identity preserves legacy calls without defaults.
    """
    if not isinstance(p,dict) or 'project' not in p:raise Fault('INVALID_FIELDS','project is required',400)
    p=copy.deepcopy(p);ident(p['project']);a.require(p['project'],'operator')
    supplied=('robot_id' in p,'robot_revision' in p)
    if any(supplied) and not all(supplied):raise Fault('INCOMPLETE_TARGET','robot_id and robot_revision must be supplied together',400)
    selected=p.get('mode',mode)
    if 'mode' in p and selected is None:raise Fault('INVALID_MODE','Explicit mode cannot be null',400)
    if selected is not None and selected not in COLLECTION_MODES:raise Fault('INVALID_MODE','Supported collection teleoperation mode required',400)
    if mode is not None and selected!=mode:raise Fault('INVALID_MODE','Mode differs from the requested operation',400)
    if all(supplied):
        ident(p['robot_id']);ident(p['robot_revision'])
    else:
        selected,binding=_fixed_binding(platform,p['project'],selected)
        p.update(binding)
    robot=platform._robot(p,a)
    selected=selected or robot['kind']+'_teleop'
    if selected not in COLLECTION_MODES or selected.split('_',1)[0]!=robot['kind']:
        raise Fault('WRONG_TARGET','Registered target kind differs from collection mode')
    p['mode']=selected
    return p,robot
