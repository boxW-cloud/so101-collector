"""Stable integration module names with physically grouped source files.

Adding search directories does not import or execute hardware/Isaac scripts.
Callers keep using integration.ros_sim_contract, integration.action_primitives,
etc. Directory names organize implementation, not new public module aliases.
"""
import json as _json
from pathlib import Path as _Path

_root = _Path(__file__).resolve().parent
_index = _root / 'layout.json'
if _index.is_file():
    for _group in sorted({_Path(value).parent for value in _json.loads(_index.read_text()).values()}):
        _directory = (_root / _group).resolve()
        if _directory.is_relative_to(_root) and _directory.is_dir():
            __path__.append(str(_directory))
