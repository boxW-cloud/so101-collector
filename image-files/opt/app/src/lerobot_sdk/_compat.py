"""Compatibility for pre-layout imports; aliases share the real module object.

No copies, forwarding class wrappers, or meta-path import hooks. This preserves
old module imports and monkeypatch identity; new implementation imports use the
canonical grouped paths directly. CLI modules retain physical entry shims.
"""
from importlib import import_module
import sys

LEGACY_MODULES = {'types': 'core.types', 'errors': 'core.errors', 'events': 'core.events', 'locking': 'core.locking', 'operation': 'core.operation', 'async_operation': 'core.async_operation', 'robot': 'control.robot', 'backend': 'control.backend', 'collection': 'data.collection', 'collection_types': 'data.contracts', 'datasets': 'data.manager', '_dataset_fs': 'data.filesystem', 'dataset_migration': 'data.migration', 'dataset_transfer': 'data.transfer', 'dataset_sinks': 'data.sinks', 'training': 'learning.trainer', 'training_types': 'learning.contracts', 'training_runtime': 'learning.runtime', 'models': 'policies.models', 'policy_session': 'policies.session', 'inference': 'policies.runner', 'inference_types': 'policies.contracts'}

def install(package_name):
    package = sys.modules[package_name]
    for old, new in LEGACY_MODULES.items():
        module = import_module('.' + new, package_name)
        sys.modules[package_name + '.' + old] = module
        setattr(package, old, module)
