"""Backward-compatible import and python -m entry point."""
if __name__ == '__main__':
    from runpy import run_module
    run_module('lerobot_sdk.learning.worker', run_name='__main__')
else:
    from importlib import import_module
    import sys
    sys.modules[__name__] = import_module('lerobot_sdk.learning.worker')
