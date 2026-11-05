"""Metadata API name/type adapter; execute the frozen upstream converter unchanged."""
import argparse,runpy,sys
from pathlib import Path

def main():
    p=argparse.ArgumentParser();p.add_argument('--script',required=True);a,rest=p.parse_known_args()
    from lerobot_sdk.adapters.seeed_dataset import upstream
    upstream()
    from lerobot.datasets import utils,io_utils
    from lerobot.utils.utils import unflatten_dict
    if not hasattr(utils,'load_info'):utils.load_info=lambda path:io_utils.load_info(path).to_dict()
    if not hasattr(utils,'write_info'):utils.write_info=lambda value,path:io_utils.write_info(utils.DatasetInfo.from_dict(value),path)
    if not hasattr(utils,'load_tasks'):utils.load_tasks=io_utils.load_tasks
    if not hasattr(utils,'unflatten_dict'):utils.unflatten_dict=unflatten_dict
    sys.argv=[a.script,*rest];runpy.run_path(a.script,run_name='__main__')
if __name__=='__main__':main()
