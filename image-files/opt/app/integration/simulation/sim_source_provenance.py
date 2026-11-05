"""Add simulation ROS/scene provenance as a new immutable dataset version."""
from pathlib import Path
import argparse,json
from lerobot_sdk import DatasetManager
from integration.ros_sim_contract import validate_contract,contract_hash

def attach_source(workspace,parameters):
    (workspace/'meta/simulation-source.json').write_text(json.dumps(parameters,indent=2),encoding='utf-8')
    return {}

def main():
    p=argparse.ArgumentParser();p.add_argument('--dataset',type=Path,required=True);p.add_argument('--target',type=Path,required=True);p.add_argument('--ros-contract',type=Path,required=True);p.add_argument('--scene-record',type=Path,required=True);p.add_argument('--report',type=Path,required=True);a=p.parse_args()
    c=validate_contract(json.loads(a.ros_contract.read_text()));scene=json.loads(a.scene_record.read_text())
    source={'source_kind':'agent_scripted_simulation','ros_contract':c,'ros_contract_hash':contract_hash(c),'scene':scene,
            'time_source':c['time_source'],'real_robot_connected':False}
    manager=DatasetManager();original=manager.inspect(a.dataset)
    result=manager.migrate(original.ref,a.target,attach_source,parameters=source).wait(180)
    checked=manager.inspect(result);assert checked.frames==original.frames
    a.report.write_text(json.dumps({'passed':True,'original_hash':original.ref.content_hash,'final_hash':result.content_hash,'root':str(result.root),'frames':checked.frames,'episodes':checked.episodes,'source':source},indent=2))
if __name__=='__main__':main()
