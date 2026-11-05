"""One repository entrypoint for grouped integration scripts; no eager imports."""
import argparse
import json
from pathlib import Path
import runpy
import sys

ROOT=Path(__file__).resolve().parent
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--list',action='store_true',help='List scripts without executing them')
    parser.add_argument('script',nargs='?')
    parser.add_argument('arguments',nargs=argparse.REMAINDER)
    args=parser.parse_args()
    index=json.loads((ROOT/'layout.json').read_text())
    if args.list:
        for name,path in sorted(index.items()):
            if name.endswith('.py'):print(f'{name:38} {path}')
        return
    name=args.script or ''
    if not name.endswith('.py'):name+='.py'
    if name not in index or not name.endswith('.py'):parser.error('Choose a script from --list')
    # Bare sibling imports remain supported for the historical standalone tools.
    sys.path.insert(0,str(ROOT.parent))
    for directory in sorted({str(ROOT/Path(p).parent) for p in index.values()}):sys.path.insert(0,directory)
    target=ROOT/name if (ROOT/name).is_file() else ROOT/index[name]
    if not target.resolve().is_relative_to(ROOT) or not target.is_file():parser.error('Script path leaves the integration directory or is missing')
    sys.argv=[str(target),*args.arguments]
    runpy.run_path(str(target),run_name='__main__')

if __name__=='__main__':main()
