"""Resolve grouped sources or export a standalone legacy-layout deployment.

Only copies files; never imports executable simulator or hardware modules.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parent

def source_path(name, root=ROOT):
    root = Path(root).resolve()
    if not isinstance(name, str) or Path(name).name != name or '/' in name or '\\' in name:
        raise ValueError('Expected one integration basename')
    path = root / name
    if not path.is_file():
        index = root / 'layout.json'
        mapping = json.loads(index.read_text()) if index.is_file() else {}
        path = root / mapping.get(name, name)
    if not path.is_file() or path.is_symlink() or not path.resolve().is_relative_to(root):
        raise FileNotFoundError(name)
    return path

def export_flat(destination, root=ROOT):
    root = Path(root).resolve()
    destination = Path(destination).resolve()
    if destination == root or destination.is_relative_to(root):
        raise ValueError('Export must use a separate directory')
    if destination.exists():
        raise FileExistsError('Refuse to overwrite deployment output')
    index = root / 'layout.json'
    mapping = json.loads(index.read_text()) if index.exists() else {}
    names = set(mapping) | {p.name for p in root.iterdir() if p.is_file() and p.suffix in ('.py','.json','.xml')}
    files = {name: source_path(name, root) for name in names}
    destination.mkdir(parents=True)
    hashes = {}
    for name, path in files.items():
        shutil.copy2(path, destination/name)
        hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    (destination/'deployment-manifest.json').write_text(json.dumps(hashes,indent=2),encoding='utf-8')
    return hashes

if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--export-flat',type=Path,required=True)
    args=parser.parse_args()
    print(json.dumps({'files':len(export_flat(args.export_flat)),'output':str(args.export_flat)}))
