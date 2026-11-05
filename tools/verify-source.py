#!/usr/bin/env python3
"""Verify released collection payload without importing hardware or services."""
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent.parent
release = json.loads((root / 'release.json').read_text(encoding='utf-8'))
errors = []
for name, expected in release['source_files'].items():
    p = root / name
    if not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest() != expected:
        errors.append(name)
if errors:
    raise SystemExit('Source differs from release: ' + ', '.join(errors))
print(f"{release['release']}: {len(release['source_files'])} collection files verified")
