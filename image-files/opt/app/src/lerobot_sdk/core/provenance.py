"""Optional bounded JSON source evidence; no device or platform dependencies."""
from collections.abc import Mapping
import json

from .errors import ContractMismatch

MAX_PROVENANCE_BYTES = 32768


def bounded_metadata(value):
    if value is None:return None
    if not isinstance(value, Mapping):raise ContractMismatch('Source provenance must be a JSON object')
    def plain(item, depth=0):
        if depth>24:raise ContractMismatch('Source provenance nesting exceeds its bound')
        if isinstance(item, Mapping):
            if any(not isinstance(key,str) for key in item):raise ContractMismatch('Source provenance keys must be strings')
            return {key:plain(child,depth+1) for key,child in item.items()}
        if isinstance(item,(list,tuple)):return [plain(child,depth+1) for child in item]
        if item is None or type(item) in (str,int,float,bool):return item
        raise ContractMismatch('Source provenance contains a non-JSON value')
    try:
        text=json.dumps(plain(value),sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
    except (ValueError,TypeError,RecursionError) as error:
        raise ContractMismatch('Source provenance is not finite bounded JSON') from error
    if len(text.encode('utf-8'))>MAX_PROVENANCE_BYTES:raise ContractMismatch('Source provenance exceeds 32 KiB')
    return json.loads(text)
