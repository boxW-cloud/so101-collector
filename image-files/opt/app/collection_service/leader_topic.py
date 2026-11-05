"""Validate real-leader Topic samples before passing a fresh snapshot to SDK."""
import time
from .protocol import verified

def accept_leader(message,key,profile_hash,clock_id,previous,*,now_ns=None):
    value=verified(message,key)
    if set(value)!= {'profile_hash','clock_id','sequence','captured_monotonic_ns','native_positions'}:raise ValueError('Leader fields differ')
    if value['profile_hash']!=profile_hash or value['clock_id']!=clock_id:raise ValueError('Leader profile/clock identity mismatch')
    now_ns=time.monotonic_ns() if now_ns is None else now_ns
    stamp=value['captured_monotonic_ns'];seq=value['sequence'];raw=value['native_positions']
    if type(stamp) is not int or not 0<=now_ns-stamp<800_000_000:raise ValueError('Leader timestamp stale/future')
    if type(seq) is not int or seq<=previous:raise ValueError('Old leader sequence')
    if not isinstance(raw,dict) or set(raw)!=set('123456') or any(type(v) is not int or not 0<=v<=4095 for v in raw.values()):raise ValueError('Invalid six-axis encoder sample')
    return {'profile_hash':profile_hash,'sequence':seq,'native_positions':raw,'received_monotonic':stamp/1e9,'expires_monotonic':(stamp+800_000_000)/1e9}
