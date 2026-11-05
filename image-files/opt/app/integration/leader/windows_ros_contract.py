"""Conservative Linux timestamp for a Windows scan inside a Linux challenge."""
import math

def ros_sample(packet,profile_hash,clock_id,previous,now_ns):
    if packet.get('profile_hash')!=profile_hash:raise ValueError('Wrong leader profile')
    seq=packet.get('sequence');raw=packet.get('native_positions')
    if type(seq) is not int or seq<=previous:raise ValueError('Old leader sequence')
    if not isinstance(raw,dict) or set(raw)!=set('123456') or any(type(v) is not int or not 0<=v<=4095 for v in raw.values()):raise ValueError('Bad encoder data')
    expiry=packet.get('expires_monotonic');received=packet.get('received_monotonic')
    if any(type(v) not in (int,float) or not math.isfinite(v) for v in (expiry,received)):raise ValueError('Bad timestamps')
    # Challenge issue time is BEFORE the Windows scan. Using it overestimates
    # sample age and cannot renew an old sample's lifetime on this relay.
    stamp=round((expiry-.8)*1e9)
    if not stamp<=round(received*1e9)<=now_ns or not 0<=now_ns-stamp<800_000_000:raise ValueError('Expired or future input')
    return {'profile_hash':profile_hash,'clock_id':clock_id,'sequence':seq,'captured_monotonic_ns':stamp,'native_positions':raw}
