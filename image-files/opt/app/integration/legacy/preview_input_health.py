"""Receiver-clock input health; video freshness does not imply a live leader."""
import math


def input_health(packet, now):
    if not packet:
        return {'input_live': False, 'reader_remaining_s': None, 'input_reason': 'NO_INPUT'}
    received = packet.get('received_monotonic')
    remaining = packet.get('source_remaining_s')
    if not isinstance(received, (int, float)) or isinstance(received, bool) or not math.isfinite(received):
        return {'input_live': False, 'reader_remaining_s': None, 'input_reason': 'INVALID_TIMESTAMP'}
    age = now-received
    if age < 0:
        return {'input_live': False, 'reader_remaining_s': None, 'input_reason': 'INVALID_TIMESTAMP'}
    known = type(remaining) in (int, float) and math.isfinite(remaining) and remaining >= 0
    left = max(0., remaining-age) if known else None
    live = age < .8 and (left is None or left > 0)
    reason = 'LIVE' if live else ('WINDOW_EXPIRED' if left == 0 else 'INPUT_STALE')
    return {'input_live': live, 'reader_remaining_s': None if left is None else round(left, 1), 'input_reason': reason}
