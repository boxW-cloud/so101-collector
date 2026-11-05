"""Bounded passive range recording for the user-confirmed COM15 leader.

Only manufacturer PRALL/PRTV queries. Records an observed comfortable range,
not mechanical limits or accepted absolute-angle calibration.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import time

from native_leader_snapshot import query_frame, parse_telemetry


def main():
    import serial
    from serial.tools import list_ports
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--seconds', type=float, default=90)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 120:
        parser.error('Recording duration must be 1..120 seconds')
    ref = json.loads(args.reference.read_text(encoding='utf-8'))
    if not ref.get('capture_passed') or ref.get('port') != 'COM15':
        raise ValueError('A successful COM15 reference snapshot is required')
    args.output.mkdir(parents=True, exist_ok=False)
    report = {'started_utc': datetime.now(timezone.utc).isoformat(),
              'port': 'COM15', 'reference': str(args.reference.resolve()),
              'read_only': True, 'control_writes_sent': 0, 'requested_duration_s': args.seconds,
              'frames': 0, 'query_count': 0, 'observed_ranges': {},
              'calibration_accepted': False, 'range_kind': 'user-demonstrated comfortable range'}
    for i in map(str, range(1, 7)):
        v = ref['motors'][i]['telemetry'][-1]['native_encoder']
        report['observed_ranges'][i] = {'reference': v, 'min': None, 'max': None}
    try:
        dev = next((p for p in list_ports.comports() if p.device == 'COM15'), None)
        if not dev or (dev.vid, dev.pid) != (0x1a86, 0x7523) or dev.hwid != ref['usb_hwid']:
            raise ValueError('Leader USB identity differs from the reference snapshot')
        with serial.Serial('COM15', 1000000, timeout=.02, write_timeout=.25) as bus:
            def query(motor, kind):
                frame = query_frame(motor, kind)
                bus.reset_input_buffer()
                bus.write(frame)
                bus.flush()
                report['query_count'] += 1
                data = bytearray()
                deadline = time.monotonic()+1
                while time.monotonic() < deadline and len(data) < 16000:
                    data.extend(bus.read(bus.in_waiting or 1))
                    if b'!' in data:
                        return bytes(data[:data.index(b'!')+1]).decode('ascii')
                raise TimeoutError(f'ID{motor} {kind} timeout; capture stops')

            def verify_parameters():
                for i in range(1, 7):
                    raw = query(i, 'PRALL')
                    prefix = f'#{i:03d}PRALL:'
                    if not raw.startswith(prefix) or json.loads(raw[len(prefix):-1]) != ref['motors'][str(i)]['parameters']:
                        raise ValueError(f'ID{i} parameters differ from reference; no automatic acceptance')

            verify_parameters()
            start = time.monotonic()
            print(json.dumps({'event': 'READY_RECORDING', 'duration_s': args.seconds}), flush=True)
            with (args.output/'samples.jsonl').open('x', encoding='utf-8') as samples:
                while time.monotonic()-start < args.seconds:
                    tick = time.monotonic()
                    row = {'t_s': tick-start, 'joints': {}}
                    for i in range(1, 7):
                        value = parse_telemetry(i, query(i, 'PRTV'))
                        v = value['native_encoder']
                        if not 0 <= v <= 4095:
                            raise ValueError(f'ID{i} encoder outside expected domain; investigate wrap/mode')
                        value['sampled_t_s'] = time.monotonic()-start
                        row['joints'][str(i)] = value
                    # Publish a complete six-axis sample only.
                    samples.write(json.dumps(row)+'\n')
                    samples.flush()
                    for i, value in row['joints'].items():
                        stat = report['observed_ranges'][i]
                        v = value['native_encoder']
                        stat['min'] = v if stat['min'] is None else min(stat['min'], v)
                        stat['max'] = v if stat['max'] is None else max(stat['max'], v)
                    report['frames'] += 1
                    time.sleep(max(0, .2-(time.monotonic()-tick)))
            report['actual_duration_s'] = time.monotonic()-start
            verify_parameters()
            report['parameters_unchanged'] = True
            report['capture_passed'] = report['frames'] > 0
    except (Exception, KeyboardInterrupt) as exc:
        report.update(capture_passed=False, error=repr(exc))
    finally:
        for stat in report['observed_ranges'].values():
            if stat['min'] is not None:
                stat.update(span=stat['max']-stat['min'],
                            below_reference=stat['reference']-stat['min'],
                            above_reference=stat['max']-stat['reference'])
        (args.output/'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False), flush=True)
    return 0 if report.get('capture_passed') else 1


if __name__ == '__main__':
    raise SystemExit(main())
