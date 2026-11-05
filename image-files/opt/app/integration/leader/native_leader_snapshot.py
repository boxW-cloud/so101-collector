"""Read-only, COM15-only manufacturer diagnostics; never a motion interface.

Reads the documented PRALL/PRTV queries over pyserial. Does not construct a
Seeed robot, load/write calibration, configure motors, or connect COM17.
"""
import argparse
import json
from pathlib import Path
import re
import time


def query_frame(motor, query):
    if type(motor) is not int or motor not in range(1, 7) or query not in ('PRALL', 'PRTV'):
        raise ValueError('Only ID1..6 PRALL/PRTV reads are allowed')
    return f'#{motor:03d}{query}!'.encode('ascii')


def parse_telemetry(motor, text):
    match = re.fullmatch(rf'#{motor:03d}PDA(-?\d+)P(-?\d+)T(-?\d+(?:\.\d+)?)V(\d+(?:\.\d+)?)!', text)
    if not match:
        raise ValueError('Telemetry identity or format mismatch')
    return dict(zip(('native_encoder', 'native_pulse', 'temperature_c', 'voltage'),
                    (int(match[1]), int(match[2]), float(match[3]), float(match[4]))))


def main():
    import serial
    from serial.tools import list_ports
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--pose', required=True, help='User-described physical pose, not inferred calibration')
    args = parser.parse_args()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive creation prevents overwriting previous evidence.
    with args.report.open('x', encoding='utf-8') as output:
        result = {'time': time.time(), 'port': 'COM15', 'user_pose': args.pose,
                  'read_only': True, 'control_writes_sent': 0, 'motors': {}, 'tx': []}
        try:
            dev = next((p for p in list_ports.comports() if p.device == 'COM15'), None)
            if dev is None or (dev.vid, dev.pid) != (0x1a86, 0x7523):
                raise ValueError('COM15 leader CH340 not present')
            result['usb_hwid'] = dev.hwid
            with serial.Serial('COM15', 1000000, timeout=.02, write_timeout=.25) as bus:
                def query(motor, kind):
                    frame = query_frame(motor, kind)
                    bus.reset_input_buffer()
                    bus.write(frame)
                    bus.flush()
                    result['tx'].append(frame.decode('ascii'))
                    data = bytearray()
                    deadline = time.monotonic()+1
                    while time.monotonic() < deadline and len(data) < 16000:
                        data.extend(bus.read(bus.in_waiting or 1))
                        if b'!' in data:
                            return bytes(data[:data.index(b'!')+1]).decode('ascii')
                    raise TimeoutError(f'No complete response: ID{motor} {kind}')

                for motor in range(1, 7):
                    raw = query(motor, 'PRALL')
                    prefix = f'#{motor:03d}PRALL:'
                    if not raw.startswith(prefix):
                        raise ValueError('Parameter response identity mismatch')
                    params = json.loads(raw[len(prefix):-1])
                    if params.get('ID') != motor or params.get('MDL') != 'SERVO.C':
                        raise ValueError('Different hardware: do not reuse current mapping')
                    telemetry = [parse_telemetry(motor, query(motor, 'PRTV')) for _ in range(3)]
                    result['motors'][str(motor)] = {'parameters': params, 'telemetry': telemetry}
            result['capture_passed'] = True
        except Exception as exc:
            result.update(capture_passed=False, error=repr(exc))
        finally:
            json.dump(result, output, ensure_ascii=False, indent=2)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result['capture_passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
