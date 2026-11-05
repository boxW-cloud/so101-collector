"""Read-only COM16 telemetry to the dedicated Spark collection receiver."""
import argparse
import json
import os
from pathlib import Path
import time
from http.client import HTTPConnection, RemoteDisconnected, IncompleteRead
from urllib.parse import urlsplit
import serial
from serial.tools import list_ports
try:
    from integration.native_leader_snapshot import query_frame, parse_telemetry
except ModuleNotFoundError:
    from native_leader_snapshot import query_frame, parse_telemetry


class InputReceiverRejected(ValueError):
    def __init__(self, status, code):
        super().__init__('Input receiver rejected request: HTTP ' + str(status) +
            (' ' + code if isinstance(code, str) else ''))
        self.status = status
        self.code = code


class LeaderInputConnection:
    """One persistent TCP stream; connection setup precedes challenge issuance.

    A timeout closes the stream. A caller may acquire a new challenge, but must
    never retry a consumed/expired packet or extend its 0.8 second validity.
    """
    def __init__(self, url, token):
        parsed = urlsplit(url)
        if (parsed.scheme != 'http' or not parsed.hostname or parsed.username or parsed.password
                or parsed.path not in ('', '/') or parsed.query or parsed.fragment):
            raise ValueError('Registered HTTP receiver origin required')
        self.connection = HTTPConnection(parsed.hostname, parsed.port or 80, timeout=10.0)
        self.token = token
        self.recovery_deadline = None

    def _timeout(self, maximum):
        if self.recovery_deadline is None:
            return maximum
        remaining = self.recovery_deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError('Input link recovery exceeded 30 seconds')
        return min(maximum, remaining)

    def request(self, route, value=None):
        if route not in ('/challenge', '/input'):
            raise ValueError('Only native input receiver routes are allowed')
        try:
            if self.connection.sock is None:
                # Slow WiFi SYN setup must happen before the receiver creates
                # the short-lived challenge, then every request remains bounded.
                self.connection.timeout = self._timeout(10.0)
                self.connection.connect()
            # An acknowledgement may arrive after the packet's validity. The
            # receiver/consumer still reject expired input independently; this
            # only bounds waiting for the actual acknowledgement.
            self.connection.sock.settimeout(self._timeout(1.0))
            self.connection.request('GET' if value is None else 'POST', route,
                body=None if value is None else json.dumps(value).encode(),
                headers={'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'})
            response = self.connection.getresponse()
            length = response.getheader('Content-Length')
            if length is None or not 0 < int(length) <= 4096:
                raise ValueError('Receiver response length outside bound')
            data = response.read()
            receipt = json.loads(data)
            if response.status != 200:
                code = receipt.get('code') if isinstance(receipt, dict) else None
                raise InputReceiverRejected(response.status, code)
            return receipt
        except BaseException:
            self.close()
            raise

    def close(self):
        self.connection.close()


class NativeInputStream:
    """Recover read-only network input; serial/protocol errors remain fatal.

    Failed POST outcome is unknown: abandon that packet. Every attempt obtains
    a new challenge, takes a fresh six-axis scan, then increases its sequence.
    Successful ACKs may carry the next one-use challenge to avoid another GET.
    Nothing here starts/resumes SDK recording or issues simulation commands.
    """
    def __init__(self, client, profile_hash, *, sequence, status, publish,
                 clock=time.monotonic, sleep=time.sleep):
        self.client = client; self.profile_hash = profile_hash; self.sequence = sequence
        self.status = status; self.publish = publish; self.clock = clock; self.sleep = sleep
        self.failure_since = None
        self.next_challenge = None
        self.last_ack_monotonic = None

    def _http(self, route, packet=None):
        if self.failure_since is not None and self.clock() - self.failure_since >= 30:
            raise RuntimeError('Input link recovery exceeded 30 seconds')
        self.client.recovery_deadline = None if self.failure_since is None else self.failure_since + 30
        try:
            return self.client.request(route, packet)
        except (OSError, RemoteDisconnected, IncompleteRead, InputReceiverRejected) as error:
            if isinstance(error, InputReceiverRejected) and not (
                    route == '/input' and error.status == 400
                    and error.code in ('CHALLENGE_EXPIRED', 'SOURCE_EXPIRED')):
                raise
            self.client.close()
            self.next_challenge = None
            if self.failure_since is None:
                self.failure_since = self.clock()
            elapsed = self.clock() - self.failure_since
            if elapsed >= 30:
                raise RuntimeError('Input link recovery exceeded 30 seconds') from error
            self.status.update(state='RECONNECTING', error=repr(error),
                reconnect_elapsed_s=elapsed, reconnect_budget_s=30,
                reconnect_attempts=self.status.get('reconnect_attempts', 0) + 1,
                rejection_code=getattr(error, 'code', None))
            self.publish()
            self.sleep(min(.2, max(0, 30 - elapsed)))
            return None

    def step(self, scan):
        challenge = self.next_challenge
        self.next_challenge = None  # Consume locally exactly once, even on failure.
        if challenge is None:
            challenge = self._http('/challenge')
        if challenge is None:
            return None
        begin = self.clock()
        readings = scan()  # Serial reads deliberately outside transport recovery.
        duration = self.clock() - begin
        self.sequence += 1
        packet = {'sequence': self.sequence, 'challenge': challenge['challenge'],
            'request_next_challenge': True,
            'challenge_issued_monotonic': challenge.get('challenge_issued_monotonic'),
            'valid_for_s': challenge.get('valid_for_s'),
            'profile_hash': self.profile_hash,
            'native_positions': {k: v['native_encoder'] for k, v in readings.items()},
            'scan_duration_s': duration}
        ack = self._http('/input', packet)
        if ack is None:
            return None
        if ack['accepted_sequence'] != self.sequence:
            raise ValueError('Input acknowledgement mismatch')
        next_challenge = ack.get('next_challenge')
        if next_challenge is not None:
            lifetime = next_challenge.get('valid_for_s') if isinstance(next_challenge, dict) else None
            if (not isinstance(next_challenge, dict) or not isinstance(next_challenge.get('challenge'), str)
                    or not next_challenge['challenge'] or type(lifetime) not in (int, float)
                    or not .1 <= lifetime <= 2):
                raise ValueError('Invalid next challenge receipt')
            self.next_challenge = next_challenge
        ack_at = self.clock()
        gap = None if self.last_ack_monotonic is None else ack_at - self.last_ack_monotonic
        self.last_ack_monotonic = ack_at
        self.status['ack_gap_s'] = gap
        if gap is not None:
            self.status['max_ack_gap_s'] = max(self.status.get('max_ack_gap_s', 0), gap)
        self.failure_since = None; self.client.recovery_deadline = None
        self.status.pop('error', None); self.status.pop('reconnect_elapsed_s', None)
        self.status.pop('rejection_code', None)
        return packet, readings, duration


def publish_status(root, status):
    """Diagnostic-file sharing contention must not kill the live input stream."""
    temp = root / 'producer-status.tmp'
    temp.write_text(json.dumps(status), encoding='utf-8')
    for attempt in range(4):
        try:
            temp.replace(root / 'producer-status.json')
            return True
        except PermissionError:
            if attempt < 3: time.sleep(.01)
    return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--reference', type=Path, required=True)
    p.add_argument('--profile', type=Path, required=True)
    p.add_argument('--url', default='http://192.168.100.100:8795')
    def nonnegative_seconds(value):
        seconds = int(value)
        if seconds < 0:
            raise argparse.ArgumentTypeError('seconds must be nonnegative; 0 runs until explicitly stopped')
        return seconds
    p.add_argument('--seconds', type=nonnegative_seconds, default=1200,
        help='Maximum run duration; 0 runs until stop request or operator interrupt')
    a = p.parse_args()
    ref = json.loads(a.reference.read_text(encoding='utf-8'))
    profile = json.loads(a.profile.read_text(encoding='utf-8'))
    token = (a.root / 'input.token').read_text().strip()
    client = LeaderInputConnection(a.url, token)
    started_unix = time.time()
    status = {'port': 'COM16', 'state': 'STARTING', 'control_writes_sent': 0, 'frames': 0,
        'process_id': os.getpid(), 'started_unix': started_unix, 'run_seconds': a.seconds,
        'deadline_unix': None if a.seconds == 0 else started_unix + a.seconds,
        'stop_reason': None}
    def publish():
        status['updated_unix'] = time.time()
        if not publish_status(a.root, status):
            status['status_publish_contentions'] = status.get('status_publish_contentions', 0) + 1
    trace = (a.root / ('producer-' + str(time.time_ns()) + '.jsonl')).open('x', encoding='utf-8')
    try:
        dev = next(p for p in list_ports.comports() if p.device == 'COM16')
        expected = next(p['hwid'] for p in ref['ports'] if p['port'] == 'COM16')
        if dev.hwid != expected: raise ValueError('COM16 USB identity changed')
        bus = serial.Serial(port=None, baudrate=1000000, timeout=.01, write_timeout=.25)
        bus.dtr = False
        bus.rts = False
        bus.port = 'COM16'
        with bus:
            def query(motor, kind):
                bus.reset_input_buffer()
                bus.write(query_frame(motor, kind))
                bus.flush()
                data = bytearray()
                end = time.monotonic() + .4
                while time.monotonic() < end and len(data) < 16000:
                    data.extend(bus.read(bus.in_waiting or 1))
                    if b'!' in data: return bytes(data[:data.index(b'!')+1]).decode('ascii')
                raise TimeoutError(f'ID{motor} read timed out')
            for i in range(1, 7):
                text = query(i, 'PRALL')
                prefix = f'#{i:03d}PRALL:'
                if not text.startswith(prefix) or json.loads(text[len(prefix):-1]) != ref['motors'][str(i)]['parameters']:
                    raise ValueError(f'ID{i} configuration changed')
            start = time.monotonic()
            status['deadline_unix'] = None if a.seconds == 0 else time.time() + a.seconds
            publish()
            print(json.dumps({'producer_pid': os.getpid(), 'input_runtime': str(a.root.resolve()),
                'stop_request': str((a.root / 'producer-stop.request').resolve()),
                'run_seconds': a.seconds, 'deadline_unix': status['deadline_unix'],
                'control_writes_sent': 0}), flush=True)
            source = NativeInputStream(client, profile['profile_hash'], sequence=time.time_ns(),
                status=status, publish=publish)
            while (a.seconds == 0 or time.monotonic()-start < a.seconds) and not (a.root / 'producer-stop.request').exists():
                tick = time.monotonic()
                result = source.step(lambda: {str(i): parse_telemetry(i, query(i, 'PRTV')) for i in range(1, 7)})
                if result is None:
                    continue
                packet, readings, duration = result
                status.update(state='STREAMING', frames=status['frames']+1, native_positions=packet['native_positions'],
                    cycle_s=time.monotonic()-tick, scan_s=duration)
                trace.write(json.dumps({'time_unix': time.time(), 'sequence': packet['sequence'], 'telemetry': readings,
                    'cycle_s': status['cycle_s'], 'ack_gap_s': status.get('ack_gap_s'),
                    'challenge_issued_monotonic': packet.get('challenge_issued_monotonic'),
                    'valid_for_s': packet.get('valid_for_s')})+'\n')
                trace.flush()
                publish()
                time.sleep(max(0, .1-(time.monotonic()-tick)))
        status['state'] = 'STOPPED'
        status['stop_reason'] = 'STOP_REQUESTED' if (a.root / 'producer-stop.request').exists() else 'TIME_LIMIT_REACHED'
    except KeyboardInterrupt:
        status.update(state='STOPPED', stop_reason='OPERATOR_INTERRUPT')
    except Exception as exc:
        status.update(state='FAILED', stop_reason='INPUT_ERROR', error=repr(exc))
        raise
    finally:
        client.close()
        trace.close()
        status['stopped_unix'] = time.time()
        publish()
        (a.root / ('producer-final-' + str(time.time_ns()) + '.json')).write_text(json.dumps(status), encoding='utf-8')

if __name__ == '__main__': main()
