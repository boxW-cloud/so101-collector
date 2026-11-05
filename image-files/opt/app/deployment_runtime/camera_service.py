"""本机摄像头常驻进程：一直开着摄像头，把最新画面写进共享目录。录制、8920 工作台、视频 WS 都从这里读。

同一时刻只有这个进程打开摄像头，所以录制和预览不会抢设备；摄像头拔插后它自己重新打开。

共享目录（容器内存盘 /tmp，同一容器里的进程共用），每个摄像头：
  <名字>-<0|1>.rgb   原始 RGB 字节（高×宽×3），录制用，无损
  <名字>-<0|1>.jpg   JPEG，预览和视频 WS 用
  <名字>.json        最新一帧的说明：序号、用哪个槽、采集时刻、尺寸、两份文件的 sha256
写的顺序：先写帧文件（两个槽轮流），最后原子替换 json。读的时候按 json 找槽并核对 sha256，
正好被下一帧覆盖就重读一次——不会读到半帧，也不会把旧画面当成新的。

用法（由容器 supervisor 启动）：python -m deployment_runtime.camera_service --config /data/isaac-console/real-arm/camera.json
"""
import argparse
import hashlib
import json
import os
import threading
import time
from pathlib import Path

DEFAULT_STORE = '/tmp/so101-camera'


class FrameUnavailable(Exception):
    """没有足够新的完整画面。"""


class FrameStore:
    def __init__(self, root=DEFAULT_STORE):
        self.root = Path(root)

    # —— 写（只有摄像头进程调用）
    def write(self, name, rgb, jpeg, *, captured_monotonic_ns, captured_unix_ns, sequence):
        self.root.mkdir(parents=True, exist_ok=True)
        slot = sequence % 2
        raw = rgb.tobytes()
        for suffix, data in (('rgb', raw), ('jpg', jpeg)):
            path = self.root / f'{name}-{slot}.{suffix}'
            with open(path, 'wb') as stream:
                stream.write(data)
        meta = {'name': name, 'sequence': sequence, 'slot': slot, 'shape': list(rgb.shape),
                'captured_monotonic_ns': captured_monotonic_ns, 'captured_unix_ns': captured_unix_ns,
                'rgb_sha256': hashlib.sha256(raw).hexdigest(), 'jpeg_sha256': hashlib.sha256(jpeg).hexdigest(),
                'jpeg_bytes': len(jpeg)}
        temporary = self.root / f'.{name}.json.tmp'
        temporary.write_text(json.dumps(meta), encoding='utf-8')
        temporary.replace(self.root / f'{name}.json')

    # —— 读（录制设备进程、采集接口、工作台）
    def names(self):
        if not self.root.is_dir():
            return []
        return sorted(p.stem for p in self.root.glob('*.json') if not p.name.startswith('.'))

    def meta(self, name, max_age_s):
        try:
            meta = json.loads((self.root / f'{name}.json').read_text(encoding='utf-8'))
        except (OSError, ValueError) as error:
            raise FrameUnavailable(f'摄像头 {name} 还没有画面') from error
        age = time.monotonic_ns() - meta['captured_monotonic_ns']
        if not 0 <= age <= max_age_s * 1e9:
            raise FrameUnavailable(f'摄像头 {name} 的画面已经 {age / 1e9:.1f} 秒没有更新')
        return meta

    def read(self, name, kind, max_age_s=1.0):
        """kind='rgb' 返回 (ndarray, meta)，kind='jpeg' 返回 (bytes, meta)。"""
        for _ in range(3):
            meta = self.meta(name, max_age_s)
            suffix, digest = ('rgb', meta['rgb_sha256']) if kind == 'rgb' else ('jpg', meta['jpeg_sha256'])
            try:
                data = (self.root / f'{name}-{meta["slot"]}.{suffix}').read_bytes()
            except OSError:
                continue
            if hashlib.sha256(data).hexdigest() != digest:
                continue                      # 刚好被下一帧覆盖，重读
            if kind == 'rgb':
                import numpy as np
                height, width, channels = meta['shape']
                if len(data) != height * width * channels:
                    continue
                return np.frombuffer(data, dtype=np.uint8).reshape(height, width, channels).copy(), meta
            return data, meta
        raise FrameUnavailable(f'摄像头 {name} 的画面正在更新，请重试')

    def status(self, max_age_s=1.0):
        result = {}
        for name in self.names():
            try:
                meta = self.meta(name, max_age_s)
                result[name] = {'fresh': True, 'age_s': round((time.monotonic_ns() - meta['captured_monotonic_ns']) / 1e9, 3),
                                'width': meta['shape'][1], 'height': meta['shape'][0], 'sequence': meta['sequence']}
            except FrameUnavailable as error:
                result[name] = {'fresh': False, 'age_s': None, 'message': str(error)}
        return result


class SharedCamera:
    """录制设备进程用：从共享目录读画面，接口：wait_ready / latest / close。"""

    def __init__(self, name, store_root=DEFAULT_STORE):
        self.name = name
        self.store = FrameStore(store_root)

    def wait_ready(self, timeout_s):
        from .store import Fault
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                self.store.meta(self.name, 0.5)
                return
            except FrameUnavailable as error:
                if time.monotonic() >= deadline:
                    raise Fault('CAMERA_STALE', f'{error}（摄像头常驻进程没有在出画面）') from None
                time.sleep(.05)

    def latest(self, max_age_s):
        from .store import Fault
        try:
            rgb, meta = self.store.read(self.name, 'rgb', max_age_s)
        except FrameUnavailable as error:
            raise Fault('CAMERA_STALE', str(error)) from None
        self.last_frame_metadata = {key: meta[key] for key in ('sequence','captured_monotonic_ns','captured_unix_ns')}
        return rgb, meta['captured_monotonic_ns']

    def close(self):
        pass


def fit(frame, width, height):
    """摄像头给的分辨率和设定不一样时：居中裁成同样的宽高比再缩放（不拉伸变形），保证两路画面尺寸和登记的一致。"""
    import cv2
    h, w = frame.shape[:2]
    if (h, w) == (height, width):
        return frame
    scale = min(w / width, h / height)
    cw, ch = round(width * scale), round(height * scale)
    x, y = (w - cw) // 2, (h - ch) // 2
    return cv2.resize(frame[y:y + ch, x:x + cw], (width, height), interpolation=cv2.INTER_AREA)


class CameraWorker:
    """一个摄像头：打开 → 按设定帧率取最新帧写进共享目录；连续 2 秒取不到帧就关掉重新打开（拔插、ttyACM/video 编号变化都能恢复）。"""

    def __init__(self, name, path, store, *, width, height, fps, jpeg_quality=80, capture_factory=None):
        self.name, self.path, self.store = name, path, store
        self.width, self.height, self.fps, self.quality = width, height, fps, jpeg_quality
        self.capture_factory = capture_factory or self._open
        self.stop = threading.Event()
        self.sequence = 0
        self.state = 'STARTING'
        self.thread = threading.Thread(target=self.run, name='camera-' + name, daemon=True)

    def _open(self):
        import cv2
        real = os.path.realpath(self.path)
        if not os.path.exists(real):
            raise OSError(f'找不到 {self.path}')
        cap = cv2.VideoCapture(real, cv2.CAP_V4L2)
        cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*'MJPG'))
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)
        cap.set(cv2.CAP_PROP_FPS, 30)
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        if not cap.isOpened():
            raise OSError(f'打不开 {real}')
        return cap

    def run(self):
        import cv2
        period = 1 / self.fps
        while not self.stop.is_set():
            try:
                cap = self.capture_factory()
            except Exception as error:
                self.state = f'打不开：{error}'
                self.stop.wait(1)
                continue
            self.state = 'RUNNING'
            last_ok = time.monotonic()
            next_publish = 0.0
            try:
                while not self.stop.is_set():
                    ok, frame = cap.read()            # 一直读，保证拿到的是最新帧（驱动缓冲里不积压旧帧）
                    now = time.monotonic()
                    if not ok or frame is None or frame.ndim != 3 or frame.shape[2] != 3:
                        if now - last_ok > 2:
                            self.state = '2 秒没有画面，重新打开'
                            break
                        time.sleep(.01)
                        continue
                    last_ok = now
                    if now < next_publish:
                        continue
                    next_publish = now + period
                    if frame.shape[:2] != (self.height, self.width):
                        self.state = f'RUNNING（摄像头给的是 {frame.shape[1]}x{frame.shape[0]}，裁剪缩放到 {self.width}x{self.height}）'
                        frame = fit(frame, self.width, self.height)
                    ok, jpeg = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), self.quality])
                    if not ok:
                        continue
                    self.sequence += 1
                    self.store.write(self.name, cv2.cvtColor(frame, cv2.COLOR_BGR2RGB), jpeg.tobytes(),
                                     captured_monotonic_ns=time.monotonic_ns(), captured_unix_ns=time.time_ns(),
                                     sequence=self.sequence)
            except Exception as error:
                self.state = f'出错：{error}'
            finally:
                try:
                    cap.release()
                except Exception:
                    pass
            self.stop.wait(.5)


def main(argv=None):
    parser = argparse.ArgumentParser(description='本机摄像头常驻进程')
    parser.add_argument('--config', required=True)
    args = parser.parse_args(argv)
    config = json.loads(Path(args.config).read_text(encoding='utf-8'))
    store = FrameStore(config.get('store', DEFAULT_STORE))
    workers = [CameraWorker(name, path, store, width=config['width'], height=config['height'],
                            fps=config.get('fps', 15), jpeg_quality=config.get('jpeg_quality', 80))
               for name, path in config['cameras'].items()]
    for worker in workers:
        worker.thread.start()
    print(json.dumps({'cameras': list(config['cameras']), 'store': str(store.root), 'fps': config.get('fps', 15)},
                     ensure_ascii=False), flush=True)
    last = {}
    while True:                                   # 永不退出；每 10 秒把状态变化写进日志
        time.sleep(10)
        states = {w.name: w.state for w in workers}
        if states != last:
            print(json.dumps({'time': time.strftime('%F %T'), 'states': states, 'frames': store.status()}, ensure_ascii=False), flush=True)
            last = states


if __name__ == '__main__':
    raise SystemExit(main())
