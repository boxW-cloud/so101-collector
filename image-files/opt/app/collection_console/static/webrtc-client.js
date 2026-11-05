"use strict";

// WebSocket carries SDP/control messages only. Video is received by WebRTC.
export class VideoPreviewError extends Error {
  constructor(code, message) { super(message); this.name = "VideoPreviewError"; this.code = code; }
}
function aborted() { const error = new Error("视频连接已取消。"); error.name = "AbortError"; return error; }

export async function loadVideoCatalog({fetchImpl = globalThis.fetch, signal} = {}) {
  const response = await fetchImpl("/api/state", {method: "GET", credentials: "same-origin", cache: "no-store", signal});
  const value = await response.json();
  if (!response.ok || value.ok !== true || !Array.isArray(value.result?.robots)) {
    throw new VideoPreviewError("CATALOG_UNAVAILABLE", "无法获取视频服务目录，请检查连接后重试。");
  }
  return value.result;
}

export function waitForIceGatheringComplete(pc, {signal, timeoutMs = 15000} = {}) {
  if (signal?.aborted) return Promise.reject(aborted());
  if (pc.iceGatheringState === "complete") return Promise.resolve();
  return new Promise((resolve, reject) => {
    const finish = error => { clearTimeout(timer); pc.removeEventListener("icegatheringstatechange", changed); signal?.removeEventListener("abort", cancelled); error ? reject(error) : resolve(); };
    const changed = () => { if (pc.iceGatheringState === "complete") finish(); };
    const cancelled = () => finish(aborted());
    const timer = setTimeout(() => finish(new VideoPreviewError("ICE_TIMEOUT", "视频连接信息收集超时，请检查网络后重连。")), timeoutMs);
    pc.addEventListener("icegatheringstatechange", changed); signal?.addEventListener("abort", cancelled, {once: true}); changed();
  });
}

function serverError(value) {
  const code = typeof value.code === "string" && /^[A-Za-z][A-Za-z0-9_]{0,63}$/.test(value.code) ? value.code : "VIDEO_SERVICE_ERROR";
  const messages = {FORBIDDEN: "视频连接认证已失效，请刷新页面后重连。", CSRF_INVALID: "视频连接认证已失效，请刷新页面后重连。",
    VIDEO_SOURCE_STALE: "视频源已过期，请等待画面恢复后重连。", VIDEO_UNAVAILABLE: "当前视频源不可用，请稍后重连。"};
  return new VideoPreviewError(code, Object.hasOwn(messages, code) ? messages[code] : "视频服务未能完成连接（" + code + "）。");
}

export class WebRTCVideoClient {
  constructor(videoElement, {origin = globalThis.location?.origin, direct = false, legacyTarget = false, onState = () => {}, onError = () => {},
    loadCatalog = loadVideoCatalog, createPeerConnection = config => new RTCPeerConnection(config),
    createWebSocket = url => new WebSocket(url), iceTimeoutMs = 15000, signalTimeoutMs = 20000, rtcConfig = {iceServers: []}} = {}) {
    if (!videoElement) throw new TypeError("videoElement is required");
    let base;
    try { base = new URL(origin); }
    catch { throw new VideoPreviewError("INVALID_ORIGIN", "请输入有效的 HTTP 或 HTTPS 视频服务地址。"); }
    if (!["http:", "https:"].includes(base.protocol) || base.username || base.password) {
      throw new VideoPreviewError("INVALID_ORIGIN", "视频服务地址只接受无用户凭据的 HTTP 或 HTTPS 地址。");
    }
    // The collection service owns its video target. Legacy standalone services opt in explicitly.
    this.legacyTarget = legacyTarget === true;
    this.direct = !this.legacyTarget || direct === true || !!(globalThis.location && base.origin !== globalThis.location.origin);
    this.video = videoElement; this.onState = onState; this.onError = onError; this.loadCatalog = loadCatalog;
    this.createPeerConnection = createPeerConnection; this.createWebSocket = createWebSocket;
    this.iceTimeoutMs = iceTimeoutMs; this.signalTimeoutMs = signalTimeoutMs; this.rtcConfig = rtcConfig;
    const wsURL = new URL("/api/video/ws", base); wsURL.protocol = base.protocol === "https:" ? "wss:" : "ws:"; this.signalingURL = wsURL.href;
    this._run = null; this._promise = null;
    this.video.autoplay = true; this.video.muted = true; this.video.playsInline = true;
  }

  connect(options = {}) {
    if (!options || typeof options !== "object" || Array.isArray(options)) {
      return Promise.reject(new VideoPreviewError("INVALID_TARGET", "请选择前置或腕部相机。"));
    }
    const {robotKey, camera = "front"} = options;
    if (!["front", "wrist"].includes(camera) || (this.legacyTarget && (typeof robotKey !== "string" || !robotKey)) ||
        (!this.legacyTarget && (Object.hasOwn(options, "robotKey") || Object.hasOwn(options, "robot_key")))) {
      return Promise.reject(new VideoPreviewError("INVALID_TARGET", "请选择前置或腕部相机；视频目标由服务端配置。"));
    }
    if (this._run && this._run.robotKey === robotKey && this._run.camera === camera) return this._promise;
    this.close();
    const run = {robotKey, camera, abort: new AbortController(), closed: false, listeners: [], pc: null, ws: null};
    this._run = run; this._notify("connecting", run);
    this._promise = this._connect(run);
    return this._promise;
  }

  _current(run) { if (this._run !== run || run.closed || run.abort.signal.aborted) throw aborted(); }
  _target(run) { return this.legacyTarget ? {robotKey: run?.robotKey, camera: run?.camera} : {camera: run?.camera}; }
  _notify(phase, run, detail = {}) { this.onState({phase, ...this._target(run), ...detail}); }
  _listen(run, object, event, listener) { object.addEventListener(event, listener); run.listeners.push(() => object.removeEventListener(event, listener)); }

  async _connect(run) {
    try {
      let catalog = null;
      if (!this.direct) {
        catalog = await this.loadCatalog({signal: run.abort.signal}); this._current(run);
        const robot = catalog.robots.find(item => item.key === run.robotKey && item.available === true);
        const cameras = Array.isArray(robot?.cameras) ? robot.cameras : Object.keys(robot?.cameras || {});
        if (!robot || !cameras.includes(run.camera)) throw new VideoPreviewError("TARGET_UNAVAILABLE", "所选机器人或相机尚未就绪。");
      }
      this._current(run);
      const pc = this.createPeerConnection(this.rtcConfig); run.pc = pc;
      pc.addTransceiver("video", {direction: "recvonly"});
      pc.ontrack = event => {
        if (this._run !== run || run.closed) return;
        this.video.srcObject = event.streams?.[0] || new MediaStream([event.track]);
        const played = this.video.play?.();
        played?.catch(() => { if (this._run === run && !run.closed) this.onError({code: "PLAYBACK_BLOCKED", message: "浏览器阻止自动播放，请点击视频播放。"}); });
      };
      pc.onconnectionstatechange = () => {
        if (this._run !== run || run.closed) return;
        if (["failed", "disconnected", "closed"].includes(pc.connectionState)) this._fail(run, new VideoPreviewError("VIDEO_DISCONNECTED", "视频连接已断开，请重新连接。"));
        else this._notify(pc.connectionState === "connected" ? "connected" : "connecting", run);
      };
      await pc.setLocalDescription(await pc.createOffer()); this._current(run);
      this._notify("gathering", run);
      await waitForIceGatheringComplete(pc, {signal: run.abort.signal, timeoutMs: this.iceTimeoutMs}); this._current(run);
      const ws = this.createWebSocket(this.signalingURL); run.ws = ws;
      const offer = {type: "offer", camera: run.camera, sdp: pc.localDescription.sdp};
      if (this.legacyTarget) offer.robot_key = run.robotKey;
      if (typeof catalog?.csrf_token === "string" && catalog.csrf_token) offer.csrf_token = catalog.csrf_token;
      const answer = await this._signal(run, offer);
      this._current(run); await pc.setRemoteDescription({type: "answer", sdp: answer.sdp}); this._current(run);
      this._notify(pc.connectionState === "connected" ? "connected" : "negotiated", run);
      return this._target(run);
    } catch (error) {
      const safe = error.name === "AbortError" || error instanceof VideoPreviewError ? error : new VideoPreviewError("VIDEO_CONNECTION_FAILED", "视频连接未完成，请重试。");
      if (this._run === run && !run.closed) this._fail(run, safe);
      throw safe;
    }
  }

  _signal(run, offer) {
    return new Promise((resolve, reject) => {
      let sent = false; let settled = false;
      const finish = (error, value) => { if (settled) return; settled = true; clearTimeout(timer); run.abort.signal.removeEventListener("abort", cancelled); error ? reject(error) : resolve(value); };
      const fail = error => { finish(error); this._fail(run, error); };
      const cancelled = () => finish(aborted());
      const timer = setTimeout(() => fail(new VideoPreviewError("SIGNAL_TIMEOUT", "视频信令响应超时，请重新连接。")), this.signalTimeoutMs);
      run.abort.signal.addEventListener("abort", cancelled, {once: true});
      this._listen(run, run.ws, "open", () => {
        if (sent || run.closed) return; sent = true; run.ws.send(JSON.stringify(offer)); this._notify("negotiating", run);
      });
      this._listen(run, run.ws, "message", event => {
        if (run.closed) return;
        let value;
        try { value = JSON.parse(event.data); }
        catch { fail(new VideoPreviewError("SIGNAL_FORMAT", "视频信令格式无效，连接已关闭。")); return; }
        if (!value || typeof value !== "object" || Array.isArray(value)) { fail(new VideoPreviewError("SIGNAL_FORMAT", "视频信令格式无效，连接已关闭。")); return; }
        if (value.type === "error") { fail(serverError(value)); return; }
        if (value.type === "state") { this._notify("server-state", run, {state: String(value.state || "")}); return; }
        if (value.type !== "answer" || typeof value.sdp !== "string" || !value.sdp) { fail(new VideoPreviewError("SIGNAL_FORMAT", "视频信令响应无效，连接已关闭。")); return; }
        if ((this.legacyTarget && value.robot_key !== run.robotKey) || value.camera !== run.camera) { fail(new VideoPreviewError("SIGNAL_IDENTITY_MISMATCH", "视频信令目标不匹配，连接已关闭。")); return; }
        finish(null, value);
      });
      this._listen(run, run.ws, "close", () => fail(new VideoPreviewError("SIGNAL_DISCONNECTED", "视频信令连接已断开，请重新连接。")));
      this._listen(run, run.ws, "error", () => fail(new VideoPreviewError("SIGNAL_UNAVAILABLE", "无法连接视频信令服务，请检查入口与网络。")));
      if (run.ws.readyState === 1) { sent = true; run.ws.send(JSON.stringify(offer)); this._notify("negotiating", run); }
    });
  }

  _fail(run, error) {
    if (this._run !== run || run.closed) return;
    this.onError({code: error.code || "VIDEO_CONNECTION_FAILED", message: error instanceof VideoPreviewError ? error.message : "视频连接未完成，请重试。"});
    this._dispose(run); this._run = null; this._promise = null; this._notify("error", run);
  }

  _dispose(run) {
    if (run.closed) return; run.closed = true; run.abort.abort();
    for (const remove of run.listeners) remove(); run.listeners.length = 0;
    if (run.pc) { run.pc.ontrack = null; run.pc.onconnectionstatechange = null; run.pc.close(); }
    if (run.ws && run.ws.readyState < 2) run.ws.close();
    const stream = this.video.srcObject; stream?.getTracks?.().forEach(track => track.stop());
    this.video.pause?.(); this.video.srcObject = null; this.video.removeAttribute?.("src"); this.video.load?.();
  }

  close() {
    const run = this._run;
    if (run) this._dispose(run);
    this._run = null; this._promise = null;
    if (run) this._notify("closed", run);
  }

  async getStats() { return this._run?.pc?.getStats ? this._run.pc.getStats() : null; }
}
