"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const actions = {start: "开始采集", redo: "重采本条", complete: "完成本条", stop: "结束采集", resume: "恢复采集"};
  const storageKey = "robot.collection-console.unconfirmed.v1";
  const supersededStorageKey = "robot.collection-console.superseded.v1";
  const recoveryCodes = new Set(["COLLECTION_STATE_UNKNOWN", "RECONCILE_REQUIRED", "RECOVERABLE"]);
  const unconfirmedCodes = new Set(["UPSTREAM_OUTCOME_UNKNOWN", "REQUEST_IN_PROGRESS", "COMMAND_BUSY"]);
  const errors = {
    SOURCE_STALE: "遥操输入已断开或过期，请重新连接输入后再操作。",
    STOPPED: "本次操作已中断，尚未完成。请确认连接正常后重试。",
    DataNotReady: "关节反馈或相机数据尚未就绪。请等待数据恢复后再操作。",
    DeviceDisconnected: "设备或仿真连接已断开。请恢复连接后重试。",
    IOTimeout: "读取或控制响应超时，本次操作未确认完成。请确认连接后重试。",
    TARGET_STATUS_UNAVAILABLE: "机器人状态暂时无法确认。连接恢复前，请勿开始新的采集。",
    COLLECTION_STATE_UNKNOWN: "采集连接暂时中断。点击结束采集后，已有数据将自动保存并发送。",
    TARGET_STATE_CONFLICT: "机器人正在执行其他操作，请确认现场状态后重试。",
    TARGET_FAULT: "机器人需要现场检查，暂时不能继续采集。",
    STALE_EPISODE: "本条数据已发生变化。请确认最新状态后重新操作。",
    STALE_GENERATION: "本条已重新采集。请确认最新状态后重新操作。",
    ALREADY_SUBMITTED: "这条数据已提交，不能再次重采或重复提交。",
    NOT_RECORDING: "当前未在采集中，请先确认采集状态。",
    FORBIDDEN: "当前账号没有执行此操作的权限。",
    BUSY: "机器人正在被使用，请等待当前操作结束。",
    WORKSPACE_CONFIRMATION_REQUIRED: "请先在现场确认工作空间安全。",
    PHYSICAL_NOT_AVAILABLE: "真机遥操尚未就绪，请选择可用的机器人或使用仿真遥操。",
    INVALID_ROBOT: "所选机器人不可用，请重新选择。",
    INVALID_MODE: "所选采集模式暂时不可用。",
    INPUT_NOT_READY: "遥操输入尚未就绪，请先手工连接输入。",
    CSRF_INVALID: "页面连接已更新，请刷新状态后重试原操作。",
    RECONCILE_REQUIRED: "采集进度已保留。点击结束采集后，已有数据将自动保存并发送。",
    NEXT_EPISODE_FAILED: "本条数据已提交，下一条未能启动。点击结束采集即可保存并发送已有数据。",
    STOP_UNCONFIRMED: "服务未能确认停止。请保持现场状态，检查后再次请求结束采集。"
  };
  let snapshot = null;
  let stateFresh = false;
  let selection = null;
  let lastKnownSession = null;
  let csrf = "";
  let sending = false;
  let commandSequence = 0;
  let selecting = false;
  let polling = false;
  let pollAgain = false;
  let stateController = null;
  let stateSequence = 0;
  let pollTimer = null;
  let pending = readPending();
  let localNotice = null;
  let dialogAction = "start";
  let dialogContext = null;
  let confirmedCommit = null;
  let cameraKey = "front";
  let cameraTarget = "";
  let cameraSequence = 0;
  let cameraLoading = false;
  let cameraFailed = false;
  let lastImageAt = 0;
  let videoClient = null;
  let videoModule = null;
  let videoFrameCallback = null;
  let videoRetryTimer = null;
  let videoRetryAt = 0;
  let videoBackoff = 1000;
  let videoLastTime = 0;
  let videoPaused = false;
  let datasetsFingerprint = "";

  function text(id, value) {
    const next = value === null || value === undefined || value === "" ? "—" : String(value);
    if ($(id).textContent !== next) $(id).textContent = next;
  }

  function badge(id, label, tone = "neutral") {
    text(id, label);
    $(id).className = "badge tone-" + tone;
  }

  function numeric(value) { return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value.toLocaleString("zh-CN") : "—"; }
  function running() { return stateFresh && snapshot?.status?.state === "RUNNING"; }
  function finishing() { return stateFresh && snapshot?.status?.state === "FINISHING"; }
  function finishStarted() { return !!snapshot?.status?.finish_state || snapshot?.status?.finished === true || finishing(); }
  function selectedRobot() { return (snapshot?.robots || []).find((robot) => robot.key === selection?.robot_key) || null; }
  function isPhysical() { return selection?.mode === "physical_teleop"; }
  function serverPending() {
    const value = snapshot?.pending;
    const original = value?.client_payload || value;
    if (original?.action === "stop" && snapshot?.status?.finish_state &&
        original.session_id === snapshot.status.session_id && sameSelection(original, selection)) return null;
    return value;
  }
  function hasPending() { return sending || !!pending || !!serverPending(); }
  function selectionLocked() {
    return hasPending() || selecting || running() || finishing() || (!!lastKnownSession && (!stateFresh || !!snapshot?.status_error || !snapshot?.status));
  }
  function hasEpisode() {
    const status = snapshot?.status;
    return running() && !!status.session_id && !!status.episode_id && Number.isInteger(status.recording_generation) && status.recording_generation > 0;
  }
  function sameSelection(a, b) { return !!a && !!b && a.robot_key === b.robot_key && a.mode === b.mode; }
  function finishNotice(status) {
    const phase = status?.finish_state;
    if (phase === "RETRY_WAIT") return {message: "正在自动重试结束采集，已有数据会保留，完成后自动发送。无需其他操作。", tone: "warning"};
    if (status?.state === "FINISHING" || ["QUEUED", "STOPPING", "FINALIZING"].includes(phase)) return {message: "正在结束采集，已有数据将自动保存并发送。", tone: "blue"};
    if (phase === "QUEUED_UPLOAD") return {message: "采集已结束，数据已保存，正在后台自动上传。无需其他操作。", tone: "success"};
    if (["UPLOADED", "READY", "SUCCEEDED"].includes(phase)) return {message: "采集已结束，数据已上传。", tone: "success"};
    if (phase === "EMPTY") return {message: "采集已结束，没有新增数据。", tone: "success"};
    if (phase === "ERROR") return {message: "采集已停止，数据仍然保留。自动发送暂未完成。", tone: "warning"};
    return null;
  }

  function humanMessage(value, fallback = "服务暂时无法完成操作，请确认状态后重试。") {
    const object = typeof value === "string" ? {message: value} : value || {};
    if (Object.hasOwn(errors, object.code)) return errors[object.code];
    const safeCode = typeof object.code === "string" && /^[A-Za-z][A-Za-z0-9_]{0,63}$/.test(object.code) ? object.code : "";
    const safeFallback = safeCode ? fallback + "（错误码 " + safeCode + "）" : fallback;
    const message = object.message || object.reason;
    if (typeof message !== "string" || !message.trim()) return safeFallback;
    // Backend diagnostics may contain credentials or network details. The operator UI keeps those in server logs.
    if (/(?:https?:\/\/|\b\d{1,3}(?:\.\d{1,3}){3}\b|[a-z]:[\\/]|\/home\/|\/run\/|token|bearer|secret|csrf|ros2?\b|traceback|api[_ -]?key)/i.test(message)) return safeFallback;
    return message.length > 240 ? message.slice(0, 240) + "…" : message;
  }

  function readPending() {
    try {
      const value = JSON.parse(sessionStorage.getItem(storageKey));
      if (!value || !actions[value.action] || typeof value.request_id !== "string" || typeof value.robot_key !== "string" || typeof value.mode !== "string") return null;
      return value;
    } catch { return null; }
  }

  function savePending(value) {
    pending = value;
    try {
      if (value) sessionStorage.setItem(storageKey, JSON.stringify(value));
      else sessionStorage.removeItem(storageKey);
    } catch { /* Keep the exact request in memory when browser storage is blocked. */ }
  }

  function preserveSuperseded(value, includeStop = false) {
    if (!value?.request_id || value.action === "stop" && !includeStop) return;
    // Keep the old frozen intent for diagnostics. It is never automatically replayed.
    // The service also retains its durable request record if browser storage is unavailable.
    try {
      const saved = JSON.parse(sessionStorage.getItem(supersededStorageKey) || "[]");
      const items = Array.isArray(saved) ? saved : [];
      if (!items.some(item => item.request_id === value.request_id)) items.push(value);
      sessionStorage.setItem(supersededStorageKey, JSON.stringify(items));
    } catch { /* The original request remains in the service ledger. */ }
  }

  function requestId() {
    if (window.crypto?.randomUUID) return window.crypto.randomUUID();
    if (!window.crypto?.getRandomValues) throw new Error("浏览器无法创建操作编号，请使用现代浏览器。");
    const bytes = new Uint8Array(16);
    window.crypto.getRandomValues(bytes);
    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;
    const hex = Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
    return [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20)].join("-");
  }

  function setNotice(message, tone = "neutral") { localNotice = {message, tone}; renderNotice(); }

  function renderNotice() {
    let notice = finishNotice(snapshot?.status) || localNotice;
    if (!stateFresh) notice = {message: (localNotice ? localNotice.message + " " : "") + "当前状态尚未确认，这不代表机器人已停止。连接恢复前不能开始、重采或提交数据。", tone: "warning"};
    else if (snapshot?.status_error) {
      const statusMessage = humanMessage(snapshot.status_error, "机器人状态暂时无法确认，请检查服务连接。");
      notice = {message: localNotice && localNotice.message !== statusMessage ? localNotice.message + " " + statusMessage : statusMessage, tone: "warning"};
    }
    else if (snapshot?.resume_session?.session_id && snapshot?.status?.state === "IDLE" && !finishStarted() && !notice) {
      const frames = snapshot.resume_session.preserved_current_frames;
      notice = {message: (Number.isInteger(frames) ? "已保留当前条 " + frames + " 帧。" : "当前条数据已保留。") + "点击结束采集后，保留数据将自动保存并发送。", tone: "neutral"};
    }
    else if (snapshot?.notice && !notice) notice = {message: humanMessage(snapshot.notice), tone: "neutral"};
    $("notice").hidden = !notice;
    if (notice) { text("notice-text", notice.message); $("notice").className = "notice tone-" + notice.tone; }
  }

  function renderSelectors() {
    const kind = (selection?.mode || "simulation_teleop").split("_")[0];
    const robots = Array.isArray(snapshot?.robots) ? snapshot.robots.filter((robot) => !robot.kind || robot.kind === kind) : [];
    const select = $("robot-select");
    const signature = JSON.stringify(robots.map((robot) => [robot.key, robot.label, robot.available]));
    if (select.dataset.signature !== signature) {
      select.replaceChildren();
      if (!robots.length) {
        const option = document.createElement("option"); option.value = ""; option.textContent = stateFresh ? kind === "physical" ? "暂无真机机器人" : "暂无仿真机器人" : "正在读取机器人"; select.append(option);
      }
      for (const robot of robots) {
        const option = document.createElement("option"); option.value = robot.key; option.textContent = robot.label || robot.id || "未命名机器人";
        if (!robot.available) option.textContent += " · 未就绪";
        select.append(option);
      }
      select.dataset.signature = signature;
    }
    if (selection) { select.value = selection.robot_key || ""; $("mode-select").value = selection.mode; }
    const locked = selectionLocked();
    select.disabled = locked || !robots.length;
    $("mode-select").disabled = locked || !snapshot;
  }

  function can(action) {
    if (selecting || !csrf || !selection?.robot_key) return false;
    if (action === "stop") {
      const server = serverPending();
      const priors = [pending, server?.client_payload || server].filter(Boolean);
      return !finishStarted() && priors.every(prior => prior.action !== "stop" &&
        sameSelection(prior, selection) && (!prior.session_id || prior.session_id === lastKnownSession?.session_id)) &&
        !!lastKnownSession?.session_id && sameSelection(lastKnownSession.selection, selection);
    }
    if (hasPending()) return false;
    if (!stateFresh || snapshot?.status_error || !snapshot?.connection?.available || !selectedRobot()?.available) return false;
    if (action === "start") return snapshot?.status?.state === "IDLE";
    if (action === "redo" || action === "complete") return hasEpisode();
    return false;
  }

  function recoverySession() {
    const detail = snapshot?.status_error?.details;
    if (snapshot?.resume_session?.session_id) return snapshot.resume_session.session_id;
    if (snapshot?.recoverable_session_id) return snapshot.recoverable_session_id;
    if (recoveryCodes.has(snapshot?.status_error?.code) && detail?.recoverable_session_id) return detail.recoverable_session_id;
    return null;
  }

  function renderButtons() {
    for (const action of ["start", "redo", "complete", "stop"]) $(action + "-button").disabled = !can(action);
    let help = "请先手工连接遥操输入，再开始采集。";
    if (hasPending()) help = "操作尚待确认，请等待结果或重试原操作。";
    else if (!stateFresh || snapshot?.status_error) help = "可以结束已有采集，数据将自动保存并发送。";
    else if (finishing()) help = "正在结束采集并保存数据，完成后自动发送。";
    else if (!selection?.robot_key) help = isPhysical() ? "暂未登记可用的真机遥操目标。" : "请先选择可用的仿真机器人。";
    else if (!selectedRobot()?.available) help = humanMessage({message: selectedRobot()?.reason}, isPhysical() ? "真机遥操尚未就绪。" : "所选机器人尚未就绪。");
    else if (!snapshot?.connection?.available) help = "采集服务尚未连接，请稍后重试。";
    else if (running()) help = "完成本条后进入下一条；结束采集会自动保存当前条并发送。";
    else if (recoverySession() && !finishStarted()) help = "点击结束采集，保留的数据将自动保存并发送。";
    else if (finishStarted()) help = "数据自动发送，你可以开始新的采集。";
    else if (isPhysical()) help = "请先连接真机遥操输入并确认现场工作空间。";
    text("action-help", help);
    const recoverable = !finishStarted() && snapshot?.status_error && recoverySession();
    $("resume-panel").hidden = !recoverable;
    $("resume-button").disabled = hasPending() || selecting || !stateFresh || !csrf || !snapshot?.connection?.available;
  }

  function renderOperation() {
    const server = serverPending();
    const operation = pending || server;
    $("operation-panel").hidden = !operation;
    if (!operation) return;
    const label = actions[operation.action] || "采集操作";
    const serverBusy = !!server && server.request_id === operation.request_id;
    text("operation-text", sending || serverBusy ? "正在确认“" + label + "”，请稍候…" : "“" + label + "”的结果尚未收到。重试会使用原操作编号。");
    $("operation-spinner").hidden = !sending && !serverBusy;
    $("retry-button").hidden = !pending || sending;
    $("retry-button").disabled = !csrf || selecting;
  }

  function renderStatus() {
    const status = snapshot?.status;
    const unknown = !stateFresh || !!snapshot?.status_error || !status;
    const active = !unknown && status.state === "RUNNING";
    const label = unknown ? "状态待确认" : active ? "采集中" : status.state === "FINISHING" ? "正在结束" : status.state === "IDLE" ? "待开始" : "状态待确认";
    const tone = unknown ? "warning" : active || status.state === "FINISHING" ? "blue" : "neutral";
    badge("header-state", label, tone);
    text("collection-state", label);
    $("collection-state").className = "status-value tone-" + tone;
    const followingKnown = !unknown && snapshot?.connection?.available === true;
    const followingLabel = followingKnown && active ? "采集并跟随中（需观察实际动作）" :
      followingKnown && status.state === "FINISHING" ? "正在停止跟随并自动保存数据" :
      followingKnown && status.state === "IDLE" ? "虚拟从臂保持；开始采集后跟随" :
      "跟随待确认或已中断，请检查并结束当前会话";
    text("following-status", followingLabel);
    $("following-status").hidden = isPhysical();
    $("following-status").className = "following-status tone-" + (followingKnown && active ? "blue" : followingKnown && status.state === "IDLE" ? "neutral" : "warning");
    text("episode-number", active && Number.isInteger(status.episode_index) ? "第 " + (status.episode_index + 1) + " 条" : "—");
    text("frame-count", active ? numeric(status.current_frames) : Number.isInteger(snapshot?.resume_session?.preserved_current_frames) ? numeric(snapshot.resume_session.preserved_current_frames) + "（保留）" : "—");
    text("committed-count", unknown ? "已完成条数待确认" : !active && status.committed_episodes === undefined ? "开始采集后查看本次已完成条数" : "本次已完成 " + numeric(status.committed_episodes) + " 条");
    const committed = confirmedCommit && sameSelection(confirmedCommit.selection, selection) ? confirmedCommit.episode : null;
    if (unknown && committed) text("committed-count", (Number.isInteger(committed.episode_index) ? "第 " + (committed.episode_index + 1) + " 条已提交" : "本条已提交") + "；下一条状态待确认");
    const taskName = typeof snapshot?.task_name === "string" ? snapshot.task_name : "";
    $("task-label").hidden = !taskName || (!active && !unknown);
    if (taskName) text("task-name", taskName);
    const record = active ? status : lastKnownSession;
    $("session-details").hidden = !record?.session_id && !pending && !committed;
    text("session-id", record?.session_id);
    text("episode-id", record?.episode_id);
    $("request-row").hidden = !pending;
    text("request-id", pending?.request_id);
    $("committed-row").hidden = !committed;
    text("committed-episode-id", committed?.episode_id);
  }

  function uploadBadge(dataset) {
    const state = String(dataset.upload_state || "").toUpperCase();
    if (["READY", "UPLOADED", "VERIFIED", "SUCCEEDED", "COMPLETED"].includes(state)) return [dataset.delivery_kind === "directory" ? "已同步到目录" : "已上传", "success"];
    if (state === "PREPARING") return ["准备上传", "blue"];
    if (["UPLOADING", "RUNNING"].includes(state)) return ["上传中", "blue"];
    if (["PENDING", "QUEUED"].includes(state)) return ["等待上传", "neutral"];
    if (["RETRY_WAIT", "PENDING_RETRY", "TRANSFER_RETRY", "NOTIFY_RETRY"].includes(state)) return ["后台重试中", "warning"];
    if (["VERIFYING", "NOTIFYING"].includes(state)) return ["等待平台确认", "blue"];
    if (state === "REJECTED") return ["平台未通过", "warning"];
    if (["FAILED", "ERROR"].includes(state)) return ["上传失败", "warning"];
    if (dataset.upload_alert || dataset.upload_error) return ["上传需检查", "warning"];
    if (["LOCAL_READY", "LOCAL_ONLY"].includes(state)) return ["本地已保存", "neutral"];
    if (state === "NOT_REQUESTED") return ["尚未上传", "neutral"];
    return [state ? "上传状态待确认" : "已保存", "neutral"];
  }

  function appendDetail(list, label, value) {
    if (value === undefined || value === null || value === "") return;
    const row = document.createElement("div"); const term = document.createElement("dt"); const description = document.createElement("dd");
    term.textContent = label; description.textContent = String(value); row.append(term, description); list.append(row);
  }

  function renderDatasets() {
    const datasets = Array.isArray(snapshot?.datasets) ? [...snapshot.datasets].sort((a, b) => (Number(b.created) || 0) - (Number(a.created) || 0)) : [];
    if (!snapshot) return;
    if (snapshot.dataset_error || !snapshot.connection?.available) {
      text("dataset-count", "—");
      $("datasets-empty").hidden = false;
      $("dataset-list").hidden = true;
      text("datasets-empty-title", "数据列表暂时不可用");
      text("datasets-empty-text", "已有数据会保留，连接恢复后将重新读取。");
      text("recent-title", "数据摘要待确认");
      text("recent-detail", "等待重新读取已保存的数据");
      return;
    }
    text("dataset-count", numeric(datasets.length));
    $("datasets-empty").hidden = datasets.length > 0;
    $("dataset-list").hidden = !datasets.length;
    text("datasets-empty-title", "还没有已保存的数据");
    text("datasets-empty-text", "完成本条采集后，数据会显示在这里。");
    if (datasets.length) {
      const latest = datasets[0];
      text("recent-title", numeric(latest.episodes) + " 条数据 · " + numeric(latest.frames) + " 帧");
      text("recent-detail", uploadBadge(latest)[0]);
    } else { text("recent-title", "暂无已保存数据"); text("recent-detail", "完成本条后查看采集摘要"); }
    const fingerprint = JSON.stringify(datasets);
    if (fingerprint === datasetsFingerprint) return;
    datasetsFingerprint = fingerprint;
    const fragment = document.createDocumentFragment();
    datasets.forEach((dataset, index) => {
      const item = document.createElement("li"); item.className = "dataset-item";
      const row = document.createElement("div"); row.className = "dataset-row";
      const title = document.createElement("p"); title.className = "dataset-title"; title.textContent = dataset.name || dataset.task_name || "采集数据 " + (datasets.length - index);
      const [label, tone] = uploadBadge(dataset); const upload = document.createElement("span"); upload.className = "badge tone-" + tone; upload.textContent = label; row.append(title, upload);
      const stats = document.createElement("p"); stats.className = "dataset-stats"; stats.textContent = numeric(dataset.episodes) + " 条 · " + numeric(dataset.frames) + " 帧 · " + (dataset.target_type === "simulation" || dataset.mode?.startsWith("simulation") ? "仿真" : dataset.target_type === "physical" || dataset.mode?.startsWith("physical") ? "真机" : "历史数据") + " · " + (dataset.robot_id || "");
      item.append(row, stats);
      if (dataset.upload_alert || dataset.upload_error) {
        const alert = document.createElement("p"); alert.className = "dataset-alert"; alert.textContent = "同步尚未完成，已保存的数据仍然保留。"; item.append(alert);
      }
      const details = document.createElement("details"); details.className = "technical-details";
      const summary = document.createElement("summary"); summary.textContent = "数据标识";
      const list = document.createElement("dl"); appendDetail(list, "数据集", dataset.dataset_id); appendDetail(list, "版本", dataset.version); appendDetail(list, "校验", dataset.content_hash);
      details.append(summary, list); item.append(details); fragment.append(item);
    });
    $("dataset-list").replaceChildren(fragment);
  }

  function cameraOptions() {
    const cameras = selectedRobot()?.cameras;
    const options = Array.isArray(cameras) ? cameras : cameras && typeof cameras === "object" ? Object.keys(cameras) : [];
    return options.map((camera) => typeof camera === "string" ? {key: camera, label: camera === "front" ? "前方相机" : camera === "wrist" ? "腕部相机" : camera} : {key: camera.key || camera.id || camera.name, label: camera.label || camera.name || camera.id || camera.key}).filter((camera) => typeof camera.key === "string" && camera.key);
  }

  function clearVideo({resetRetry = true} = {}) {
    cameraSequence++;
    clearTimeout(videoRetryTimer); videoRetryTimer = null; videoRetryAt = 0;
    const previous = videoClient; videoClient = null;
    previous?.close(); window.collectionVideoPreview = null;
    const video = $("camera-image");
    if (videoFrameCallback !== null) video.cancelVideoFrameCallback?.(videoFrameCallback);
    videoFrameCallback = null; videoLastTime = 0;
    cameraLoading = false;
    cameraFailed = false;
    lastImageAt = 0;
    if (resetRetry) videoBackoff = 1000;
    video.pause?.(); video.srcObject = null; video.removeAttribute("src"); video.load?.(); video.hidden = true;
    video.dataset.decodedFrames = "0";
    $("video-stage").classList.remove("has-image");
  }

  function decodedVideoFrame(request, frames) {
    const video = $("camera-image");
    if (request !== cameraSequence || !videoClient || document.hidden || videoPaused || !video.srcObject || video.videoWidth <= 0 || video.videoHeight <= 0) return;
    lastImageAt = Date.now(); cameraFailed = false; videoBackoff = 1000;
    video.hidden = false; $("video-stage").classList.add("has-image");
    video.dataset.decodedFrames = String(Number(frames) || Number(video.dataset.decodedFrames || 0) + 1);
    renderCamera();
  }

  function watchVideoFrames(request) {
    const video = $("camera-image");
    if (request !== cameraSequence || !videoClient || document.hidden || videoPaused || typeof video.requestVideoFrameCallback !== "function") return;
    videoFrameCallback = video.requestVideoFrameCallback((_now, metadata) => {
      videoFrameCallback = null;
      if (request !== cameraSequence) return;
      decodedVideoFrame(request, metadata?.presentedFrames);
      watchVideoFrames(request);
    });
  }

  function videoFailed(request) {
    if (request !== cameraSequence) return;
    const target = cameraTarget; const delay = videoBackoff;
    clearVideo({resetRetry: false}); cameraFailed = true;
    videoBackoff = Math.min(delay * 2, 15000); videoRetryAt = Date.now() + delay;
    const sequence = cameraSequence;
    videoRetryTimer = setTimeout(() => {
      if (sequence !== cameraSequence || cameraTarget !== target || document.hidden || videoPaused) return;
      videoRetryTimer = null; videoRetryAt = 0; void refreshVideo();
    }, delay);
    renderCamera();
  }

  function renderCamera() {
    const options = cameraOptions();
    const signature = JSON.stringify(options);
    if ($("camera-select").dataset.signature !== signature) {
      $("camera-select").replaceChildren();
      for (const camera of options) { const option = document.createElement("option"); option.value = camera.key; option.textContent = camera.label; $("camera-select").append(option); }
      $("camera-select").dataset.signature = signature;
    }
    if (!options.some((camera) => camera.key === cameraKey)) cameraKey = options.some((camera) => camera.key === "front") ? "front" : options[0]?.key || "front";
    $("camera-select").value = cameraKey; $("camera-select").hidden = options.length < 2;
    const target = (selection?.robot_key || "") + ":" + cameraKey;
    if (cameraTarget !== target) { cameraTarget = target; clearVideo(); }
    text("video-title", isPhysical() ? "机器人画面" : "仿真画面");
    const fresh = !!videoClient && lastImageAt > 0 && Date.now() - lastImageAt <= 3000 && !document.hidden && !videoPaused;
    let title = isPhysical() ? "等待机器人画面" : "等待仿真画面";
    let description = "连接服务后，视频将自动显示。";
    let label = "等待画面"; let tone = "neutral";
    if (document.hidden || videoPaused) { title = "视频已暂停"; description = "返回页面后将自动连接。"; label = "视频已暂停"; }
    else if (fresh) { label = "实时视频"; tone = "success"; }
    else if (!options.length && snapshot) { title = "暂无可用画面"; description = "所选机器人尚未提供相机画面。"; }
    else if (cameraFailed) { title = "视频暂时不可用"; description = "视频将在后台自动重连，采集操作不受影响。"; label = "视频重连中"; tone = "warning"; }
    else if (cameraLoading || videoClient) { title = "等待视频帧"; description = "正在连接视频，收到实际画面后显示。"; label = "连接视频中"; }
    else if (!stateFresh) { title = "画面状态待确认"; description = "服务连接恢复后会重新确认画面。"; label = "状态待确认"; tone = "warning"; }
    badge("video-badge", label, tone);
    $("video-overlay").hidden = fresh && lastImageAt > 0 && !cameraFailed;
    text("video-empty-title", title); text("video-empty-description", description);
    $("image-time").hidden = !lastImageAt || !$("video-overlay").hidden;
    if (lastImageAt) text("image-time", "视频更新 " + new Date(lastImageAt).toLocaleTimeString("zh-CN", {hour12: false}));
  }

  async function refreshVideo() {
    if (cameraLoading || videoClient || document.hidden || videoPaused || !selection?.robot_key || !cameraOptions().length || !stateFresh || !snapshot?.connection?.available || Date.now() < videoRetryAt) return;
    clearTimeout(videoRetryTimer); videoRetryTimer = null;
    const request = ++cameraSequence;
    cameraLoading = true;
    renderCamera();
    try {
      const options = {onState: () => { if (request === cameraSequence) renderCamera(); }, onError: () => videoFailed(request)};
      let client;
      if (typeof window.collectionVideoClientFactory === "function") client = window.collectionVideoClientFactory($("camera-image"), options);
      else {
        videoModule ||= import("/static/webrtc-client.js");
        const module = await videoModule;
        if (request !== cameraSequence || document.hidden || videoPaused) return;
        client = new module.WebRTCVideoClient($("camera-image"), options);
      }
      if (request !== cameraSequence || document.hidden || videoPaused) { client.close(); return; }
      videoClient = client; window.collectionVideoPreview = client;
      // Keep the video compositable under the waiting overlay until an actual frame arrives.
      $("camera-image").hidden = false;
      watchVideoFrames(request);
      await client.connect({camera: cameraKey});
    } catch { videoFailed(request); }
    finally { if (request === cameraSequence) { cameraLoading = false; renderCamera(); } }
  }

  function render() {
    const connected = stateFresh && snapshot?.connection?.available === true;
    text("connection-label", connected ? "服务已连接" : stateFresh ? "服务未就绪" : "服务连接中断");
    $("connection-badge").className = "connection-badge tone-" + (connected ? "success" : "warning");
    text("collection-title", isPhysical() ? "真机遥操采集" : "仿真遥操采集");
    renderSelectors(); renderStatus(); renderButtons(); renderOperation(); renderDatasets(); renderCamera(); renderNotice();
    text("refresh-status", stateFresh ? "状态每秒自动更新" : "等待重新连接 · 状态未确认");
  }

  async function refreshState(nextSelection = null, force = false) {
    if (polling && !nextSelection && !force) { pollAgain = true; return; }
    const request = ++stateSequence;
    if (force) stateController?.abort();
    if (nextSelection) { selecting = true; stateController?.abort(); render(); }
    polling = true;
    const controller = new AbortController(); stateController = controller;
    const timeout = setTimeout(() => controller.abort(), 8000);
    const chosen = nextSelection || selection || (pending ? {robot_key: pending.robot_key, mode: pending.mode} : null);
    const query = chosen ? "?" + new URLSearchParams(chosen).toString() : "";
    try {
      const response = await fetch("/api/state" + query, {credentials: "same-origin", cache: "no-store", signal: controller.signal});
      const data = await response.json();
      if (!response.ok || data.ok !== true || !data.result || !Array.isArray(data.result.robots)) throw new Error("state unavailable");
      if (request !== stateSequence) return;
      const previousSelection = selection;
      snapshot = data.result;
      selection = snapshot.selection || chosen;
      if (!sameSelection(previousSelection, selection)) { lastKnownSession = null; confirmedCommit = null; }
      csrf = typeof snapshot.csrf_token === "string" ? snapshot.csrf_token : "";
      stateFresh = true;
      if (snapshot.status?.session_id) lastKnownSession = {...snapshot.status, selection: {...selection}};
      else if (snapshot.resume_session?.session_id) lastKnownSession = {...snapshot.resume_session, selection: {...selection}};
      else if (!snapshot.status_error && snapshot.status?.state === "IDLE") lastKnownSession = null;
      else if (snapshot.status_error?.details?.session_id) lastKnownSession = {...snapshot.status_error.details, selection: {...selection}};
      else if (snapshot.known_session?.session_id) lastKnownSession = {...snapshot.known_session, selection: {...selection}};
      if (pending?.action === "stop" && snapshot.status?.finish_state &&
          snapshot.status.session_id === pending.session_id && sameSelection(pending, selection)) {
        // The service confirms this session is ending even when the original HTTP acknowledgement was lost.
        // Retain the frozen request without replaying it or asking the operator to reconcile it.
        preserveSuperseded(pending, true); savePending(null);
      }
      if (!pending && snapshot.pending?.retryable === true && snapshot.pending?.client_payload) {
        const original = snapshot.pending.client_payload;
        const finishConfirmed = original.action === "stop" && snapshot.status?.finish_state &&
          snapshot.status.session_id === original.session_id && sameSelection(original, selection);
        if (!finishConfirmed && actions[original.action] && typeof original.request_id === "string" && typeof original.robot_key === "string" && typeof original.mode === "string") savePending({...original});
      }
      render();
      void refreshVideo();
    } catch {
      if (request !== stateSequence) return;
      stateFresh = false; render();
    } finally {
      clearTimeout(timeout);
      if (request === stateSequence) {
        polling = false; selecting = false; renderSelectors(); renderButtons();
        clearTimeout(pollTimer);
        const delay = pollAgain ? 0 : 1000; pollAgain = false;
        pollTimer = setTimeout(() => { void refreshState(); }, delay);
      }
    }
  }

  function commandPayload(action, extra = {}) {
    const status = action === "stop" && snapshot?.status?.state === "IDLE" && snapshot?.resume_session?.session_id ? snapshot.resume_session : snapshot?.status?.session_id ? snapshot.status : lastKnownSession;
    const payload = {action, request_id: requestId(), robot_key: selection.robot_key, mode: selection.mode, ...extra};
    if (action !== "start" && payload.session_id === undefined) payload.session_id = action === "resume" ? recoverySession() : status?.session_id;
    if (action === "redo" || action === "complete") { payload.episode_id = status.episode_id; payload.recording_generation = status.recording_generation; }
    return payload;
  }

  async function sendCommand(action, extra = {}, retry = null) {
    if ((sending && (action !== "stop" || retry)) || (!retry && !can(action) && action !== "resume")) return;
    if (!csrf || !selection?.robot_key) { setNotice("请先等待服务连接恢复。", "warning"); return; }
    let payload;
    try { payload = retry || commandPayload(action, extra); }
    catch (error) { setNotice(error.message, "error"); return; }
    if (action === "stop") preserveSuperseded(pending || snapshot?.pending?.client_payload || snapshot?.pending);
    const command = ++commandSequence;
    savePending(payload); sending = true; localNotice = null; render();
    const controller = new AbortController(); const timeout = setTimeout(() => controller.abort(), 30000);
    try {
      const response = await fetch("/api/command", {method: "POST", credentials: "same-origin", cache: "no-store", headers: {"Content-Type": "application/json", "X-CSRF-Token": csrf}, body: JSON.stringify(payload), signal: controller.signal});
      let data;
      try { data = await response.json(); } catch { throw new Error("unconfirmed response"); }
      if (command !== commandSequence) return;
      if (data.ok !== true && data.ok !== false) throw new Error("unconfirmed response");
      if (data.ok === true && response.ok) {
        savePending(null);
        if (payload.action === "start") confirmedCommit = null;
        if (payload.action === "stop") {
          const notice = finishNotice(data.result) || {message: "结束请求已接收，数据将自动保存并发送。", tone: "success"};
          setNotice(notice.message, notice.tone);
        } else {
          setNotice(actions[payload.action] + "已完成。", "success");
        }
      } else {
        const error = data.error || {};
        if (data.ok !== false || typeof error.code !== "string" || !error.code) throw new Error("unconfirmed response");
        if (!unconfirmedCodes.has(error.code)) savePending(null);
        if (error.code === "NEXT_EPISODE_FAILED" && error.details?.committed_episode) confirmedCommit = {episode: {...error.details.committed_episode}, selection: {robot_key: payload.robot_key, mode: payload.mode}};
        setNotice(humanMessage(error), "error");
      }
    } catch {
      if (command === commandSequence) setNotice("操作结果尚未收到。请重试原操作，避免重复创建采集任务。", "warning");
    } finally {
      clearTimeout(timeout);
      if (command === commandSequence) { sending = false; stateFresh = false; render(); void refreshState(null, true); }
    }
  }

  function openDialog(action = "start") {
    if (action === "start" && !can("start")) return;
    if (action === "resume" && (!recoverySession() || hasPending() || selecting || !stateFresh)) return;
    dialogAction = action;
    dialogContext = {selection: {...selection}, session_id: action === "resume" ? recoverySession() : null};
    const resume = action === "resume";
    const controlled = resume;
    text("dialog-title", resume ? "恢复保留的采集" : "开始新的采集任务");
    text("dialog-description", resume ? "请先确认现场环境，再恢复原有采集进度。" : "为这次采集填写名称，便于之后查找数据。");
    text("dialog-submit", resume ? "确认并恢复" : "开始采集");
    const fields = [$("task-name-input").closest(".field"), $("task-description-input").closest(".field")];
    fields.forEach((field) => { field.hidden = controlled; });
    $("task-name-input").required = !controlled;
    $("workspace-confirmation").hidden = !controlled && !isPhysical();
    $("workspace-confirmed").checked = false;
    $("workspace-confirmed").required = controlled || isPhysical();
    $("name-error").hidden = true;
    text("workspace-confirmation-text", resume ? "我已在现场确认工作空间安全，可以恢复采集。" : "我已在现场确认工作空间安全，可以开始采集。");
    text("dialog-mode-hint", resume ? "恢复将继续采集已保留的当前条。请确保遥操输入已经手工连接。" : isPhysical() ? "启动前，请手工连接真机遥操输入并确认工作空间安全。" : "启动前，请手工连接仿真遥操输入。");
    $("task-dialog").showModal();
    (controlled ? $("workspace-confirmed") : $("task-name-input")).focus();
  }

  $("start-button").addEventListener("click", () => openDialog("start"));
  for (const action of ["redo", "complete"]) $(action + "-button").addEventListener("click", () => { void sendCommand(action); });
  $("stop-button").addEventListener("click", () => { void sendCommand("stop"); });
  $("retry-button").addEventListener("click", () => { if (pending) void sendCommand(pending.action, {}, pending); });
  $("resume-button").addEventListener("click", () => openDialog("resume"));
  for (const id of ["dialog-close", "dialog-cancel"]) $(id).addEventListener("click", () => $("task-dialog").close());
  $("task-dialog").addEventListener("click", (event) => {
    if (event.target !== $("task-dialog")) return;
    const rect = $("task-dialog").getBoundingClientRect();
    if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) $("task-dialog").close();
  });
  $("task-name-input").addEventListener("input", () => { $("name-error").hidden = true; $("task-name-input").removeAttribute("aria-invalid"); });
  $("task-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const resume = dialogAction === "resume";
    const controlled = resume;
    const name = $("task-name-input").value.trim();
    if (!controlled && !name) { $("name-error").hidden = false; $("task-name-input").setAttribute("aria-invalid", "true"); $("task-name-input").focus(); return; }
    if (!sameSelection(dialogContext?.selection, selection) || (!controlled && !can("start")) || (resume && (recoverySession() !== dialogContext?.session_id || hasPending() || selecting || !stateFresh))) { $("task-dialog").close(); setNotice("采集状态已变化，请确认最新状态后再操作。", "warning"); return; }
    const extra = controlled ? {workspace_confirmed: true, session_id: dialogContext.session_id} : {name, description: $("task-description-input").value.trim()};
    if (!$("workspace-confirmation").hidden) {
      if (!$("workspace-confirmed").checked) { $("workspace-confirmed").focus(); return; }
      extra.workspace_confirmed = true;
    }
    $("task-dialog").close(); void sendCommand(dialogAction, extra);
  });
  for (const id of ["mode-select", "robot-select"]) $(id).addEventListener("change", () => {
    if (selectionLocked()) { renderSelectors(); return; }
    localNotice = null;
    void refreshState({robot_key: $("robot-select").value, mode: $("mode-select").value});
  });
  $("camera-select").addEventListener("change", () => { cameraKey = $("camera-select").value; renderCamera(); void refreshVideo(); });
  $("camera-image").addEventListener("loadeddata", () => { videoLastTime = Number($("camera-image").currentTime) || 0; });
  $("camera-image").addEventListener("timeupdate", () => {
    const video = $("camera-image"); const current = Number(video.currentTime) || 0;
    if (typeof video.requestVideoFrameCallback !== "function" && current > videoLastTime && video.readyState >= 2) decodedVideoFrame(cameraSequence);
    videoLastTime = current;
  });
  document.addEventListener("visibilitychange", () => {
    videoPaused = document.hidden;
    if (document.hidden) { clearVideo({resetRetry: false}); renderCamera(); }
    else { void refreshState(); void refreshVideo(); }
  });
  window.addEventListener?.("pagehide", () => { videoPaused = true; clearVideo({resetRetry: false}); });
  window.addEventListener?.("pageshow", () => { videoPaused = document.hidden; if (!videoPaused) void refreshVideo(); });
  // Navigation and closing the page never send stop, release, or any other robot command.
  void refreshState();
})();
