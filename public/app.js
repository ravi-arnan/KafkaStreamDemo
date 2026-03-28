/**
 * KafkaStream Dashboard — Frontend App
 * Real-time Kafka log processing demo
 */

// ─── State ─────────────────────────────────────────────────────────────────

const state = {
  connected: false,
  paused: false,
  topics: [],
  consumerGroups: [],
  stats: { totalProduced: 0, totalConsumed: 0, throughput: 0, uptime: 0 },
  levelCounts: { DEBUG: 0, INFO: 0, WARN: 0, ERROR: 0, FATAL: 0 },
  serviceCounts: {},
  throughputHistory: new Array(60).fill(null),
  maxMessages: 500,
  messages: [],
  producerConfig: { speed: 1000, batchSize: 3, running: true },
  // Tahap 3 — Partition Heatmap + Consumer Lag Demo
  partitionActivity: {}, // { "topic:partition": [timestamp, ...] } rolling 30s
  consumerPaused: {}, // { groupId: boolean }
};

// ─── DOM references ─────────────────────────────────────────────────────────

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

const els = {
  connectionBadge: $("#connectionBadge"),
  connDot: $("#connectionBadge .conn-dot"),
  connLabel: $("#connectionBadge .conn-label"),
  uptimeDisplay: $("#uptimeDisplay"),
  kpiProduced: $("#kpiProduced"),
  kpiConsumed: $("#kpiConsumed"),
  kpiThroughput: $("#kpiThroughput"),
  kpiTopics: $("#kpiTopics"),
  currentThroughputBadge: $("#currentThroughputBadge"),
  logStream: $("#logStream"),
  topicsGrid: $("#topicsGrid"),
  consumersList: $("#consumersList"),
  producerLog: $("#producerLog"),
  streamTopicFilter: $("#streamTopicFilter"),
  streamLevelFilter: $("#streamLevelFilter"),
  topicModal: $("#topicModal"),
};

// ─── SSE Connection ──────────────────────────────────────────────────────────

let sse;

function connectSSE() {
  sse = new EventSource("./api/stream");

  sse.addEventListener("init", (e) => {
    const data = JSON.parse(e.data);
    state.topics = data.topics || [];
    state.consumerGroups = data.consumerGroups || [];
    Object.assign(state.stats, data.stats || {});

    setConnected(true);
    renderTopics();
    renderConsumers();
    updateStats();
    populateTopicFilters();
    updateArchStats(state.stats);
  });

  sse.addEventListener("messages", (e) => {
    const { records, stats, consumerLags } = JSON.parse(e.data);
    if (stats) Object.assign(state.stats, stats);

    records.forEach((record) => {
      processRecord(record, false);
    });

    updateStats();
    updateThroughputChart(state.stats.throughput);
    updateLevelBars();
    updateServiceList();
    updateArchView(records, state.stats);
    if (consumerLags) updateConsumerLagsFromSSE(consumerLags);
  });

  sse.addEventListener("manual-message", (e) => {
    const { record } = JSON.parse(e.data);
    processRecord(record, true);
    appendProducerLog(record);
    showToast(`Message sent to ${record.topic}`, "success");
  });

  sse.addEventListener("topic-created", (e) => {
    const topic = JSON.parse(e.data);
    if (!state.topics.find((t) => t.name === topic.name)) {
      state.topics.push(topic);
    }
    renderTopics();
    populateTopicFilters();
    updateTopicCount();
    showToast(`Topic "${topic.name}" created`, "success");
  });

  sse.addEventListener("topic-deleted", (e) => {
    const { name } = JSON.parse(e.data);
    state.topics = state.topics.filter((t) => t.name !== name);
    renderTopics();
    populateTopicFilters();
    updateTopicCount();
    showToast(`Topic "${name}" deleted`, "info");
  });

  sse.addEventListener("producer-config", (e) => {
    const cfg = JSON.parse(e.data);
    Object.assign(state.producerConfig, cfg);
  });

  sse.addEventListener("consumer-paused", (e) => {
    const { id } = JSON.parse(e.data);
    state.consumerPaused[id] = true;
    updateLagMeter(id, null, true);
    // Re-render card to show paused badge
    renderConsumers();
    showToast(`Consumer "${id}" di-pause — lag mulai menumpuk`, "info");
  });

  sse.addEventListener("consumer-resumed", (e) => {
    const { id } = JSON.parse(e.data);
    state.consumerPaused[id] = false;
    updateLagMeter(id, null, false);
    renderConsumers();
    showToast(`Consumer "${id}" di-resume — lag mulai turun`, "success");
  });

  sse.onerror = () => {
    setConnected(false);
    setTimeout(connectSSE, 3000);
  };
}

function setConnected(val) {
  state.connected = val;
  els.connDot.className = "conn-dot" + (val ? " connected" : " error");
  els.connLabel.textContent = val ? "Connected" : "Reconnecting...";
}

// ─── Message Processing ──────────────────────────────────────────────────────

function processRecord(record, isManual) {
  const val = record.value || {};
  const level = val.level || "INFO";
  const service = val.service || "unknown";

  // Level stats
  state.levelCounts[level] = (state.levelCounts[level] || 0) + 1;

  // Service stats
  state.serviceCounts[service] = (state.serviceCounts[service] || 0) + 1;

  // Partition activity tracking (heatmap rolling window)
  trackPartitionActivity(record);

  // Add to message buffer
  state.messages.unshift(record);
  if (state.messages.length > state.maxMessages) state.messages.pop();

  // Append to live stream
  if (!state.paused) {
    const topicFilter = els.streamTopicFilter.value;
    const levelFilter = els.streamLevelFilter.value;
    if (
      (topicFilter === "all" || record.topic === topicFilter) &&
      (levelFilter === "all" || level === levelFilter)
    ) {
      prependStreamEntry(record, true);
    }
  }
}

function prependStreamEntry(record, isNew) {
  const stream = els.logStream;
  const empty = stream.querySelector(".stream-empty");
  if (empty) empty.remove();

  const val = record.value || {};
  const level = val.level || "INFO";
  const time = formatTime(record.timestamp);

  const entry = document.createElement("div");
  entry.className = "log-entry" + (isNew ? " new" : "");
  entry.innerHTML = `
    <span class="log-time">${time}</span>
    <span class="log-level-cell"><span class="level-badge ${level.toLowerCase()}">${level}</span></span>
    <span class="log-service">${escapeHtml(val.service || "")}</span>
    <span class="log-message">${escapeHtml(val.message || JSON.stringify(val))}</span>
    <span class="log-topic">${escapeHtml(record.topic)}</span>
  `;

  stream.insertBefore(entry, stream.firstChild);

  // Keep DOM lean: remove entries beyond 200
  const entries = stream.querySelectorAll(".log-entry");
  if (entries.length > 200) {
    entries[entries.length - 1].remove();
  }
}

function appendProducerLog(record) {
  const container = els.producerLog;
  const empty = container.querySelector(".stream-empty");
  if (empty) empty.remove();
  prependTo(container, record, true);
}

function prependTo(container, record, isNew) {
  const val = record.value || {};
  const level = val.level || "INFO";
  const entry = document.createElement("div");
  entry.className = "log-entry" + (isNew ? " new" : "");
  entry.innerHTML = `
    <span class="log-time">${formatTime(record.timestamp)}</span>
    <span class="log-level-cell"><span class="level-badge ${level.toLowerCase()}">${level}</span></span>
    <span class="log-service">${escapeHtml(val.service || "")}</span>
    <span class="log-message">${escapeHtml(val.message || "")}</span>
    <span class="log-topic">${escapeHtml(record.topic)}</span>
  `;
  container.insertBefore(entry, container.firstChild);
  const entries = container.querySelectorAll(".log-entry");
  if (entries.length > 50) entries[entries.length - 1].remove();
}

// ─── Stats & Charts ──────────────────────────────────────────────────────────

let chartCanvas,
  chartCtx,
  chartLastRender = 0;

function initChart() {
  chartCanvas = $("#throughputChart");
  chartCtx = chartCanvas.getContext("2d");
}

function updateThroughputChart(current) {
  state.throughputHistory.push(current);
  state.throughputHistory.shift();

  const now = Date.now();
  if (now - chartLastRender < 200) return; // throttle at 5fps
  chartLastRender = now;

  const canvas = chartCanvas;
  const ctx = chartCtx;
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.offsetWidth;
  const h = 120;
  if (w === 0) return;

  canvas.width = w * dpr;
  canvas.height = h * dpr;
  ctx.scale(dpr, dpr);
  canvas.style.height = h + "px";

  // Only draw points that have real values
  const history = state.throughputHistory;
  const realPoints = history.filter((v) => v !== null);
  const maxVal = Math.max(1, ...realPoints) * 1.3;
  const stepX = w / (history.length - 1);

  ctx.clearRect(0, 0, w, h);

  // Grid lines
  const dividerColor =
    getComputedStyle(document.documentElement)
      .getPropertyValue("--divider")
      .trim() || "#1e2030";
  ctx.strokeStyle = dividerColor;
  ctx.lineWidth = 1;
  [0.25, 0.5, 0.75].forEach((frac) => {
    const y = Math.round(h * frac) + 0.5;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(w, y);
    ctx.stroke();
  });

  // Only render if we have data
  if (realPoints.length < 2) return;

  const accent =
    getComputedStyle(document.documentElement)
      .getPropertyValue("--accent")
      .trim() || "#f07030";
  const gradient = ctx.createLinearGradient(0, 0, 0, h);
  gradient.addColorStop(0, accent + "55");
  gradient.addColorStop(1, accent + "00");

  // Build path from real data points (skip nulls)
  const points = [];
  history.forEach((val, i) => {
    if (val !== null) {
      const x = i * stepX;
      const y = h - (val / maxVal) * (h - 16) - 8;
      points.push({ x, y });
    }
  });

  if (points.length < 2) return;

  // Fill area
  ctx.beginPath();
  ctx.moveTo(points[0].x, points[0].y);
  for (let i = 1; i < points.length; i++) {
    ctx.lineTo(points[i].x, points[i].y);
  }
  ctx.lineTo(points[points.length - 1].x, h);
  ctx.lineTo(points[0].x, h);
  ctx.closePath();
  ctx.fillStyle = gradient;
  ctx.fill();

  // Line stroke
  ctx.beginPath();
  ctx.moveTo(points[0].x, points[0].y);
  for (let i = 1; i < points.length; i++) {
    ctx.lineTo(points[i].x, points[i].y);
  }
  ctx.strokeStyle = accent;
  ctx.lineWidth = 2;
  ctx.lineJoin = "round";
  ctx.lineCap = "round";
  ctx.stroke();
}

function updateStats() {
  animateNumber(els.kpiProduced, state.stats.totalProduced);
  animateNumber(els.kpiConsumed, state.stats.totalConsumed);
  animateNumber(els.kpiThroughput, state.stats.throughput);
  els.currentThroughputBadge.textContent = `${state.stats.throughput} msg/s`;
  formatUptime(state.stats.uptime);
}

function updateTopicCount() {
  animateNumber(els.kpiTopics, state.topics.length);
}

function updateLevelBars() {
  const totals = Object.values(state.levelCounts).reduce((a, b) => a + b, 1);
  Object.entries(state.levelCounts).forEach(([level, count]) => {
    const fill = $(`[data-level="${level}"]`);
    const counter = $(`[data-count="${level}"]`);
    if (fill) fill.style.width = ((count / totals) * 100).toFixed(1) + "%";
    if (counter) counter.textContent = formatNumber(count);
  });
}

function updateServiceList() {
  const list = $("#serviceList");
  const sorted = Object.entries(state.serviceCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 7);
  if (!sorted.length) return;

  list.innerHTML = sorted
    .map(
      ([name, count]) => `
    <div class="service-item">
      <span class="service-name">${escapeHtml(name)}</span>
      <span class="service-count">${formatNumber(count)}</span>
    </div>
  `,
    )
    .join("");
}

// ─── Architecture View ────────────────────────────────────────────────────────

/**
 * Hash a string key to a partition index (mirrors server-side logic).
 * Used to determine which partition chip to highlight.
 */
function hashKeyToPartition(key, numPartitions) {
  if (!key) return 0;
  let hash = 0;
  for (let i = 0; i < key.length; i++) {
    hash = (hash << 5) - hash + key.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash) % numPartitions;
}

/**
 * Briefly highlight a partition chip in the architecture diagram.
 * @param {string} topic  - topic name (e.g. "app-logs")
 * @param {number} partition - partition index (0, 1, 2)
 */
function flashPartition(topic, partition) {
  const el = $(`#ap-${topic}-${partition}`);
  if (!el) return;
  el.classList.add("active");
  setTimeout(() => el.classList.remove("active"), 600);
}

/**
 * Update the static stats section of the Architecture view
 * (produced count, broker total, throughput badge).
 */
function updateArchStats(stats) {
  const produced = $(`#archStatProduced`);
  const brokerTotal = $(`#archBrokerTotal`);
  const badge = $(`#archThroughputBadge`);
  if (produced)
    produced.textContent = formatNumber(stats.totalProduced) + " msgs";
  if (brokerTotal) brokerTotal.textContent = formatNumber(stats.totalProduced);
  if (badge) badge.textContent = stats.throughput + " msg/s";
}

/**
 * Called every time a batch of SSE messages arrives.
 * Updates architecture stats and flashes the corresponding partitions.
 */
function updateArchView(records, stats) {
  updateArchStats(stats);

  // Flash partitions for each incoming record (throttle to avoid visual overload)
  const MAX_FLASHES = 6;
  const toFlash = records.slice(0, MAX_FLASHES);
  toFlash.forEach((record, i) => {
    setTimeout(() => {
      const partition =
        record.partition !== undefined
          ? record.partition
          : hashKeyToPartition(record.key, 3);
      flashPartition(record.topic, partition);
    }, i * 80);
  });
}

// ─── Partition Heatmap ────────────────────────────────────────────────────────

/**
 * Record a message arrival for a specific topic+partition.
 * Timestamps older than 30s are pruned on render.
 */
function trackPartitionActivity(record) {
  const partition =
    record.partition !== undefined
      ? record.partition
      : hashKeyToPartition(record.key, 3);
  const key = `${record.topic}:${partition}`;
  if (!state.partitionActivity[key]) state.partitionActivity[key] = [];
  state.partitionActivity[key].push(Date.now());
}

/**
 * Re-render the heatmap grid in the Architecture view.
 * Called every 2 seconds via setInterval.
 */
function renderHeatmap() {
  const container = $("#partitionHeatmap");
  if (!container) return;

  const WINDOW_MS = 30_000;
  const cutoff = Date.now() - WINDOW_MS;

  // Prune old timestamps
  Object.keys(state.partitionActivity).forEach((k) => {
    state.partitionActivity[k] = state.partitionActivity[k].filter(
      (t) => t > cutoff,
    );
  });

  const topics = ["app-logs", "error-logs", "audit-events"];
  const partitions = [0, 1, 2];

  // Find max count for normalisation
  let maxCount = 1;
  topics.forEach((topic) => {
    partitions.forEach((p) => {
      const count = (state.partitionActivity[`${topic}:${p}`] || []).length;
      if (count > maxCount) maxCount = count;
    });
  });

  // Build HTML — keep static header row, replace data rows
  const rows = topics
    .map((topic) => {
      const cells = partitions
        .map((p) => {
          const count = (state.partitionActivity[`${topic}:${p}`] || []).length;
          const intensity = maxCount > 0 ? count / maxCount : 0;
          const isHot = intensity > 0.55;
          return `<div
            class="heatmap-cell ${isHot ? "hot" : ""}"
            style="--cell-opacity: ${(intensity * 0.65).toFixed(3)}"
            title="${topic} · P${p} · ${count} msgs/30s"
          >
            <span class="heatmap-cell-count">${count}</span>
            <span class="heatmap-cell-label">msgs/30s</span>
          </div>`;
        })
        .join("");
      return `<div class="heatmap-row-label">${topic}</div>${cells}`;
    })
    .join("");

  // Replace everything except the 4 static header divs
  // Remove old row content first
  const children = Array.from(container.children);
  // First 4 children are the col-headers — keep them
  children.slice(4).forEach((el) => el.remove());
  const frag = document.createDocumentFragment();
  const tmp = document.createElement("div");
  tmp.innerHTML = rows;
  while (tmp.firstChild) frag.appendChild(tmp.firstChild);
  container.appendChild(frag);

  // Remove "menunggu data" placeholder if present
  const empty = container.querySelector(".heatmap-empty");
  if (empty && Object.keys(state.partitionActivity).length > 0) empty.remove();
}

// ─── Consumer Lag Demo ────────────────────────────────────────────────────────

/** Map lag value → CSS color string */
function lagToColor(lag) {
  if (lag === 0) return "var(--info-color)";
  if (lag < 15) return "#7ec84a";
  if (lag < 50) return "var(--warn-color)";
  if (lag < 150) return "#e07530";
  return "var(--error-color)";
}

/** Map lag value → percentage width for the meter bar (max = 300) */
function lagToPct(lag) {
  return Math.min(100, (lag / 300) * 100).toFixed(1);
}

/** CSS class name for the lag value number */
function lagClass(lag) {
  if (lag === 0) return "lag-ok";
  if (lag < 50) return "lag-warn";
  return "lag-crit";
}

/**
 * Update lag meter + value without full re-render.
 * Called from SSE broadcast data (every ~500ms).
 * @param {string} groupId
 * @param {number|null} lag  — null = keep existing value
 * @param {boolean|null} paused — null = keep existing state
 */
function updateLagMeter(groupId, lag, paused) {
  // Lag value text
  const lagEl = $(`#lag-${groupId}`);
  if (lagEl && lag !== null) {
    lagEl.textContent = lag;
    lagEl.className = `lag-value ${lagClass(lag)}`;
  }

  // Meter fill
  const fillEl = $(`#lagfill-${groupId}`);
  if (fillEl && lag !== null) {
    fillEl.style.width = lagToPct(lag) + "%";
    fillEl.style.background = lagToColor(lag);
    fillEl.classList.toggle("lag-growing", paused === true && lag > 0);
  }

  // Meter label
  const labelEl = $(`#lagval-${groupId}`);
  if (labelEl && lag !== null) {
    labelEl.textContent = `${lag} unread msgs`;
  }

  // Architecture view lag
  const archEl = $(`#archLag-${groupId}`);
  if (archEl && lag !== null) {
    archEl.textContent = lag;
  }
}

/**
 * Bulk-update all consumer lags from SSE "messages" event payload.
 * Data shape: { [groupId]: { lag: number, paused: boolean } }
 */
function updateConsumerLagsFromSSE(lagsData) {
  Object.entries(lagsData).forEach(([id, { lag, paused }]) => {
    // Update client-side paused state silently
    state.consumerPaused[id] = paused;
    updateLagMeter(id, lag, paused);
  });
}

/**
 * Toggle pause/resume for a consumer group via REST API.
 */
async function toggleConsumerPause(groupId) {
  const isPaused = !!state.consumerPaused[groupId];
  const action = isPaused ? "resume" : "pause";
  try {
    await postJSON(
      `./api/consumer-groups/${encodeURIComponent(groupId)}/${action}`,
      {},
    );
  } catch (e) {
    showToast(`Gagal ${action} consumer "${groupId}"`, "error");
  }
}

// ─── Topics ──────────────────────────────────────────────────────────────────

function renderTopics() {
  const grid = els.topicsGrid;
  if (!state.topics.length) {
    grid.innerHTML = '<div class="empty-state">No topics. Create one!</div>';
    return;
  }
  grid.innerHTML = state.topics
    .map(
      (t) => `
    <div class="topic-card" data-topic="${escapeHtml(t.name)}">
      <div class="topic-name">
        <span>${escapeHtml(t.name)}</span>
        <button class="topic-delete" data-delete-topic="${escapeHtml(t.name)}" title="Delete topic">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4h6v2"/></svg>
        </button>
      </div>
      <div class="topic-meta">
        <span class="topic-tag">📦 ${t.partitions} partitions</span>
        <span class="topic-tag">🔄 RF: ${t.replicationFactor}</span>
        <span class="topic-tag mono">${formatNumber(t.messageCount)} msgs</span>
      </div>
      <div class="topic-partitions">
        ${(t.partitionDetails || []).map((p) => `<span class="partition-chip">P${p.id}: offset ${p.offset}</span>`).join("")}
      </div>
    </div>
  `,
    )
    .join("");

  updateTopicCount();
}

// ─── Consumers ───────────────────────────────────────────────────────────────

function renderConsumers() {
  const list = els.consumersList;
  if (!state.consumerGroups.length) {
    list.innerHTML = '<div class="empty-state">No consumer groups</div>';
    return;
  }

  list.innerHTML = state.consumerGroups
    .map((g) => {
      const paused = !!state.consumerPaused[g.id];
      const lag = g.lag ?? 0;
      const pauseIcon = paused
        ? '<svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg> Resume'
        : '<svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg> Pause';
      return `
    <div class="consumer-card" id="ccard-${escapeHtml(g.id)}">
      <div class="consumer-card-header">
        <div class="consumer-card-left">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:3px">
            <div class="consumer-id">${escapeHtml(g.id)}</div>
            ${paused ? '<span class="paused-badge">⏸ PAUSED</span>' : ""}
          </div>
          <div class="consumer-role">${escapeHtml(g.role || "")}</div>
        </div>
        <div class="consumer-card-right">
          <div class="consumer-lag-block">
            <div class="lag-value ${lagClass(lag)}" id="lag-${escapeHtml(g.id)}">${lag}</div>
            <div class="lag-label">msg lag</div>
          </div>
          <button
            class="btn-pause${paused ? " paused" : ""}"
            data-group="${escapeHtml(g.id)}"
            title="${paused ? "Resume consumer" : "Pause consumer"}"
          >${pauseIcon}</button>
        </div>
      </div>
      <div class="lag-meter">
        <div class="lag-meter-track">
          <div
            class="lag-meter-fill${paused && lag > 0 ? " lag-growing" : ""}"
            id="lagfill-${escapeHtml(g.id)}"
            style="width:${lagToPct(lag)}%;background:${lagToColor(lag)}"
          ></div>
        </div>
        <div class="lag-meter-labels">
          <span class="lag-meter-val" id="lagval-${escapeHtml(g.id)}">${lag} unread msgs</span>
          <span class="lag-meter-max">max ~300</span>
        </div>
      </div>
      <div class="consumer-topics">
        ${(g.topics || []).map((t) => `<span class="consumer-topic-chip">${escapeHtml(t)}</span>`).join("")}
      </div>
    </div>`;
    })
    .join("");

  // Bind pause/resume buttons via event delegation
  list.querySelectorAll(".btn-pause").forEach((btn) => {
    btn.addEventListener("click", () => toggleConsumerPause(btn.dataset.group));
  });
}

// Refresh consumer lag every 5s (fallback — primary update via SSE broadcast)
setInterval(async () => {
  try {
    const groups = await fetchJSON("./api/consumer-groups");
    groups.forEach((g) => {
      // Sync paused state from server in case of page reload
      state.consumerPaused[g.id] = !!g.paused;
      updateLagMeter(g.id, g.lag, g.paused);
    });
  } catch (_) {}
}, 5000);

// ─── Producer Controls ────────────────────────────────────────────────────────

function initProducerControls() {
  const toggle = $("#producerToggle");
  const speed = $("#speedSlider");
  const batch = $("#batchSlider");

  toggle.addEventListener("change", () => {
    state.producerConfig.running = toggle.checked;
    $("#producerStatusHint").textContent = toggle.checked
      ? "Running"
      : "Paused";
    postJSON("./api/producer/config", { running: toggle.checked });
  });

  speed.addEventListener("input", () => {
    const val = parseInt(speed.value);
    $("#speedHint").textContent = val + "ms";
    state.producerConfig.speed = val;
  });
  speed.addEventListener("change", () => {
    postJSON("./api/producer/config", { speed: parseInt(speed.value) });
  });

  batch.addEventListener("input", () => {
    const val = parseInt(batch.value);
    $("#batchHint").textContent = val + " msgs";
    state.producerConfig.batchSize = val;
  });
  batch.addEventListener("change", () => {
    postJSON("./api/producer/config", { batchSize: parseInt(batch.value) });
  });

  $("#sendManualBtn").addEventListener("click", async () => {
    const topic = $("#manualTopic").value;
    const level = $("#manualLevel").value;
    const message = $("#manualMessage").value.trim();

    try {
      await postJSON("./api/produce", {
        topic,
        level,
        value: message || undefined,
      });
      $("#manualMessage").value = "";
    } catch (e) {
      showToast("Failed to produce message", "error");
    }
  });
}

// ─── Navigation ───────────────────────────────────────────────────────────────

function initNavigation() {
  $$(".nav-item").forEach((btn) => {
    btn.addEventListener("click", () => {
      const view = btn.dataset.view;
      $$(".nav-item").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      $$(".view").forEach((v) => v.classList.remove("active"));
      $(`#view-${view}`).classList.add("active");

      if (view === "topics") renderTopics();
      if (view === "consumers") renderConsumers();
    });
  });
}

// ─── Stream Controls ──────────────────────────────────────────────────────────

function initStreamControls() {
  $("#clearStreamBtn").addEventListener("click", () => {
    els.logStream.innerHTML =
      '<div class="stream-empty"><p>Stream cleared</p></div>';
    state.messages = [];
  });

  const pauseBtn = $("#pauseStreamBtn");
  pauseBtn.addEventListener("click", () => {
    state.paused = !state.paused;
    if (state.paused) {
      pauseBtn.innerHTML =
        '<svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg> Resume';
    } else {
      pauseBtn.innerHTML =
        '<svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg> Pause';
    }
  });

  els.streamTopicFilter.addEventListener("change", () => replayStream());
  els.streamLevelFilter.addEventListener("change", () => replayStream());
}

function replayStream() {
  if (!state.messages.length) return;
  els.logStream.innerHTML = "";
  const tf = els.streamTopicFilter.value;
  const lf = els.streamLevelFilter.value;
  const filtered = state.messages
    .filter((m) => {
      const level = (m.value || {}).level || "INFO";
      return (tf === "all" || m.topic === tf) && (lf === "all" || level === lf);
    })
    .slice(0, 200);
  filtered.forEach((m) => prependStreamEntry(m, false));
}

// ─── Topics Management ────────────────────────────────────────────────────────

function initTopicsManagement() {
  $("#newTopicBtn").addEventListener("click", () => {
    els.topicModal.classList.remove("hidden");
    $("#topicNameInput").focus();
  });

  $("#closeTopicModal, #cancelTopicModal").forEach
    ? $$("#closeTopicModal, #cancelTopicModal").forEach((b) =>
        b.addEventListener("click", closeTopicModal),
      )
    : null;

  $("#confirmCreateTopic").addEventListener("click", async () => {
    const name = $("#topicNameInput").value.trim();
    if (!name) return;
    const partitions = parseInt($("#topicPartitions").value) || 3;
    const replicationFactor = parseInt($("#topicReplication").value) || 1;
    try {
      await postJSON("./api/topics", { name, partitions, replicationFactor });
      closeTopicModal();
      $("#topicNameInput").value = "";
    } catch (e) {
      showToast(e.message || "Failed to create topic", "error");
    }
  });

  // Delete topic via event delegation
  els.topicsGrid.addEventListener("click", async (e) => {
    const btn = e.target.closest("[data-delete-topic]");
    if (!btn) return;
    const name = btn.dataset.deleteTopic;
    if (!confirm(`Delete topic "${name}"?`)) return;
    try {
      await fetch(`./api/topics/${encodeURIComponent(name)}`, {
        method: "DELETE",
      });
    } catch (e) {
      showToast("Failed to delete topic", "error");
    }
  });

  // Close modal on overlay click
  els.topicModal.addEventListener("click", (e) => {
    if (e.target === els.topicModal) closeTopicModal();
  });
}

function closeTopicModal() {
  els.topicModal.classList.add("hidden");
}

function populateTopicFilters() {
  const sel = els.streamTopicFilter;
  const current = sel.value;
  sel.innerHTML =
    '<option value="all">All Topics</option>' +
    state.topics
      .map(
        (t) =>
          `<option value="${escapeHtml(t.name)}" ${t.name === current ? "selected" : ""}>${escapeHtml(t.name)}</option>`,
      )
      .join("");

  const manualTopic = $("#manualTopic");
  const mCurrent = manualTopic.value;
  manualTopic.innerHTML = state.topics
    .map(
      (t) =>
        `<option value="${escapeHtml(t.name)}" ${t.name === mCurrent ? "selected" : ""}>${escapeHtml(t.name)}</option>`,
    )
    .join("");
}

// ─── Uptime ticker ────────────────────────────────────────────────────────────

setInterval(() => {
  if (state.stats.uptime !== undefined) {
    state.stats.uptime++;
    formatUptime(state.stats.uptime);
  }
}, 1000);

function formatUptime(secs) {
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  els.uptimeDisplay.textContent =
    h > 0 ? `${h}h ${m}m` : m > 0 ? `${m}m ${s}s` : `${s}s`;
}

// ─── Theme Toggle ─────────────────────────────────────────────────────────────

function initTheme() {
  const btn = $("[data-theme-toggle]");
  const html = document.documentElement;
  let theme = matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
  html.setAttribute("data-theme", theme);
  updateThemeIcon(btn, theme);
  btn.addEventListener("click", () => {
    theme = theme === "dark" ? "light" : "dark";
    html.setAttribute("data-theme", theme);
    updateThemeIcon(btn, theme);
  });
}
function updateThemeIcon(btn, theme) {
  btn.innerHTML =
    theme === "dark"
      ? '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>'
      : '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="5"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/></svg>';
}

// ─── Utils ────────────────────────────────────────────────────────────────────

function formatTime(ts) {
  const d = new Date(ts);
  return d.toLocaleTimeString("en-US", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatNumber(n) {
  if (n >= 1000000) return (n / 1000000).toFixed(1) + "M";
  if (n >= 1000) return (n / 1000).toFixed(1) + "K";
  return String(n);
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// Animate number counter
const numAnimations = new WeakMap();
function animateNumber(el, target) {
  if (!el) return;
  const current = parseInt(el.textContent.replace(/[KM,]/g, "")) || 0;
  if (current === target) return;
  const diff = target - current;
  const duration = Math.min(800, Math.max(200, Math.abs(diff) * 2));
  const start = performance.now();
  const startVal = current;

  if (numAnimations.has(el)) cancelAnimationFrame(numAnimations.get(el));

  function tick(now) {
    const progress = Math.min(1, (now - start) / duration);
    const eased = 1 - Math.pow(1 - progress, 3);
    el.textContent = formatNumber(Math.round(startVal + diff * eased));
    if (progress < 1) {
      numAnimations.set(el, requestAnimationFrame(tick));
    } else {
      el.textContent = formatNumber(target);
    }
  }
  numAnimations.set(el, requestAnimationFrame(tick));
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Request failed" }));
    throw new Error(err.error || "Request failed");
  }
  return res.json();
}

async function postJSON(url, data) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Request failed" }));
    throw new Error(err.error || "Request failed");
  }
  return res.json();
}

// Toast notifications
const toastContainer = document.createElement("div");
toastContainer.className = "toast-container";
document.body.appendChild(toastContainer);

function showToast(message, type = "info") {
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.textContent = message;
  toastContainer.appendChild(toast);
  setTimeout(() => toast.remove(), 3100);
}

// ─── Boot ─────────────────────────────────────────────────────────────────────

function init() {
  initTheme();
  initChart();
  initNavigation();
  initStreamControls();
  initTopicsManagement();
  initProducerControls();
  connectSSE();

  // Heatmap refresh every 2s
  setInterval(renderHeatmap, 2000);
}

document.addEventListener("DOMContentLoaded", init);
