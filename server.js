/**
 * Kafka Real-Time Log Processing Demo
 * Backend: Express + SSE + In-memory Kafka simulation
 *
 * Architecture:
 *  - KafkaBroker: manages topics, partitions, offsets (in-memory)
 *  - Producer: publishes log messages to topics
 *  - Consumer: reads messages and processes them (filter, aggregate)
 *  - SSE: streams real-time events to dashboard
 */

const express = require("express");
const { v4: uuidv4 } = require("uuid");
const path = require("path");

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// ─── Kafka Broker (In-Memory Simulation) ────────────────────────────────────

class KafkaBroker {
  constructor() {
    this.topics = {};
    this.consumerGroups = {};
    this.stats = {
      totalProduced: 0,
      totalConsumed: 0,
      totalErrors: 0,
      throughputHistory: [],
      startTime: Date.now(),
    };
    this.messageWindow = []; // last 60s messages for throughput calc
  }

  createTopic(name, partitions = 3, replicationFactor = 1) {
    if (!this.topics[name]) {
      this.topics[name] = {
        name,
        partitions: Array.from({ length: partitions }, (_, i) => ({
          id: i,
          messages: [],
          offset: 0,
        })),
        replicationFactor,
        createdAt: Date.now(),
        messageCount: 0,
      };
    }
    return this.topics[name];
  }

  produce(topicName, message) {
    const topic = this.topics[topicName];
    if (!topic) throw new Error(`Topic '${topicName}' does not exist`);

    // Partition assignment: key-based or round-robin
    const partitionIdx = message.key
      ? this._hashKey(message.key) % topic.partitions.length
      : topic.messageCount % topic.partitions.length;

    const partition = topic.partitions[partitionIdx];
    const record = {
      id: uuidv4(),
      offset: partition.offset++,
      partition: partitionIdx,
      topic: topicName,
      key: message.key || null,
      value: message.value,
      headers: message.headers || {},
      timestamp: Date.now(),
    };

    partition.messages.push(record);
    // Keep last 500 messages per partition to avoid memory bloat
    if (partition.messages.length > 500) partition.messages.shift();

    topic.messageCount++;
    this.stats.totalProduced++;
    this.messageWindow.push(Date.now());

    // Prune window older than 60s
    const cutoff = Date.now() - 60000;
    this.messageWindow = this.messageWindow.filter((t) => t > cutoff);

    return record;
  }

  consume(topicName, groupId, maxMessages = 20) {
    const topic = this.topics[topicName];
    if (!topic) return [];

    if (!this.consumerGroups[groupId]) {
      this.consumerGroups[groupId] = {};
    }

    const messages = [];
    topic.partitions.forEach((partition, i) => {
      const key = `${topicName}:${i}`;
      const committedOffset = this.consumerGroups[groupId][key] || 0;

      const newMessages = partition.messages.filter(
        (m) => m.offset >= committedOffset,
      );

      messages.push(...newMessages);

      // Commit offset
      if (newMessages.length > 0) {
        this.consumerGroups[groupId][key] =
          newMessages[newMessages.length - 1].offset + 1;
      }
    });

    messages.sort((a, b) => a.timestamp - b.timestamp);
    const batch = messages.slice(0, maxMessages);
    this.stats.totalConsumed += batch.length;
    return batch;
  }

  getThroughput() {
    const now = Date.now();
    const last5s = this.messageWindow.filter((t) => t > now - 5000).length;
    return Math.round(last5s / 5); // msgs/sec
  }

  getTopicInfo(name) {
    const topic = this.topics[name];
    if (!topic) return null;
    return {
      name: topic.name,
      partitions: topic.partitions.length,
      replicationFactor: topic.replicationFactor,
      messageCount: topic.messageCount,
      createdAt: topic.createdAt,
      partitionDetails: topic.partitions.map((p) => ({
        id: p.id,
        offset: p.offset,
        messageCount: p.messages.length,
      })),
    };
  }

  _hashKey(key) {
    let hash = 0;
    for (let i = 0; i < key.length; i++) {
      hash = (hash << 5) - hash + key.charCodeAt(i);
      hash |= 0;
    }
    return Math.abs(hash);
  }
}

// ─── Log Producer ────────────────────────────────────────────────────────────

const LOG_LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR", "FATAL"];
const SERVICES = [
  "auth-service",
  "api-gateway",
  "user-service",
  "payment-service",
  "notification-service",
  "db-proxy",
  "cache-service",
];
const LOG_TEMPLATES = {
  DEBUG: [
    "Cache hit for key {key}",
    "Query executed in {ms}ms",
    "Session validated for user {userId}",
    "Config reloaded",
  ],
  INFO: [
    "Request received: {method} {path}",
    "User {userId} authenticated successfully",
    "Transaction {txId} processed",
    "Email sent to {email}",
    "Service health check passed",
    "Connection pool: {n}/{max} active",
  ],
  WARN: [
    "Rate limit approaching for IP {ip}",
    "Slow query detected: {ms}ms",
    "Retry attempt {n} for service {svc}",
    "Memory usage at {pct}%",
    "Deprecated endpoint called: {path}",
  ],
  ERROR: [
    "Failed to connect to database: timeout",
    "Payment gateway returned 503",
    "JWT validation failed: {reason}",
    "Unhandled exception in {handler}",
    "S3 upload failed: access denied",
  ],
  FATAL: [
    "Out of memory - shutting down",
    "Disk quota exceeded",
    "Critical dependency unavailable",
  ],
};

function randomChoice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function generateLogMessage(level) {
  const templates = LOG_TEMPLATES[level] || LOG_TEMPLATES.INFO;
  let msg = randomChoice(templates);
  return msg
    .replace("{key}", `usr:${Math.floor(Math.random() * 9999)}`)
    .replace("{ms}", Math.floor(Math.random() * 2000))
    .replace("{userId}", `U-${Math.floor(Math.random() * 10000)}`)
    .replace("{txId}", `TX-${uuidv4().slice(0, 8).toUpperCase()}`)
    .replace("{email}", `user${Math.floor(Math.random() * 999)}@example.com`)
    .replace("{n}", Math.floor(Math.random() * 10))
    .replace("{max}", "50")
    .replace(
      "{ip}",
      `10.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`,
    )
    .replace("{pct}", Math.floor(Math.random() * 40) + 60)
    .replace("{svc}", randomChoice(SERVICES))
    .replace(
      "{reason}",
      randomChoice(["expired", "invalid signature", "malformed"]),
    )
    .replace(
      "{handler}",
      randomChoice(["OrderController", "PaymentProcessor", "AuthMiddleware"]),
    )
    .replace(
      "{path}",
      randomChoice([
        "/api/v1/orders",
        "/api/v2/payments",
        "/health",
        "/metrics",
      ]),
    );
}

// ─── Global Broker Setup ─────────────────────────────────────────────────────

const broker = new KafkaBroker();

// Create default topics
["app-logs", "error-logs", "audit-events"].forEach((t) => {
  broker.createTopic(t, 3, 2);
});

// SSE Clients registry
const sseClients = new Set();

function broadcastSSE(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  sseClients.forEach((client) => {
    try {
      client.res.write(payload);
    } catch (e) {
      sseClients.delete(client);
    }
  });
}

// ─── Auto Producer (background loop) ─────────────────────────────────────────

let producerRunning = true;
let producerSpeed = 1000; // ms between batches
let producerBatchSize = 3;
let scenarioErrorMode = false; // when true: force all msgs to ERROR/FATAL

async function autoProducer() {
  while (true) {
    await new Promise((r) => setTimeout(r, producerSpeed));
    if (!producerRunning) continue;

    const produced = [];
    for (let i = 0; i < producerBatchSize; i++) {
      const level = scenarioErrorMode
        ? Math.random() < 0.7
          ? "ERROR"
          : "FATAL"
        : randomChoice(LOG_LEVELS);
      const service = randomChoice(SERVICES);
      const topic =
        level === "ERROR" || level === "FATAL" ? "error-logs" : "app-logs";

      try {
        const record = broker.produce(topic, {
          key: service,
          value: {
            level,
            service,
            message: generateLogMessage(level),
            traceId: uuidv4().slice(0, 16),
            environment: randomChoice(["production", "staging"]),
          },
          headers: { "content-type": "application/json" },
        });

        produced.push(record);

        // Also produce to audit-events for specific levels
        if (level === "ERROR" || level === "FATAL" || level === "WARN") {
          broker.produce("audit-events", {
            key: level,
            value: {
              event: "log-alert",
              severity: level,
              sourceService: service,
              sourceMessage: record.value.message,
              recordId: record.id,
            },
          });
        }
      } catch (e) {
        broker.stats.totalErrors++;
      }
    }

    // Simulate consumer processing (auto-consume, skip paused groups)
    ["app-logs", "error-logs", "audit-events"].forEach((t) => {
      if (broker.topics[t]) {
        CONSUMER_GROUPS.forEach((g) => {
          if (!pausedConsumers.has(g.id)) broker.consume(t, g.id, 100);
        });
      }
    });

    // Broadcast new messages to SSE clients
    if (produced.length > 0) {
      broadcastSSE("messages", {
        records: produced,
        stats: {
          totalProduced: broker.stats.totalProduced,
          totalConsumed: broker.stats.totalConsumed,
          throughput: broker.getThroughput(),
          uptime: Math.floor((Date.now() - broker.stats.startTime) / 1000),
        },
        consumerLags: getConsumerLags(),
      });
    }
  }
}

// Start the auto producer
autoProducer().catch(console.error);

// ─── Consumer groups ──────────────────────────────────────────────────────────

// ─── Paused Consumer Groups ───────────────────────────────────────────────────

const pausedConsumers = new Set();

/**
 * Compute lag + paused state for every consumer group.
 * Returns: { [groupId]: { lag: number, paused: boolean } }
 */
function getConsumerLags() {
  const result = {};
  CONSUMER_GROUPS.forEach((g) => {
    const lag = g.topics.reduce((total, topicName) => {
      const topic = broker.topics[topicName];
      if (!topic) return total;
      const committed = broker.consumerGroups[g.id] || {};
      topic.partitions.forEach((p, i) => {
        const committedOffset = committed[`${topicName}:${i}`] || 0;
        total += Math.max(0, p.offset - committedOffset);
      });
      return total;
    }, 0);
    result[g.id] = { lag, paused: pausedConsumers.has(g.id) };
  });
  return result;
}

const CONSUMER_GROUPS = [
  {
    id: "log-aggregator",
    topics: ["app-logs"],
    role: "Aggregates all app logs",
  },
  {
    id: "error-monitor",
    topics: ["error-logs", "audit-events"],
    role: "Monitors critical errors",
  },
  {
    id: "audit-processor",
    topics: ["audit-events"],
    role: "Processes audit trail",
  },
];

// ─── Scenario Mode ────────────────────────────────────────────────────────────

let activeScenario = null;
let scenarioTimer = null;
const scenarioStepTimers = [];

const SCENARIOS_DEF = [
  {
    id: "high-traffic",
    name: "High Traffic Burst",
    emoji: "🚀",
    tagline: "Spike throughput 10× — lihat sistem menangani beban tinggi",
    concept: "Throughput Scaling · Partition Load · Batching",
    duration: 15,
    theme: "accent",
    setup() {
      producerSpeed = 100;
      producerBatchSize = 15;
      producerRunning = true;
      broadcastSSE("producer-config", {
        speed: 100,
        batchSize: 15,
        running: true,
      });
      broadcastSSE("scenario-step", {
        msg: "⚡ Producer diset ke 100ms / 15 msgs per batch — throughput spike dimulai",
      });
    },
    steps: [
      {
        at: 5000,
        msg: "📊 Throughput memuncak — buka Overview (chart) & Architecture (heatmap) sekarang",
      },
      {
        at: 10000,
        msg: "🔥 Bandingkan: sebelumnya ~3-6 msg/s vs sekarang >50 msg/s — itulah horizontal scaling",
      },
    ],
    cleanup() {
      producerSpeed = 1000;
      producerBatchSize = 3;
      broadcastSSE("producer-config", {
        speed: 1000,
        batchSize: 3,
        running: true,
      });
      broadcastSSE("scenario-step", {
        msg: "✅ Selesai — producer kembali ke 1000ms, 3 msgs/batch",
      });
    },
  },
  {
    id: "lag-buildup",
    name: "Consumer Lag Buildup",
    emoji: "🐢",
    tagline: "Pause semua consumer — pesan menumpuk, lihat lag membengkak",
    concept: "Consumer Lag · Backpressure · Offset Gap",
    duration: 20,
    theme: "warn",
    setup() {
      CONSUMER_GROUPS.forEach((g) => {
        pausedConsumers.add(g.id);
        broadcastSSE("consumer-paused", { id: g.id });
      });
      broadcastSSE("scenario-step", {
        msg: "⏸ Semua consumer di-pause — pesan terus datang dari producer tapi tidak dikonsumsi",
      });
    },
    steps: [
      {
        at: 7000,
        msg: "📈 Lag bertambah pesat — buka tab Consumers untuk lihat lag meter tumbuh merah",
      },
      {
        at: 14000,
        msg: "⚠️ Backpressure: producer terus publish, consumer berhenti → offset gap melebar drastis",
      },
    ],
    cleanup() {
      CONSUMER_GROUPS.forEach((g) => {
        pausedConsumers.delete(g.id);
        broadcastSSE("consumer-resumed", { id: g.id });
      });
      broadcastSSE("scenario-step", {
        msg: "✅ Selesai — semua consumer di-resume, lag akan turun perlahan",
      });
    },
  },
  {
    id: "consumer-recovery",
    name: "Consumer Recovery",
    emoji: "⚡",
    tagline: "Resume consumer setelah lag menumpuk — lihat consumer catch-up",
    concept: "Lag Drain · Offset Commit · Consumer Catch-up",
    duration: 15,
    theme: "info",
    setup() {
      CONSUMER_GROUPS.forEach((g) => {
        pausedConsumers.delete(g.id);
        broadcastSSE("consumer-resumed", { id: g.id });
      });
      producerSpeed = 2000;
      producerBatchSize = 2;
      broadcastSSE("producer-config", {
        speed: 2000,
        batchSize: 2,
        running: true,
      });
      broadcastSSE("scenario-step", {
        msg: "▶ Semua consumer di-resume + producer diperlambat (2000ms/2msgs) — fase recovery dimulai",
      });
    },
    steps: [
      {
        at: 5000,
        msg: "📉 Lag mulai turun — consumer sedang memproses backlog yang menumpuk",
      },
      {
        at: 10000,
        msg: "🎯 Consumer catch-up hampir selesai — offset mendekati latest partition offset",
      },
    ],
    cleanup() {
      producerSpeed = 1000;
      producerBatchSize = 3;
      broadcastSSE("producer-config", {
        speed: 1000,
        batchSize: 3,
        running: true,
      });
      broadcastSSE("scenario-step", {
        msg: "✅ Selesai — sistem kembali normal",
      });
    },
  },
  {
    id: "error-flood",
    name: "Error Storm",
    emoji: "🔥",
    tagline: "Banjiri pipeline dengan ERROR/FATAL — stress test error handling",
    concept: "Error Handling · Alert Threshold · Dead Letter Queue",
    duration: 10,
    theme: "error",
    setup() {
      scenarioErrorMode = true;
      producerSpeed = 300;
      producerBatchSize = 8;
      broadcastSSE("producer-config", {
        speed: 300,
        batchSize: 8,
        running: true,
      });
      broadcastSSE("scenario-step", {
        msg: "🔥 Mode ERROR aktif — semua pesan dipaksa ERROR/FATAL ke error-logs topic",
      });
    },
    steps: [
      {
        at: 4000,
        msg: "⚠️ error-logs meluap — buka Live Stream, filter level ERROR untuk lihat banjir pesan",
      },
      {
        at: 7000,
        msg: "🚨 Skenario nyata: service crash massal — pipeline & consumer harus tahan beban spike",
      },
    ],
    cleanup() {
      scenarioErrorMode = false;
      producerSpeed = 1000;
      producerBatchSize = 3;
      broadcastSSE("producer-config", {
        speed: 1000,
        batchSize: 3,
        running: true,
      });
      broadcastSSE("scenario-step", {
        msg: "✅ Selesai — mode normal dikembalikan, error mode off",
      });
    },
  },
  {
    id: "reset",
    name: "Reset to Normal",
    emoji: "🔄",
    tagline:
      "Kembalikan semua ke default: producer normal, semua consumer aktif",
    concept: "",
    duration: 0,
    theme: "muted",
    instant: true,
    setup() {
      scenarioErrorMode = false;
      producerRunning = true;
      producerSpeed = 1000;
      producerBatchSize = 3;
      CONSUMER_GROUPS.forEach((g) => {
        pausedConsumers.delete(g.id);
        broadcastSSE("consumer-resumed", { id: g.id });
      });
      broadcastSSE("producer-config", {
        speed: 1000,
        batchSize: 3,
        running: true,
      });
      broadcastSSE("scenario-step", {
        msg: "🔄 Reset lengkap — producer 1000ms/3msgs, semua consumer aktif, error mode off",
      });
    },
    steps: [],
    cleanup() {},
  },
];

function stopActiveScenario(broadcast = true) {
  if (scenarioTimer) {
    clearTimeout(scenarioTimer);
    scenarioTimer = null;
  }
  scenarioStepTimers.forEach((t) => clearTimeout(t));
  scenarioStepTimers.length = 0;
  const prevId = activeScenario;
  activeScenario = null;
  if (broadcast && prevId) {
    const def = SCENARIOS_DEF.find((s) => s.id === prevId);
    if (def) def.cleanup();
    broadcastSSE("scenario-stopped", { id: prevId });
  }
}

function runScenario(scenarioId) {
  const def = SCENARIOS_DEF.find((s) => s.id === scenarioId);
  if (!def) return false;

  // Stop any running scenario first (no broadcast for interrupted scenario)
  if (activeScenario) stopActiveScenario(false);

  def.setup();

  // Instant scenarios (duration = 0)
  if (def.instant || def.duration === 0) {
    broadcastSSE("scenario-started", {
      id: def.id,
      name: def.name,
      emoji: def.emoji,
      concept: def.concept,
      duration: 0,
      instant: true,
    });
    broadcastSSE("scenario-stopped", { id: def.id, completed: true });
    return true;
  }

  activeScenario = def.id;
  broadcastSSE("scenario-started", {
    id: def.id,
    name: def.name,
    emoji: def.emoji,
    concept: def.concept,
    duration: def.duration,
  });

  // Schedule intermediate step messages
  def.steps.forEach(({ at, msg }) => {
    scenarioStepTimers.push(
      setTimeout(() => broadcastSSE("scenario-step", { msg }), at),
    );
  });

  // Auto-stop after duration
  scenarioTimer = setTimeout(() => {
    const id = activeScenario;
    scenarioStepTimers.forEach((t) => clearTimeout(t));
    scenarioStepTimers.length = 0;
    def.cleanup();
    activeScenario = null;
    broadcastSSE("scenario-stopped", { id, completed: true });
  }, def.duration * 1000);

  return true;
}

// ─── API Routes ───────────────────────────────────────────────────────────────

// SSE endpoint
app.get("/api/stream", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  res.flushHeaders();

  const client = { id: uuidv4(), res };
  sseClients.add(client);

  // Send initial state
  res.write(
    `event: init\ndata: ${JSON.stringify({
      topics: Object.keys(broker.topics).map((t) => broker.getTopicInfo(t)),
      consumerGroups: CONSUMER_GROUPS,
      stats: {
        totalProduced: broker.stats.totalProduced,
        totalConsumed: broker.stats.totalConsumed,
        throughput: broker.getThroughput(),
        uptime: Math.floor((Date.now() - broker.stats.startTime) / 1000),
      },
    })}\n\n`,
  );

  // Heartbeat every 15s
  const heartbeat = setInterval(() => {
    try {
      res.write(`:heartbeat\n\n`);
    } catch (e) {
      clearInterval(heartbeat);
      sseClients.delete(client);
    }
  }, 15000);

  req.on("close", () => {
    clearInterval(heartbeat);
    sseClients.delete(client);
  });
});

// Get all topics
app.get("/api/topics", (req, res) => {
  const topics = Object.keys(broker.topics).map((t) => broker.getTopicInfo(t));
  res.json(topics);
});

// Create topic
app.post("/api/topics", (req, res) => {
  const { name, partitions = 3, replicationFactor = 1 } = req.body;
  if (!name) return res.status(400).json({ error: "Topic name required" });
  if (broker.topics[name])
    return res.status(409).json({ error: "Topic already exists" });
  broker.createTopic(name, partitions, replicationFactor);
  broadcastSSE("topic-created", broker.getTopicInfo(name));
  res.status(201).json(broker.getTopicInfo(name));
});

// Delete topic
app.delete("/api/topics/:name", (req, res) => {
  const { name } = req.params;
  if (!broker.topics[name])
    return res.status(404).json({ error: "Topic not found" });
  delete broker.topics[name];
  broadcastSSE("topic-deleted", { name });
  res.json({ deleted: name });
});

// Produce message manually
app.post("/api/produce", (req, res) => {
  const {
    topic,
    key,
    value,
    level = "INFO",
    service = "manual-producer",
  } = req.body;
  if (!topic) return res.status(400).json({ error: "topic required" });
  if (!broker.topics[topic])
    return res.status(404).json({ error: "Topic not found" });

  try {
    const record = broker.produce(topic, {
      key: key || service,
      value: {
        level,
        service,
        message: value || generateLogMessage(level),
        traceId: uuidv4().slice(0, 16),
        environment: "manual",
      },
    });
    broadcastSSE("manual-message", { record });
    res.json(record);
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// Consume messages from a topic
app.get("/api/consume/:topic", (req, res) => {
  const { topic } = req.params;
  const { group = "default-consumer", max = 20 } = req.query;
  if (!broker.topics[topic])
    return res.status(404).json({ error: "Topic not found" });
  const messages = broker.consume(topic, group, parseInt(max));
  res.json({ topic, group, messages, count: messages.length });
});

// Get stats
app.get("/api/stats", (req, res) => {
  res.json({
    totalProduced: broker.stats.totalProduced,
    totalConsumed: broker.stats.totalConsumed,
    totalErrors: broker.stats.totalErrors,
    throughput: broker.getThroughput(),
    uptime: Math.floor((Date.now() - broker.stats.startTime) / 1000),
    connectedClients: sseClients.size,
    topicCount: Object.keys(broker.topics).length,
    consumerGroups: Object.keys(broker.consumerGroups).length,
  });
});

// Get consumer groups
app.get("/api/consumer-groups", (req, res) => {
  const lags = getConsumerLags();
  res.json(
    CONSUMER_GROUPS.map((g) => ({
      ...g,
      paused: pausedConsumers.has(g.id),
      offsets: broker.consumerGroups[g.id] || {},
      lag: lags[g.id]?.lag ?? 0,
    })),
  );
});

// Pause a consumer group (stop consuming → lag builds up)
app.post("/api/consumer-groups/:id/pause", (req, res) => {
  const { id } = req.params;
  if (!CONSUMER_GROUPS.find((g) => g.id === id))
    return res.status(404).json({ error: "Consumer group not found" });
  pausedConsumers.add(id);
  broadcastSSE("consumer-paused", { id });
  res.json({ id, paused: true });
});

// Resume a consumer group
app.post("/api/consumer-groups/:id/resume", (req, res) => {
  const { id } = req.params;
  if (!CONSUMER_GROUPS.find((g) => g.id === id))
    return res.status(404).json({ error: "Consumer group not found" });
  pausedConsumers.delete(id);
  broadcastSSE("consumer-resumed", { id });
  res.json({ id, paused: false });
});

// List scenarios
app.get("/api/scenarios", (req, res) => {
  res.json(
    SCENARIOS_DEF.map((s) => ({
      id: s.id,
      name: s.name,
      emoji: s.emoji,
      tagline: s.tagline,
      concept: s.concept,
      duration: s.duration,
      theme: s.theme,
      instant: !!s.instant,
      active: activeScenario === s.id,
    })),
  );
});

// Run a scenario
app.post("/api/scenarios/:id/run", (req, res) => {
  const { id } = req.params;
  const ok = runScenario(id);
  if (!ok) return res.status(404).json({ error: "Scenario not found" });
  res.json({ started: id });
});

// Stop the active scenario
app.post("/api/scenarios/stop", (req, res) => {
  stopActiveScenario(true);
  res.json({ stopped: true });
});

// Control producer speed
app.post("/api/producer/config", (req, res) => {
  const { speed, batchSize, running } = req.body;
  if (speed !== undefined) producerSpeed = Math.max(100, Math.min(5000, speed));
  if (batchSize !== undefined)
    producerBatchSize = Math.max(1, Math.min(20, batchSize));
  if (running !== undefined) producerRunning = running;
  broadcastSSE("producer-config", {
    speed: producerSpeed,
    batchSize: producerBatchSize,
    running: producerRunning,
  });
  res.json({
    speed: producerSpeed,
    batchSize: producerBatchSize,
    running: producerRunning,
  });
});

// Get recent messages from topic (for initial load)
app.get("/api/topics/:name/messages", (req, res) => {
  const { name } = req.params;
  const topic = broker.topics[name];
  if (!topic) return res.status(404).json({ error: "Topic not found" });

  const messages = [];
  topic.partitions.forEach((p) => messages.push(...p.messages));
  messages.sort((a, b) => b.timestamp - a.timestamp);
  res.json(messages.slice(0, 100));
});

// Catch-all → serve index.html
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Kafka Demo running on port ${PORT}`);
  console.log(`Dashboard: http://localhost:${PORT}`);
});
