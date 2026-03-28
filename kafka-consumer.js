/**
 * Kafka Consumer — Real-Time Log Processor
 * Connects to actual Apache Kafka broker and consumes log events.
 * Performs: filtering, aggregation, alerting.
 *
 * Usage (standalone):
 *   KAFKA_BROKERS=localhost:29092 node kafka-consumer.js
 *
 * Or via Docker Compose:
 *   docker compose up consumer
 */

const { Kafka, logLevel } = require("kafkajs");

const BROKERS = (process.env.KAFKA_BROKERS || "localhost:29092").split(",");
const GROUP_ID = process.env.CONSUMER_GROUP_ID || "log-aggregator";
const TOPICS = (process.env.CONSUMER_TOPICS || "app-logs,error-logs,audit-events").split(",");

const kafka = new Kafka({
  clientId: `consumer-${GROUP_ID}`,
  brokers: BROKERS,
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 1000, retries: 10 },
});

const consumer = kafka.consumer({
  groupId: GROUP_ID,
  maxWaitTimeInMs: 500,
  minBytes: 1,
  maxBytes: 10485760, // 10 MB
});

// ── Aggregation State ─────────────────────────────────────────────────────────
const stats = {
  processed: 0,
  byLevel: { DEBUG: 0, INFO: 0, WARN: 0, ERROR: 0, FATAL: 0 },
  byService: {},
  byTopic: {},
  errors: 0,
  startTime: Date.now(),
};

const ALERT_THRESHOLD = 10; // alert if >10 errors in 60s
const errorWindow = [];

function processMessage(topic, partition, message) {
  let value;
  try {
    value = JSON.parse(message.value.toString());
  } catch {
    stats.errors++;
    return;
  }

  stats.processed++;
  const level = value.level || "INFO";
  const service = value.service || "unknown";

  // Aggregation
  stats.byLevel[level] = (stats.byLevel[level] || 0) + 1;
  stats.byService[service] = (stats.byService[service] || 0) + 1;
  stats.byTopic[topic] = (stats.byTopic[topic] || 0) + 1;

  // Log to stdout (formatted)
  const ts = new Date().toISOString().slice(11, 23);
  const levelPad = level.padEnd(5);
  console.log(`[${ts}] [${GROUP_ID}] [${topic}/P${partition}] ${levelPad} [${service}] ${value.message || JSON.stringify(value)}`);

  // Error rate monitoring
  if (level === "ERROR" || level === "FATAL") {
    const now = Date.now();
    errorWindow.push(now);
    // Prune window
    const cutoff = now - 60000;
    while (errorWindow.length > 0 && errorWindow[0] < cutoff) errorWindow.shift();

    if (errorWindow.length >= ALERT_THRESHOLD) {
      console.warn(`\n⚠️  ALERT: ${errorWindow.length} errors in the last 60s! Service: ${service}\n`);
      errorWindow.length = 0; // reset to avoid spam
    }
  }
}

function printStats() {
  const uptime = Math.floor((Date.now() - stats.startTime) / 1000);
  const rate = uptime > 0 ? (stats.processed / uptime).toFixed(1) : 0;
  console.log(`\n── Consumer Stats ────────────────────────────────`);
  console.log(`  Uptime:     ${uptime}s`);
  console.log(`  Processed:  ${stats.processed} messages (${rate} msg/s)`);
  console.log(`  Errors:     ${stats.errors}`);
  console.log(`  By Level:   ${JSON.stringify(stats.byLevel)}`);
  const topServices = Object.entries(stats.byService)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([k, v]) => `${k}:${v}`)
    .join(", ");
  console.log(`  Top SVCs:   ${topServices}`);
  console.log(`  By Topic:   ${JSON.stringify(stats.byTopic)}`);
  console.log(`──────────────────────────────────────────────────\n`);
}

async function run() {
  await consumer.connect();
  console.log(`[Consumer] Connected to Kafka: ${BROKERS.join(", ")}`);
  console.log(`[Consumer] Group: ${GROUP_ID} | Topics: ${TOPICS.join(", ")}`);

  await consumer.subscribe({
    topics: TOPICS,
    fromBeginning: false,
  });

  // Print stats every 30s
  setInterval(printStats, 30000);

  await consumer.run({
    eachBatch: async ({ batch, heartbeat, resolveOffset, commitOffsetsIfNecessary }) => {
      for (const message of batch.messages) {
        processMessage(batch.topic, batch.partition, message);
        resolveOffset(message.offset);
        await heartbeat();
      }
      await commitOffsetsIfNecessary();
    },
  });
}

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("[Consumer] Shutting down...");
  printStats();
  await consumer.disconnect();
  process.exit(0);
});

process.on("SIGINT", async () => {
  console.log("[Consumer] Interrupted...");
  printStats();
  await consumer.disconnect();
  process.exit(0);
});

run().catch(console.error);
