/**
 * Kafka Producer — Real Log Event Generator
 * Connects to actual Apache Kafka broker and produces log events.
 *
 * Usage (standalone):
 *   KAFKA_BROKERS=localhost:29092 node kafka-producer.js
 *
 * Or via Docker Compose:
 *   docker compose up producer
 */

const { Kafka, logLevel } = require("kafkajs");
const { v4: uuidv4 } = require("uuid");

const BROKERS = (process.env.KAFKA_BROKERS || "localhost:29092").split(",");
const INTERVAL_MS = parseInt(process.env.PRODUCER_INTERVAL_MS || "500");
const BATCH_SIZE = parseInt(process.env.PRODUCER_BATCH_SIZE || "5");

const kafka = new Kafka({
  clientId: "log-producer",
  brokers: BROKERS,
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 1000, retries: 10 },
});

const producer = kafka.producer({ allowAutoTopicCreation: true });

const LOG_LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR", "FATAL"];
const SERVICES = [
  "auth-service", "api-gateway", "user-service", "payment-service",
  "notification-service", "db-proxy", "cache-service",
];
const TEMPLATES = {
  DEBUG: ["Cache hit for key usr:{n}", "Query executed in {ms}ms", "Session validated for user U-{n}"],
  INFO:  ["Request received: {method} /api/v1/{path}", "User U-{n} authenticated successfully", "Transaction TX-{id} processed", "Service health check passed"],
  WARN:  ["Rate limit approaching for IP 10.0.{n}.{n}", "Slow query: {ms}ms", "Memory usage at {pct}%", "Retry attempt {retry} for {svc}"],
  ERROR: ["Failed to connect to database: timeout after {ms}ms", "Payment gateway returned 503", "JWT validation failed: {reason}"],
  FATAL: ["Out of memory - initiating shutdown", "Disk quota exceeded on /var/data", "Critical dependency unreachable"],
};

function rand(n) { return Math.floor(Math.random() * n); }
function pick(arr) { return arr[rand(arr.length)]; }

function generateLog(level) {
  const templates = TEMPLATES[level] || TEMPLATES.INFO;
  return pick(templates)
    .replace(/{n}/g, () => rand(9999))
    .replace(/{ms}/g, () => rand(3000))
    .replace(/{ms}/g, () => rand(3000))
    .replace(/{id}/g, () => uuidv4().slice(0, 8).toUpperCase())
    .replace(/{method}/g, () => pick(["GET", "POST", "PUT", "DELETE"]))
    .replace(/{path}/g, () => pick(["orders", "payments", "users", "sessions", "products"]))
    .replace(/{pct}/g, () => rand(40) + 60)
    .replace(/{retry}/g, () => rand(5) + 1)
    .replace(/{svc}/g, () => pick(SERVICES))
    .replace(/{reason}/g, () => pick(["expired", "invalid signature", "malformed token"]));
}

function buildRecord(level, service) {
  const topic = (level === "ERROR" || level === "FATAL") ? "error-logs" : "app-logs";
  const value = JSON.stringify({
    level,
    service,
    message: generateLog(level),
    traceId: uuidv4().replace(/-/g, "").slice(0, 16),
    environment: pick(["production", "staging"]),
    timestamp: new Date().toISOString(),
  });
  return { topic, key: service, value, headers: { "content-type": "application/json" } };
}

async function run() {
  await producer.connect();
  console.log(`[Producer] Connected to Kafka: ${BROKERS.join(", ")}`);
  console.log(`[Producer] Producing ${BATCH_SIZE} msgs every ${INTERVAL_MS}ms`);

  let produced = 0;

  while (true) {
    try {
      const records = [];

      for (let i = 0; i < BATCH_SIZE; i++) {
        const level = pick(LOG_LEVELS);
        const service = pick(SERVICES);
        const record = buildRecord(level, service);
        records.push(record);

        // Produce audit event for warnings/errors
        if (["WARN", "ERROR", "FATAL"].includes(level)) {
          records.push({
            topic: "audit-events",
            key: level,
            value: JSON.stringify({
              event: "log-alert",
              severity: level,
              sourceService: service,
              timestamp: new Date().toISOString(),
            }),
          });
        }
      }

      // Group by topic for batch send
      const byTopic = records.reduce((acc, r) => {
        if (!acc[r.topic]) acc[r.topic] = [];
        acc[r.topic].push({ key: r.key, value: r.value, headers: r.headers });
        return acc;
      }, {});

      for (const [topic, messages] of Object.entries(byTopic)) {
        await producer.send({ topic, messages });
      }

      produced += BATCH_SIZE;
      if (produced % 100 === 0) {
        console.log(`[Producer] Total produced: ${produced} messages`);
      }
    } catch (err) {
      console.error("[Producer] Error:", err.message);
    }

    await new Promise((r) => setTimeout(r, INTERVAL_MS));
  }
}

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("[Producer] Shutting down...");
  await producer.disconnect();
  process.exit(0);
});

run().catch(console.error);
