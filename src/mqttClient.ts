import mqtt from "mqtt";
import { prisma } from "./db.js";
import type { VehicleMessage } from "./types.js";

const MQTT_BROKER = "mqtt://localhost:1883";
const MQTT_USER   = "ESP32MQTT";
const MQTT_PASS   = "ESP32MQTT";

// Topics
const MQTT_TOPIC_VEHICLE = "esp32mqtt/vehicle";
const MQTT_TOPIC_BE_REQ  = "esp32mqtt/handshake/be/request";
const MQTT_TOPIC_BE_RES  = "esp32mqtt/handshake/be/response";

const client = mqtt.connect(MQTT_BROKER, {
  username: MQTT_USER,
  password: MQTT_PASS,
});

const buffer: VehicleMessage[] = [];
const FLUSH_MS = 1000;

client.on("connect", () => {
  console.log("✅ MQTT connected (Backend)");
  client.subscribe(MQTT_TOPIC_VEHICLE);
  client.subscribe(MQTT_TOPIC_BE_REQ);
});

// Handle all incoming messages
client.on("message", async (topic, msg) => {
  const message = msg.toString();

  // 🧩 1️⃣ Handle handshake from ESP
  if (topic === MQTT_TOPIC_BE_REQ) {
    console.log("📩 Handshake request from ESP:", message);
    try {
      const payload = JSON.parse(message);
      if (payload.status === "ping") {
        client.publish(MQTT_TOPIC_BE_RES, JSON.stringify({ status: "ack" }));
        console.log("📤 Sent ACK to ESP (Backend)");
      }
    } catch (err) {
      console.error("❌ Invalid handshake payload:", err);
    }
    return;
  }

  // 🧩 2️⃣ Handle telemetry data
  if (topic === MQTT_TOPIC_VEHICLE) {
    try {
      const p = JSON.parse(message) as VehicleMessage;

      if (
        typeof p.rpm === "number" &&
        typeof p.throttle === "number" &&
        typeof p.speed === "number" &&
        typeof p.gear === "number" &&
        typeof p.brake === "number" &&
        typeof p.engineCoolantTemp === "number" &&
        typeof p.airIntakeTemp === "number" &&
        typeof p.odoMeter === "number"
      ) {
        buffer.push({
          ...p,
          vehicleId: p.vehicleId ?? "ESP32",
          timestamp: new Date(),
        });
      } else {
        console.warn("⚠️ Invalid payload shape:", p);
      }
    } catch (e) {
      console.error("❌ Invalid JSON:", e);
    }
  }
});

// 🧩 3️⃣ Periodic database flush
setInterval(async () => {
  if (!buffer.length) return;
  const batch = buffer.splice(0, buffer.length);
  try {
    await prisma.vehicle.createMany({ data: batch });
    console.log(`📥 Inserted ${batch.length} records`);
  } catch (e) {
    console.error("❌ Batch insert failed:", e);
  }
}, FLUSH_MS);
