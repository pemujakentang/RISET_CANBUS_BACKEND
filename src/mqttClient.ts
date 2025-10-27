import mqtt from "mqtt";
import { prisma } from "./db";
import { VehicleMessage } from "./types";

const MQTT_BROKER = "mqtt://localhost:1883";
const MQTT_TOPIC = "esp32mqtt/vehicle";
const MQTT_USER = "ESP32MQTT";
const MQTT_PASS = "ESP32MQTT";

const client = mqtt.connect(MQTT_BROKER, {
  username: MQTT_USER,
  password: MQTT_PASS,
});

const buffer: VehicleMessage[] = [];
const FLUSH_INTERVAL_MS = 1000; // flush to DB every second

client.on("connect", () => {
  console.log("✅ Connected to MQTT broker");
  client.subscribe(MQTT_TOPIC);
});

client.on("message", async (topic, message) => {
  try {
    const payload = JSON.parse(message.toString()) as VehicleMessage;

    // Basic type validation
    if (
      typeof payload.rpm === "number" &&
      typeof payload.throttle === "number" &&
      typeof payload.speed === "number"
    ) {
      buffer.push({
        ...payload,
        vehicleId: payload.vehicleId ?? "ESP32",
        timestamp: new Date(),
      });
    } else {
      console.warn("⚠️ Invalid payload:", payload);
    }
  } catch (err) {
    console.error("❌ Invalid MQTT message:", err);
  }
});

// Periodically flush data to MongoDB
setInterval(async () => {
  if (buffer.length > 0) {
    const batch = buffer.splice(0, buffer.length);
    try {
      await prisma.vehicleTelemetry.createMany({ data: batch });
      console.log(`📥 Inserted ${batch.length} telemetry records`);
    } catch (err) {
      console.error("❌ Failed to insert batch:", err);
    }
  }
}, FLUSH_INTERVAL_MS);
