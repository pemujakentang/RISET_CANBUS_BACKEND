import mqtt from "mqtt";
import { prisma } from "./db.js";
import type { VehicleMessage } from "./types.js";

const MQTT_BROKER = "mqtt://localhost:1883";
const MQTT_TOPIC  = "esp32mqtt/vehicle";
const MQTT_USER   = "ESP32MQTT";
const MQTT_PASS   = "ESP32MQTT";

const client = mqtt.connect(MQTT_BROKER, {
  username: MQTT_USER,
  password: MQTT_PASS,
});

const buffer: VehicleMessage[] = [];
const FLUSH_MS = 1000;

client.on("connect", () => {
  console.log("✅ MQTT connected");
  client.subscribe(MQTT_TOPIC);
});

client.on("message", (_, msg) => {
  try {
    const p = JSON.parse(msg.toString()) as VehicleMessage;
    // basic guards
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
});

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
