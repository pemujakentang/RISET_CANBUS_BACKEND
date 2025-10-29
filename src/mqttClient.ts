import mqtt from "mqtt";
import { prisma } from "./db.js";
import type { VehicleMessage } from "./types.js";

const MQTT_BROKER = "mqtt://localhost:1883";
const MQTT_USER = "ESP32MQTT";
const MQTT_PASS = "ESP32MQTT";

// Topics
const MQTT_TOPIC_VEHICLE = "esp32mqtt/vehicle";
const MQTT_TOPIC_BE_REQ = "esp32mqtt/handshake/be/request";
const MQTT_TOPIC_BE_RES = "esp32mqtt/handshake/be/response";

const client = mqtt.connect(MQTT_BROKER, {
  username: MQTT_USER,
  password: MQTT_PASS,
});

const buffer: VehicleMessage[] = [];
const FLUSH_MS = 1000;

// ✅ Odometer tracking cache per vehicle
interface OdoState {
  lastOdoByte: number;
  totalKm: number;
  bootId?: string;
}

const odoCache = new Map<string, OdoState>();

client.on("connect", () => {
  console.log("✅ MQTT connected (Backend)");
  client.subscribe(MQTT_TOPIC_VEHICLE);
  client.subscribe(MQTT_TOPIC_BE_REQ);
});

// ============================================================
// 🧩 Handle incoming MQTT messages
// ============================================================
client.on("message", async (topic, msg) => {
  const message = msg.toString();

  // 🧩 1️⃣ Handshake from ESP
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

  // 🧩 1️⃣.5️⃣ Odometer Sync Request (NEW)
  if (topic === "esp32mqtt/odo/sync/request") {
    try {
      const last = await prisma.vehicle.findFirst({
        orderBy: { timestamp: "desc" },
      });
      const totalOdoKm = last ? last.odoMeter ?? 0 : 0;
      client.publish(
        "esp32mqtt/odo/sync/response",
        JSON.stringify({ totalOdoKm })
      );
      console.log("📤 Sent odo sync:", totalOdoKm);
    } catch (err) {
      console.error("❌ Odo sync failed:", err);
    }
    return;
  }

  // 🧩 2️⃣ Telemetry data
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
        const vehicleId = p.vehicleId ?? "ESP32";

        // ============================================================
        // 🚗 Compute odometer accumulation (using cache)
        // ============================================================
        const prev = odoCache.get(vehicleId);

        // Convert raw 0–255 byte to "partial distance" in km
        // 100 units = 1 km → 1 unit = 0.01 km
        const byteKm = p.odoMeter * 0.01;

        let totalKm = prev?.totalKm ?? 0;
        let lastByte = prev?.lastOdoByte ?? p.odoMeter;

        if (prev) {
          if (p.bootId && p.bootId !== prev.bootId) {
            // ESP reset — keep total, reset lastByte
            console.log(`🔁 ESP reset detected for ${vehicleId}`);
            odoCache.set(vehicleId, {
              lastOdoByte: p.odoMeter,
              totalKm,
              bootId: p.bootId,
            });
          } else {
            // Normal operation
            let deltaByte = p.odoMeter - lastByte;
            if (deltaByte < 0) deltaByte += 256; // handle wraparound

            const deltaKm = deltaByte / 100.0; // 100 bytes per km
            totalKm += deltaKm;

            odoCache.set(vehicleId, {
              lastOdoByte: p.odoMeter,
              totalKm,
              bootId: p.bootId ?? prev.bootId,
            });
          }
        } else {
          // First time seeing this vehicle
          odoCache.set(vehicleId, {
            lastOdoByte: p.odoMeter,
            totalKm,
            bootId: p.bootId,
          });
        }

        // Attach backend-calculated odometer fields
        const enriched: VehicleMessage = {
          ...p,
          vehicleId,
          timestamp: new Date(),
          totalOdoKm: totalKm,
        };

        buffer.push(enriched);
      } else {
        console.warn("⚠️ Invalid payload shape:", message);
      }
    } catch (e) {
      console.error("❌ Invalid JSON:", e);
    }
  }
});

// ============================================================
// 🧩 Periodic database flush
// ============================================================
setInterval(async () => {
  if (!buffer.length) return;
  const batch = buffer.splice(0, buffer.length);

  try {
    await prisma.vehicle.createMany({ data: batch });
    console.log(`📥 Inserted ${batch.length} telemetry records`);

    // ✅ Update VehicleOdometer totals (optional)
    for (const record of batch) {
      if (record.totalOdoKm != null) {
        await prisma.vehicleOdometer.upsert({
          where: { vehicleId: record.vehicleId },
          update: { totalOdoKm: record.totalOdoKm },
          create: {
            vehicleId: record.vehicleId,
            totalOdoKm: record.totalOdoKm,
          },
        });
      }
    }
  } catch (e) {
    console.error("❌ Batch insert failed:", e);
  }
}, FLUSH_MS);
