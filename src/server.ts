import express from "express";
import { prisma } from "./db";
import "./mqttClient";

const app = express();
app.use(express.json());

app.get("/api/telemetry/latest", async (req, res) => {
  const latest = await prisma.vehicleTelemetry.findMany({
    orderBy: { timestamp: "desc" },
    take: 10,
  });
  res.json(latest);
});

app.get("/api/telemetry/:deviceId", async (req, res) => {
  const { deviceId } = req.params;
  const data = await prisma.vehicleTelemetry.findMany({
    where: { deviceId },
    orderBy: { timestamp: "desc" },
    take: 100,
  });
  res.json(data);
});

const PORT = 4000;
app.listen(PORT, () => console.log(`🚀 Backend running on http://localhost:${PORT}`));
