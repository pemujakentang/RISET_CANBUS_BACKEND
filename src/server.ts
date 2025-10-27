import express from "express";
import { prisma } from "./db.js";
import "./mqttClient.js";

const app = express();
app.use(express.json());

app.get("/api/telemetry/latest", async (_req, res) => {
  const latest = await prisma.vehicle.findMany({
    orderBy: { timestamp: "desc" },
    take: 10,
  });
  res.json(latest);
});

app.get("/api/telemetry/:vehicleId", async (req, res) => {
  const { vehicleId } = req.params;
  const data = await prisma.vehicle.findMany({
    where: { vehicleId },
    orderBy: { timestamp: "desc" },
    take: 100,
  });
  res.json(data);
});

const PORT = 4000;
app.listen(PORT, () =>
  console.log(`🚀 Backend running at http://localhost:${PORT}`)
);
