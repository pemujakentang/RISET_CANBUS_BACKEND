import express from "express";
import cors from "cors";
import { prisma } from "./db.js";
import "./mqttClient.js";

function getRangeStart(range: string): Date | null {
  const now = new Date();

  switch (range) {
    case "1h":
      return new Date(now.getTime() - 60 * 60 * 1000);
    case "24h":
      return new Date(now.getTime() - 24 * 60 * 60 * 1000);
    case "7d":
      return new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    case "30d":
      return new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    case "365d":
      return new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000);
    case "ytd":
      return new Date(now.getFullYear(), 0, 1);
    case "all":
      return null; // no filter
    default:
      return null;
  }
}

function getAggregationInterval(range: string): number {
  let intervalMinutes: number;

  switch (range) {
    case "1h":
      intervalMinutes = 1;
      break;
    case "24h":
      intervalMinutes = 1;
      break;
    case "7d":
      intervalMinutes = 1; // 1 hour
      break;
    case "30d":
      intervalMinutes = 1; // 6 hours (matches your 21600000 ms)
      break;
    case "365d":
    case "ytd":
    case "all":
      intervalMinutes = 1; // 1 day
      break;
    default:
      intervalMinutes = 1;
  }
  return intervalMinutes * 60 * 1000; // Return milliseconds
}

function getAggregationIntervalMs(range: string): number {
  let intervalMinutes: number;

  switch (range) {
    case "1h":
      intervalMinutes = 1;
      break;
    case "24h":
      intervalMinutes = 1;
      break;
    case "7d":
      intervalMinutes = 1; // 1 hour
      break;
    case "30d":
      intervalMinutes = 1; // 6 hours
      break;
    case "365d":
    case "ytd":
    case "all":
      intervalMinutes = 1; // 1 day
      break;
    default:
      intervalMinutes = 1;
  }
  return intervalMinutes * 60 * 1000; // Return milliseconds
}

const app = express();
app.use(
  cors({
    // Replace 3000 with your actual Next.js port if different
    origin: "http://localhost:3000",
    methods: "GET,HEAD,PUT,PATCH,POST,DELETE",
    credentials: true,
  })
);
app.use(express.json());

app.get("/api/telemetry/latest", async (_req, res) => {
  const latest = await prisma.vehicle.findMany({
    orderBy: { timestamp: "desc" },
    take: 10,
  });
  res.json(latest);
});

// Define the name for the temporary date field
const TEMP_DATE_FIELD = "_dateTimestamp"; 

// --- Odometer Endpoint ---

app.get("/api/telemetry/odometer", async (req, res) => {
  const { vehicleId, range = "30d" } = req.query as {
    vehicleId?: string;
    range?: string;
  };

  if (!vehicleId) {
    return res.status(400).json({ error: "vehicleId is required" });
  }

  const rangeStart = getRangeStart(range);
  const bucketMs = getAggregationIntervalMs(range);

  const pipeline = [
    // Stage 0: Convert String timestamp to Date object
    {
      $addFields: {
        [TEMP_DATE_FIELD]: { $toDate: "$timestamp" } 
      }
    },
    // Stage 1: Filter documents by vehicleId and time range
    {
      $match: {
        vehicleId,
        // Match against the BSON Date structure
        ...(rangeStart ? 
            { [TEMP_DATE_FIELD]: { $gte: { $date: rangeStart.toISOString() } } } : 
            {}
        ),
      },
    },
    // Stage 2: Project and calculate the bucket timestamp
    {
      $project: {
        odoMeter: 1, 
        bucket: {
          $toDate: {
            $multiply: [
              {
                $floor: {
                  $divide: [
                    { $toLong: `$${TEMP_DATE_FIELD}` }, 
                    bucketMs
                  ]
                }
              },
              bucketMs
            ]
          }
        }
      }
    },
    // Stage 3: Group by the bucket and get the MAX odometer reading (Best for cumulative data)
    {
      $group: {
        _id: "$bucket",
        // ⭐️ FIX: Use $max to get the highest (most recent) odometer value in the bucket
        odo: { $max: "$odoMeter" } 
      }
    },
    // Stage 4: Sort chronologically
    { $sort: { _id: 1 } },
    // Stage 5: Final Projection/Formatting
    {
      $project: {
        _id: 0,
        bucket: "$_id", 
        odo: 1
      }
    }
  ];

  // Execute the raw aggregation pipeline
  const aggregatedData = await (prisma as any).vehicle.aggregateRaw({
    pipeline,
  });

  res.json({ vehicleId, range, intervalMinutes: bucketMs / (60 * 1000), count: aggregatedData.length, data: aggregatedData });
});

const ALLOWED_METRICS = [
  "speed",
  "rpm",
  "throttle",
  "gear",
  "brake",
  "engineCoolantTemp",
  "airIntakeTemp",
  "odoMeter",
  "steeringAngle",
] as const;

type MetricKey = (typeof ALLOWED_METRICS)[number];

app.get("/api/telemetry/history", async (req, res) => {
  const { vehicleId, metric, range = "24h" } = req.query as {
    vehicleId?: string;
    metric?: MetricKey;
    range?: string;
  };

  if (!vehicleId) return res.status(400).json({ error: "vehicleId required" });
  if (!metric || !ALLOWED_METRICS.includes(metric))
    return res.status(400).json({ error: "Invalid or missing metric" });

  const rangeStart = getRangeStart(range);

  const data = await prisma.vehicle.findMany({
    where: {
      vehicleId,
      ...(rangeStart ? { timestamp: { gte: rangeStart } } : {})
    },
    orderBy: { timestamp: "asc" },
    select: {
      timestamp: true,
      [metric]: true,
    },
  });

  res.json({ vehicleId, metric, range, count: data.length, data });
});


const PORT = 4000;
app.listen(PORT, () =>
  console.log(`🚀 Backend running at http://localhost:${PORT}`)
);
