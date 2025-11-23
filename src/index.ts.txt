import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

async function main() {
  console.log("🚀 Connecting to MongoDB...");

  // Test connection by creating one record
  const newData = await prisma.vehicleTelemetry.create({
    data: {
      vehicleId: "CAR-001",
      rpm: 2500,
      throttle: 43.5,
      speed: 72.3,
      gear: 4,
      brake: 0,
      intakeTemp: 32.1,
      engineTemp: 88.6,
      odoMeter: 52340,
    },
  });

  console.log("✅ Data inserted:", newData);

  // Retrieve all records
  const allData = await prisma.vehicleTelemetry.findMany();
  console.log(`📦 Found ${allData.length} records in DB`);
}

main()
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error("❌ Error connecting to MongoDB:", e);
    await prisma.$disconnect();
    process.exit(1);
  });
