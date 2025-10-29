export interface VehicleData {
  rpm: number;
  throttle: number;
  speed: number;
  gear: number;
  brake: number;
  engineCoolantTemp: number;
  airIntakeTemp: number;
  odoMeter: number; // raw byte value (0–255) from CAN
  tripOdoKm?: number; // computed distance since boot (ESP-side)
  totalOdoKm?: number; // persistent odometer (BE-side)
  bootId?: string; // unique ID per ESP boot session
}

export interface VehicleMessage extends VehicleData {
  vehicleId: string;
  timestamp?: Date;
}