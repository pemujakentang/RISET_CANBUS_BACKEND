export interface VehicleData {
  rpm: number;
  throttle: number;
  speed: number;
  gear: number;
  brake: number;
  intakeTemp: number;
  engineTemp: number;
  odoMeter: number;
}

export interface VehicleMessage extends VehicleData {
  vehicleId: string;
  timestamp?: Date;
}
