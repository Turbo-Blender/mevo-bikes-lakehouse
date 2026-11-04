export interface WarehouseStats {
  factRowCount: number;
  stationCount: number;
  latestSnapshotUtc: string;
}

export interface StationSnapshot {
  stationId: string;
  stationName: string | null;
  address: string | null;
  lat: number;
  lon: number;
  capacity: number | null;
  bikesAvailable: number;
  ebikesAvailable: number;
  numDocksAvailable: number;
  occupancyRatio: number | null;
  empty: boolean;
  full: boolean;
  snapshotAt: string;
  snapshotAtUtc: string;
}
