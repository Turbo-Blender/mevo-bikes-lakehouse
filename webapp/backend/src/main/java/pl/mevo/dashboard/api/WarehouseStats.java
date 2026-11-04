package pl.mevo.dashboard.api;

import java.time.LocalDateTime;

public record WarehouseStats(
        long factRowCount,
        long stationCount,
        LocalDateTime latestSnapshotUtc
) {
}
