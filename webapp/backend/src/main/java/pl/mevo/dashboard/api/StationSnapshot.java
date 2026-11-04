package pl.mevo.dashboard.api;

import java.time.LocalDateTime;

public record StationSnapshot(
        String stationId,
        String stationName,
        String address,
        double lat,
        double lon,
        Integer capacity,
        long bikesAvailable,
        long ebikesAvailable,
        long numDocksAvailable,
        Double occupancyRatio,
        boolean empty,
        boolean full,
        LocalDateTime snapshotAt,
        LocalDateTime snapshotAtUtc
) {
}
