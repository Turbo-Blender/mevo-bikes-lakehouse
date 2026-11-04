package pl.mevo.dashboard.repository;

import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import pl.mevo.dashboard.api.StationSnapshot;
import pl.mevo.dashboard.api.WarehouseStats;

@Repository
public class WarehouseRepository {

    private static final String STATS_SQL = """
            SELECT
                (SELECT count() FROM fact_station_status) AS fact_row_count,
                (SELECT count(DISTINCT station_id) FROM fact_station_status) AS station_count,
                (SELECT max(snapshot_at_utc) FROM fact_station_status) AS latest_snapshot_utc
            """;

    private static final String STATIONS_SQL = """
            SELECT
                f.station_id AS station_id,
                d.station_name AS station_name,
                d.address AS address,
                d.lat AS lat,
                d.lon AS lon,
                d.capacity AS capacity,
                coalesce(f.bikes_available, 0) AS bikes_available,
                coalesce(f.ebikes_available, 0) AS ebikes_available,
                coalesce(f.num_docks_available, 0) AS num_docks_available,
                f.occupancy_ratio AS occupancy_ratio,
                coalesce(f.is_empty, false) AS is_empty,
                coalesce(f.is_full, false) AS is_full,
                f.snapshot_at AS snapshot_at,
                f.snapshot_at_utc AS snapshot_at_utc
            FROM fact_station_status AS f
            INNER JOIN dim_station AS d ON f.station_key = d.station_key
            INNER JOIN (
                SELECT station_id, max(snapshot_at_utc) AS max_snapshot_at_utc
                FROM fact_station_status
                GROUP BY station_id
            ) AS latest
                ON f.station_id = latest.station_id
                AND f.snapshot_at_utc = latest.max_snapshot_at_utc
            WHERE d.lat IS NOT NULL AND d.lon IS NOT NULL
            ORDER BY f.station_id
            """;

    private final DataSource dataSource;

    public WarehouseRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public WarehouseStats fetchStats() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(STATS_SQL);
             ResultSet rs = statement.executeQuery()) {
            if (!rs.next()) {
                throw new SQLException("No stats row returned from ClickHouse");
            }
            return new WarehouseStats(
                    rs.getLong("fact_row_count"),
                    rs.getLong("station_count"),
                    toLocalDateTime(rs.getObject("latest_snapshot_utc"))
            );
        }
    }

    public List<StationSnapshot> fetchStationSnapshots() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(STATIONS_SQL);
             ResultSet rs = statement.executeQuery()) {
            List<StationSnapshot> snapshots = new ArrayList<>();
            while (rs.next()) {
                snapshots.add(mapStation(rs));
            }
            return snapshots;
        }
    }

    private static StationSnapshot mapStation(ResultSet rs) throws SQLException {
        Integer capacity = null;
        Object capacityValue = rs.getObject("capacity");
        if (capacityValue instanceof Number number) {
            capacity = number.intValue();
        }

        Double occupancy = null;
        Object occupancyValue = rs.getObject("occupancy_ratio");
        if (occupancyValue instanceof Number number) {
            occupancy = number.doubleValue();
        }

        return new StationSnapshot(
                rs.getString("station_id"),
                rs.getString("station_name"),
                rs.getString("address"),
                rs.getDouble("lat"),
                rs.getDouble("lon"),
                capacity,
                rs.getLong("bikes_available"),
                rs.getLong("ebikes_available"),
                rs.getLong("num_docks_available"),
                occupancy,
                readBoolean(rs, "is_empty"),
                readBoolean(rs, "is_full"),
                toLocalDateTime(rs.getObject("snapshot_at")),
                toLocalDateTime(rs.getObject("snapshot_at_utc"))
        );
    }

    private static boolean readBoolean(ResultSet rs, String column) throws SQLException {
        Object value = rs.getObject(column);
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof Number number) {
            return number.intValue() != 0;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private static LocalDateTime toLocalDateTime(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp.toLocalDateTime();
        }
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime;
        }
        String text = value.toString();
        if (text.length() == 19) {
            text = text.replace(' ', 'T');
        }
        return LocalDateTime.parse(text);
    }
}
