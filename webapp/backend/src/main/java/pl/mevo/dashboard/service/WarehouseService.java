package pl.mevo.dashboard.service;

import org.springframework.stereotype.Service;

import pl.mevo.dashboard.api.StationSnapshot;
import pl.mevo.dashboard.api.WarehouseStats;
import pl.mevo.dashboard.repository.WarehouseRepository;

import java.sql.SQLException;
import java.util.List;

@Service
public class WarehouseService {

    private final WarehouseRepository repository;

    public WarehouseService(WarehouseRepository repository) {
        this.repository = repository;
    }

    public WarehouseStats getStats() {
        try {
            return repository.fetchStats();
        } catch (SQLException e) {
            throw new WarehouseQueryException("Failed to load warehouse stats", e);
        }
    }

    public List<StationSnapshot> getStationSnapshots() {
        try {
            return repository.fetchStationSnapshots();
        } catch (SQLException e) {
            throw new WarehouseQueryException("Failed to load station snapshots", e);
        }
    }
}
