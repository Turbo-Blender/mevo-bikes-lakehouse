package pl.mevo.dashboard.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import pl.mevo.dashboard.api.StationSnapshot;
import pl.mevo.dashboard.api.WarehouseStats;
import pl.mevo.dashboard.service.WarehouseService;

import java.util.List;

@RestController
@RequestMapping("/api")
public class WarehouseController {

    private final WarehouseService warehouseService;

    public WarehouseController(WarehouseService warehouseService) {
        this.warehouseService = warehouseService;
    }

    @GetMapping("/health")
    public String health() {
        return "ok";
    }

    @GetMapping("/stats")
    public WarehouseStats stats() {
        return warehouseService.getStats();
    }

    @GetMapping("/stations")
    public List<StationSnapshot> stations() {
        return warehouseService.getStationSnapshots();
    }
}
