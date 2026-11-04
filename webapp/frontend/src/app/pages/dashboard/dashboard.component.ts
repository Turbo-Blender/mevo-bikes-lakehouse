import { CommonModule } from '@angular/common';
import { AfterViewInit, Component, ElementRef, OnDestroy, ViewChild, inject, signal } from '@angular/core';
import { forkJoin } from 'rxjs';

import * as L from 'leaflet';
import { StationSnapshot, WarehouseStats } from '../../models/warehouse';
import { WarehouseService } from '../../services/warehouse.service';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './dashboard.component.html',
  styleUrl: './dashboard.component.scss',
})
export class DashboardComponent implements AfterViewInit, OnDestroy {
  @ViewChild('mapHost', { static: true }) mapHost!: ElementRef<HTMLDivElement>;

  private readonly warehouseService = inject(WarehouseService);
  private map?: L.Map;
  private markersLayer?: L.LayerGroup;

  readonly stats = signal<WarehouseStats | null>(null);
  readonly stations = signal<StationSnapshot[]>([]);
  readonly loading = signal(true);
  readonly error = signal<string | null>(null);

  ngAfterViewInit(): void {
    this.initMap();
    this.loadData();
  }

  ngOnDestroy(): void {
    this.map?.remove();
  }

  reload(): void {
    this.loadData();
  }

  private loadData(): void {
    this.loading.set(true);
    this.error.set(null);

    forkJoin({
      stats: this.warehouseService.getStats(),
      stations: this.warehouseService.getStations(),
    }).subscribe({
      next: ({ stats, stations }) => {
        this.stats.set(stats);
        this.stations.set(stations);
        this.renderMarkers(stations);
        this.loading.set(false);
      },
      error: () => {
        this.error.set(
          'Nie udało się pobrać danych z API. Upewnij się, że backend działa na porcie 8081 i ClickHouse ma dane.',
        );
        this.loading.set(false);
      },
    });
  }

  private initMap(): void {
    this.map = L.map(this.mapHost.nativeElement, {
      center: [54.35, 18.65],
      zoom: 11,
    });

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 18,
      attribution: '&copy; OpenStreetMap',
    }).addTo(this.map);

    this.markersLayer = L.layerGroup().addTo(this.map);
  }

  private renderMarkers(stations: StationSnapshot[]): void {
    if (!this.map || !this.markersLayer) {
      return;
    }

    this.markersLayer.clearLayers();

    const bounds = L.latLngBounds([]);

    for (const station of stations) {
      const totalBikes = station.bikesAvailable + station.ebikesAvailable;
      const marker = L.circleMarker([station.lat, station.lon], {
        radius: 7,
        color: '#1f2937',
        weight: 1,
        fillColor: this.markerColor(station),
        fillOpacity: 0.85,
      });

      marker.bindPopup(`
        <strong>${station.stationName ?? station.stationId}</strong><br/>
        ID: ${station.stationId}<br/>
        Rowery: ${totalBikes} (e: ${station.ebikesAvailable})<br/>
        Wolne miejsca: ${station.numDocksAvailable}<br/>
        Obłożenie: ${this.formatOccupancy(station.occupancyRatio)}<br/>
        Snapshot: ${station.snapshotAt}
      `);

      marker.addTo(this.markersLayer);
      bounds.extend([station.lat, station.lon]);
    }

    if (bounds.isValid()) {
      this.map.fitBounds(bounds, { padding: [24, 24] });
    }
  }

  private markerColor(station: StationSnapshot): string {
    if (station.empty) {
      return '#ef4444';
    }
    if (station.full) {
      return '#6366f1';
    }
    if (station.occupancyRatio == null) {
      return '#9ca3af';
    }
    if (station.occupancyRatio >= 0.85) {
      return '#f59e0b';
    }
    return '#22c55e';
  }

  private formatOccupancy(value: number | null): string {
    if (value == null) {
      return '—';
    }
    return `${Math.round(value * 100)}%`;
  }
}
