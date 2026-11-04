import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

import { environment } from '../../environments/environment';
import { StationSnapshot, WarehouseStats } from '../models/warehouse';

@Injectable({ providedIn: 'root' })
export class WarehouseService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = environment.apiUrl;

  getStats(): Observable<WarehouseStats> {
    return this.http.get<WarehouseStats>(`${this.baseUrl}/stats`);
  }

  getStations(): Observable<StationSnapshot[]> {
    return this.http.get<StationSnapshot[]>(`${this.baseUrl}/stations`);
  }
}
