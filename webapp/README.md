# Mevo Dashboard (webapp)

Prosta aplikacja **Angular + Spring Boot** do podglądu danych z hurtowni ClickHouse: liczba wierszy w `fact_station_status` oraz mapa stacji z ostatniego snapshotu.

## Wymagania

- Działający stack lakehouse (`docker compose up`) z danymi w ClickHouse
- Node.js 20+ (frontend lokalnie)
- Java 21 opcjonalnie (backend można budować tylko przez Docker)

## 1. Backend (API)

### Docker (zalecane)

```powershell
docker compose up -d dashboard-api
```

API: http://localhost:8081/api/stats , http://localhost:8081/api/stations

### Lokalnie (bez Maven w PATH)

```powershell
docker build -t mevo-dashboard-api .\webapp\backend
docker run --rm -p 8081:8081 `
  -e CLICKHOUSE_HOST=host.docker.internal `
  -e CLICKHOUSE_PORT=8123 `
  -e CLICKHOUSE_USER=clickhouse `
  -e CLICKHOUSE_PASSWORD=clickhouse `
  -e CLICKHOUSE_DB=warehouse `
  mevo-dashboard-api
```

## 2. Frontend

```powershell
cd webapp\frontend
npm install
npm start
```

UI: http://localhost:4200

Frontend domyślnie woła API pod `http://localhost:8081/api` (plik `src/environments/environment.ts`).

## Endpointy

| Metoda | Ścieżka | Opis |
| --- | --- | --- |
| GET | `/api/health` | healthcheck |
| GET | `/api/stats` | liczba wierszy, stacji, czas ostatniego snapshotu |
| GET | `/api/stations` | stacje z współrzędnymi i stanem z faktu |

## Uwagi o danych

- Obecnie w `fact_station_status` masz **870 wierszy = 870 stacji × 1 snapshot** — wystarczy do mapy i KPI.
- Po kolejnych przebiegach pipeline'u (co 30 min) będzie więcej snapshotów historycznych; wtedy warto dodać filtr „ostatni snapshot” w SQL (backend już zakłada join po `station_key` z wymiaru).
