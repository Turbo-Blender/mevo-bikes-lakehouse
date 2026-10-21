# Mevo Bikes Lakehouse

**Lokalny lakehouse dla danych o rowerach miejskich Mevo (Trójmiasto)**, zbudowany na Apache Spark, Delta Lake, MinIO, Airflow i ClickHouse.

`Apache Airflow · PySpark · Delta Lake · MinIO · ClickHouse · Docker · Spark SQL`

Projekt co 30 minut pobiera dane o stacjach i rowerach z publicznego API GBFS systemu Mevo, zapisuje je w data lake (MinIO) jako tabele Delta w architekturze medallion (Bronze → Silver → Gold), a gotową warstwę Gold ładuje do hurtowni ClickHouse. Całość jest orkiestrowana przez Apache Airflow i uruchamiana lokalnie w Docker Compose.

[English version ↓](#english-version)

## Interfejsy webowe

Po uruchomieniu `docker compose up -d --build` dostępne są:

| Interfejs | Adres | Logowanie |
| --- | --- | --- |
| **Airflow** (DAG-i, logi zadań) | http://localhost:8080 | `admin` / `admin` |
| **MinIO Console** (bucket `lakehouse`) | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` z `.env` |
| **Marquez** (data lineage) | http://localhost:3000 | bez logowania |
| **ClickHouse Play** (zapytania SQL do hurtowni) | http://localhost:8123/play | `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` z `.env` |
| **Flower** (monitoring workerów Celery, opcjonalny) | http://localhost:5555 | uruchamiany przez `docker compose --profile flower up -d` |

## Spis treści

- [Funkcje](#funkcje)
- [Architektura](#architektura)
- [Warstwy danych](#warstwy-danych)
- [Model danych](#model-danych)
- [Orkiestracja w Airflow](#orkiestracja-w-airflow)
- [Stack technologiczny](#stack-technologiczny)
- [Źródło danych](#źródło-danych)
- [Struktura projektu](#struktura-projektu)
- [Szybki start](#szybki-start)
- [Walidacja pipeline'u](#walidacja-pipelineu)
- [Data lineage](#data-lineage)
- [Decyzje projektowe](#decyzje-projektowe)
- [Autor i kontakt](#autor-i-kontakt)

## Funkcje

- **Konteneryzowane środowisko** - cały stack uruchamiany przez Docker Compose
- **Orkiestracja ETL** - DAG Airflow wykonujący pipeline co 30 minut
- **Lakehouse na Delta Lake** - Bronze, Silver i Gold jako tabele Delta w MinIO (protokół S3A)
- **Odporne pobieranie danych** - ponawianie zapytań do API z exponential backoff
- **Przetwarzanie przyrostowe** - Silver czyta z Bronze tylko nowe snapshoty i zapisuje je przez `MERGE`
- **Kontrola jakości danych** - flagi jakości w Silver (współrzędne poza obszarem usługi, ujemne liczniki)
- **Star schema w Gold** - wymiary stacji, typów rowerów i daty oraz fakt stanu stacji
- **Agregaty analityczne** - godzinowe obłożenie stacji
- **Hurtownia ClickHouse** - Gold ładowany do tabel `MergeTree` jako warstwa serwowania danych
- **Data lineage** - OpenLineage i Marquez: graf przepływu danych od API, przez Bronze, Silver i Gold, do ClickHouse
- **Eksploracja danych** - notebook Jupyter z EDA wszystkich feedów GBFS
- **Trwałe wolumeny** - osobne wolumeny dla MinIO, ClickHouse i metadanych Airflow

## Architektura

```mermaid
flowchart LR
    API[Mevo GBFS API] --> Extract[Spark Extract]
    Extract --> Bronze[(MinIO Bronze<br/>Delta)]
    Bronze --> Transform[Spark Transform]
    Transform --> Silver[(MinIO Silver<br/>Delta)]
    Silver --> Gold[Spark SQL Gold]
    Gold --> GoldLake[(MinIO Gold<br/>Delta)]
    GoldLake --> Load[Load]
    Load --> CH[(ClickHouse<br/>warehouse)]
    CH -. planowane .-> BI[Dashboard]
    Airflow[Apache Airflow] --> Extract
    Airflow --> Transform
    Airflow --> Gold
    Airflow --> Load
```

| Komponent | Rola |
| --- | --- |
| **Apache Airflow** | Harmonogram, zależności i monitoring zadań |
| **Apache Spark** | Ekstrakcja, transformacje Silver, budowa Gold |
| **Delta Lake** | Format tabel w MinIO: transakcje, `MERGE`, time travel |
| **MinIO** | Lokalny storage zgodny z S3 dla warstw Bronze, Silver i Gold |
| **ClickHouse** | Hurtownia danych: szybka warstwa serwowania Gold |
| **Redis** | Broker komunikatów dla CeleryExecutor |
| **Marquez + OpenLineage** | Data lineage: zbiera zdarzenia z Airflow i Sparka i pokazuje graf zależności |
| **Jupyter** | Eksploracyjna analiza danych |

Lakehouse jest źródłem prawdy: cała logika transformacji siedzi w Sparku, a ClickHouse tylko udostępnia gotowy Gold do szybkich zapytań. Hurtownię można w każdej chwili odbudować z bucketa.

## Warstwy danych

Wszystkie warstwy leżą w bucketcie `lakehouse` w MinIO.

### Bronze - `s3a://lakehouse/bronze/mevo/`

Rekordy z API zapisane bez zmian, z dodaną kolumną `last_updated` (czas wygenerowania zrzutu przez API).

- `station_status`, `free_bike_status` - snapshoty, tryb `append`, partycje `year/month`. Zostaje wszystko, co zwróciło API, a deduplikacja należy do Silvera.
- `vehicle_types`, `system_pricing_plans`, `station_information` - rzadko się zmieniają, więc tryb `overwrite` bez partycji (poprzednie wersje dostępne przez time travel).

**Kontrola jakości na wejściu** (`src/etl/quality.py`):

- odpowiedź API musi mieć poprawną strukturę GBFS (`last_updated`, lista rekordów w `data`), inaczej zadanie kończy się błędem,
- feed musi zawierać wymagane kolumny (np. `station_id`, `lat`, `lon`, `last_reported`),
- każdy rekord jest sprawdzany regułami (klucze nie-NULL, poprawne współrzędne, nieujemna cena i pojemność); rekordy łamiące reguły trafiają do `s3a://lakehouse/quarantine/mevo/<feed>` z listą złamanych reguł w `failed_rules`,
- jeśli odrzuconych jest więcej niż `BRONZE_MAX_REJECTED_RATIO` (domyślnie 10%), feed uznawany jest za uszkodzony i Bronze nie jest aktualizowany,
- te same reguły są założone na tabelach Bronze jako Delta `CHECK` constraints, więc zapis z pominięciem walidacji też zostanie odrzucony.

Ujemne liczniki w `station_status` nie są odrzucane - Silver je zachowuje i oznacza flagą `has_negative_counts`.

### Silver - `s3a://lakehouse/silver/mevo/`

Oczyszczone, otypowane i znormalizowane dane.

- `station_status` - pełna historia snapshotów (klucz `station_id` + `last_updated`), spłaszczone `vehicle_types_available` do `bikes_available` i `ebikes_available`, flaga `has_negative_counts`.
- `free_bike_status` - historia zmian stanu rowerów (klucz `bike_id` + `last_reported`), flaga `has_invalid_coordinates`, usunięte `rental_uris`.
- `station_information` - wymiar stacji jako **SCD Type 2**: jeden wiersz na wersję stacji, z kolumnami `valid_from`, `valid_to`, `is_current` i kluczem zastępczym `station_key`. Zmiany wykrywane są hashem atrybutów (`name`, `address`, `lat`, `lon`, `capacity`, `station_area` itd.), a jeden `MERGE` zamyka starą wersję i otwiera nową. Stacja, która zniknęła z feedu, dostaje tylko zamkniętą wersję.
- `vehicle_types`, `system_pricing_plans` - wymiary nadpisywane przy każdym uruchomieniu.

Tabele zdarzeń są aktualizowane przyrostowo: filtr po `last_updated` ogranicza odczyt z Bronze, a `MERGE ... WHEN NOT MATCHED THEN INSERT` pomija rekordy z kluczem, który już istnieje.

### Gold - `s3a://lakehouse/gold/mevo/`

Zdenormalizowana star schema i agregaty, budowane zapytaniami Spark SQL z `src/SQL/gold/`. Gold jest w całości przeliczany z Silvera (`overwrite`).

### ClickHouse - baza `warehouse`

Kopia tabel Gold w silniku `MergeTree`, z partycjami miesięcznymi dla faktu i agregatu. Tabele tworzy `src/SQL/clickhouse/00_create_tables.sql`, a `load.py` przed każdym ładowaniem robi `TRUNCATE`.

### Schematy tabel

Poniżej znajduje się pełny słownik kolumn dla tabel `bronze`, `silver` i `gold`. Typy podano w konwencji Spark SQL / Delta Lake.

#### Bronze

<details>
<summary><code>vehicle_types</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `vehicle_type_id` | `string` | Id typu roweru, np. `bike` lub `ebike` |
| `form_factor` | `string` | Forma pojazdu wg GBFS |
| `default_pricing_plan_id` | `string` | Domyślny plan cenowy powiązany z typem roweru |
| `propulsion_type` | `string` | Typ napędu, np. `human` lub `electric_assist` |
| `name` | `string` | Nazwa typu roweru |
| `rider_capacity` | `bigint` | Liczba miejsc |
| `max_range_meters` | `bigint` | Maksymalny zasięg w metrach |
| `last_updated` | `timestamp` | Czas snapshotu zwrócony przez API |

</details>

<details>
<summary><code>system_pricing_plans</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `plan_id` | `string` | Id planu cenowego |
| `name` | `string` | Nazwa planu |
| `currency` | `string` | Waluta, np. `PLN` |
| `price` | `bigint` | Cena planu |
| `description` | `string` | Opis planu |
| `is_taxable` | `boolean` | Flaga opodatkowania |
| `per_min_pricing` | `array<struct<start:bigint,rate:double,end:bigint,interval:bigint>>` | Progi rozliczeń minutowych |
| `last_updated` | `timestamp` | Czas snapshotu zwrócony przez API |

</details>

<details>
<summary><code>station_information</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `station_id` | `string` | Id stacji w systemie źródłowym |
| `name` | `string` | Nazwa stacji |
| `address` | `string` | Adres stacji |
| `cross_street` | `string` | Dodatkowy opis lokalizacji |
| `lat` | `double` | Szerokość geograficzna |
| `lon` | `double` | Długość geograficzna |
| `is_virtual_station` | `boolean` | Czy stacja jest wirtualna |
| `capacity` | `bigint` | Liczba miejsc dokujących |
| `station_area` | `struct<type:string,coordinates:array<array<array<array<double>>>>>` | Geometria obszaru stacji |
| `rental_uris` | `struct<android:string,ios:string>` | Linki deep-link do aplikacji |
| `last_updated` | `timestamp` | Czas snapshotu zwrócony przez API |

</details>

<details>
<summary><code>station_status</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `station_id` | `string` | Id stacji |
| `is_installed` | `boolean` | Czy stacja jest zainstalowana |
| `is_renting` | `boolean` | Czy można wypożyczać rowery |
| `is_returning` | `boolean` | Czy można zwracać rowery |
| `last_reported` | `bigint` | Czas raportu stacji w epoce Unix |
| `num_vehicles_available` | `bigint` | Liczba dostępnych pojazdów |
| `num_bikes_available` | `bigint` | Liczba dostępnych rowerów |
| `num_docks_available` | `bigint` | Liczba wolnych miejsc |
| `vehicle_types_available` | `array<struct<vehicle_type_id:string,count:bigint>>` | Dostępność wg typu roweru |
| `last_updated` | `timestamp` | Czas snapshotu zwrócony przez API |
| `year` | `int` | Rok partycji wyliczony z `last_updated` |
| `month` | `int` | Miesiąc partycji wyliczony z `last_updated` |

</details>

<details>
<summary><code>free_bike_status</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `bike_id` | `string` | Id roweru poza stacją |
| `lat` | `double` | Szerokość geograficzna |
| `lon` | `double` | Długość geograficzna |
| `is_reserved` | `boolean` | Czy rower jest zarezerwowany |
| `is_disabled` | `boolean` | Czy rower jest wyłączony z użycia |
| `rental_uris` | `struct<android:string,ios:string>` | Linki deep-link do aplikacji |
| `vehicle_type_id` | `string` | Typ roweru |
| `last_reported` | `bigint` | Czas raportu roweru w epoce Unix |
| `current_range_meters` | `bigint` | Aktualny zasięg roweru w metrach |
| `last_updated` | `timestamp` | Czas snapshotu zwrócony przez API |
| `year` | `int` | Rok partycji wyliczony z `last_updated` |
| `month` | `int` | Miesiąc partycji wyliczony z `last_updated` |

</details>

#### Silver

<details>
<summary><code>vehicle_types</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `vehicle_type_id` | `string` | Id typu roweru |
| `form_factor` | `string` | Forma pojazdu wg GBFS |
| `default_pricing_plan_id` | `string` | Domyślny plan cenowy |
| `propulsion_type` | `string` | Typ napędu |
| `name` | `string` | Nazwa typu roweru |
| `rider_capacity` | `bigint` | Liczba miejsc |
| `max_range_meters` | `bigint` | Maksymalny zasięg w metrach |
| `last_updated` | `timestamp` | Czas snapshotu źródłowego |

</details>

<details>
<summary><code>system_pricing_plans</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `plan_id` | `string` | Id planu cenowego |
| `name` | `string` | Nazwa planu |
| `currency` | `string` | Waluta |
| `price` | `bigint` | Cena planu |
| `description` | `string` | Opis planu |
| `is_taxable` | `boolean` | Flaga opodatkowania |
| `per_min_pricing` | `array<struct<start:bigint,rate:double,end:bigint,interval:bigint>>` | Progi rozliczeń minutowych |
| `last_updated` | `timestamp` | Czas snapshotu źródłowego |

</details>

<details>
<summary><code>station_information</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `station_key` | `bigint` | Klucz zastępczy wersji stacji |
| `station_id` | `string` | Id stacji w systemie źródłowym |
| `name` | `string` | Nazwa stacji |
| `address` | `string` | Adres stacji |
| `cross_street` | `string` | Dodatkowy opis lokalizacji |
| `lat` | `double` | Szerokość geograficzna |
| `lon` | `double` | Długość geograficzna |
| `capacity` | `bigint` | Liczba miejsc dokujących |
| `is_virtual_station` | `boolean` | Czy stacja jest wirtualna |
| `station_area` | `struct<type:string,coordinates:array<array<array<array<double>>>>>` | Geometria obszaru stacji |
| `attr_hash` | `string` | Hash atrybutów śledzonych przez SCD2 |
| `valid_from` | `timestamp` | Początek obowiązywania wersji |
| `valid_to` | `timestamp` | Koniec obowiązywania wersji |
| `is_current` | `boolean` | Czy to aktualna wersja stacji |

</details>

<details>
<summary><code>station_status</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `station_id` | `string` | Id stacji |
| `is_installed` | `boolean` | Czy stacja jest zainstalowana |
| `is_renting` | `boolean` | Czy można wypożyczać rowery |
| `is_returning` | `boolean` | Czy można zwracać rowery |
| `last_reported` | `timestamp` | Czas raportu stacji |
| `num_vehicles_available` | `bigint` | Liczba dostępnych pojazdów |
| `num_bikes_available` | `bigint` | Liczba dostępnych rowerów |
| `num_docks_available` | `bigint` | Liczba wolnych miejsc |
| `last_updated` | `timestamp` | Czas snapshotu API |
| `bikes_available` | `bigint` | Liczba zwykłych rowerów |
| `ebikes_available` | `bigint` | Liczba e-rowerów |
| `has_negative_counts` | `boolean` | Flaga jakości dla ujemnych liczników |
| `year` | `int` | Rok partycji wyliczony z `last_updated` |
| `month` | `int` | Miesiąc partycji wyliczony z `last_updated` |

</details>

<details>
<summary><code>free_bike_status</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `bike_id` | `string` | Id roweru poza stacją |
| `lat` | `double` | Szerokość geograficzna |
| `lon` | `double` | Długość geograficzna |
| `is_reserved` | `boolean` | Czy rower jest zarezerwowany |
| `is_disabled` | `boolean` | Czy rower jest wyłączony z użycia |
| `vehicle_type_id` | `string` | Typ roweru |
| `last_reported` | `timestamp` | Czas raportu roweru |
| `current_range_meters` | `bigint` | Aktualny zasięg roweru w metrach |
| `last_updated` | `timestamp` | Czas snapshotu API |
| `has_invalid_coordinates` | `boolean` | Flaga jakości dla współrzędnych poza obszarem usługi |
| `year` | `int` | Rok partycji wyliczony z `last_reported` |
| `month` | `int` | Miesiąc partycji wyliczony z `last_reported` |

</details>

#### Gold

<details>
<summary><code>dim_station</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `station_key` | `bigint` | Klucz zastępczy wersji stacji |
| `station_id` | `string` | Id stacji |
| `station_name` | `string` | Nazwa stacji |
| `address` | `string` | Adres stacji |
| `cross_street` | `string` | Dodatkowy opis lokalizacji |
| `lat` | `double` | Szerokość geograficzna |
| `lon` | `double` | Długość geograficzna |
| `capacity` | `bigint` | Liczba miejsc dokujących |
| `is_virtual_station` | `boolean` | Czy stacja jest wirtualna |
| `valid_from` | `timestamp` | Początek obowiązywania wersji |
| `valid_to` | `timestamp` | Koniec obowiązywania wersji |
| `is_current` | `boolean` | Czy to aktualna wersja stacji |

</details>

<details>
<summary><code>dim_vehicle_type</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `vehicle_type_id` | `string` | Id typu roweru |
| `vehicle_type_name` | `string` | Nazwa typu roweru |
| `form_factor` | `string` | Forma pojazdu wg GBFS |
| `propulsion_type` | `string` | Typ napędu |
| `rider_capacity` | `bigint` | Liczba miejsc |
| `max_range_meters` | `bigint` | Maksymalny zasięg w metrach |
| `pricing_plan_id` | `string` | Id planu cenowego |
| `pricing_plan_name` | `string` | Nazwa planu cenowego |
| `price` | `bigint` | Cena planu |
| `currency` | `string` | Waluta |

</details>

<details>
<summary><code>dim_date</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `date_key` | `int` | Klucz daty w formacie `yyyyMMdd` |
| `full_date` | `date` | Pełna data |
| `year` | `int` | Rok |
| `quarter` | `int` | Kwartał |
| `month` | `int` | Miesiąc |
| `month_name` | `string` | Nazwa miesiąca |
| `day` | `int` | Dzień miesiąca |
| `day_of_week` | `int` | Dzień tygodnia od 1 do 7 |
| `day_name` | `string` | Nazwa dnia tygodnia |
| `is_weekend` | `boolean` | Czy dzień jest weekendem |

</details>

<details>
<summary><code>fact_station_status</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `station_key` | `bigint` | Klucz wersji stacji aktywnej w chwili snapshotu |
| `station_id` | `string` | Id stacji |
| `date_key` | `int` | Klucz daty lokalnej (`Europe/Warsaw`) |
| `hour` | `int` | Godzina lokalna snapshotu |
| `snapshot_at` | `timestamp` | Czas snapshotu w strefie `Europe/Warsaw` |
| `snapshot_at_utc` | `timestamp` | Czas snapshotu w UTC |
| `num_bikes_available` | `bigint` | Liczba rowerów dostępnych ogółem |
| `bikes_available` | `bigint` | Liczba zwykłych rowerów |
| `ebikes_available` | `bigint` | Liczba e-rowerów |
| `num_docks_available` | `bigint` | Liczba wolnych miejsc |
| `occupancy_ratio` | `double` | Udział zajętych miejsc względem `capacity` |
| `is_empty` | `boolean` | Czy stacja jest pusta |
| `is_full` | `boolean` | Czy stacja jest pełna |
| `is_installed` | `boolean` | Czy stacja jest zainstalowana |
| `is_renting` | `boolean` | Czy można wypożyczać rowery |
| `is_returning` | `boolean` | Czy można zwracać rowery |

</details>

<details>
<summary><code>station_hourly_occupancy</code></summary>

| Kolumna | Typ | Opis |
| --- | --- | --- |
| `station_id` | `string` | Id stacji |
| `date_key` | `int` | Klucz daty lokalnej (`Europe/Warsaw`) |
| `hour` | `int` | Godzina lokalna |
| `snapshot_count` | `bigint` | Liczba snapshotów w danej godzinie |
| `avg_bikes_available` | `double` | Średnia liczba rowerów |
| `min_bikes_available` | `bigint` | Minimalna liczba rowerów |
| `max_bikes_available` | `bigint` | Maksymalna liczba rowerów |
| `avg_ebikes_available` | `double` | Średnia liczba e-rowerów |
| `avg_docks_available` | `double` | Średnia liczba wolnych miejsc |
| `avg_occupancy_ratio` | `double` | Średnie obłożenie stacji |
| `empty_ratio` | `double` | Udział snapshotów, w których stacja była pusta |
| `full_ratio` | `double` | Udział snapshotów, w których stacja była pełna |

</details>

## Model danych

### Wymiary

| Tabela | Opis |
| --- | --- |
| `dim_station` | stacje z historią wersji (SCD2): nazwa, adres, współrzędne, `capacity`, `valid_from`, `valid_to`, `is_current` |
| `dim_vehicle_type` | typy rowerów połączone z cennikiem (`bike`, `ebike`) |
| `dim_date` | kalendarz: rok, kwartał, miesiąc, dzień tygodnia, `is_weekend` |

### Fakt

`fact_station_status` - ziarno: jedna stacja w jednym snapshocie. Zawiera liczbę rowerów (zwykłych i elektrycznych), wolne doki, `occupancy_ratio`, flagi `is_empty` i `is_full` oraz status stacji. `date_key` i `hour` liczone są w strefie `Europe/Warsaw`, a pełny czas zapisany także w UTC. Snapshoty z ujemnymi licznikami są odrzucane. Fakt łączy się z wersją stacji obowiązującą w momencie snapshotu (`station_key`), więc np. `occupancy_ratio` używa ówczesnej wartości `capacity`.

### Agregat

`station_hourly_occupancy` - per stacja, dzień i godzina: średnia, minimum i maksimum rowerów, średnia liczba e-rowerów i wolnych doków, średnie obłożenie oraz odsetek czasu, gdy stacja była pusta albo pełna. Uwzględnia tylko stacje z `is_installed`.

## Orkiestracja w Airflow

DAG `mevo_lakehouse` działa co 30 minut (`max_active_runs=1`, 3 ponowienia zadań co 2 minuty):

```
extract_bronze → transform_silver → build_gold → load_clickhouse
```

| Zadanie | Działanie |
| --- | --- |
| `extract_bronze` | `extract.py`: API → Bronze |
| `transform_silver` | `transform.py`: Bronze → Silver |
| `build_gold` | `gold.py`: Silver → Gold |
| `load_clickhouse` | `load.py`: Gold → ClickHouse |

Zadania Spark Airflow uruchamia przez `docker exec ... spark-submit` w kontenerze `mevo-bikes-spark` (worker ma dostęp do `docker.sock`).

## Stack technologiczny

| Kategoria | Technologie |
| --- | --- |
| Orkiestracja | Apache Airflow 3 (CeleryExecutor), Redis |
| Przetwarzanie | Apache Spark 3.5, PySpark, Spark SQL |
| Format tabel | Delta Lake 3.3 |
| Storage | MinIO (S3A) |
| Hurtownia | ClickHouse 24.8, `clickhouse-connect` |
| Infrastruktura | Docker, Docker Compose |
| Pobieranie danych | `requests` z `urllib3.Retry` |
| Analiza | Jupyter, pandas |

## Źródło danych

[GBFS](https://gbfs.org/) systemu rowerowego Mevo, publikowany przez Urban Sharing: `https://gbfs.urbansharing.com/rowermevo.pl`.

| Feed | Opis | Rekordów |
| --- | --- | --- |
| `vehicle_types` | typy rowerów | ~2 |
| `system_pricing_plans` | cenniki | ~2 |
| `station_information` | stacje | ~865 |
| `station_status` | stan stacji | ~865 na pobranie |
| `free_bike_status` | rowery poza stacjami | ~890 na pobranie |

Dane są publiczne i odświeżane przez API co kilkanaście sekund.

## Struktura projektu

```
mevo-bikes-lakehouse/
├── airflow/
│   └── dags/dag.py              # DAG mevo_lakehouse
├── docker/
│   ├── Dockerfile               # Spark + Delta + S3A
│   └── Dockerfile.airflow       # Airflow + docker CLI
├── notebooks/
│   └── eda.ipynb                # EDA feedów GBFS
├── src/
│   ├── etl/
│   │   ├── extract.py           # API -> Bronze
│   │   ├── transform.py         # Bronze -> Silver
│   │   ├── gold.py              # Silver -> Gold
│   │   └── load.py              # Gold -> ClickHouse
│   ├── common/
│   │   ├── lineage.py           # wspólne funkcje OpenLineage
│   │   └── quality.py           # reguły jakości i constrainty Bronze
│   └── SQL/
│       ├── gold/
│       │   ├── dimensions/      # dim_station, dim_vehicle_type, dim_date
│       │   ├── facts/           # fact_station_status
│       │   └── aggregates/      # station_hourly_occupancy
│       └── clickhouse/
│           └── 00_create_tables.sql
├── docker-compose.yaml
├── requirements.txt
└── .env.example
```

## Szybki start

### Wymagania

- Docker Desktop z Docker Compose
- ok. 8 GB wolnej pamięci RAM dla całego stacku

### 1. Skonfiguruj środowisko

```powershell
Copy-Item .env.example .env
```

W pliku `.env` ustaw dane dostępowe do MinIO i ClickHouse oraz `FERNET_KEY` dla Airflow. Klucz wygenerujesz poleceniem:

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 2. Uruchom stack

```powershell
docker compose up -d --build
```

### 3. Uruchom pipeline

W Airflow włącz DAG `mevo_lakehouse` (jest domyślnie wstrzymany). Od tej chwili pipeline działa co 30 minut, a pierwszy przebieg możesz wywołać ręcznie przyciskiem *Trigger*.

### Dostępne usługi

| Usługa | Adres |
| --- | --- |
| Airflow | http://localhost:8080 (domyślnie `admin` / `admin`) |
| MinIO Console | http://localhost:9001 |
| ClickHouse HTTP | http://localhost:8123 |
| ClickHouse native | `localhost:9002` |
| Marquez (lineage UI) | http://localhost:3000 |
| Marquez API | http://localhost:5000 |

## Walidacja pipeline'u

Do czasu dodania testów automatycznych walidacja jest ręczna.

Zawartość hurtowni:

```powershell
docker exec mevo-clickhouse clickhouse-client --user clickhouse --password clickhouse -d warehouse -q "SELECT count() FROM fact_station_status"
docker exec mevo-clickhouse clickhouse-client --user clickhouse --password clickhouse -d warehouse -q "SELECT * FROM station_hourly_occupancy ORDER BY avg_occupancy_ratio DESC LIMIT 10"
```

Przykład analizy: stacje, na których najczęściej brakuje rowerów.

```sql
SELECT
    s.station_name,
    round(avg(h.empty_ratio), 3) AS empty_ratio
FROM station_hourly_occupancy AS h
INNER JOIN dim_station AS s ON h.station_id = s.station_id AND s.is_current
GROUP BY s.station_name
ORDER BY empty_ratio DESC
LIMIT 10
```

Dodatkowo: statusy zadań w Airflow, obiekty w MinIO (`bronze`, `silver`, `gold`) oraz historia wersji tabel Delta (`DESCRIBE HISTORY`).

## Data lineage

Pochodzenie danych śledzi standard **OpenLineage**, a zdarzenia zbiera **Marquez** (interfejs na http://localhost:3000, namespace `mevo`).

Źródła zdarzeń:

| Źródło | Co raportuje |
| --- | --- |
| **Airflow** (provider `apache-airflow-providers-openlineage`) | start, sukces i błąd każdego zadania DAG-a |
| **Spark** (listener `openlineage-spark`) | zbiory wejściowe i wyjściowe każdego zadania, np. `gold.py` czyta `silver/station_status` i zapisuje `gold/fact_station_status` |
| **`extract.py`** | krok API → Bronze (`fetch_<feed>`), którego Spark sam nie widzi |
| **`load.py`** | krok Gold → ClickHouse (`load_clickhouse.<tabela>`), bo zapis idzie przez HTTP, a nie przez Sparka |

Zadania Spark startują przez `docker exec`, więc DAG przekazuje do kontenera identyfikatory zadania Airflow (zmienne `OPENLINEAGE_PARENT_*` wypełniane makrami providera). Dzięki temu uruchomienie Sparka pojawia się w Marquez jako dziecko zadania Airflow, a nie jako osobny, niepołączony graf.

Gdy zmienna `OPENLINEAGE_URL` nie jest ustawiona, skrypty działają bez lineage. Błąd wysyłki zdarzenia nigdy nie przerywa pipeline'u.

## Decyzje projektowe

- **Bronze trzyma wszystko.** Nie filtruję pojedynczych rekordów przy pobieraniu: usuwanie duplikatów to zadanie Silvera, dzięki czemu Silver można przeliczyć od nowa.
- **`last_updated` obok `last_reported`.** `last_updated` opisuje moment snapshotu i służy do przetwarzania przyrostowego oraz analiz w czasie. W danych Mevo `last_reported` stacji pokrywa się z czasem zrzutu.
- **Silver znormalizowany, Gold zdenormalizowany.** Wymiary i fakty są osobno w Silverze, a Gold to star schema pod zapytania analityczne.
- **Statyczne feedy bez partycji.** Dwuwierszowe tabele nie zyskują na partycjach, a `overwrite` zapobiega narastaniu identycznych rekordów.
- **ClickHouse jako warstwa serwowania.** Logika zostaje w lakehousie. ClickHouse jest kolumnowy i szybki na agregatach szeregów czasowych, a Gold ładuję do niego sterownikiem `clickhouse-connect`, żeby mieć pełną kontrolę nad typami.
- **Docker Compose zamiast Kubernetesa.** Skala projektu tego nie wymaga.

## O projekcie

Mevo Bikes Lakehouse to praktyczny projekt data engineeringowy. Pokazuje kompletny, lokalny pipeline: pobieranie danych z API, lakehouse w architekturze medallion na Delta Lake, orkiestrację, modelowanie wymiarowe i hurtownię analityczną.

## Autor i kontakt

| | |
| --- | --- |
| **Autor** | Maciej Spychalski ([Turbo-Blender](https://github.com/Turbo-Blender)) |
| **Email** | [m.spychalskipv@gmail.com](mailto:m.spychalskipv@gmail.com) |
| **Repozytorium** | [github.com/Turbo-Blender/mevo-bikes-lakehouse](https://github.com/Turbo-Blender/mevo-bikes-lakehouse) |

---

<a id="english-version"></a>

# Mevo Bikes Lakehouse (English)

**A local lakehouse for Mevo city bike data (Tricity, Poland)**, built with Apache Spark, Delta Lake, MinIO, Airflow, and ClickHouse.

`Apache Airflow · PySpark · Delta Lake · MinIO · ClickHouse · Docker · Spark SQL`

Every 30 minutes the pipeline pulls station and bike data from Mevo's public GBFS API, stores it in a data lake (MinIO) as Delta tables using the medallion architecture (Bronze → Silver → Gold), and loads the Gold layer into a ClickHouse data warehouse. Everything is orchestrated by Apache Airflow and runs locally with Docker Compose.

## Web interfaces

After `docker compose up -d --build`:

| Interface | URL | Login |
| --- | --- | --- |
| **Airflow** (DAGs, task logs) | http://localhost:8080 | `admin` / `admin` |
| **MinIO Console** (`lakehouse` bucket) | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env` |
| **Marquez** (data lineage) | http://localhost:3000 | none |
| **ClickHouse Play** (SQL on the warehouse) | http://localhost:8123/play | `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` from `.env` |
| **Flower** (Celery worker monitoring, optional) | http://localhost:5555 | started with `docker compose --profile flower up -d` |

## Implemented features

- **Containerized environment** - the whole stack runs through Docker Compose
- **ETL orchestration** - an Airflow DAG running every 30 minutes
- **Delta Lake lakehouse** - Bronze, Silver, and Gold as Delta tables in MinIO (S3A)
- **Resilient ingestion** - API requests retried with exponential backoff
- **Incremental processing** - Silver reads only new snapshots from Bronze and writes them with `MERGE`
- **Data quality flags** - out-of-area coordinates and negative counters flagged in Silver
- **Gold star schema** - station, vehicle type, and date dimensions plus a station status fact
- **Analytical aggregate** - hourly station occupancy
- **ClickHouse warehouse** - Gold loaded into `MergeTree` tables as the serving layer
- **Data lineage** - OpenLineage and Marquez: a dependency graph from the API through Bronze, Silver, and Gold to ClickHouse
- **Exploratory analysis** - Jupyter notebook covering all GBFS feeds

## Architecture

```
Mevo GBFS API -> Spark Extract -> Bronze (Delta, MinIO)
              -> Spark Transform -> Silver (Delta, MinIO)
              -> Spark SQL -> Gold (Delta, MinIO)
              -> Load -> ClickHouse
Airflow orchestrates all four Spark jobs.
```

The lakehouse is the source of truth: all transformation logic lives in Spark, and ClickHouse only serves the finished Gold tables. The warehouse can be rebuilt from the bucket at any time.

## Data layers

| Layer | Location | Contents |
| --- | --- | --- |
| Bronze | `s3a://lakehouse/bronze/mevo/` | API records as returned, plus `last_updated`. Snapshots (`station_status`, `free_bike_status`) are appended and partitioned by `year/month`; static feeds are overwritten without partitions |
| Silver | `s3a://lakehouse/silver/mevo/` | Typed, deduplicated, normalized data with quality flags. Event tables are updated incrementally via `MERGE`; dimensions are overwritten |
| Gold | `s3a://lakehouse/gold/mevo/` | Star schema and aggregates built with Spark SQL from `src/SQL/gold/` |
| ClickHouse | database `warehouse` | `MergeTree` copy of Gold, monthly partitions for the fact and the aggregate |

## Data model

| Table | Type | Description |
| --- | --- | --- |
| `dim_station` | dimension (SCD2) | station versions: name, address, coordinates, capacity, `valid_from`, `valid_to`, `is_current` |
| `dim_vehicle_type` | dimension | bike types joined with pricing plans |
| `dim_date` | dimension | calendar with `is_weekend` |
| `fact_station_status` | fact | one station in one snapshot: bikes, e-bikes, free docks, `occupancy_ratio`, `is_empty`, `is_full` |
| `station_hourly_occupancy` | aggregate | per station, day, and hour: average/min/max bikes, share of time empty or full |

`date_key` and `hour` are computed in the `Europe/Warsaw` time zone; the UTC timestamp is stored as well.

## Airflow DAG

`mevo_lakehouse` runs every 30 minutes (`max_active_runs=1`, 3 task retries):

```
extract_bronze -> transform_silver -> build_gold -> load_clickhouse
```

Spark jobs are started via `docker exec ... spark-submit` in the `mevo-bikes-spark` container.

## Quick start

```powershell
Copy-Item .env.example .env        # set credentials and FERNET_KEY
docker compose up -d --build       # full stack
```

Then enable the `mevo_lakehouse` DAG in Airflow (http://localhost:8080). It is paused by default; once enabled it runs every 30 minutes, and you can trigger the first run manually.

| Service | URL |
| --- | --- |
| Airflow | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| ClickHouse HTTP | http://localhost:8123 |

## Data lineage

Data provenance is tracked with **OpenLineage** and collected by **Marquez** (UI at http://localhost:3000, namespace `mevo`). Events come from Airflow (task runs), the Spark listener (input and output datasets of every job), and two explicit events for steps Spark cannot see: API → Bronze in `extract.py` and Gold → ClickHouse in `load.py`. Because Spark jobs start through `docker exec`, the DAG passes the Airflow task identifiers into the container, so each Spark run is linked to its Airflow task in the graph. Without `OPENLINEAGE_URL` the jobs run without lineage, and a failed event never breaks the pipeline.

## Design decisions

- **Bronze keeps everything.** Record-level deduplication belongs to Silver, so Silver can always be rebuilt.
- **Silver normalized, Gold denormalized.** Dimensions and facts stay separate in Silver; Gold is a star schema built for analytical queries.
- **Static feeds are not partitioned.** Two-row tables gain nothing from partitions, and `overwrite` avoids piling up identical records.
- **ClickHouse as the serving layer.** Logic stays in the lakehouse; Gold is loaded with `clickhouse-connect` for full control over types.
- **Docker Compose over Kubernetes.** The project's scale does not call for it.

## Data source

Mevo's [GBFS](https://gbfs.org/) feeds published by Urban Sharing: `https://gbfs.urbansharing.com/rowermevo.pl`.

## Author & contact

| | |
| --- | --- |
| **Author** | Maciej Spychalski ([Turbo-Blender](https://github.com/Turbo-Blender)) |
| **Email** | [m.spychalskipv@gmail.com](mailto:m.spychalskipv@gmail.com) |
| **Repository** | [github.com/Turbo-Blender/mevo-bikes-lakehouse](https://github.com/Turbo-Blender/mevo-bikes-lakehouse) |
