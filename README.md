# NYC Taxi Data Pipeline (Mage + PostgreSQL)

## Descripción del proyecto

Este proyecto implementa una solución **end-to-end ELT** para procesar datos históricos de NYC Taxi, utilizando:

- Mage (orquestador)
- PostgreSQL (data warehouse)
- pgAdmin (visualización y validación)
- Docker Compose (infraestructura reproducible)

El objetivo es construir una arquitectura con dos capas:

- **raw** → datos crudos e inmutables  
- **clean** → datos transformados con modelo dimensional  

---

## Arquitectura del proyecto

Flujo general:

Fuente NYC Taxi → Mage (raw pipeline) → PostgreSQL (schema raw)
→ Mage (clean pipeline) → PostgreSQL (schema clean)
→ pgAdmin (validación)


---

## Configuraciones y Secrets

## Manejo de credenciales

Las credenciales del sistema no están hardcodeadas en el código.

Se gestionan mediante variables de entorno definidas en un archivo `.env`, el cual no se incluye en el repositorio por seguridad.

Para ejecutar el proyecto:

1. Crear un archivo `.env` basado en `.env.example`
2. Definir las variables necesarias:

```env
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=warehouse
PGADMIN_DEFAULT_EMAIL=your_email@example.com
PGADMIN_DEFAULT_PASSWORD=your_password
```

### Secrets en Mage AI
La conexion a PostgreSQL desde los pipelines se maneja EXCLUSIVAMENTE mediante secrets de Mage AI. Ningun bloque de codigo contiene credenciales en texto plano.

Configurar los siguientes secrets antes de ejecutar cualquier pipeline:

Mage UI → Settings → Secrets → New Secret

Nombre secret Valor ────── ─────── POSTGRES_USER → root POSTGRES_PASSWORD → root POSTGRES_HOST → data-warehouse POSTGRES_PORT → 5432 POSTGRES_DB → warehouse

Estos secrets se referencian en data-orquestador/orquestador/io_config.yaml:

default:
  POSTGRES_DBNAME:   "{{ env_var('POSTGRES_DB') }}"
  POSTGRES_HOST:     "{{ env_var('POSTGRES_HOST') }}"
  POSTGRES_PORT:     "{{ env_var('POSTGRES_PORT') }}"
  POSTGRES_USER:     "{{ env_var('POSTGRES_USER') }}"
  POSTGRES_PASSWORD: "{{ env_var('POSTGRES_PASSWORD') }}"


## Cómo levantar el entorno
```bash
1. Clonar el repositorio:

git clone https://github.com/Vel-Bermeo/pset-2.git
cd pset-2

2. Levantar los servicios:
docker compose up

3. Esperar que los contenedores estén activos:
PostgreSQL
Mage
pgAdmin

Acceso a herramientas
Mage: http://localhost:6789
pgAdmin: http://localhost:9000

```

## Ejecución de pipelines


 ▶Pipeline RAW (Ingesta)

Responsable de:

- Descarga de datos .parquet
- Carga en PostgreSQL
- Almacenamiento en raw.ny_taxi_trips

Características:

- Carga mensual
- Idempotente (borra e inserta por mes)
- Manejo de errores

 ▶Pipeline CLEAN (Transformación)

Responsable de:

- Limpieza de datos
- Validación de calidad
- Construcción de modelo dimensional

Ejecutado mediante SQL en PostgreSQL


## Modelo de datos
- Granularidad

La tabla de hechos representa:

- un viaje individual

## Tabla de hechos

clean.fact_trips

Contiene:

- métricas del viaje
- duración
- montos
- claves de dimensiones

## Dimensiones
clean.dim_vendor
clean.dim_payment_type
clean.dim_pickup_location
clean.dim_dropoff_location

## Relaciones
fact_trips.vendor_key → dim_vendor.vendor_key
fact_trips.payment_type_key → dim_payment_type.payment_type_key
fact_trips.pickup_location_key → dim_pickup_location.pickup_location_key
fact_trips.dropoff_location_key → dim_dropoff_location.dropoff_location_key

## Validaciones realizadas
```
SELECT COUNT(*) FROM raw.ny_taxi_trips;
```
```
SELECT COUNT(*) FROM clean.fact_trips;
```
```
SELECT source_year, source_month, COUNT(*)
FROM raw.ny_taxi_trips
GROUP BY source_year, source_month
ORDER BY source_year, source_month;
```

## Reglas de limpieza aplicadas

- Eliminación de registros inválidos
- Validación de fechas (pickup ≤ dropoff)
- Filtrado de valores negativos
- Eliminación de nulos críticos
- Cálculo de duración del viaje


## Decisiones de diseño

- Separación raw / clean
- Modelo tipo star schema
- Claves sustitutas
- Uso de UNLOGGED TABLE para performance
- Procesamiento eficiente por memoria

## Limitaciones y decisiones técnicas

Debido a limitaciones de almacenamiento local:

La tabla clean.fact_trips se generó con una muestra representativa (100,000 registros de 2024-01)

Esto permite:
- Demostrar el modelo dimensional
- Evitar errores de memoria
- Mantener reproducibilidad

## Triggers y automatización
- Pipeline RAW: ejecución por mes
- Pipeline CLEAN: ejecución posterior

## Manejo de configuración

- Variables en .env
- Configuración en io_config.yaml
- Sin credenciales hardcodeadas

## Volumen de datos
- 14 meses procesados
- ~3M registros por mes
- +40M registros en raw

## Estructura del proyecto

```
pset-2/
├── docker-compose.yaml
├── ingest-data.py
├── requirements.txt
├── README.md
├── notebooks/
└── data-orquestador/
```

## Pipelines RAW y CLEAN

### Pipeline Raw

Se desarrolló en Python:

```env
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import logging
import gc

from os import path
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres

logger = logging.getLogger(__name__)


@data_loader
def load_data(*args, **kwargs):
    execution_date = kwargs.get('execution_date')
    year = kwargs.get('year')
    month = kwargs.get('month')

    print(f'execution_date: {execution_date}', flush=True)
    print(f'year manual: {year}', flush=True)
    print(f'month manual: {month}', flush=True)

    if year is not None and month is not None:
        year = int(year)
        month = int(month)
    elif execution_date is not None:
        year = execution_date.year
        month = execution_date.month
    else:
        raise ValueError('No se recibió ni year/month manuales ni execution_date.')

    schema_name = 'raw'
    table_name = 'ny_taxi_trips'
    full_table_name = f'{schema_name}.{table_name}'

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    df = None
    resumen = []
    filas_insertadas = 0

    print('\n***** INICIO DATA LOADER RAW *****', flush=True)
    print(f'Mes a procesar: {year}-{month:02d}', flush=True)

    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
    print(f'Archivo fuente: {url}', flush=True)

    try:
        print('Descargando datos...', flush=True)
        df = pd.read_parquet(url)

        print(f'Filas leídas: {len(df):,}', flush=True)
        mem_mb = df.memory_usage(deep=True).sum() / 1024**2
        print(f'Memoria estimada del DataFrame: {mem_mb:,.2f} MB', flush=True)

        # Estandarización técnica mínima permitida en RAW
        df.columns = [c.lower() for c in df.columns]
        df.rename(columns={
            'vendorid': 'vendor_id',
            'ratecodeid': 'rate_code_id',
            'pulocationid': 'pu_location_id',
            'dolocationid': 'do_location_id',
        }, inplace=True)

        # Metadata técnica mínima
        df['source_year'] = year
        df['source_month'] = month
        df['source_file'] = f'yellow_tripdata_{year}-{month:02d}.parquet'

        # Tipado básico permitido en RAW
        datetime_cols = [
            'tpep_pickup_datetime',
            'tpep_dropoff_datetime',
        ]
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        numeric_cols = [
            'vendor_id',
            'rate_code_id',
            'pu_location_id',
            'do_location_id',
            'passenger_count',
            'payment_type',
            'trip_distance',
            'fare_amount',
            'extra',
            'mta_tax',
            'tip_amount',
            'tolls_amount',
            'improvement_surcharge',
            'total_amount',
            'congestion_surcharge',
            'airport_fee',
            'cbd_congestion_fee',
            'source_year',
            'source_month',
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Reemplazar NaN por None para export
        df = df.where(pd.notnull(df), None)

        filas_insertadas = len(df)

        create_table_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name};

        CREATE TABLE IF NOT EXISTS {full_table_name} (
            vendor_id DOUBLE PRECISION,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count DOUBLE PRECISION,
            trip_distance DOUBLE PRECISION,
            rate_code_id DOUBLE PRECISION,
            store_and_fwd_flag TEXT,
            pu_location_id DOUBLE PRECISION,
            do_location_id DOUBLE PRECISION,
            payment_type DOUBLE PRECISION,
            fare_amount DOUBLE PRECISION,
            extra DOUBLE PRECISION,
            mta_tax DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            tolls_amount DOUBLE PRECISION,
            improvement_surcharge DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,
            congestion_surcharge DOUBLE PRECISION,
            airport_fee DOUBLE PRECISION,
            cbd_congestion_fee DOUBLE PRECISION,
            source_year DOUBLE PRECISION,
            source_month DOUBLE PRECISION,
            source_file TEXT
        );
        """

        with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            print(f'Creando tabla {full_table_name} si no existe...', flush=True)
            loader.execute(create_table_sql)

            print(f'Eliminando datos previos de {year}-{month:02d}...', flush=True)
            delete_sql = f"""
            DELETE FROM {full_table_name}
            WHERE source_year = {year}
              AND source_month = {month};
            """
            loader.execute(delete_sql)

            print(f'Insertando datos en {full_table_name}...', flush=True)

            chunk_size = 100000
            total_chunks = (len(df) + chunk_size - 1) // chunk_size

            for i, start in enumerate(range(0, len(df), chunk_size), start=1):
                end = start + chunk_size
                chunk = df.iloc[start:end]

                print(
                    f'Chunk {i}/{total_chunks} | filas {start:,} a {min(end, len(df)):,}...',
                    flush=True
                )

                loader.export(
                    chunk,
                    schema_name,
                    table_name,
                    index=False,
                    if_exists='append',
                )

                chunk = None
                gc.collect()

        print(f'Filas insertadas: {filas_insertadas:,}', flush=True)

        del df
        df = None
        gc.collect()

        resumen.append({
            'year': year,
            'month': month,
            'status': 'ok',
            'rows_loaded': filas_insertadas,
        })

        print(f'COMPLETADO {year}-{month:02d}', flush=True)

    except Exception as e:
        logger.exception(f'Error cargando {url}: {e}')
        print(f'ERROR en {year}-{month:02d}: {e}', flush=True)

        resumen.append({
            'year': year,
            'month': month,
            'status': 'error',
            'rows_loaded': 0,
            'error': str(e),
        })

    finally:
        if df is not None:
            del df
        gc.collect()

    print('***** FIN DATA LOADER RAW *****', flush=True)

    return pd.DataFrame(resumen)


@test
def test_output(output, *args):
    assert output is not None, 'El output es undefined'
    assert isinstance(output, pd.DataFrame), 'Debe ser DataFrame'
    assert len(output) > 0, 'No se procesó ningún mes'
    assert 'status' in output.columns, 'Debe existir la columna status'
    assert output['status'].iloc[0] == 'ok', f"Carga fallida: {output.to_dict('records')}"

```

### Pipeline Clean

Se realizó un flujo de 7 bloques en SQL:

▶Bloque 1: Crear esquema

```env
CREATE SCHEMA IF NOT EXISTS clean;
```

▶Bloque 2: Tabla Staging

```env
DROP TABLE IF EXISTS clean.stg_trips_valid;

CREATE UNLOGGED TABLE clean.stg_trips_valid AS
SELECT
    vendor_id,
    rate_code_id,
    pu_location_id,
    do_location_id,
    payment_type,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    source_year,
    source_month,
    source_file,
    EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0 AS trip_duration_minutes
FROM raw.ny_taxi_trips
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_dropoff_datetime >= tpep_pickup_datetime
  AND COALESCE(trip_distance, 0) >= 0
  AND COALESCE(fare_amount, 0) >= 0
  AND COALESCE(total_amount, 0) >= 0
  AND COALESCE(passenger_count, 0) >= 0
  AND pu_location_id IS NOT NULL
  AND do_location_id IS NOT NULL;
```

▶Bloque 3: Dim vendor

```env
DROP TABLE IF EXISTS clean.dim_vendor;

CREATE TABLE clean.dim_vendor AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY vendor_id) AS vendor_key,
    vendor_id
FROM clean.stg_trips_valid
WHERE vendor_id IS NOT NULL;
```

▶Bloque 4: Dim payment_type

```env
DROP TABLE IF EXISTS clean.dim_payment_type;

CREATE TABLE clean.dim_payment_type AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY payment_type) AS payment_type_key,
    payment_type
FROM clean.stg_trips_valid
WHERE payment_type IS NOT NULL;
```

▶Bloque 5: Dim pickup_location

```env
DROP TABLE IF EXISTS clean.dim_pickup_location;

CREATE TABLE clean.dim_pickup_location AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY pu_location_id) AS pickup_location_key,
    pu_location_id
FROM clean.stg_trips_valid
WHERE pu_location_id IS NOT NULL;
```

▶Bloque 6: Dim dropoff_location

```env
DROP TABLE IF EXISTS clean.dim_dropoff_location;

CREATE TABLE clean.dim_dropoff_location AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY do_location_id) AS dropoff_location_key,
    do_location_id
FROM clean.stg_trips_valid
WHERE do_location_id IS NOT NULL;

```
▶Bloque 7: Fact Table

```env
DROP TABLE IF EXISTS clean.fact_trips;

CREATE UNLOGGED TABLE clean.fact_trips AS
SELECT
    ROW_NUMBER() OVER () AS trip_id,
    v.vendor_key,
    p.payment_type_key,
    pu.pickup_location_key,
    dof.dropoff_location_key,

    t.tpep_pickup_datetime,
    t.tpep_dropoff_datetime,

    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.airport_fee,
    t.cbd_congestion_fee,

    t.trip_duration_minutes,
    t.source_year,
    t.source_month

FROM clean.stg_trips_valid t
LEFT JOIN clean.dim_vendor v
    ON t.vendor_id = v.vendor_id
LEFT JOIN clean.dim_payment_type p
    ON t.payment_type = p.payment_type
LEFT JOIN clean.dim_pickup_location pu
    ON t.pu_location_id = pu.pu_location_id
LEFT JOIN clean.dim_dropoff_location dof
    ON t.do_location_id = dof.do_location_id;
```


## Conclusión

Este proyecto implementa un pipeline ELT completo que:

- Ingiere datos históricos
- Construye un data warehouse
- Aplica limpieza de datos
- Implementa modelo dimensional
- Maneja limitaciones de infraestructura

Autor:

* Evelyn Bermeo
  
  USFQ
