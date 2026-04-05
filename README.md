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

## Cómo levantar el entorno

1. Clonar el repositorio:

```bash
git clone <TU_REPO>
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
 -Pipeline RAW (Ingesta)

Responsable de:

descarga de datos .parquet
carga en PostgreSQL
almacenamiento en raw.ny_taxi_trips

Características:

carga mensual
idempotente (borra e inserta por mes)
manejo de errores
 -Pipeline CLEAN (Transformación)

Responsable de:

limpieza de datos
validación de calidad
construcción de modelo dimensional

Ejecutado mediante SQL en PostgreSQL (pgAdmin).

▶ Modelo de datos
- Granularidad

La tabla de hechos representa:

- un viaje individual

▶ Tabla de hechos
clean.fact_trips

Contiene:

métricas del viaje
duración
montos
claves de dimensiones

▶ Dimensiones
clean.dim_vendor
clean.dim_payment_type
clean.dim_pickup_location
clean.dim_dropoff_location

▶ Relaciones
fact_trips.vendor_key → dim_vendor.vendor_key
fact_trips.payment_type_key → dim_payment_type.payment_type_key
fact_trips.pickup_location_key → dim_pickup_location.pickup_location_key
fact_trips.dropoff_location_key → dim_dropoff_location.dropoff_location_key

▶ Validaciones realizadas
SELECT COUNT(*) FROM raw.ny_taxi_trips;

SELECT COUNT(*) FROM clean.fact_trips;

SELECT source_year, source_month, COUNT(*)
FROM raw.ny_taxi_trips
GROUP BY source_year, source_month
ORDER BY source_year, source_month;

▶ Reglas de limpieza aplicadas
eliminación de registros inválidos
validación de fechas (pickup ≤ dropoff)
filtrado de valores negativos
eliminación de nulos críticos
cálculo de duración del viaje

▶ Decisiones de diseño
separación raw / clean
modelo tipo star schema
claves sustitutas
uso de UNLOGGED TABLE para performance
procesamiento eficiente por memoria

▶ Limitaciones y decisiones técnicas

Debido a limitaciones de almacenamiento local:

la tabla clean.fact_trips se generó con una muestra representativa (100,000 registros de 2024-01)

Esto permite:

demostrar el modelo dimensional
evitar errores de memoria
mantener reproducibilidad

▶Triggers y automatización
Pipeline RAW: ejecución manual por mes
Pipeline CLEAN: ejecución posterior

▶ Manejo de configuración
variables en .env
configuración en io_config.yaml
sin credenciales hardcodeadas

▶ Volumen de datos
14 meses procesados
~3M registros por mes
+40M registros en raw

▶ Estructura del proyecto
pset-2/
├── docker-compose.yaml
├── ingest-data.py
├── requirements.txt
├── README.md
├── notebooks/
└── data-orquestador/

▶ Conclusión

Este proyecto implementa un pipeline ELT completo que:

ingiere datos históricos
construye un data warehouse
aplica limpieza
implementa modelo dimensional
maneja limitaciones de infraestructura

Autor:

Evelyn Bermeo