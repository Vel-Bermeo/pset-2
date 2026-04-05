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