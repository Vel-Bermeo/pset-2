import pandas as pd
import sqlalchemy
import math
from tqdm.auto import tqdm

def main():
    
    URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

    print('Iniciando la descarga de los datos')

    datos_crudos = pd.read_parquet(URL)

    print('Datos descargados del internet (NY TAXI GOV)')

    print(f'Cantidad de datos: {datos_crudos.shape[0]}')
    print(datos_crudos.info())
    print(f'Columnas: {datos_crudos.columns}')  
    print(datos_crudos.head())
    print()

    conexion = sqlalchemy.create_engine('postgresql://root:root@pset-2-data-warehouse-1:5432/warehouse')

    # round() -> .5 -> 1
    # ceil() -> .00000000001 -> 1
    # floor() -> .9999999 -> 0
    
    # Chunking -> segmentar los datos en grupos: grupos de 10000; Agregar las filas por chunks (grupos)

    tamano = 100000

    num_chunks = math.ceil(datos_crudos.shape[0]/tamano)
    
    inicio = 0
    fin = tamano

    # Idempotencia: No importa cuando yo ejecute el script, simpre da el mismo resultado

    print('Creacion de la tabla')
    datos_crudos.head(0).to_sql(
        name='viajes_taxi_amarillo',
        con=conexion,
        if_exists='replace'
    )

    print('Inicio de guardado de datos en el warehouse')

    for i in tqdm(range(1, num_chunks)):
        # indexacion - slice [incluyente:excluyente]
        # [0: 10000]
        # [10000: 20000]
        # [20000: 30000]
        datos_crudos.iloc[inicio:fin].to_sql(
            name='viajes_taxi_amarillo',
            con=conexion,
            if_exists='append'
        )

        inicio = fin
        fin = tamano * i

    print('Se guardaron los datos exitosamente en el warehouse')


# Verificamos que el archivo se ejecuta como principal
if __name__ == '__main__':
    main()