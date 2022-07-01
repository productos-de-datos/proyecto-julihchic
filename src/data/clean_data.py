def clean_data():
    """Realice la limpieza y transformación de los archivos CSV.

    Usando los archivos data_lake/raw/*.csv, cree el archivo data_lake/cleansed/precios-horarios.csv.
    Las columnas de este archivo son:

    * fecha: fecha en formato YYYY-MM-DD
    * hora: hora en formato HH
    * precio: precio de la electricidad en la bolsa nacional

    Este archivo contiene toda la información del 1997 a 2021.


    """
    import pandas as pd
    import glob
    import os

    df = pd.DataFrame()
    for file in range(1995,2022):
        df = pd.concat([df,pd.read_csv('../../data_lake/raw/{}.csv'.format(file))])
    
    df_unpivoted = df.melt(id_vars=['Fecha'], var_name='Hora', value_name='Precio')
    
    df_unpivoted.to_csv('../../data_lake/cleansed/precios-horarios.csv', encoding='utf-8', index=False)
    
    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
