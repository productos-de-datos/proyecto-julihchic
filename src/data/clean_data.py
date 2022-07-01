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

    path_file = glob.glob(r'data_lake/raw/*.csv')
    li = []

    for filename in path_file:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
    read_file = pd.concat(li, axis=0, ignore_index=True)
    read_file = read_file[read_file["Fecha"].notnull()]

    fecha1 = read_file.iloc[:, 0]  # fecha
    lista_datos = []

    precio = 0
    contador_filas = 0
    for fecha in fecha1:
        for hora in range(0, 24):
            precio = (read_file.iloc[contador_filas, (hora+1)])
            lista_datos.append([fecha, hora, precio])
        contador_filas += 1
    df = pd.DataFrame(lista_datos, columns=["fecha", "hora", "precio"])
    df = df[df["fecha"].notnull()]
    df.to_csv("data_lake/cleansed/precios-horarios.csv",
              index=None, header=True)
    #raise NotImplementedError("Implementar esta función")
    # return

# clean_data()


if __name__ == "__main__":

    import doctest

    doctest.testmod()
    clean_data()