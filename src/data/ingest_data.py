def ingest_data():
    """Ingeste los datos externos a la capa landing del data lake.
    Del repositorio jdvelasq/datalabs/precio_bolsa_nacional/xls/ descarge los
    archivos de precios de bolsa nacional en formato xls a la capa landing. La
    descarga debe realizarse usando únicamente funciones de Python.
    """
    import pandas as pd
    import xlwt

    def descargar_archivo(ruta, file_name, extension):

        for año in file_name:
            url_rute = ruta + '/' + año + extension + "?raw=true"
            nombre_archivo = "data_lake/landing/" + \
                '{}{}'.format(año, extension)
            descarga = pd.read_excel(url_rute)
            descarga.to_excel("data_lake/landing/{}{}".format(año,
                                                              extension), index=None, header=True)
        return

    ruta = "https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/"
    file_name1 = [str(año) for año in range(1995, 2016)]
    file_name2 = [str(año) for año in range(2018, 2022)]

    file_name_xlsx = file_name1 + file_name2
    descargar_archivo(ruta, file_name_xlsx, ".xlsx")

    file_name_xls = ["2016", "2017"]
    descargar_archivo(ruta, file_name_xls, ".xls")
    #raise NotImplementedError("Implementar esta función")
    # return


# ingest_data()


if __name__ == "__main__":

    import doctest

    doctest.testmod()
    ingest_data()