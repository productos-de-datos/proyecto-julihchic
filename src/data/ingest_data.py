"""
Módulo de ingestión de datos.
-------------------------------------------------------------------------------

"""


def ingest_data():
    """Ingeste los datos externos a la capa landing del data lake.

    Del repositorio jdvelasq/datalabs/precio_bolsa_nacional/xls/ descarge los
    archivos de precios de bolsa nacional en formato xls a la capa landing. La
    descarga debe realizarse usando únicamente funciones de Python.

    """
    import github
    import requests
    g = github.Github()
    repo = g.get_repo('jdvelasq/datalabs')
    contents = repo.get_contents('datasets/precio_bolsa_nacional/xls')

    for contentFile in contents:
        url_descarga = contentFile.download_url
        nombre_archivo = url_descarga.rsplit('/', 1)[1]
        with open('data_lake/landing/' + nombre_archivo, 'wb') as f:
            f.write(requests.get(url_descarga).content)

    

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()

    ingest_data()
