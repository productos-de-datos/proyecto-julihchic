def compute_monthly_prices():
    """Compute los precios promedios mensuales.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio mensual. Las
    columnas del archivo data_lake/business/precios-mensuales.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio mensual de la electricidad en la bolsa nacional



    """
    
    import pandas as pd

    
    data = pd.read_csv("data_lake/cleansed/precios-horarios.csv")

    data["fecha"] = pd.to_datetime(data["fecha"])

    
    data = data.set_index("fecha").resample("M")["precio"].mean()

    data.to_csv("data_lake/business/precios-mensuales.csv", index=True)

    #raise NotImplementedError("Implementar esta función")

   
def test_cantidad_meses():
    import pandas as pd
    data = pd.read_csv("data_lake/business/precios-mensuales.csv")
    assert len(data) == 310

if __name__ == "__main__":
    import doctest

    doctest.testmod()
