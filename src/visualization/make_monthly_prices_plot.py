def make_monthly_prices_plot():
    """Crea un grafico de lines que representa los precios promedios mesuales.
    Usando el archivo data_lake/business/precios-mesuales.csv, crea un grafico de
    lines que representa los precios promedios mensuales.
    El archivo se debe salvar en formato PNG en data_lake/business/reports/figures/monthly_prices.png.
    """
    import pandas as pd
    import matplotlib.pyplot as plt

   
    path_file = r'data_lake/business/precios-mensuales.csv'

    
    datos = pd.read_csv(path_file, index_col=None, sep=',', header=0)
    datos["fecha"] = pd.to_datetime(datos["fecha"])

   
    x = datos.fecha
    y = datos.precio

    
    plt.figure(figsize=(15, 6))
    plt.plot(x, y, 'b', label='Precio Promedio Mensual')
    plt.title('Precios Promedios Mensuales')
    plt.xlabel('Fecha (Años)')
    plt.ylabel('Precio ($/kWh)')
    plt.legend()
    plt.xticks(rotation="vertical")

  
    plt.savefig("data_lake/business/reports/figures/monthly_prices.png")

    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    make_monthly_prices_plot()