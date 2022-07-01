def make_forecasts():
    """Construya los pronosticos con el modelo entrenado final.

    Cree el archivo data_lake/business/forecasts/precios-diarios.csv. Este
    archivo contiene tres columnas:

    * La fecha.

    * El precio promedio real de la electricidad.

    * El pronóstico del precio promedio real.


    """
    import pandas as pd
    from train_daily_model import load_best_estimator
    from train_daily_model import make_train_test_split
    from train_daily_model import load_data

 

    x, y = load_data()
    estimator = load_best_estimator()
    x_train, x_test, y_train, y_test = make_train_test_split(x, y)
    y_pred = estimator.predict(x)

    
    path_file = r'data_lake/business/precios-diarios.csv'

    
    datos = pd.read_csv(path_file, index_col=None, sep=',', header=0)
    datos['pronostico'] = y_pred
    datos.columns = ['fecha', 'precio promedio real de la electricidad', 'pronóstico del precio promedio real']
    
   
    datos.to_csv('data_lake/business/forecasts/precios-diarios.csv', index=None)


    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    make_forecasts()
