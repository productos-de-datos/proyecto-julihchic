def make_forecasts():
    """Construya los pronosticos con el modelo entrenado final.
    Cree el archivo data_lake/business/forecasts/precios-diarios.csv. Este
    archivo contiene tres columnas:
    * La fecha.
    * El precio promedio real de la electricidad.
    * El pronóstico del precio promedio real.
    """
    from train_daily_model import train_daily_model, load_data, data_preparation, make_train_test_split

    def load_pkl():
        import pickle
        with open("src/models/precios-diarios.pickle", "rb") as file:
            model_RF = pickle.load(file)
        return model_RF

    def score(x_train, y_train):
        import numpy as np

        # Se agrega distintas estimaciones
        estimators = np.arange(10, 200, 10)
        scores = []
        for n in estimators:
            model_RF.set_params(n_estimators=n)
            model_RF.fit(x_train, y_train)
            scores.append(model_RF.score(x_test, y_test))

        return scores

    def best_score(scores):

        import pandas as pd
        estimador_n = pd.DataFrame(scores)
        estimador_n.reset_index(inplace=True)
        estimador_n = estimador_n.rename(columns={0: 'scores'})
        estimador_n = estimador_n[estimador_n['scores']
                                  == estimador_n['scores'].max()]
        estimador_n['index'] = (estimador_n['index']+1)*10
        estimador_n = estimador_n['index'].iloc[0]

        return estimador_n

    def trein_model_with_best_estimator(estimador_n):

        from sklearn.ensemble import RandomForestRegressor

        # Entrenamiento del moodelo con los estimadores sugeridos
        model_RF = RandomForestRegressor(
            n_estimators=estimador_n, random_state=12345)
        model_RF.fit(x_train, y_train)
        return model_RF

    def prediction_test_model(model_RF):
        # Prediccion de x_test
        y_pred_RF_testeo = model_RF.predict(x_test)
        return y_pred_RF_testeo

    def forecasts(y_pred_RF_testeo, y_test, data):
        import pandas as pd
        import numpy as np

        df_model_RF = pd.DataFrame(y_test).reset_index(drop=True)
        df_model_RF['pronostico'] = y_pred_RF_testeo

        np.random.seed(0)

        # idenfificar indices de y_test
        # ------------------------------------------
        df_series = pd.Series(y_test)
        df_series = df_series.to_frame()
        df_series.reset_index(inplace=True)
        df_series = df_series[['index']]
        # ------------------------------------------

        df_model_RF = pd.concat([df_model_RF, df_series], axis=1)

        data = data[['fecha']]
        data.reset_index(inplace=True)

        df_model_RF = pd.merge(df_model_RF, data, on='index', how='left')
        df_model_RF = df_model_RF[['fecha', 'precio', 'pronostico']]
        return df_model_RF

    def save_forecasts(df_model_RF):
        df_model_RF.to_csv(
            'data_lake/business/forecasts/precios-diarios.csv', index=None)

    # ejecución
    data = load_data()
    x, y = data_preparation(data)
    x_train, x_test, y_train, y_test = make_train_test_split(x, y)
    model_RF = load_pkl()
    scores = score(x_train, y_train)
    estimador_n = best_score(scores)
    model_RF = trein_model_with_best_estimator(estimador_n)
    y_pred_RF_testeo = prediction_test_model(model_RF)
    df_model_RF = forecasts(y_pred_RF_testeo, y_test, data)
    save_forecasts(df_model_RF)


    #raise NotImplementedError("Implementar esta función")
if __name__ == "__main__":
    import doctest

    doctest.testmod()
    make_forecasts()