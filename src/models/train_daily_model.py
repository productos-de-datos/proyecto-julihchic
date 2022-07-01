
"""Entrena el modelo de pronóstico de precios diarios.

Con las features entrene el modelo de proóstico de precios diarios y
salvelo en models/precios-diarios.pkl


"""

def load_data():

    import pandas as pd

    path = 'data_lake/business/features/precios_diarios.csv'
    df = pd.read_csv(path, sep=",")
    df['fecha'] = pd.to_datetime(df['fecha'], format='%Y-%m-%d')
    df['year'], df['month'], df['day'] = df['fecha'].dt.year, df['fecha'].dt.month, df['fecha'].dt.day

    y = df["precio"]
    x = df.copy()
    x.pop("precio")
    x.pop("fecha")
    return x, y


def make_train_test_split(x, y):

    from sklearn.model_selection import train_test_split

    (x_train, x_test, y_train, y_test) = train_test_split(
        x,
        y,
        test_size=0.25,
        random_state=123456,
    )
    return x_train, x_test, y_train, y_test


def eval_metrics(y_true, y_pred):

    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    mse = mean_squared_error(y_true, y_pred)
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)

    return mse, mae, r2


def report(estimator, mse, mae, r2):

    print(estimator, ":", sep="")
    print(f"  MSE: {mse}")
    print(f"  MAE: {mae}")
    print(f"  R2: {r2}")


def save_best_estimator(estimator):
    import os
    import pickle

    if not os.path.exists("src/models/"):
        return None
    with open("src/models/precios-diarios.pickle", "wb") as file:
        pickle.dump(estimator, file)


def load_best_estimator():

    import os
    import pickle

    if not os.path.exists("src/models/"):
        return None
    with open("src/models/precios-diarios.pickle", "rb") as file:
        estimator = pickle.load(file)

    return estimator

def train_daily_model():

    import numpy as np
    from sklearn.linear_model import ElasticNet
    from sklearn.model_selection import GridSearchCV

    alphas=np.linspace(0.0001, 0.5, 10)
    l1_ratios=np.linspace(0.0001, 0.5, 10)
    n_splits=5
    
    
    x, y = load_data()
    x_train, x_test, y_train, y_test = make_train_test_split(x, y)
    
    
    estimator = GridSearchCV(
            ElasticNet(
            random_state=12345,
        ),
        param_grid={
            "alpha": alphas,
            "l1_ratio": l1_ratios,
        },
        cv=n_splits,
        refit=True,
        return_train_score=False,
    )

    estimator.fit(x_train, y_train)

    y_pred=estimator.predict(x_test)

    mse, mae, r2 = eval_metrics(y_test, y_pred)
    
    report(estimator, mse, mae, r2)

    save_best_estimator(estimator)
   
    print(len(y_pred))
    #raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    train_daily_model()
