from pyspark.sql.functions import col, dayofmonth, dayofweek, month, lag, avg
from pyspark.sql.window import Window

def add_date_features(df):
    """Se agregan columnas de mes, día y día de la semana a partir de columna datetime."""
    return df.withColumn('month', month('datetime')) \
             .withColumn('day', dayofmonth('datetime')) \
             .withColumn('day_of_week', dayofweek('datetime'))


def add_lag_features(df, column, max_lag):
    """Se generan variables de desfase (lag) usando funciones de ventana."""
    window_spec = Window.orderBy('datetime')
    for i in range(1, max_lag + 1):
        df = df.withColumn(f'lag_{i}', lag(column, i).over(window_spec))
    return df.fillna(0)


def add_rolling_mean(df, column, window_size):
    """Se agrega la media móvil utilizando funciones de ventana."""
    window_spec = Window.orderBy('datetime').rowsBetween(-window_size, -1)
    return df.withColumn('rolling_mean', avg(column).over(window_spec)).fillna(0)


def build_features(df, column='num_orders', max_lag=5, rolling_window=3):
    """Pipeline completo."""
    df = add_date_features(df)
    df = add_lag_features(df, column, max_lag)
    df = add_rolling_mean(df, column, rolling_window)
    return df

