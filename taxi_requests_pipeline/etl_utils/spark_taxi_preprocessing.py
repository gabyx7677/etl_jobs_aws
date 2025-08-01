from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import DataFrame
from pyspark.ml.functions import vector_to_array

def split_data_temporal(df: DataFrame, test_fraction: float = 0.1):
    """Se divide el conjunto de datos en entrenamiento y prueba sin barajar, respetando el orden temporal."""
    total_count = df.count()
    test_count = int(total_count * test_fraction)
    train_count = total_count - test_count

    train_df = df.limit(train_count)
    test_df = df.subtract(train_df)  # preserva orden

    return train_df, test_df


def scale_features(train_df: DataFrame, test_df: DataFrame, target_col: str = 'num_orders'):
    """Se escalan las características numéricas y se devuelven como columnas separadas."""

    # Seleccionar columnas numéricas (excepto el objetivo)
    feature_cols = [c for c, t in train_df.dtypes if t in ['double', 'int'] and c != target_col]

    # Vectorizar
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_vec")
    train_vec = assembler.transform(train_df)
    test_vec = assembler.transform(test_df)

    # Escalar
    scaler = StandardScaler(inputCol="features_vec", outputCol="features_scaled", withMean=True, withStd=True)
    scaler_model = scaler.fit(train_vec)
    train_scaled = scaler_model.transform(train_vec)
    test_scaled = scaler_model.transform(test_vec)

    # Convertir vector escalado a array
    train_scaled = train_scaled.withColumn("features_array", vector_to_array(col("features_scaled")))
    test_scaled = test_scaled.withColumn("features_array", vector_to_array(col("features_scaled")))

    # Extraer columnas individuales
    num_features = len(feature_cols)

    train_scaled = train_scaled.select(
        *[col("features_array")[i].alias(f"{feature_cols[i]}_scaled") for i in range(num_features)],
        col(target_col)
    )

    test_scaled = test_scaled.select(
        *[col("features_array")[i].alias(f"{feature_cols[i]}_scaled") for i in range(num_features)],
        col(target_col)
    )

    return train_scaled, test_scaled, scaler_model



def build_preprocessing(df: DataFrame, target_col: str = 'num_orders', test_fraction: float = 0.1):
    """Pipeline completo de preprocesamiento y escalado."""
    # Dividir en entrenamiento y prueba
    train_df, test_df = split_data_temporal(df, test_fraction)

    # Escalar características
    train_scaled, test_scaled, scaler_model = scale_features(train_df, test_df, target_col)

    return train_scaled, test_scaled, scaler_model

