import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, to_timestamp
from spark_taxi_features import build_features
from spark_taxi_preprocessing import build_preprocessing
from spark_taxi_file_name_organizer import rename_s3_file

# Inicializar contexto
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Leer datos desde S3
input_path = "s3://taxi.project.gabyx7677-1/datasets/raw/taxi.csv"
train_path = "s3://taxi.project.gabyx7677-1/datasets/clean/train/"
test_path = "s3://taxi.project.gabyx7677-1/datasets/clean/test/"
model_path = "s3://taxi.project.gabyx7677-1/models/"

df = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
               .csv(input_path) \
               .withColumn("datetime", to_timestamp(col("datetime"))) \
               .withColumnRenamed("num_orders", "num_orders")

# Aplicar ingeniería de características
df_features = build_features(df, column='num_orders', max_lag=5, rolling_window=3)

# Aplicar preprocesamiento y escalado
train_scaled, test_scaled, scaler_model = build_preprocessing(
    df_features, target_col='num_orders', test_fraction=0.1
)

# Guardar conjuntos con nombres temporales
tmp_train_path = train_path + "tmp_train/"
tmp_test_path = test_path + "tmp_test/"

train_scaled.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_train_path)
test_scaled.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_test_path)

# Guardar modelo de escalado
scaler_model.write().overwrite().save(model_path)

# Extraer nombre del bucket
bucket_name = 'taxi.project.gabyx7677-1'

# Renombrar archivos a train.csv y test.csv
ok_train = rename_s3_file(bucket_name, 'datasets/clean/train/tmp_train/', 'datasets/clean/train/train.csv')
ok_test = rename_s3_file(bucket_name, 'datasets/clean/test/tmp_test/', 'datasets/clean/test/test.csv')

if not ok_train or not ok_test:
    raise Exception("Falló la escritura o renombrado de archivos CSV en S3.")


