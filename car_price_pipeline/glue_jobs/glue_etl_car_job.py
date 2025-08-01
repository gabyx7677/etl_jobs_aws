# Se importan las bibliotecas necesarias
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.functions import vector_to_array  # Necesario para separar el vector en columnas

# Inicialización de contextos
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Rutas S3
input_path = "s3://car.data.gabyx7677/car_data_raw/car_data.csv"
output_path = "s3://car.data.gabyx7677/car_data_clean/car_data_cleaned.csv"

# Carga de datos desde S3
datasource = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [input_path]},
    transformation_ctx="datasource"
)

# Conversión a DataFrame
df = datasource.toDF()

# Conversión de columnas numéricas a tipos adecuados
df = df.withColumn("RegistrationYear", col("RegistrationYear").cast(IntegerType()))
df = df.withColumn("Power", col("Power").cast(IntegerType()))
df = df.withColumn("Mileage", col("Mileage").cast(IntegerType()))
df = df.withColumn("RegistrationMonth", col("RegistrationMonth").cast(IntegerType()))
df = df.withColumn("Price", col("Price").cast(DoubleType()))

# Limpieza de columnas irrelevantes
df = df.drop("DateCrawled", "DateCreated", "LastSeen", "NumberOfPictures", "PostalCode")

# Filtros lógicos
df = df.filter(df["Price"] > 100)
df = df.filter((df["RegistrationYear"] >= 1990) & (df["RegistrationYear"] <= 2022))
df = df.filter((df["Power"] > 10) & (df["Power"] < 500))

# Relleno de nulos en columnas categóricas
categorical_cols = ["VehicleType", "Gearbox", "Model", "FuelType", "Brand", "NotRepaired"]
for c in categorical_cols:
    df = df.withColumn(c, when(col(c).isNull(), "unknown").otherwise(col(c)))

# Ensamblado y escalado de columnas numéricas
num_cols = ["RegistrationYear", "Power", "Mileage", "RegistrationMonth"]
assembler = VectorAssembler(inputCols=num_cols, outputCol="num_features")
scaler = StandardScaler(inputCol="num_features", outputCol="num_scaled")

df = assembler.transform(df)
df = scaler.fit(df).transform(df)

# Conversión del vector escalado a array para separar en columnas
df = df.withColumn("num_scaled_array", vector_to_array("num_scaled"))

# Creación de columnas escaladas individuales
for i, col_name in enumerate(num_cols):
    df = df.withColumn(f"{col_name}_scaled", col("num_scaled_array")[i])

# Eliminación de columnas intermedias
df = df.drop("num_features", "num_scaled", "num_scaled_array")

# Selección de columnas finales
final_cols = categorical_cols + ["Price"] + [f"{c}_scaled" for c in num_cols]
df_final = df.select(final_cols)

# Conversión a DynamicFrame
df_final_dynamic = DynamicFrame.fromDF(df_final, glueContext, "df_final_dynamic")

# Escritura en S3 como CSV
glueContext.write_dynamic_frame.from_options(
    frame=df_final_dynamic,
    connection_type="s3",
    connection_options={"path": output_path},
    format="csv"
)
