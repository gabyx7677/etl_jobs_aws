# **AWS Glue ETL Jobs**

Este repositorio contiene una colección de scripts desarrollados para ejecutar procesos ETL utilizando AWS Glue. Los trabajos están enfocados en la limpieza, transformación y preparación de datos almacenados en Amazon S3, con posibilidad de ser integrados con herramientas como Athena, Redshift o visualización externa.

## **Objetivo**

Desarrollar y mantener una base organizada de trabajos ETL en AWS Glue que permitan:

- Procesar datos crudos provenientes de diferentes fuentes.
- Realizar transformaciones estructuradas, escalado de variables y limpieza de valores nulos o inconsistentes.
- Exportar los resultados a ubicaciones específicas en S3, listas para su análisis posterior o carga a otras plataformas.

## **Tecnologías utilizadas**

- AWS Glue (PySpark)
- Amazon S3
- Amazon Athena (consultas sobre datos procesados)
- Python 3
- PySpark (para pruebas locales)

## **Ejecución local**

Algunos de los scripts han sido adaptados para pruebas en entornos locales mediante notebooks en PySpark. Estos permiten validar la lógica del proceso sin necesidad de desplegar directamente en AWS.

Los scripts locales replican los pasos del Glue Job original, incluyendo lectura desde archivos CSV, transformación de columnas numéricas y categóricas, escalado de variables y escritura de resultados.

## **Trabajos desarrollados**

- `glue_etl_car_job`: proceso de limpieza y transformación de un dataset de vehículos, incluye escalado y codificación.
- `glue_etl_sales_job`: preparación de datos de ventas para análisis de ingresos y segmentación.
- `glue_etl_logs_job`: tratamiento de registros de logs para análisis de errores y trazabilidad.

Cada trabajo cuenta con su respectiva versión en PySpark para depuración local.

## **Autor**

Gabriel García Montoya   
Ingeniero Electricista y Científico de Datos  
Repositorio técnico de proyectos ETL en AWS
