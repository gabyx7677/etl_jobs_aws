import boto3

s3 = boto3.client('s3')

def rename_s3_file(bucket, src_prefix, dst_key):
    """
    Copia el archivo .csv único de una carpeta generada por Spark a un nombre específico,
    y luego elimina los archivos temporales y la carpeta original.
    """
    # Listar archivos en el folder temporal
    response = s3.list_objects_v2(Bucket=bucket, Prefix=src_prefix)
    
    if 'Contents' not in response:
        print(f"No se encontraron archivos en: {src_prefix}")
        return False

    csv_found = False
    for obj in response['Contents']:
        key = obj['Key']
        if key.endswith('.csv'):
            # Copiar a destino
            s3.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=dst_key
            )
            print(f"Archivo copiado de {key} a {dst_key}")
            csv_found = True

    if not csv_found:
        print(f"No se encontró archivo .csv en {src_prefix}")
        return False

    # Borrar todos los archivos de la carpeta temporal
    for obj in response['Contents']:
        s3.delete_object(Bucket=bucket, Key=obj['Key'])
    
    print(f"Archivos temporales eliminados de {src_prefix}")
    return True

