import os
import boto3
from pyspark.sql import SparkSession
from airflow import DAG
from datetime import datetime
from io import BytesIO
import psycopg2
from airflow.operators.python import PythonOperator
import zipfile

# Parámetros de conexión
MINIO_ENDPOINT = 'http://data-lake:9000'
MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'minio123'
POSTGRES_HOST = 'postgres_host'  # Reemplaza con la dirección de tu host PostgreSQL
POSTGRES_PORT = '5432'
POSTGRES_DB = 'your_db'
POSTGRES_USER = 'your_user'
POSTGRES_PASSWORD = 'your_password'
BUCKET_NAME = 'rawdata_{}'.format(datetime.now().strftime('%Y%m%d'))  # Nombre del bucket con la fecha de hoy

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("Data Transformation with Spark") \
    .config("spark.jars", "/opt/spark/jars/*") \
    .getOrCreate()


def download_and_load_to_spark(**kwargs):
    s3_client = boto3.client('s3',
                             endpoint_url=MINIO_ENDPOINT,
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY,
                             region_name='us-east-1')

    objects = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    files = [obj['Key'] for obj in objects.get('Contents', [])]

    for file in files:
        file_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file)
        zip_content = BytesIO(file_obj['Body'].read())
        

        with zipfile.ZipFile(zip_content, 'r') as zip_ref:
            zip_files = zip_ref.namelist()
            
            for zip_file in zip_files:
                with zip_ref.open(zip_file) as extracted_file:
                    df = spark.read.csv(extracted_file, header=True, inferSchema=True)
                    
                    transformed_df = df.select("column1", "column2")
                    
                    kwargs['ti'].xcom_push(key='transformed_df', value=transformed_df)

# Función para cargar los datos transformados en PostgreSQL
def load_to_postgres(**kwargs):
    # Obtener el DataFrame transformado desde XCom
    transformed_df = kwargs['ti'].xcom_pull(key='transformed_df', task_ids='download_and_load_to_spark')

    # Conectar a PostgreSQL
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS your_table_name (
            column1 TEXT,
            column2 TEXT
        );
    """)

    # Insertar los datos en PostgreSQL
    for row in transformed_df.collect():
        # Asegúrate de que el tipo de datos sea compatible con PostgreSQL
        cursor.execute("INSERT INTO your_table_name (column1, column2) VALUES (%s, %s)", (row['column1'], row['column2']))

    conn.commit()
    cursor.close()

# Definir el DAG
dag = DAG(
    'data_transformation_dag',
    description='DAG para transformar datos con Spark y cargarlos en PostgreSQL',
    schedule_interval='@daily',  # Ejecutar diariamente
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Definir las tareas del DAG
download_and_load_task = PythonOperator(
    task_id='download_and_load_to_spark',
    python_callable=download_and_load_to_spark,
    provide_context=True,  # Asegúrate de que puedas usar XCom
    dag=dag,
)

load_to_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,  # Para acceder a XCom
    dag=dag,
)

# Orden de ejecución de las tareas
download_and_load_task >> load_to_postgres_task
