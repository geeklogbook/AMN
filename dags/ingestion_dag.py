import io
import requests
import boto3
from botocore.exceptions import NoCredentialsError, EndpointConnectionError
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

urls = [
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/PurchasesFINAL12312016csv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/BegInvFINAL12312016csv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/2017PurchasePricesDeccsv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/VendorInvoices12312016csv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/EndInvFINAL12312016csv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/SalesFINAL12312016csv.zip"
]


def check_minio_connection():
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url='http://data-lake:9000', 
            aws_access_key_id='minio',  
            aws_secret_access_key='minio123',
            region_name='us-east-1',
            config=boto3.session.Config(signature_version='s3v4')
        )

        response = s3_client.list_buckets()
        print("Conexión a MinIO exitosa!")
        print("Buckets:", [bucket['Name'] for bucket in response['Buckets']])
    except NoCredentialsError:
        print("Error: No se proporcionaron credenciales válidas!")
    except EndpointConnectionError:
        print("Error: No se puede conectar al endpoint de MinIO!")
    except Exception as e:
        print(f"Ocurrió un error: {e}")

def create_bucket_if_not_exists(bucket_name):
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url='http://data-lake:9000', 
            aws_access_key_id='minio',  
            aws_secret_access_key='minio123',
            region_name='us-east-1',
            config=boto3.session.Config(signature_version='s3v4')
        )

        buckets = s3_client.list_buckets()['Buckets']
        bucket_names = [bucket['Name'] for bucket in buckets]
        
        if bucket_name not in bucket_names:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket creado: {bucket_name}")
        else:
            print(f"El bucket {bucket_name} ya existe.")
    except Exception as e:
        print(f"Error al crear el bucket: {e}")

def upload_to_minio_in_memory(file_name, file_content, bucket_name):
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url='http://data-lake:9000',
            aws_access_key_id='minio',
            aws_secret_access_key='minio123',
            region_name='us-east-1',
            config=boto3.session.Config(signature_version='s3v4')
        )

        # Subir el archivo directamente desde la memoria (usando un buffer en memoria)
        s3_client.upload_fileobj(file_content, bucket_name, file_name)
        print(f"Archivo subido a MinIO: {file_name}")
    except Exception as e:
        print(f"Error al subir archivo a MinIO: {e}")

def ingest_data(**kwargs):
    date_today = datetime.now().strftime('%Y%m%d')
    bucket_name = f'rawdata-{date_today}'

    create_bucket_if_not_exists(bucket_name)

    for url in urls:
        file_name = url.split("/")[-1]
        
        response = requests.get(url)
        if response.status_code == 200:
            print(f"Archivo descargado en memoria: {file_name}")

            file_content = io.BytesIO(response.content)
            upload_to_minio_in_memory(file_name, file_content, bucket_name)
        else:
            print(f"Error al descargar el archivo. Código de estado: {response.status_code}")


dag = DAG(
    'data_ingestion_minio_dag',
    description='DAG para descargar archivos y subir a MinIO con verificación de conexión',
    schedule_interval='@daily',  
    start_date=datetime(2025, 1, 1),
    catchup=False,
)


check_connection_task = PythonOperator(
    task_id='check_minio_connection',
    python_callable=check_minio_connection,
    dag=dag,
)

ingest_task = PythonOperator(
    task_id='ingest_data_task',
    python_callable=ingest_data,
    dag=dag,
)

create_bucket_task = PythonOperator(
    task_id='create_bucket_task',
    python_callable=create_bucket_if_not_exists,
    op_args=[f'rawdata_{datetime.now().strftime("%Y%m%d")}'],
    dag=dag,
)


check_connection_task >> create_bucket_task >> ingest_task
