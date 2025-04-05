import os
import boto3
from pyspark.sql import SparkSession
from airflow import DAG
from datetime import datetime
from io import BytesIO
import psycopg2
from airflow.operators.python import PythonOperator
import zipfile

MINIO_ENDPOINT = 'http://data-lake:9000'
MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'minio123'
POSTGRES_HOST = 'postgresql-data' 
POSTGRES_PORT = '5432'
POSTGRES_DB = 'amn_datawarehouse'
POSTGRES_USER = 'myuser'
POSTGRES_PASSWORD = 'mypassword'
#BUCKET_NAME = 'rawdata_{}'.format(datetime.now().strftime('%Y%m%d'))  
BUCKET_NAME = 'rawdata'


spark = SparkSession.builder \
    .appName("Data Transformation with Spark") \
    .master("spark://spark-master:7077") \
    .getOrCreate()


def download_and_load_to_spark(**kwargs):
    s3_client = boto3.client('s3',
                             endpoint_url=MINIO_ENDPOINT,
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY,
                             region_name='us-east-1')

    objects = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    files = {obj['Key'] for obj in objects.get('Contents', [])}

    target_files = {
        "2017PurchasePricesDec.zip": "purchasePrices",
        "SalesFINAL12312016.zip": "sales"
    }

    for file, dataset_name in target_files.items():
        if file in files:
            file_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file)
            zip_content = BytesIO(file_obj['Body'].read())

            with zipfile.ZipFile(zip_content, 'r') as zip_ref:
                zip_files = zip_ref.namelist()

                for zip_file in zip_files:
                    with zip_ref.open(zip_file) as extracted_file:
                        df = spark.read.csv(extracted_file, header=True, inferSchema=True)

                        kwargs['ti'].xcom_push(key=dataset_name, value=df)

def load_to_postgres(**kwargs):
    purchase_prices_df = kwargs['ti'].xcom_pull(key='purchasePrices', task_ids='download_and_load_to_spark')
    sales_df = kwargs['ti'].xcom_pull(key='sales', task_ids='download_and_load_to_spark')

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            InventoryId TEXT,
            Store INT,
            Brand INT,
            Description TEXT,
            Size TEXT,
            SalesQuantity INT,
            SalesDollars FLOAT,
            SalesPrice FLOAT,
            SalesDate DATE,
            Volume FLOAT,
            Classification INT,
            ExciseTax FLOAT,
            VendorNo INT,
            VendorName TEXT
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS purchase_prices (
            InventoryId TEXT,
            Store INT,
            Brand INT,
            Description TEXT,
            Size TEXT,
            VendorNumber INT,
            VendorName TEXT,
            PONumber INT,
            PODate DATE,
            ReceivingDate DATE,
            InvoiceDate DATE,
            PayDate DATE,
            PurchasePrice FLOAT,
            Quantity INT,
            Dollars FLOAT,
            Classification INT
        );
    """)

    # Insertar datos en la tabla sales
    if sales_df:
        for row in sales_df.collect():
            cursor.execute("""
                INSERT INTO sales (
                    InventoryId, Store, Brand, Description, Size, SalesQuantity,
                    SalesDollars, SalesPrice, SalesDate, Volume, Classification,
                    ExciseTax, VendorNo, VendorName
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row["InventoryId"], row["Store"], row["Brand"], row["Description"], row["Size"],
                row["SalesQuantity"], row["SalesDollars"], row["SalesPrice"], row["SalesDate"],
                row["Volume"], row["Classification"], row["ExciseTax"], row["VendorNo"], row["VendorName"]
            ))

    # Insertar datos en la tabla purchase_prices
    if purchase_prices_df:
        for row in purchase_prices_df.collect():
            cursor.execute("""
                INSERT INTO purchase_prices (
                    InventoryId, Store, Brand, Description, Size, VendorNumber,
                    VendorName, PONumber, PODate, ReceivingDate, InvoiceDate,
                    PayDate, PurchasePrice, Quantity, Dollars, Classification
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row["InventoryId"], row["Store"], row["Brand"], row["Description"], row["Size"],
                row["VendorNumber"], row["VendorName"], row["PONumber"], row["PODate"], row["ReceivingDate"],
                row["InvoiceDate"], row["PayDate"], row["PurchasePrice"], row["Quantity"], row["Dollars"],
                row["Classification"]
            ))

    conn.commit()
    cursor.close()
    conn.close()


dag = DAG(
    'data_transformation_dag',
    description='DAG para transformar datos con Spark y cargarlos en PostgreSQL',
    schedule_interval='@daily',  
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

download_and_load_task = PythonOperator(
    task_id='download_and_load_to_spark',
    python_callable=download_and_load_to_spark,
    provide_context=True,  
    dag=dag,
)

load_to_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,  
    dag=dag,
)

download_and_load_task >> load_to_postgres_task
