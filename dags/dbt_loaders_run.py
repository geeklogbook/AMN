from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_run_dag',
    default_args=default_args,
    description='DAG para ejecutar DBT',
    schedule_interval='@daily', 
    start_date=days_ago(1),
    catchup=False,
)

with TaskGroup("dbt_tasks", dag=dag) as dbt_tasks:
    run_dbt = DockerOperator(
        task_id='run_dbt',
        image='ghcr.io/dbt-labs/dbt-postgres:latest',
        api_version='auto',
        auto_remove=True,
        command='dbt run',
        docker_url='unix://var/run/docker.sock',
        network_mode='airflow-network',
        volumes=['/absolute/path/to/dbt:/usr/app/dbt:rw'],  # Replace with a valid absolute path
        environment={
            'DBT_PROFILES_DIR': '/usr/app/dbt',
        },
        dag=dag,
    )

    run_dbt