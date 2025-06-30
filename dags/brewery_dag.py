from airflow.decorators import dag, task
from datetime import datetime, timedelta
from scripts.bronze.raw_ingest import ingest_bronze
from scripts.silver.silver_ingestion import silver_layer
from scripts.gold.gold_aggregation import gold_layer
import subprocess
default_args = {
    "owner": "gabriel",
    "email": ["gabriel.almeida99.job@gmail.com"],
    "email_on_failure": True,
    "email_on_success": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="brewery_dag_pipeline",
    description="Ingestão de dados da API para camada Bronze no S3",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze", "api", "s3"],
)
def pipeline():

    @task
    def run_bronze():
        ingest_bronze()

    @task
    def test_bronze():
        result = subprocess.run(["pytest", "tests/bronze/test_raw_ingestion.py"], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            raise Exception("Testes da camada Bronze falharam!")

    @task
    def run_silver():
        silver_layer()

    @task
    def test_silver():
        result = subprocess.run(["pytest", "tests/silver/test_transform_silver.py"], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            raise Exception("Testes da camada Silver falharam!")
    @task
    def run_gold():
        gold_layer()
    
    @task 
    def test_gold():
        result = subprocess.run(["pytest", "tests/gold/test_transform_gold.py"], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            raise Exception("Testes da camada Gold falharam!")

    bronze_task = run_bronze()
    bronze_test= test_bronze()
    silver_task = run_silver()
    silver_test = test_silver()
    gold_task = run_gold()
    gold_test = test_gold()
    

    bronze_test >> bronze_task >> silver_task >> silver_test >> gold_task >> gold_test

pipeline_dag = pipeline()
