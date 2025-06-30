import requests
import json
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def ingest_bronze():
    load_dotenv()
    bucket = os.getenv("AWS_S3_BUCKET")
    bronze_path = f"s3a://{bucket}/bronze/"

    spark = SparkSession.builder \
        .appName("brewery_bronze_ingest") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .getOrCreate()
    
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    hadoop_conf.set("fs.s3a.threads.keepalivetime", "60000") 
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "30000")
    hadoop_conf.set("fs.s3a.connection.request.timeout", "200000")
    hadoop_conf.set("fs.s3a.connection.timeout", "60000")
    hadoop_conf.set("fs.s3a.multipart.purge.age", "8640000")

    base_url = "https://api.openbrewerydb.org/v1/breweries"
    all_data = []
    page = 1
    per_page = 50

    while True:
        url = f"{base_url}?page={page}&per_page={per_page}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Erro na API, status code {response.status_code}")
            break

        data = response.json()
        if not data:
            break

        all_data.extend(data)
        print(f"Página {page} lida com {len(data)} registros")
        page += 1

    print(f"Total de registros coletados: {len(all_data)}")

    raw_json = [json.dumps(item) for item in all_data]
    df = spark.read.json(spark.sparkContext.parallelize(raw_json))

    df = df.withColumn("ingestion_ts", current_timestamp())

    df.write.mode("overwrite").parquet(bronze_path)

    print(f"Dados gravados no S3 em {bronze_path}")

if __name__ == "__main__":
    ingest_bronze()
