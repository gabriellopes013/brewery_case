from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import count
from dotenv import load_dotenv
import os

def transform_gold(df_silver: DataFrame) -> DataFrame:
    return (
        df_silver
        .groupBy("brewery_type", "country")
        .agg(count("*").alias("brewery_count"))
        .orderBy("country", "brewery_type")
    )

def gold_layer():
    load_dotenv()

    bucket = os.getenv("AWS_S3_BUCKET")
    silver_path = f"s3a://{bucket}/silver/"
    gold_path = f"s3a://{bucket}/gold/"

    spark = SparkSession.builder \
        .appName("brewery_bronze_ingest") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .master("local[*]") \
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

    df_silver = spark.read.parquet(silver_path)
    df_gold = transform_gold(df_silver)
    df_gold.write.mode("overwrite").parquet(gold_path)
    print(f"Camada Gold salva em {gold_path}")

if __name__ == "__main__":
    gold_layer()