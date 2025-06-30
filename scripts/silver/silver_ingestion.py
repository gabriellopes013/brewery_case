from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import upper, col, current_timestamp, trim
import os
from dotenv import load_dotenv

def transform_silver(df_bronze: DataFrame) -> DataFrame:
    return (
        df_bronze
        .withColumn("country", upper(trim(col("country"))))
        .withColumn("processed_ts", current_timestamp())
        .na.drop(subset=["id", "name", "country"])              
    )

def silver_layer():
    load_dotenv()

    bucket = os.getenv("AWS_S3_BUCKET")
    silver_path = f"s3a://{bucket}/silver/"
    bronze_path = f"s3a://{bucket}/bronze/"

    spark = SparkSession.builder \
        .appName("brewery_bronze_ingest") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.speculation", "false") \
        .config("spark.sql.parquet.output.committer.class", "org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter") \
        .config("spark.hadoop.fs.s3a.committer.name", "directory") \
        .config("spark.hadoop.fs.s3a.committer.magic.enabled", "false") \
        .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/s3a") \
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
    os.makedirs("/tmp/s3a", exist_ok=True)
    
    df_bronze = spark.read.parquet(bronze_path)
    df_silver = transform_silver(df_bronze)

    df_silver.write.mode("overwrite").partitionBy("country").parquet(silver_path)
    print(f"Camada Silver salva em {silver_path}")

if __name__ == "__main__":
    silver_layer()