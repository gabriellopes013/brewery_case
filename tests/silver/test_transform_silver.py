from pyspark.sql import SparkSession
from scripts.silver.silver_ingestion import transform_silver

spark = SparkSession.builder.master("local").appName("test_silver").getOrCreate()

def test_transform_silver():
    mock_data = [
        {"id": "1", "name": "Brew A", "country": " United States "},
        {"id": "2", "name": "Brew B", "country": "Canada"},
        {"id": None, "name": "Brew C", "country": "Mexico"},
        {"id": "3", "name": None, "country": "Brazil"},
        {"id": "4", "name": "Brew D", "country": None},
    ]

    df_mock = spark.createDataFrame(mock_data)
    df_result = transform_silver(df_mock)

    result = df_result.select("id", "name", "country").collect()

    assert len(result) == 2
    assert all(row["country"] in ["UNITED STATES", "CANADA"] for row in result)