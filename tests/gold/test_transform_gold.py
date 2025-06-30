from pyspark.sql import SparkSession
from scripts.gold.gold_aggregation import transform_gold

spark = SparkSession.builder.master("local").appName("test_gold").getOrCreate()

def test_gold_aggregation_logic():
    # Mock de dados da silver
    mock_data = [
        {"brewery_type": "micro", "country": "UNITED STATES"},
        {"brewery_type": "micro", "country": "UNITED STATES"},
        {"brewery_type": "brewpub", "country": "CANADA"},
        {"brewery_type": "brewpub", "country": "CANADA"},
        {"brewery_type": "brewpub", "country": "CANADA"},
    ]
    
    df_mock = spark.createDataFrame(mock_data)
    
    df_result = transform_gold(df_mock)
    rows = df_result.collect()
    
    result_dict = {(row["brewery_type"], row["country"]): row["brewery_count"] for row in rows}

    assert result_dict[("micro", "UNITED STATES")] == 2
    assert result_dict[("brewpub", "CANADA")] == 3
    assert set(df_result.columns) == {"brewery_type", "country", "brewery_count"}
    assert df_result.count() == 2