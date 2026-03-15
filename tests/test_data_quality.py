"""
Module: test_data_quality.py
Description: Unit tests for validating data quality and schema compliance in the Silver layer.
Author: Azure Cloud Data Engineer
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType
from pipelines.bronze_to_silver import BronzeToSilverPipeline

@pytest.fixture(scope="session")
def spark():
    """Session-wide Spark Session for testing."""
    return SparkSession.builder \
        .appName("DataQuality-Tests") \
        .master("local[*]") \
        .getOrCreate()

@pytest.fixture
def target_schema():
    """Standard target schema for tests."""
    return StructType() \
        .add("id", StringType(), False) \
        .add("name", StringType(), True) \
        .add("value", IntegerType(), True) \
        .add("updated_at", TimestampType(), True)

def test_schema_enforcement(spark, target_schema):
    """
    Test that the schema enforcement logic correctly casts and filters columns.
    """
    pipeline = BronzeToSilverPipeline(spark)
    
    # Create sample data with extra and mis-typed columns
    data = [("1", "EventA", "100", "2023-01-01 12:00:00", "ExtraColumn")]
    raw_df = spark.createDataFrame(data, ["id", "name", "value", "updated_at", "extra"])
    
    # Apply schema enforcement
    enforced_df = pipeline._apply_schema_enforcement(raw_df, target_schema)
    
    # Assertions
    assert len(enforced_df.columns) == 4
    assert "extra" not in enforced_df.columns
    assert isinstance(enforced_df.schema["value"].dataType, IntegerType)

def test_data_cleaning_deduplication(spark):
    """
    Test that duplicate records are correctly removed based on ID and timestamp.
    """
    pipeline = BronzeToSilverPipeline(spark)
    
    # Duplicate 'id' with different 'updated_at'
    data = [
        ("1", "A", 10, "2023-01-01 10:00:00"),
        ("1", "A", 20, "2023-01-01 10:00:00"), # Duplicate
        ("2", "B", 30, "2023-01-01 11:00:00")
    ]
    df = spark.createDataFrame(data, ["id", "name", "value", "updated_at"])
    
    cleaned_df = pipeline._clean_data(df)
    
    # Expecting 2 unique rows after deduplication
    assert cleaned_df.count() == 2

def test_processing_timestamp_addition(spark):
    """
    Verify that the metadata column '_processing_timestamp' is added.
    """
    pipeline = BronzeToSilverPipeline(spark)
    data = [("1", "A", 10, "2023-01-01 10:00:00")]
    df = spark.createDataFrame(data, ["id", "name", "value", "updated_at"])
    
    cleaned_df = pipeline._clean_data(df)
    
    assert "_processing_timestamp" in cleaned_df.columns
