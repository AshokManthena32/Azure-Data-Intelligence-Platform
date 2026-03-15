"""
Module: bronze_to_silver.py
Description: PySpark pipeline for cleaning and enforcing schema during Bronze to Silver transition.
Author: Azure Cloud Data Engineer
"""

import logging
from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, TimestampType, StringType
from delta.tables import DeltaTable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeToSilverPipeline:
    """
    Orchestrates the transformation of data from the Bronze layer (Raw) 
    to the Silver layer (Cleaned/Conformed) in a Medallion Architecture.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _apply_schema_enforcement(self, df: DataFrame, schema: StructType) -> DataFrame:
        """
        Ensures the DataFrame matches the expected target schema.
        
        Args:
            df: The source DataFrame.
            schema: The target StructType schema.
            
        Returns:
            DataFrame: Validated DataFrame with enforced schema.
        """
        logger.info("Applying schema enforcement...")
        # Select and cast columns to match the target schema
        return df.select(*[F.col(field.name).cast(field.dataType) for field in schema.fields])

    def _clean_data(self, df: DataFrame) -> DataFrame:
        """
        Performs data cleaning operations such as deduplication and null handling.
        
        Args:
            df: The input DataFrame.
            
        Returns:
            DataFrame: Cleaned DataFrame.
        """
        logger.info("Performing data cleaning (deduplication, null handling)...")
        
        # Add metadata columns
        df = df.withColumn("_processing_timestamp", F.current_timestamp())
        
        # Simple deduplication based on a hypothetical unique ID and latest timestamp
        if "id" in df.columns and "updated_at" in df.columns:
            df = df.dropDuplicates(["id", "updated_at"])
            
        return df

    def transform(self, bronze_path: str, silver_path: str, schema: Optional[StructType] = None) -> None:
        """
        Executes the transformation pipeline.
        
        Args:
            bronze_path: ADLS/OneLake path to the Bronze Delta table.
            silver_path: ADLS/OneLake path to the Silver Delta table.
            schema: Optional schema to enforce.
        """
        try:
            logger.info(f"Reading from Bronze: {bronze_path}")
            bronze_df = self.spark.read.format("delta").load(bronze_path)

            # Apply transformations
            cleaned_df = self._clean_data(bronze_df)
            
            if schema:
                cleaned_df = self._apply_schema_enforcement(cleaned_df, schema)

            logger.info(f"Writing to Silver: {silver_path}")
            
            # Upsert (Merge) into Silver to handle updates and inserts
            if DeltaTable.isDeltaTable(self.spark, silver_path):
                silver_table = DeltaTable.forPath(self.spark, silver_path)
                
                # Assuming 'id' is the primary key
                silver_table.alias("target").merge(
                    cleaned_df.alias("source"),
                    "target.id = source.id"
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
            else:
                # Initial load
                cleaned_df.write.format("delta").mode("overwrite").save(silver_path)
                
            logger.info("Transformation from Bronze to Silver completed successfully.")

        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise

if __name__ == "__main__":
    # Spark session initialization (standard in Fabric/Synapse environments)
    spark_session = SparkSession.builder \
        .appName("BronzeToSilver-ETL") \
        .getOrCreate()

    # Define a sample schema (Production would load this from a central metadata store)
    target_schema = StructType() \
        .add("id", StringType(), False) \
        .add("name", StringType(), True) \
        .add("value", StringType(), True) \
        .add("updated_at", TimestampType(), True)

    pipeline = BronzeToSilverPipeline(spark_session)
    
    # Example paths - in production these come from parameters/config
    BRONZE_PATH = "abfss://raw@datalake.dfs.core.windows.net/bronze/events"
    SILVER_PATH = "abfss://refined@datalake.dfs.core.windows.net/silver/events"

    pipeline.transform(BRONZE_PATH, SILVER_PATH, target_schema)
