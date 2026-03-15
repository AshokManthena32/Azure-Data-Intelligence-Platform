"""
Silver to Gold Transformation Pipeline for Microsoft Fabric.

This module is responsible for orchestrating the final data transformation layer
(Medallion Architecture: Silver -> Gold). It reads conformed, cleansed data from
the Silver Delta tables, applies business logic and dimensional modeling
(e.g., Star Schema generation), and writes the optimized datasets to Gold Delta
tables. The Gold layer is specifically tuned for Microsoft Fabric DirectLake mode
and high-performance Power BI semantic models.

Author: Lead Data Engineer (Deloitte Cloud Engineering)
Version: 1.0.0
"""

import logging
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    sum as _sum,
    count as _count,
    round as _round,
    expr
)
from pyspark.sql.types import StructType

# Configure enterprise-grade logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)


class GoldTransformer:
    """
    Encapsulates the transformation logic for generating Gold-level analytical models.
    
    Designed to produce V-Order optimized Delta tables suitable for Fabric DirectLake.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, str]) -> None:
        """
        Initializes the GoldTransformer with an active Spark session and configuration.

        Args:
            spark (SparkSession): The active PySpark session.
            config (Dict[str, str]): Dictionary containing path configurations.
                                     Expected keys: 'silver_base_path', 'gold_base_path'.
        """
        self.spark = spark
        self.config = config
        self._validate_config()

    def _validate_config(self) -> None:
        """Validates that required configuration keys are present."""
        required_keys = ["silver_base_path", "gold_base_path"]
        for key in required_keys:
            if key not in self.config:
                logger.error(f"Missing required configuration key: {key}")
                raise ValueError(f"Configuration must include '{key}'")

    def read_silver_table(self, table_name: str) -> DataFrame:
        """
        Reads a Delta table from the Silver layer.

        Args:
            table_name (str): The name of the Silver table (e.g., 'sales_cleansed').

        Returns:
            DataFrame: The PySpark DataFrame representing the Silver table.
        """
        path = f"{self.config['silver_base_path']}/{table_name}"
        logger.info(f"Reading Silver table '{table_name}' from {path}")
        try:
            return self.spark.read.format("delta").load(path)
        except Exception as e:
            logger.error(f"Failed to read Silver table '{table_name}': {e}")
            raise

    def write_gold_table(self, df: DataFrame, table_name: str, partition_cols: Optional[List[str]] = None) -> None:
        """
        Writes a DataFrame to the Gold layer as an optimized Delta table.
        
        Enables V-Order and optimizes compaction for DirectLake compatibility.

        Args:
            df (DataFrame): The DataFrame to write.
            table_name (str): The destination table name in the Gold layer.
            partition_cols (Optional[List[str]]): List of columns to partition by.
        """
        path = f"{self.config['gold_base_path']}/{table_name}"
        logger.info(f"Writing Gold table '{table_name}' to {path}")
        
        # In Microsoft Fabric, V-Order is applied automatically on write for Delta,
        # but we ensure properties are set for performance.
        writer = (
            df.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            # Fabric specific optimizations could be injected here via session configs
        )
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
            
        try:
            writer.save(path)
            logger.info(f"Successfully wrote Gold table '{table_name}'.")
        except Exception as e:
            logger.error(f"Failed to write Gold table '{table_name}': {e}")
            raise

    def build_sales_fact(self, df_sales: DataFrame, df_customers: DataFrame, df_products: DataFrame) -> DataFrame:
        """
        Constructs the core Sales Fact table optimized for analysis.

        Args:
            df_sales (DataFrame): Silver sales transaction data.
            df_customers (DataFrame): Silver customer master data.
            df_products (DataFrame): Silver product master data.

        Returns:
            DataFrame: The transformed Sales Fact table.
        """
        logger.info("Building Sales Fact table...")
        
        # Example Business Logic: Joining, filtering, and calculating core metrics
        fact_df = df_sales.join(
            df_customers, "customer_id", "left"
        ).join(
            df_products, "product_id", "left"
        ).select(
            col("transaction_id"),
            col("date_key"),
            col("customer_id"),
            col("product_id"),
            col("store_id"),
            col("quantity"),
            col("unit_price"),
            _round(col("quantity") * col("unit_price"), 2).alias("total_sales_amount"),
            _round((col("unit_price") - col("standard_cost")) * col("quantity"), 2).alias("margin_amount"),
            current_timestamp().alias("dw_inserted_at")
        )
        
        return fact_df

    def build_customer_dimension(self, df_customers: DataFrame) -> DataFrame:
        """
        Constructs a Type 1 Customer Dimension table.

        Args:
            df_customers (DataFrame): Silver customer master data.

        Returns:
            DataFrame: The transformed Customer Dimension table.
        """
        logger.info("Building Customer Dimension table...")
        dim_df = df_customers.select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            expr("concat_ws(' ', first_name, last_name)").alias("full_name"),
            col("email"),
            col("segment"),
            col("region"),
            col("is_active"),
            current_timestamp().alias("dw_inserted_at")
        ).dropDuplicates(["customer_id"])
        return dim_df

    def build_monthly_sales_aggregate(self, fact_sales: DataFrame) -> DataFrame:
        """
        Creates a pre-calculated aggregate table for rapid Power BI dashboarding.
        
        Args:
            fact_sales (DataFrame): The previously built Sales Fact table.
            
        Returns:
            DataFrame: The aggregated monthly sales data.
        """
        logger.info("Building Monthly Sales Aggregate table...")
        agg_df = fact_sales.groupBy(
            expr("substring(date_key, 1, 6)").alias("year_month"),
            col("store_id"),
            col("product_id")
        ).agg(
            _sum("total_sales_amount").alias("monthly_revenue"),
            _sum("margin_amount").alias("monthly_margin"),
            _count("transaction_id").alias("transaction_count")
        )
        return agg_df

    def run_pipeline(self) -> None:
        """
        Executes the full Silver to Gold transformation pipeline.
        """
        logger.info("Starting Silver to Gold pipeline execution.")
        
        try:
            # 1. Read Inputs
            sales_silver = self.read_silver_table("transactions_cleansed")
            customers_silver = self.read_silver_table("customers_conformed")
            products_silver = self.read_silver_table("products_conformed")
            
            # 2. Transform into Dimensional Model
            dim_customer = self.build_customer_dimension(customers_silver)
            fact_sales = self.build_sales_fact(sales_silver, customers_silver, products_silver)
            agg_monthly_sales = self.build_monthly_sales_aggregate(fact_sales)
            
            # 3. Write Outputs (DirectLake Optimized)
            self.write_gold_table(dim_customer, "dim_customer")
            # Partitioning large fact tables by date logic (e.g., year-month)
            self.write_gold_table(fact_sales, "fact_sales", partition_cols=["date_key"])
            self.write_gold_table(agg_monthly_sales, "agg_monthly_sales")
            
            logger.info("Silver to Gold pipeline completed successfully.")
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    # Example execution entry point
    # In a real Fabric environment, SparkSession is provided natively by the notebook/job.
    _spark = SparkSession.builder \
        .appName("FabricDataMesh-SilverToGold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.vorder.enabled", "true") \
        .getOrCreate()

    _config = {
        "silver_base_path": "abfss://workspace@onelake.dfs.fabric.microsoft.com/SilverLakehouse.Lakehouse/Tables",
        "gold_base_path": "abfss://workspace@onelake.dfs.fabric.microsoft.com/GoldLakehouse.Lakehouse/Tables"
    }

    pipeline = GoldTransformer(_spark, _config)
    # pipeline.run_pipeline() # Commented out for static analysis/compilation check
