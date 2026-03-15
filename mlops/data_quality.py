"""
Data Quality and Data Contract Framework using Great Expectations.

This module implements robust data validation logic to enforce Data Contracts
between the Medallion Architecture layers (Bronze -> Silver, Silver -> Gold).
It prevents corrupted or non-compliant data from cascading through the data mesh
by halting pipelines or logging severe warnings when expectations fail.

Author: Lead Data Engineer (Deloitte Cloud Engineering)
Version: 1.0.0
"""

import logging
from typing import Dict, Any, List, Optional
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, InMemoryStoreBackendDefaults
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class DataContractEnforcer:
    """
    Enforces Data Contracts on PySpark DataFrames using Great Expectations.
    
    This class sets up an ephemeral, in-memory GX context suitable for running
    within Spark notebooks or pipelines in Microsoft Fabric/Synapse.
    """

    def __init__(self, data_asset_name: str) -> None:
        """
        Initializes the Data Contract Enforcer.

        Args:
            data_asset_name (str): The logical name of the data asset being validated
                                   (e.g., 'silver_customer_data').
        """
        self.data_asset_name = data_asset_name
        self.context = self._initialize_gx_context()
        self.datasource_name = "spark_datasource"
        self._setup_datasource()

    def _initialize_gx_context(self) -> BaseDataContext:
        """
        Initializes an in-memory Great Expectations Data Context.
        
        Returns:
            BaseDataContext: The GX context.
        """
        logger.info("Initializing in-memory Great Expectations context...")
        project_config = DataContextConfig(
            store_backend_defaults=InMemoryStoreBackendDefaults()
        )
        return BaseDataContext(project_config=project_config)

    def _setup_datasource(self) -> None:
        """Configures a Spark datasource within the GX context."""
        datasource_config = {
            "name": self.datasource_name,
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SparkDFExecutionEngine"
            },
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["batch_id"],
                }
            },
        }
        self.context.add_datasource(**datasource_config)

    def define_contract(self, suite_name: str, expectations: List[ExpectationConfiguration]) -> None:
        """
        Defines a new Expectation Suite (Data Contract).

        Args:
            suite_name (str): The name of the expectation suite.
            expectations (List[ExpectationConfiguration]): A list of expectation configurations.
        """
        logger.info(f"Defining Data Contract suite: '{suite_name}' with {len(expectations)} rules.")
        suite = self.context.create_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=True
        )
        for exp in expectations:
            suite.add_expectation(expectation_configuration=exp)
        self.context.save_expectation_suite(expectation_suite=suite)

    def validate_dataframe(self, df: DataFrame, suite_name: str, fail_on_error: bool = True) -> Dict[str, Any]:
        """
        Validates a PySpark DataFrame against a predefined Data Contract.

        Args:
            df (DataFrame): The Spark DataFrame to validate.
            suite_name (str): The name of the expectation suite to use.
            fail_on_error (bool): If True, raises an Exception if validation fails.

        Returns:
            Dict[str, Any]: The validation results dictionary.
            
        Raises:
            ValueError: If validation fails and fail_on_error is True.
        """
        logger.info(f"Running validation suite '{suite_name}' for asset '{self.data_asset_name}'...")
        
        batch_request = RuntimeBatchRequest(
            datasource_name=self.datasource_name,
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name=self.data_asset_name,
            runtime_parameters={"batch_data": df},
            batch_identifiers={"batch_id": "default_identifier"}
        )

        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )

        results = validator.validate()
        
        if not results.success:
            logger.error(f"Data Contract violation detected for '{self.data_asset_name}'.")
            # In a real enterprise setup, you would push these results to an observability tool.
            logger.error(f"Validation statistics: {results.statistics}")
            if fail_on_error:
                raise ValueError(f"Data Quality validation failed for '{self.data_asset_name}'. Pipeline halted.")
        else:
            logger.info(f"Data Contract validated successfully for '{self.data_asset_name}'.")

        return results.to_json_dict()


# --- Example Contract Definitions ---

def get_silver_customer_contract() -> List[ExpectationConfiguration]:
    """
    Returns the Data Contract definition for Silver layer Customer data.
    Ensures primary keys are unique and non-null, and specific formats are met.
    """
    return [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "customer_id"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "customer_id"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_match_regex",
            kwargs={
                "column": "email",
                "regex": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
                "mostly": 0.99  # Allow 1% missing/malformed for downstream analysis if needed
            }
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "region",
                "value_set": ["NA", "EMEA", "APAC", "LATAM"]
            }
        )
    ]

# Usage Example (Commented out for static analysis):
# if __name__ == "__main__":
#     spark = SparkSession.builder.getOrCreate()
#     df = spark.read.parquet("...")
#     
#     enforcer = DataContractEnforcer("silver_customers")
#     enforcer.define_contract("silver_customer_suite", get_silver_customer_contract())
#     
#     # Will raise exception if data is bad, preventing bad data from entering Silver
#     validation_results = enforcer.validate_dataframe(df, "silver_customer_suite", fail_on_error=True)
