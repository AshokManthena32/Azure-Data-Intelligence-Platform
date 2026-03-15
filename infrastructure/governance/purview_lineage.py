"""
Microsoft Purview Custom Lineage Integration via Apache Atlas Client.

This module provides a programmatic interface to Microsoft Purview, enabling the
registration of custom data assets, processes, and lineage maps. This is essential
for capturing Data Mesh operational lineage where out-of-the-box scanners might
not reach (e.g., highly custom Python transformations or external API orchestrations).

Author: Lead Data Governance Engineer
Version: 1.0.0
"""

import logging
from typing import Dict, Any, List
import os

from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess
from pyapacheatlas.core.util import GuidTracker
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class PurviewLineagePublisher:
    """
    Manages authentication and entity creation within Microsoft Purview
    using the pyapacheatlas SDK.
    """

    def __init__(self, purview_account_name: str) -> None:
        """
        Initializes the Purview client. Requires Service Principal credentials
        in the environment (AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET)
        or uses Managed Identity if running in Azure.

        Args:
            purview_account_name (str): The name of the Azure Purview account.
        """
        self.purview_account_name = purview_account_name
        self.client = self._authenticate_client()
        self.guid_tracker = GuidTracker()

    def _authenticate_client(self) -> PurviewClient:
        """
        Authenticates to Azure and initializes the Purview Atlas client.
        """
        logger.info(f"Authenticating to Purview account: {self.purview_account_name}")
        
        # In a production environment, prefer Managed Identity (DefaultAzureCredential)
        # For this demonstration, we support Service Principal via env vars.
        tenant_id = os.environ.get("AZURE_TENANT_ID")
        client_id = os.environ.get("AZURE_CLIENT_ID")
        client_secret = os.environ.get("AZURE_CLIENT_SECRET")

        if tenant_id and client_id and client_secret:
            logger.info("Using Service Principal Authentication.")
            auth = ServicePrincipalAuthentication(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
        else:
            logger.error("Missing Service Principal credentials in environment.")
            raise EnvironmentError("AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET are required.")

        return PurviewClient(
            account_name=self.purview_account_name,
            authentication=auth
        )

    def create_custom_lineage(self, 
                              source_qualified_name: str, 
                              target_qualified_name: str, 
                              process_name: str,
                              process_qualified_name: str) -> Dict[str, Any]:
        """
        Creates a custom process entity to link a source dataset to a target dataset,
        establishing data lineage in Purview.

        Args:
            source_qualified_name (str): The unique identifier of the source asset.
            target_qualified_name (str): The unique identifier of the target asset.
            process_name (str): Display name for the transformation process.
            process_qualified_name (str): Unique identifier for the process.

        Returns:
            Dict[str, Any]: The response from the Purview Atlas API.
        """
        logger.info(f"Generating lineage map for process: {process_name}")

        # 1. Define Source Asset (assuming it already exists, we reference it by qualifiedName)
        # We use a generic 'DataSet' type here, but it should match the actual type (e.g., 'azure_datalake_gen2_path')
        source_entity = AtlasEntity(
            name="Source Asset Placeholder",
            typeName="DataSet",
            qualified_name=source_qualified_name,
            guid=self.guid_tracker.get_guid()
        )

        # 2. Define Target Asset
        target_entity = AtlasEntity(
            name="Target Asset Placeholder",
            typeName="DataSet",
            qualified_name=target_qualified_name,
            guid=self.guid_tracker.get_guid()
        )

        # 3. Define the Process connecting them
        process_entity = AtlasProcess(
            name=process_name,
            typeName="Process",
            qualified_name=process_qualified_name,
            inputs=[source_entity.to_json(minimum=True)],
            outputs=[target_entity.to_json(minimum=True)],
            guid=self.guid_tracker.get_guid(),
            attributes={"description": "Custom PySpark Silver to Gold Transformation logic."}
        )

        # 4. Upload to Purview
        logger.info(f"Uploading entities to Purview API...")
        try:
            # Note: In a real scenario, if source/target already exist, uploading them as generic 'DataSet' 
            # might overwrite or fail. Usually, you only upload the Process and reference existing assets.
            # Here, we upload the process and rely on pyapacheatlas to link them via minimum JSON (qualifiedName only).
            response = self.client.upload_entities([process_entity])
            logger.info("Lineage successfully published to Purview.")
            return response
        except Exception as e:
            logger.error(f"Failed to publish lineage to Purview: {e}")
            raise


if __name__ == "__main__":
    # Example Usage (Ensure environment variables are set before running)
    # purv_client = PurviewLineagePublisher(purview_account_name="pview-enterprise-prod")
    #
    # response = purv_client.create_custom_lineage(
    #     source_qualified_name="abfss://workspace@onelake/Silver/transactions",
    #     target_qualified_name="abfss://workspace@onelake/Gold/fact_sales",
    #     process_name="SilverToGold_SalesTransformation",
    #     process_qualified_name="fabric:notebook:SilverToGold_SalesTransformation:v1"
    # )
    # print(response)
    pass
