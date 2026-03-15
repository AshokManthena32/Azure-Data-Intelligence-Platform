"""
Module: train_orchestrator.py
Description: Orchestrates ML model training and registration using Azure Machine Learning and MLflow.
Author: Azure Cloud Data Engineer
"""

import argparse
import logging
from typing import Dict, Any, Optional
from azureml.core import Workspace, Experiment, Environment, ScriptRunConfig
from azureml.core.authentication import AzureCliAuthentication
import mlflow

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AMLOrchestrator:
    """
    Manages the lifecycle of an Azure Machine Learning experiment,
    from environment configuration to model registration.
    """

    def __init__(self, workspace_name: str, resource_group: str, subscription_id: Optional[str] = None):
        """
        Initializes the AML workspace connection.
        """
        try:
            cli_auth = AzureCliAuthentication()
            self.ws = Workspace.get(
                name=workspace_name,
                resource_group=resource_group,
                subscription_id=subscription_id,
                auth=cli_auth
            )
            logger.info(f"Connected to AML Workspace: {self.ws.name}")
        except Exception as e:
            logger.error(f"Failed to connect to AML Workspace: {str(e)}")
            raise

    def configure_environment(self, env_name: str = "prod-training-env") -> Environment:
        """
        Configures the Python environment for the training run.
        """
        logger.info(f"Configuring environment: {env_name}")
        env = Environment.from_pip_requirements(name=env_name, file_path="requirements.txt")
        env.docker.enabled = True
        return env

    def submit_training_run(self, 
                            experiment_name: str, 
                            compute_target: str, 
                            script_dir: str, 
                            script_name: str, 
                            params: Dict[str, Any]) -> str:
        """
        Submits a training script to a specified compute target.
        """
        logger.info(f"Submitting training run for experiment: {experiment_name}")
        
        experiment = Experiment(workspace=self.ws, name=experiment_name)
        env = self.configure_environment()

        src = ScriptRunConfig(
            source_directory=script_dir,
            script=script_name,
            arguments=[f"--{k}={v}" for k, v in params.items()],
            compute_target=compute_target,
            environment=env
        )

        run = experiment.submit(config=src)
        logger.info(f"Run submitted: {run.id}")
        
        # Wait for completion (optional based on workflow)
        # run.wait_for_completion(show_output=True)
        
        return run.id

    def register_model(self, run_id: str, model_name: str, model_path: str = "outputs/model"):
        """
        Registers a trained model in the AML Model Registry.
        """
        logger.info(f"Registering model '{model_name}' from run '{run_id}'")
        run = self.ws.get_run(run_id)
        model = run.register_model(model_name=model_name, model_path=model_path)
        logger.info(f"Model registered: {model.name}, version {model.version}")
        return model

def main():
    parser = argparse.ArgumentParser(description="AML Training Orchestrator")
    parser.add_argument("--workspace_name", type=str, required=True, help="AML Workspace Name")
    parser.add_argument("--resource_group", type=str, required=True, help="Resource Group Name")
    parser.add_argument("--compute_target", type=str, default="cpu-cluster", help="Compute Cluster Name")
    
    args = parser.parse_args()

    # Define orchestration logic
    orchestrator = AMLOrchestrator(
        workspace_name=args.workspace_name,
        resource_group=args.resource_group
    )

    # Example parameters for training
    training_params = {
        "alpha": 0.5,
        "l1_ratio": 0.2
    }

    # Submit run
    # run_id = orchestrator.submit_training_run(
    #     experiment_name="demand-forecasting",
    #     compute_target=args.compute_target,
    #     script_dir="src/models",
    #     script_name="train.py",
    #     params=training_params
    # )

    logger.info("Orchestration logic completed. (Run submission commented out for safety)")

if __name__ == "__main__":
    main()
