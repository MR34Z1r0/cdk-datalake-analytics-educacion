import os
import csv
import aws_cdk as cdk
from stacks.cdk_datalake_analytics_educacion_base_stack import CdkDatalakeAnalyticsEducacionBaseStack
from stacks.cdk_datalake_analytics_educacion_group_stack import CdkDatalakeAnalyticsEducacionGroupStack
from aje_cdk_libs.constants.environments import Environments
from aje_cdk_libs.constants.project_config import ProjectConfig
from constants.paths import Paths
from dotenv import load_dotenv

load_dotenv()

app = cdk.App()

# Cargar configuración del contexto CDK
CONFIG = app.node.try_get_context("project_config")
CONFIG["account_id"] = os.getenv("ACCOUNT_ID", None)
CONFIG["region_name"] = os.getenv("REGION_NAME", None)
CONFIG["environment"] = os.getenv("ENVIRONMENT", None)
CONFIG["separator"] = os.getenv("SEPARATOR", "-")

project_config = ProjectConfig.from_dict(CONFIG)
project_paths = Paths(project_config.app_config)

def read_csv_and_create_process_config(instance, base_csv_path):
    """Read instance-specific file CSV and create process configuration"""
    try:
        # Try to read instance-specific file first
        instance_csv_path = f"{base_csv_path}/{project_config.app_config.get('business_process').lower()}_{instance.lower()}.csv"
        
        # Check if instance-specific file exists
        if not os.path.exists(instance_csv_path):
            raise FileNotFoundError(f"Instance-specific configuration file not found: {instance_csv_path}")
        
        print(f"Reading instance-specific configuration: {instance_csv_path}")
        
        layers_and_process_ids = set()
        with open(instance_csv_path, newline="", encoding="utf-8") as base_file:
            base_reader = csv.DictReader(base_file, delimiter=";")
            for row in base_reader:
                if row["process_id"] and row["layer"]:
                    layer = row["layer"]
                    process_id = row["process_id"]

                    # Validation: Only consider tables in the layer
                    if layer.lower() == project_config.app_config.get('business_process').lower():
                        layers_and_process_ids.add((layer, process_id))
                    else:
                        print(f"Skipping layer: '{layer}' for process_id '{process_id}'")
        
        print(f"Found layers and process_ids for {instance}: {layers_and_process_ids}")
        return layers_and_process_ids, instance_csv_path
        
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        print(f"Available instances should have their specific {project_config.app_config.get('business_process').lower()}{{instance}}.csv files:")
        print(f"  - {project_config.app_config.get('business_process').lower()}_ec.csv")
        print(f"  - {project_config.app_config.get('business_process').lower()}_pe.csv")
        raise e
    except Exception as e:
        print(f"Error reading file CSV configuration: {e}")
        raise e

# Create base stack first
base_stack = CdkDatalakeAnalyticsEducacionBaseStack(
    app,
    "CdkDatalakeAnalyticsEducacionBaseStack",
    project_config,
    env=cdk.Environment(account=project_config.account_id, region=project_config.region_name),
)

# Collect base stack outputs
base_stack_outputs = {
    "S3ArtifactsBucketName": base_stack.s3_artifacts_bucket.bucket_name,
    "S3ExternalBucketName": base_stack.s3_external_bucket.bucket_name,
    "S3StageBucketName": base_stack.s3_stage_bucket.bucket_name,
    "S3AnalyticsBucketName": base_stack.s3_analytics_bucket.bucket_name,
    "DynamoColumnsTableName": base_stack.dynamodb_columns_table.table_name,
    "DynamoConfigurationTableName": base_stack.dynamodb_configuration_table.table_name,
    "DynamoCredentialsTableName": base_stack.dynamodb_credentials_table.table_name,
    "DynamoLogsTableName": base_stack.dynamodb_logs_table.table_name,
    "SnsFailedTopicArn": base_stack.sns_failed_topic.topic_arn,
    "SnsSuccessTopicArn": base_stack.sns_success_topic.topic_arn,
    "GlueJobRoleArn": base_stack.glue_job_role.role_arn,
    "StepFunctionRoleArn": base_stack.step_function_role.role_arn,
    "LambdaRoleArn": base_stack.lambda_role.role_arn,
    "LambdaGetDataArn": base_stack.lambda_get_data.function_arn,
    "AnalyticsCoreBaseStepFunctionArn": base_stack.analytics_core_base_step_function.state_machine_arn,
    "AnalyticsCoreEducacionCrawlerJobName": base_stack.analytics_core_domain_crawler_job_name,
    "InvokeNextLayerLambdaFunctionArn": base_stack.invoke_next_layer_lambda_function.function_arn,
    "InvokeNextLayerLambdaFunctionName": base_stack.invoke_next_layer_lambda_function.function_name,
}

print("Base stack outputs:")
for key, value in base_stack_outputs.items():
    print(f"  {key}: {value}")
    
# Get active instances
active_instances = base_stack.get_active_instances()
print(f"Active instances: {active_instances}")

if not active_instances:
    raise Exception("No active instances found. Cannot proceed with configuration.")


def sanitize_stack_name(process_id, instance):
    """Sanitize stack name to comply with AWS CloudFormation naming rules"""
    clean_process_id = str(process_id).replace(",", "-").replace("_", "-").replace(" ", "-")
    clean_instance = str(instance).replace(",", "-").replace("_", "-").replace(" ", "-")
    stack_name = f"CdkDatalakeAnalyticsEducacionGroupStack-{clean_process_id}-{clean_instance}"
    stack_name = stack_name.replace("--", "-")
    return stack_name


# Create group stacks for each (layer, process_id) combination
deployed_stacks = {}

for instance in active_instances:
    # Read instance-specific configuration
    try:
        instance_layers_and_process_ids, csv_config_path = read_csv_and_create_process_config(
            instance, 
            project_paths.LOCAL_ARTIFACTS_CONFIGURE_CSV
        )
        
        print(f"Processing instance {instance} with {len(instance_layers_and_process_ids)} process configurations")
        
        for layer, process_id in instance_layers_and_process_ids:
            stack_name = sanitize_stack_name(process_id, instance)

            group_stack = CdkDatalakeAnalyticsEducacionGroupStack(
                app,
                stack_name,
                project_config,
                process_id,
                layer,
                base_stack_outputs,
                csv_config_path=csv_config_path,  # Pass instance-specific CSV path
                instance=instance,  # Use active instance
                env=cdk.Environment(account=project_config.account_id, region=project_config.region_name),
            )

            # Add dependency on base stack
            group_stack.add_dependency(base_stack)

            # Store the stack reference
            deployed_stacks[stack_name] = group_stack

            print(f"Created group stack: {stack_name} for layer '{layer}' process_id '{process_id}' instance '{instance}'")
            
    except FileNotFoundError as e:
        print(f"CRITICAL ERROR: Cannot proceed without instance-specific configuration for {instance}")
        print(f"Error: {e}")
        raise e
    except Exception as e:
        print(f"Error processing instance {instance}: {e}")
        raise e

app.synth()
