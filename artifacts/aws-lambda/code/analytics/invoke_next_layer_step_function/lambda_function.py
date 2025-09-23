import boto3
import json
import logging
import time
import os
import ast
import csv
import io
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

stepfunctions_client = boto3.client("stepfunctions")

def _get_state_machine_arn(process_id: str, business_process: str, instance: str) -> str:

    aws_region = os.environ.get("DATALAKE_AWS_REGION")
    aws_account_id = os.environ.get("DATALAKE_AWS_ACCOUNT_ID")
    environment = os.environ.get("DATALAKE_ENVIRONMENT")
    return f"arn:aws:states:{aws_region}:{aws_account_id}:stateMachine:aje-{environment.lower()}-datalake-workflow_{business_process}_group_{process_id}_{instance.lower()}-sf"


def _read_process_matrix_from_s3(s3_path: str, layer: str) -> dict:
    try:
        # Parse S3 path
        if not s3_path.startswith("s3://"):
            raise ValueError(f"Invalid S3 path format: {s3_path}. Expected format: s3://bucket-name/path/to/file.csv")

        # Remove 's3://' prefix and split bucket and key
        path_without_prefix = s3_path[5:]  # Remove 's3://'
        bucket_name, object_key = path_without_prefix.split("/", 1)

        # Initialize S3 client
        s3_client = boto3.client("s3")

        # Download the file from S3
        logger.info(f"Reading process matrix from S3: {bucket_name}/{object_key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        csv_content = response["Body"].read().decode("utf-8")

        # Parse CSV content
        csv_reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")
        process_matrix = {}
        
        # Check if layer contains "econored"
        layer_has_econored = "econored" in layer.lower() if layer else False
        logger.info(f"Layer '{layer}' contains 'econored': {layer_has_econored}")

        for row in csv_reader:
            process_id = row.get("process_id", "").strip()
            negocio = row.get("Negocio", "").strip()

            if process_id and negocio:
                negocio_has_econored = "econored" in negocio.lower()
                
                # Logic: if layer has "econored", use negocio with "econored"
                # if layer doesn't have "econored", use negocio without "econored"
                if layer_has_econored == negocio_has_econored:
                    process_matrix[process_id] = negocio.lower()
                    logger.debug(f"Mapped process_id '{process_id}' to business '{negocio}' (econored match: {negocio_has_econored})")
                else:
                    logger.debug(f"Skipped process_id '{process_id}' with business '{negocio}' (econored mismatch: layer={layer_has_econored}, negocio={negocio_has_econored})")

        logger.info(f"Successfully loaded {len(process_matrix)} process mappings from S3")
        return process_matrix

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        logger.error(f"S3 ClientError reading process matrix: {error_code} - {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error reading process matrix from S3: {str(e)}")
        raise


def get_business_process_from_process_id(process_id: str, layer: str) -> str:
    process_matrix_s3_path = os.environ.get("PROCESS_MATRIX_S3_PATH")
    
    if not process_matrix_s3_path:
        logger.error("PROCESS_MATRIX_S3_PATH environment variable not set")
        return None
    
    try:
        process_matrix = _read_process_matrix_from_s3(process_matrix_s3_path, layer)
        business_process = process_matrix.get(process_id)
        
        if business_process:
            logger.info(f"Found business process '{business_process}' for process_id '{process_id}'")
        else:
            logger.warning(f"No business process found for process_id '{process_id}'")
        
        return business_process
        
    except Exception as e:
        logger.error(f"Failed to get business process for process_id '{process_id}': {e}")
        return None


def lambda_handler(event, context):
    process_id = event.get("process_id")
    instance = event.get("instance")
    domain_results = event.get('domain_results', {})
    layer = event.get('layer')

    business_process = get_business_process_from_process_id(process_id, layer)
    print(f"Business process for process_id '{process_id}' and layer '{layer}' : {business_process}")

    try:
        if not process_id:
            raise ValueError("process_id is required")
        if not instance:
            raise ValueError("instance is required")

        state_machine_arn = _get_state_machine_arn(process_id, business_process, instance)
        logger.info(f"Next layer state machine ARN: {state_machine_arn}")

        # Check if step function exists
        try:
            stepfunctions_client.describe_state_machine(stateMachineArn=state_machine_arn)
            logger.info("Step function exists")
        except stepfunctions_client.exceptions.StateMachineDoesNotExist:
            logger.warning(f"Step function does not exist: {state_machine_arn}")
            return {
                'statusCode': 200,
                'instance': instance,
                'process_id': process_id,
                'executions': [],
                'total_executions': 0,
                'status': 'SKIPPED',
                'message': f'Step function does not exist: {state_machine_arn}'
            }
        
        # Prepare input for next layer step function
        step_function_input = {
            "process_id": process_id,
            "instance": instance
        }
        
        # Start execution of step function
        response = stepfunctions_client.start_execution(
            stateMachineArn=state_machine_arn,
            input=json.dumps(step_function_input),
            name=f"{business_process}-{instance}-{process_id}-{int(time.time())}"
        )
        
        execution_arn = response['executionArn']
        logger.info(f"Started step function execution: {execution_arn}")
        
        return {
            'statusCode': 200,
            'instance': instance,
            'process_id': process_id,
            'executions': [execution_arn],
            'total_executions': 1,
            'status': 'RUNNING',
            'message': 'Next layer step function invoked successfully',
            'execution_arn': execution_arn,
            'state_machine_arn': state_machine_arn,
            'step_function_input': step_function_input,
            'domain_results': domain_results
        }
        
    except Exception as e:
        logger.error(f"Error invoking next layer step function: {str(e)}")
        
        return {
            'statusCode': 500,
            'instance': event.get('instance', 'PE'),
            'process_id': event.get('process_id', '10'),
            'executions': [],
            'total_executions': 0,
            'status': 'ERROR',
            'error': str(e),
            'message': 'Failed to invoke next layer step function'
        }
