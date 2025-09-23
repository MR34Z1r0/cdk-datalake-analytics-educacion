import boto3
import json
import logging
import time
import os
import ast
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def _get_state_machine_arn(process_id: str, business_process: str) -> str:
    arn = os.environ.get('COMERCIAL_STEP_FUNCTION_ARN')
    if arn:
        return arn

    aws_region = os.environ.get('DATALAKE_AWS_REGION') or os.environ.get('AWS_REGION', 'us-east-2')
    aws_account_id = os.environ.get('DATALAKE_AWS_ACCOUNT_ID', '832257724409')
    environment = os.environ.get('DATALAKE_ENVIRONMENT', 'dev')
    return f"arn:aws:states:{aws_region}:{aws_account_id}:stateMachine:aje-{environment.lower()}-datalake-workflow_{business_process}_group_{process_id}-sf"

def _is_not_found_error(err: ClientError) -> bool:
    # StateMachineDoesNotExist de Step Functions
    return err.response.get('Error', {}).get('Code') in (
        'StateMachineDoesNotExist', 'ResourceNotFoundException'
    )

def get_business_process_from_process_id(process_id: str) -> str:
    process_matrix_str = os.environ.get('PROCESS_MATRIX', '{}')
    try:
        process_matrix = ast.literal_eval(process_matrix_str)
        return process_matrix.get(process_id)
    except Exception as e:
        logger.error(f"Failed to parse PROCESS_MATRIX: {e}")
        return None

def lambda_handler(event, context):
    process_id = event.get('process_id')
    endpoint_name = event.get('endpoint_name')
    instance = event.get('instance')
    scheduled_execution = event.get('scheduled_execution', True)
    endpoint_names = event.get('endpoint_names', [])
    execution_results = event.get('execution_results', {})

    # variables para retornos seguros en cualquier except
    endpoints_to_process = []
    execution_arns = []
    comercial_step_function_arn = "unknown"

    business_process = get_business_process_from_process_id(process_id)
    print(f"Business process for process_id '{process_id}': {business_process}")

    try:
        if not process_id:
            raise ValueError("process_id is required")
        if not instance:
            raise ValueError("instance is required")

        comercial_step_function_arn = _get_state_machine_arn(process_id, business_process)
        logger.info(f"Comercial SFN ARN: {comercial_step_function_arn}")

        sfn_client = boto3.client('stepfunctions')

        # Verificar existencia de la State Machine
        try:
            sfn_client.describe_state_machine(stateMachineArn=comercial_step_function_arn)
            logger.info("Comercial Step Function existe.")
        except ClientError as e:
            if _is_not_found_error(e):
                logger.warning(f"Comercial SFN no existe: {comercial_step_function_arn}")
                return {
                    'statusCode': 200,
                    'warning': 'StateMachineDoesNotExist',
                    'message': "Domain processing OK. Comercial Step Function no disponible aún.",
                    'comercial_step_function_arn': comercial_step_function_arn,
                    'instance': instance,
                    'process_id': process_id,
                    'original_endpoint': endpoint_name,
                    'processed_endpoints': endpoint_names or ([endpoint_name] if endpoint_name else []),
                    'comercial_processing': 'SKIPPED'
                }
            raise  # otro error real

        # Determinar endpoints a procesar
        endpoints_to_process = [endpoint_name] if endpoint_name else endpoint_names

        for ep in endpoints_to_process:
            step_input = {
                "process_id": process_id,
                "endpoint_name": ep,
                "instance": instance,
                "scheduled_execution": scheduled_execution
            }
            # Nombre <= 80 chars: recorta si es necesario
            exec_name = f"comercial-{instance}-{ep}-{process_id}-{int(time.time())}"
            if len(exec_name) > 80:
                exec_name = exec_name[:80]

            logger.info(f"Iniciando ejecución para {ep} con nombre {exec_name}")
            resp = sfn_client.start_execution(
                stateMachineArn=comercial_step_function_arn,
                input=json.dumps(step_input),
                name=exec_name
            )
            execution_arns.append({'endpoint_name': ep, 'execution_arn': resp['executionArn']})

        return {
            'statusCode': 200,
            'instance': instance,
            'process_id': process_id,
            'original_endpoint': endpoint_name,
            'processed_endpoints': endpoints_to_process,
            'executions': execution_arns,
            'total_executions': len(execution_arns),
            'status': 'RUNNING',
            'message': f'Ejecuciones iniciadas correctamente para {len(execution_arns)} endpoint(s)'
        }

    except ClientError as e:
        # Límite de ejecuciones u otros errores de SFN
        code = e.response.get('Error', {}).get('Code')
        if code == 'ExecutionLimitExceeded':
            logger.error(f"ExecutionLimitExceeded: {str(e)}")
            return {
                'statusCode': 429,
                'error': 'ExecutionLimitExceeded',
                'message': str(e),
                'instance': instance,
                'process_id': process_id,
                'original_endpoint': endpoint_name,
                'processed_endpoints': endpoints_to_process,
                'partial_executions': execution_arns
            }
        logger.error(f"ClientError: {code} - {str(e)}")
        return {
            'statusCode': 502,
            'error': code or 'ClientError',
            'message': str(e),
            'instance': instance,
            'process_id': process_id,
            'original_endpoint': endpoint_name,
            'processed_endpoints': endpoints_to_process,
            'partial_executions': execution_arns
        }

    except Exception as e:
        logger.error(f"Error invocando Comercial Step Function: {str(e)}")
        return {
            'statusCode': 500,
            'error': 'InternalError',
            'message': str(e),
            'instance': instance,
            'process_id': process_id,
            'original_endpoint': endpoint_name,
            'processed_endpoints': endpoints_to_process,
            'partial_executions': execution_arns
        }
