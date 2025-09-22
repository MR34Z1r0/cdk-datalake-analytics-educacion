from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_glue_alpha as glue_alpha,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks
)
import aws_cdk as cdk
from constructs import Construct
from aje_cdk_libs.builders.resource_builder import ResourceBuilder
from aje_cdk_libs.models.configs import *
from aje_cdk_libs.constants.policies import PolicyUtils
from constants.paths import Paths
import json
#import logging
import os

#logger = logging.getLogger(__name__)
#logger.setLevel(logging.INFO)

class CdkDatalakeRedshiftStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, project_config, domain_resources: dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.PROJECT_CONFIG = project_config
        self.builder = ResourceBuilder(self, self.PROJECT_CONFIG)
        self.Paths = Paths(self.PROJECT_CONFIG.app_config)
        
        # Recursos compartidos del stack de dominio
        self.glue_redshift_connection = domain_resources.get("glue_connection")
        self.lambda_get_data = domain_resources["lambda_get_data"]
        self.sns_failed_topic = domain_resources["sns_failed_topic"]
        self.sns_success_topic = domain_resources["sns_success_topic"]
        self.glue_job_role = domain_resources["glue_job_role"]
        self.step_function_role = domain_resources["step_function_role"]
        self.state_machine_base = domain_resources["state_machine_base"]
        
        # Importar recursos S3
        self._import_s3_resources()
        
        # Crear jobs específicos de Redshift
        self._create_redshift_jobs()
        
        # Crear Step Functions para Redshift
        self._create_redshift_step_functions()
        
    def _import_s3_resources(self):
        """Importar buckets S3 necesarios"""
        self.s3_artifacts_bucket = self.builder.import_s3_bucket(
            self.PROJECT_CONFIG.app_config["s3_buckets"]["artifacts"]
        )
        self.s3_analytics_bucket = self.builder.import_s3_bucket(
            self.PROJECT_CONFIG.app_config["s3_buckets"]["analytics"]
        )
        self.s3_stage_bucket = self.builder.import_s3_bucket(
            self.PROJECT_CONFIG.app_config["s3_buckets"]["stage"]
        )
        
    def _create_redshift_jobs(self):
        """Crear jobs específicos para carga a Redshift"""
        
        # Solo crear jobs si existe conexión a Redshift
        if not self.glue_redshift_connection:
            print("No Redshift connection available, skipping Redshift jobs")
            return
            
        # Argumentos base para jobs de Redshift
        redshift_args = self._build_redshift_job_arguments()
        
        # Job para cargar datos finales a Redshift
        load_to_redshift_config = GlueJobConfig(
            job_name="load_to_redshift",
            executable=glue_alpha.JobExecutable.python_etl(
                glue_version=glue_alpha.GlueVersion.V4_0,
                python_version=glue_alpha.PythonVersion.THREE,
                script=glue_alpha.Code.from_bucket(
                    self.s3_artifacts_bucket,
                    f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE}/redshift/load_to_redshift.py"
                )
            ),
            connections=[],  # Se agrega la conexión después
            default_arguments=redshift_args,
            worker_type=glue_alpha.WorkerType.G_1_X,
            worker_count=2,
            continuous_logging=glue_alpha.ContinuousLoggingProps(enabled=True),
            timeout=Duration.hours(2),
            max_concurrent_runs=10,
            role=self.glue_job_role
        )
        
        self.load_to_redshift_job = self.builder.build_glue_job(load_to_redshift_config)
        
        # Agregar conexión manualmente (workaround para glue_alpha)
        cfn_job = self.load_to_redshift_job.node.default_child
        cfn_job.connections = {
            "connections": [self.glue_redshift_connection.ref]
        }
        
        # Job para cargar datos de stage a Redshift  
        load_stage_to_redshift_config = GlueJobConfig(
            job_name="load_stage_to_redshift",
            executable=glue_alpha.JobExecutable.python_etl(
                glue_version=glue_alpha.GlueVersion.V4_0,
                python_version=glue_alpha.PythonVersion.THREE,
                script=glue_alpha.Code.from_bucket(
                    self.s3_artifacts_bucket,
                    f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE}/redshift/loadt_stage_to_redshift.py"
                )
            ),
            connections=[],
            default_arguments=redshift_args,
            worker_type=glue_alpha.WorkerType.G_1_X,
            worker_count=2,
            continuous_logging=glue_alpha.ContinuousLoggingProps(enabled=True),
            timeout=Duration.hours(2),
            max_concurrent_runs=10,
            role=self.glue_job_role
        )
        
        self.load_stage_to_redshift_job = self.builder.build_glue_job(load_stage_to_redshift_config)
        
        # Agregar conexión manualmente
        cfn_stage_job = self.load_stage_to_redshift_job.node.default_child
        cfn_stage_job.connections = {
            "connections": [self.glue_redshift_connection.ref]
        }
        
        print("Created Redshift Glue jobs")
        
    def _build_redshift_job_arguments(self) -> dict:
        """Construir argumentos para jobs de Redshift"""
        
        return {
            '--extra-py-files': f's3://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_GLUE_LAYER}/common_jobs_functions.py',
            '--S3_PATH_STG': f"s3a://{self.s3_stage_bucket.bucket_name}/{self.PROJECT_CONFIG.app_config['team']}/",
            '--S3_PATH_ANALYTICS': f"s3a://{self.s3_analytics_bucket.bucket_name}/{self.PROJECT_CONFIG.app_config['team']}/{self.PROJECT_CONFIG.app_config['business_process']}/",
            '--S3_PATH_ARTIFACTS': f"s3a://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_GLUE}/",
            '--S3_PATH_ARTIFACTS_CSV': f"s3a://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_GLUE_CSV}",
            '--CATALOG_CONNECTION': self.glue_redshift_connection.ref if self.glue_redshift_connection else "",
            '--REGION_NAME': self.PROJECT_CONFIG.region_name,
            '--ERROR_TOPIC_ARN': self.sns_failed_topic.topic_arn,
            '--PROJECT_NAME': self.PROJECT_CONFIG.app_config["team"],
            '--datalake-formats': "delta",
            '--enable-continuous-log-filter': "true"
        }
        
    def _create_redshift_step_functions(self):
        """Crear Step Functions para operaciones de Redshift"""
        
        if not hasattr(self, 'load_to_redshift_job'):
            print("No Redshift jobs available, skipping Step Functions creation")
            return
            
        # State Machine base para Redshift (modificada)
        redshift_base_definition = self._build_redshift_base_definition()
        self.state_machine_redshift_base = sfn.StateMachine(
            self, "StateMachineRedshiftBase",
            state_machine_name=f"{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-analytics-redshift-base-sm",
            definition_body=sfn.DefinitionBody.from_chainable(redshift_base_definition),
            role=self.step_function_role
        )
        
        # State Machine para carga final a Redshift
        load_redshift_definition = self._build_load_redshift_definition()
        self.state_machine_load_redshift = sfn.StateMachine(
            self, "StateMachineLoadRedshift",
            state_machine_name=f"{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-analytics-load-redshift-sm",
            definition_body=sfn.DefinitionBody.from_chainable(load_redshift_definition),
            role=self.step_function_role
        )
        
        # State Machine para carga de stage a Redshift
        load_stage_redshift_definition = self._build_load_stage_redshift_definition()
        self.state_machine_load_stage_redshift = sfn.StateMachine(
            self, "StateMachineLoadStageRedshift", 
            state_machine_name=f"{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-analytics-load-stage-redshift-sm",
            definition_body=sfn.DefinitionBody.from_chainable(load_stage_redshift_definition),
            role=self.step_function_role
        )
        
        print("Created Redshift Step Functions")
        
    def _build_redshift_base_definition(self):
        """Construir definición base para jobs de Redshift"""
        
        # Validar ejecución (similar al base pero con parámetros de Redshift)
        validate_execution = tasks.LambdaInvoke(
            self, "ValidateRedshiftExecution",
            lambda_function=self.lambda_get_data,
            payload=sfn.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "own_process_id.$": "$.own_process_id",
                "job_name.$": "$.job_name",
                "COD_PAIS.$": "$.COD_PAIS",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "redshift": True
            }),
            result_path="$.validation_result"
        )
        
        # Ejecutar job de Redshift
        execute_redshift_job = tasks.GlueStartJobRun(
            self, "ExecuteRedshiftJob",
            glue_job_name=sfn.JsonPath.string_at("$.job_name"),
            arguments=sfn.TaskInput.from_object({
                "--COD_PAIS.$": "$.COD_PAIS",
                "--LOAD_PROCESS.$": "$.exe_process_id",
                "--ORIGIN.$": "$.ORIGIN",
                "--INSTANCIAS.$": "$.INSTANCIAS"
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB
        )
        
        # Error notification
        error_notification = tasks.SnsPublish(
            self, "NotifyRedshiftError",
            topic=self.sns_failed_topic,
            message=sfn.TaskInput.from_object({
                "error": "Redshift job execution failed",
                "job_name.$": "$.job_name",
                "timestamp.$": "$$.State.EnteredTime"
            })
        )
        
        choice = sfn.Choice(self, "RedshiftExecutionChoice")
        
        validate_execution.next(
            choice
            .when(
                sfn.Condition.boolean_equals("$.validation_result.Payload.ready", True),
                execute_redshift_job
            )
            .when(
                sfn.Condition.boolean_equals("$.validation_result.Payload.error", True),
                error_notification
            )
            .otherwise(sfn.Succeed(self, "SkipRedshiftExecution"))
        )
        
        return validate_execution
        
    def _build_load_redshift_definition(self):
        """Construir definición para carga final a Redshift"""
        
        # Ejecutar job de carga usando la state machine base
        execute_load = tasks.StepFunctionsStartExecution(
            self, "ExecuteLoadToRedshift",
            state_machine=self.state_machine_redshift_base,
            input=sfn.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "own_process_id": "redshift",
                "job_name": self.load_to_redshift_job.job_name,
                "COD_PAIS.$": "$.COD_PAIS",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "ORIGIN.$": "$.ORIGIN",
                "redshift": True
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB
        )
        
        # Notificación de éxito
        success_notification = tasks.SnsPublish(
            self, "NotifyLoadRedshiftSuccess",
            topic=self.sns_success_topic,
            message=sfn.TaskInput.from_object({
                "status": "SUCCESS", 
                "message": "Load to Redshift completed successfully",
                "timestamp.$": "$$.State.EnteredTime"
            })
        )
        
        return execute_load.next(success_notification)
        
    def _build_load_stage_redshift_definition(self):
        """Construir definición para carga de stage a Redshift"""
        
        # Ejecutar job de carga de stage
        execute_stage_load = tasks.StepFunctionsStartExecution(
            self, "ExecuteLoadStageToRedshift",
            state_machine=self.state_machine_redshift_base,
            input=sfn.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "own_process_id": "stage_redshift",
                "job_name": self.load_stage_to_redshift_job.job_name,
                "COD_PAIS.$": "$.COD_PAIS",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "ORIGIN": "BIGMAGIC",
                "redshift": True
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB
        )
        
        # Notificación de éxito
        success_notification = tasks.SnsPublish(
            self, "NotifyLoadStageRedshiftSuccess",
            topic=self.sns_success_topic,
            message=sfn.TaskInput.from_object({
                "status": "SUCCESS",
                "message": "Load stage to Redshift completed successfully", 
                "timestamp.$": "$$.State.EnteredTime"
            })
        )
        
        return execute_stage_load.next(success_notification)