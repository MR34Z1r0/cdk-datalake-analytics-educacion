from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_glue_alpha as glue_alpha,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_s3 as s3,
    aws_sns as sns,
)
import aws_cdk as cdk
from constructs import Construct
from aje_cdk_libs.builders.resource_builder import ResourceBuilder
from aje_cdk_libs.models.configs import GlueJobConfig, StepFunctionConfig
from constants.paths import Paths
from constants.layers import Layers
import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CdkDatalakeAnalyticsEducacionGroupStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, project_config, process_id, layer, base_stack_outputs, csv_config_path=None, instance=None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.PROJECT_CONFIG = project_config
        self.process_id = process_id
        self.layer = layer
        self.instance = instance
        self.base_stack_outputs = base_stack_outputs
        self.csv_config_path = csv_config_path
        self.builder = ResourceBuilder(self, self.PROJECT_CONFIG)
        self.Paths = Paths(self.PROJECT_CONFIG.app_config)
        self.Layers = Layers(self.PROJECT_CONFIG.app_config, project_config.region_name, project_config.account_id)
        self.datasource = "upeu"

        # Inicializar estructuras de datos
        self.glue_jobs = {}
        self.state_machine_order_list = {}
        self.relation_process = {}

        # Apply stack-level tags to all resources in this stack
        self._apply_stack_tags()

        # Crear recursos en orden
        self._import_base_resources()
        self._create_glue_jobs()
        self._create_step_functions()
        self._create_outputs()

    def _apply_stack_tags(self):
        """Apply stack-level tags to all resources created in this stack"""
        
        # Create common tags that apply to all resources in the stack
        stack_tags = {
            "StackName": self.stack_name
        }
        
        # Apply tags to the entire stack (will tag all taggable resources)
        for key, value in stack_tags.items():
            cdk.Tags.of(self).add(key, value)
            
        logger.info(f"Applied stack-level tags for instance {self.instance}")

    def _import_base_resources(self):
        """Importar recursos desde base stack"""

        # S3 Buckets
        self.s3_artifacts_bucket = s3.Bucket.from_bucket_name(self, "S3ArtifactsBucket", self.base_stack_outputs["S3ArtifactsBucketName"])
        self.s3_external_bucket = s3.Bucket.from_bucket_name(self, "S3ExternalBucket", self.base_stack_outputs["S3ExternalBucketName"])
        self.s3_stage_bucket = s3.Bucket.from_bucket_name(self, "S3StageBucket", self.base_stack_outputs["S3StageBucketName"])
        self.s3_analytics_bucket = s3.Bucket.from_bucket_name(self, "S3AnalyticsBucket", self.base_stack_outputs["S3AnalyticsBucketName"])

        # IAM Roles
        self.glue_job_role = iam.Role.from_role_arn(self, "GlueJobRole", self.base_stack_outputs["GlueJobRoleArn"])
        self.step_function_role = iam.Role.from_role_arn(self, "StepFunctionRole", self.base_stack_outputs["StepFunctionRoleArn"])

        # SNS Topics
        self.sns_failed_topic = sns.Topic.from_topic_arn(self, "SnsFailedTopic", self.base_stack_outputs["SnsFailedTopicArn"])
        self.sns_success_topic = sns.Topic.from_topic_arn(self, "SnsSuccessTopic", self.base_stack_outputs["SnsSuccessTopicArn"])

        # Step Functions - Use analytics-core base Step Function
        self.analytics_core_base_step_function = sfn.StateMachine.from_state_machine_arn(self, "AnalyticsCoreBaseStepFunction", self.base_stack_outputs["AnalyticsCoreBaseStepFunctionArn"])

        # Lambda Functions
        self.invoke_next_layer_lambda_function = _lambda.Function.from_function_arn(self, "InvokeNextLayerLambdaFunction", self.base_stack_outputs["InvokeNextLayerLambdaFunctionArn"])

        # Analytics Core Resources  
        self.analytics_core_domain_crawler_job_name = self.base_stack_outputs["AnalyticsCoreDomainCrawlerJobName"]

        # S3 path for Spark events
        self.s3_path_spark = f"s3://{self.s3_artifacts_bucket.bucket_name}/aws-glue/spark-events"
        
        # Additional Python modules for Glue jobs
        self.additional_python_modules = "pytz==2021.1,boto3==1.17.49,s3fs==2021.4.0"

        logger.info(f"Successfully imported base stack resources for group {self.process_id}-{self.layer}")

    def _create_glue_jobs(self):
        """Crear Glue jobs para este grupo específico"""

        # Cargar configuración de jobs desde CSV
        jobs_config = self._load_jobs_configuration()

        # Argumentos base para todos los jobs
        base_args = self._build_base_job_arguments()

        # Crear jobs solo para este layer y process_id
        if self.layer in jobs_config:
            jobs_data = jobs_config[self.layer]
            # Filtrar solo los jobs de este process_id
            filtered_jobs = [job for job in jobs_data if str(job.get("process_id", "10")) == str(self.process_id)]
            
            if filtered_jobs:
                self._create_jobs_for_layer(self.layer, filtered_jobs, base_args)
            else:
                logger.warning(f"No jobs found for layer '{self.layer}' and process_id '{self.process_id}'")
        else:
            logger.warning(f"Layer '{self.layer}' not found in configuration")

    def _load_jobs_configuration(self) -> dict:
        """Cargar configuración de jobs desde archivos CSV"""

        jobs_by_layer = {}

        # Use CSV path from constructor (should always be provided)
        csv_path = self.csv_config_path

        try:
            df = pd.read_csv(csv_path, delimiter=";")
            if not df.empty:
                jobs_data = df.to_dict("records")

                # Group jobs by layer (as found in CSV)
                for job_data in jobs_data:
                    layer_name = job_data.get("layer", "unknown")
                    if layer_name not in jobs_by_layer:
                        jobs_by_layer[layer_name] = []
                    jobs_by_layer[layer_name].append(job_data)

                # Process relations for each layer
                for layer_name, layer_jobs in jobs_by_layer.items():
                    self._process_job_relations(layer_jobs, layer_name)
                    logger.info(f"Loaded {len(layer_jobs)} jobs for layer '{layer_name}' from {csv_path}")
            else:
                logger.warning(f"CSV file {csv_path} is empty")
        except Exception as e:
            logger.error(f"Error loading CSV {csv_path}: {str(e)}")

        return jobs_by_layer

    def _process_job_relations(self, jobs_data: list, layer_name: str):
        """Procesar relaciones de jobs para Step Functions"""

        for job_data in jobs_data:
            process_id = str(job_data.get("process_id", "10"))

            # Solo procesar si es el process_id de este grupo
            if process_id == str(self.process_id):
                if process_id not in self.relation_process:
                    self.relation_process[process_id] = []

                self.relation_process[process_id].append({"table": job_data["procedure"], "periods": job_data.get("periods", 2), "layer": layer_name})

    def _build_base_job_arguments(self) -> dict:
        """Construir argumentos base para jobs de Glue"""

        base_args = {
            "--extra-py-files": f"s3://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_GLUE_LAYER}/common_jobs_functions.py",
            "--S3_PATH_STG": f"s3://{self.s3_stage_bucket.bucket_name}/{self.PROJECT_CONFIG.app_config['team']}/",
            "--S3_PATH_ANALYTICS": f"s3://{self.s3_analytics_bucket.bucket_name}/{self.PROJECT_CONFIG.app_config['team']}/",
            "--S3_PATH_EXTERNAL": f"s3://{self.s3_external_bucket.bucket_name}/{self.PROJECT_CONFIG.app_config['team']}/",
            "--S3_PATH_ARTIFACTS": f"s3://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_CONFIGURE}/",
            "--S3_PATH_ARTIFACTS_CSV": f"s3://{self.s3_artifacts_bucket.bucket_name}/{self.PROJECT_CONFIG.app_config['team']}/{self.PROJECT_CONFIG.app_config['business_process']}/configuration/csv/",
            "--S3_PATH_ARTIFACTS_CONFIG": f"s3://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_CONFIGURE_CSV}",
            "--S3_PATH_STAGE_CSV": f"s3://{self.s3_artifacts_bucket.bucket_name}/{self.PROJECT_CONFIG.app_config['team']}/{self.datasource}/configuration/csv/",
            "--TEAM": self.PROJECT_CONFIG.app_config.get("team", "aje"),
            "--BUSINESS_PROCESS": self.PROJECT_CONFIG.app_config.get("business_process", "analytics"),
            "--REGION_NAME": self.PROJECT_CONFIG.region_name,
            "--DYNAMODB_DATABASE_NAME": self.base_stack_outputs["DynamoCredentialsTableName"],
            "--DYNAMODB_COLUMNS_NAME": self.base_stack_outputs["DynamoColumnsTableName"],
            "--DYNAMODB_LOGS_TABLE": self.base_stack_outputs["DynamoLogsTableName"],
            "--ERROR_TOPIC_ARN": self.sns_failed_topic.topic_arn,
            "--PROJECT_NAME": self.PROJECT_CONFIG.project_name,
            "--ENVIRONMENT": self.PROJECT_CONFIG.environment.value.upper(),
            "--job-language": "python",
            "--job-bookmark-option": "job-bookmark-disable",
            "--TempDir": f"s3://{self.s3_artifacts_bucket.bucket_name}/tmp/",
            "--enable-metrics": "",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-observability-metrics": "true",
            "--datalake-formats": "delta",
            "--enable-continuous-log-filter": "true",
            "--INSTANCIAS": self.instance,  # Fixed instance value for this stack
            "--LOAD_PROCESS": str(self.process_id),
            "--ORIGIN": "AJE",
        }

        return base_args

    def _create_jobs_for_layer(self, layer: str, jobs_data: list, base_args: dict):
        """Crear jobs de Glue para una capa específica"""

        # Inicializar estructuras para la capa
        if layer not in self.glue_jobs:
            self.glue_jobs[layer] = {}
        if layer not in self.state_machine_order_list:
            self.state_machine_order_list[layer] = {}

        for job_data in jobs_data:
            procedure_name = job_data["procedure"]
            # Follow domain bigmagic pattern exactly: {business_process}_{procedure}_{instance}
            job_name = f"{self.PROJECT_CONFIG.app_config.get('business_process', 'analytics')}_{procedure_name}_{self.instance.lower()}"
            
            logger.info(f"Creating job: {job_name} for instance: {self.instance}")

            # Crear configuración del job usando aje-cdk-libs
            job_config = GlueJobConfig(
                job_name=job_name,
                executable=glue_alpha.JobExecutable.python_etl(
                    glue_version=self._get_glue_version(job_data.get("glue_version", "5")),
                    python_version=glue_alpha.PythonVersion.THREE,
                    script=glue_alpha.Code.from_bucket(self.s3_artifacts_bucket, f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE}/{layer}/{procedure_name}.py"),
                ),
                default_arguments={
                    **base_args,
                    "--PROCESS_NAME": f"{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-{layer}-{procedure_name}-{self.instance.lower()}-job",
                    "--FLOW_NAME": f"{layer}_{procedure_name}_{self.instance.lower()}",
                    "--INSTANCE": self.instance,  # Add instance parameter
                    "--PERIODS": str(job_data.get("periods", 2)),
                    "--additional-python-modules": self.additional_python_modules,
                    "--enable-spark-ui": "true",
                    "--spark-event-logs-path": f"{self.s3_path_spark}/{layer}/{self.instance}/{procedure_name}/",
                    "--enable-continuous-cloudwatch-log": "true",
                    "--enable-observability-metrics": "true",
                    "--TABLE_NAME" : procedure_name
                },
                worker_type=self._get_worker_type(job_data.get("worker_type", "G.1X")),
                worker_count=job_data.get("num_workers", 2),
                continuous_logging=glue_alpha.ContinuousLoggingProps(enabled=True),
                timeout=Duration.hours(12),
                max_concurrent_runs=100,
                role=self.glue_job_role,
            )

            # Crear job usando ResourceBuilder
            glue_job = self.builder.build_glue_job(job_config)
            
            # Add tags to the Glue job (following domain-siesa pattern)
            job_tags = self._create_job_tags("HeavyTransformDomain")
            for key, value in job_tags.items():
                cdk.Tags.of(glue_job).add(key, value)
            
            self.glue_jobs[layer][procedure_name] = glue_job

            # Organizar para Step Functions
            exe_order = job_data.get("exe_order", 0)
            if exe_order > 0:
                self._add_to_execution_order(layer, job_data, glue_job.job_name)

            logger.info(f"Created Glue job for group {self.process_id}: {glue_job.job_name}")

    def _add_to_execution_order(self, layer: str, job_data: dict, job_name: str):
        """Agregar job al orden de ejecución de Step Functions"""

        exe_order = job_data.get("exe_order", 0)
        order_key = str(exe_order)

        if order_key not in self.state_machine_order_list[layer]:
            self.state_machine_order_list[layer][order_key] = []

        self.state_machine_order_list[layer][order_key].append({"table": job_data["procedure"], "process_id": str(job_data.get("process_id", "10")), "job_name": job_name})

    def _create_step_functions(self):
        """Crear Step Functions para este grupo específico"""

        # Crear State Machine para este grupo
        for layer in self.state_machine_order_list.keys():
            # Crear definición para la capa/grupo
            layer_definition = self._build_layer_state_machine_definition(layer)

            # Use StepFunctionConfig pattern like original domain bigmagic but include instance
            layer_config = StepFunctionConfig(
                name=f"workflow_{self.PROJECT_CONFIG.app_config.get('business_process')}_group_{self.process_id}_{self.instance.lower()}", 
                definition_body=sfn.DefinitionBody.from_chainable(layer_definition), 
                role=self.step_function_role
            )

            layer_sf = self.builder.build_step_function(layer_config)
            setattr(self, f"state_machine_group_{self.process_id}_{self.instance}", layer_sf)

            logger.info(f"Created Step Function for layer '{layer}' instance '{self.instance}': {layer_sf.state_machine_name}")

    def _build_layer_state_machine_definition(self, layer: str):
        """Construir definición de Step Function que llama al Glue Job de analytics-core para crawlers"""

        jobs_by_order = self.state_machine_order_list.get(layer, {})

        current_state = None
        initial_state = None

        # Procesar jobs por orden de ejecución, chunking large parallel groups
        for order in sorted(jobs_by_order.keys()):
            if int(order) <= 0:
                continue

            # Crear estado paralelo para este orden - collect results then preserve input
            parallel_state = sfn.Parallel(
                self, f"{layer}_order_{order}",
                result_path="$.execution_results"
            )

            # Agregar cada job como rama paralela
            for job_config in jobs_by_order[order]:
                # Build job-specific arguments by merging base args with job-specific ones
                job_arguments = self._build_base_job_arguments().copy()
                job_arguments.update(job_config.get("job_args", {}))
                # --INSTANCIAS is already set to correct instance in base args

                job_execution = tasks.StepFunctionsStartExecution(
                    self,
                    f"{layer}_{job_config['table']}",
                    state_machine=self.analytics_core_base_step_function,
                    input=sfn.TaskInput.from_object({
                        "exe_process_id": str(self.process_id),
                        "own_process_id": job_config["process_id"],
                        "job_name": job_config["job_name"],
                        "INSTANCIAS": self.instance,
                        "redshift": False,
                        "job_arguments": job_arguments
                    }),
                    integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                )
                parallel_state.branch(job_execution)

            # Conectar estados secuencialmente
            if current_state is None:
                initial_state = parallel_state
            else:
                current_state.next(parallel_state)

            current_state = parallel_state

        # Llamar al Glue Job de analytics-core para crear crawler y database
        if current_state:
            # Call analytics-core base Step Function with crawler/database creation job
            crawler_arguments = self._build_base_job_arguments().copy()
            # Override INSTANCIAS with the fixed instance value for this stack
            crawler_arguments["--INSTANCIAS"] = self.instance

            create_crawler_db = tasks.StepFunctionsStartExecution(
                self,
                f"CreateCrawlerDatabase_{layer}",
                state_machine=self.analytics_core_base_step_function,
                input=sfn.TaskInput.from_object(
                    {
                        "exe_process_id": str(self.process_id),
                        "own_process_id": "crawler",
                        "job_name": self.analytics_core_domain_crawler_job_name,  # Reference imported job name directly
                        "INSTANCIAS": self.instance,
                        "redshift": False,
                        "layer": layer,
                        "business_process": self.PROJECT_CONFIG.app_config["business_process"],
                        "job_arguments": crawler_arguments,
                    }
                ),
                integration_pattern=sfn.IntegrationPattern.RUN_JOB
            )  # Remove result_path to avoid accumulating large results

            current_state.next(create_crawler_db)

            # Add lext layer lambda invocation after crawler creation
            invoke_next_layer_lambda_invocation = tasks.LambdaInvoke(
                self, f"InvokeNextLayerStepFunction_{layer}_{self.instance}",
                lambda_function=self.invoke_next_layer_lambda_function,
                payload=sfn.TaskInput.from_object({
                    "process_id": str(self.process_id),
                    "instance": self.instance,
                    "layer": layer,
                    "action": "invoke__next_layer_step_function"
                })
            )
            
            create_crawler_db.next(invoke_next_layer_lambda_invocation)
            current_state = invoke_next_layer_lambda_invocation

        return initial_state or sfn.Succeed(self, f"no_jobs_{layer}")

    def _create_job_tags(self, job_type, endpoint=None):
        """Generate resource tags"""
        tags = {
            'DataSource': self.PROJECT_CONFIG.app_config.get('business_process', ''),
            'Process': 'Analytic',
            'Instance': self.instance,
            'SubProcess': job_type
        }
        
        if endpoint:
            tags['Endpoint'] = endpoint
            
        return tags

    def _get_glue_version(self, version_str: str) -> glue_alpha.GlueVersion:
        """Convertir string de versión a enum GlueVersion"""
        version_map = {"3": glue_alpha.GlueVersion.V3_0, "4": glue_alpha.GlueVersion.V4_0, "5": glue_alpha.GlueVersion.of('5.0')}
        return version_map.get(str(version_str), glue_alpha.GlueVersion.of('5.0'))

    def _get_worker_type(self, worker_str: str) -> glue_alpha.WorkerType:
        """Convertir string de worker a enum WorkerType"""
        worker_map = {"G.1X": glue_alpha.WorkerType.G_1_X, "G.2X": glue_alpha.WorkerType.G_2_X}
        return worker_map.get(worker_str, glue_alpha.WorkerType.G_1_X)

    def _create_outputs(self):
        """Create CloudFormation outputs"""

        # Output the Step Function ARN for this group and instance
        if hasattr(self, f"state_machine_group_{self.process_id}_{self.instance}"):
            sf = getattr(self, f"state_machine_group_{self.process_id}_{self.instance}")
            cdk.CfnOutput(self, f"StepFunctionGroup{self.process_id}{self.instance}Arn", value=sf.state_machine_arn, description=f"ARN of Step Function for group {self.process_id} instance {self.instance}")

        # Document expected input parameters
        cdk.CfnOutput(self, "ExpectedInputParameters", value='{"process_id":"unique_id","INSTANCIAS":"PE"}', description="Expected input parameters for group Step Functions. COD_PAIS is handled within Glue job scripts.")
