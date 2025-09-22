from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_glue_alpha as glue_alpha,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_s3_deployment as s3_deployment
)
import aws_cdk as cdk
from constructs import Construct
from aje_cdk_libs.builders.resource_builder import ResourceBuilder
from aje_cdk_libs.models.configs import *
from aje_cdk_libs.constants.policies import PolicyUtils
from constants.paths import Paths
from constants.layers import Layers
import pandas as pd
import csv
import json
#import logging
import os

#logger = logging.getLogger(__name__)
#logger.setLevel(logging.INFO)

class CdkDatalakeAnalyticsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, project_config, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.PROJECT_CONFIG = project_config
        self.builder = ResourceBuilder(self, self.PROJECT_CONFIG)
        self.Paths = Paths(self.PROJECT_CONFIG.app_config)
        self.Layers = Layers(
            self.PROJECT_CONFIG.app_config, 
            project_config.region_name, 
            project_config.account_id
        )
        self.datasource = 'bigmagic'
        
        # Inicializar estructuras de datos
        self.glue_jobs = {}
        self.cr_targets = {}
        self.state_machine_order_list = {}
        self.relation_process = {}
        
        # Crear recursos en orden
        self._import_existing_resources()
        self._create_iam_roles()
        self._deploy_assets_to_s3()
        self._create_lambda_functions()
        self._create_glue_connection()
        self._create_glue_jobs()

        #Realizar un ajuste donde el Crawler debe moverse a un Domain Base para la creación de las bases de datos y crawlers
        self._create_glue_databases_and_crawlers()
        self._create_step_functions()
        
    def _import_existing_resources(self):
        """Importar recursos S3, DynamoDB y SNS existentes"""
        
        # Buckets S3 usando aje-cdk-libs
        self.s3_artifacts_bucket = self.builder.import_s3_bucket(
            self.PROJECT_CONFIG.app_config["s3_buckets"]["artifacts"]
        )
        self.s3_external_bucket = self.builder.import_s3_bucket(
            self.PROJECT_CONFIG.app_config["s3_buckets"]["external"]
        )
        self.s3_stage_bucket = self.builder.import_s3_bucket(
            self.PROJECT_CONFIG.app_config["s3_buckets"]["stage"]
        )
        self.s3_analytics_bucket = self.builder.import_s3_bucket(
            self.PROJECT_CONFIG.app_config["s3_buckets"]["analytics"]
        )
        
        # Tablas DynamoDB usando aje-cdk-libs
        self.dynamodb_columns_table = self.builder.import_dynamodb_table(
            self.PROJECT_CONFIG.app_config["dynamodb_tables"]["columns-specifications"]
        )
        self.dynamodb_configuration_table = self.builder.import_dynamodb_table(
            self.PROJECT_CONFIG.app_config["dynamodb_tables"]["configuration"]
        )
        self.dynamodb_credentials_table = self.builder.import_dynamodb_table(
            self.PROJECT_CONFIG.app_config["dynamodb_tables"]["credentials"]
        )
        self.dynamodb_logs_table = self.builder.import_dynamodb_table(
            self.PROJECT_CONFIG.app_config["dynamodb_tables"]["logs"]
        )
        
        # Tópicos SNS usando aje-cdk-libs
        self.sns_failed_topic = self.builder.import_sns_topic(
            self.PROJECT_CONFIG.app_config["topic_notifications"]["failed"]
        )
        self.sns_success_topic = self.builder.import_sns_topic(
            self.PROJECT_CONFIG.app_config["topic_notifications"]["success"]
        )
        
    def _create_iam_roles(self):
        """Crear roles IAM usando aje-cdk-libs"""
        
        # Role para Glue Jobs usando RoleConfig
        glue_job_role_config = RoleConfig(
            role_name=f"{self.PROJECT_CONFIG.app_config['business_process']}-job",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ],
            inline_policies={
                "DataLakeAccessPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.join_permissions(
                                PolicyUtils.S3_READ,
                                PolicyUtils.S3_WRITE
                            ),
                            resources=[
                                self.s3_artifacts_bucket.bucket_arn,
                                f"{self.s3_artifacts_bucket.bucket_arn}/*",
                                self.s3_external_bucket.bucket_arn,
                                f"{self.s3_external_bucket.bucket_arn}/*",
                                self.s3_stage_bucket.bucket_arn,
                                f"{self.s3_stage_bucket.bucket_arn}/*",
                                self.s3_analytics_bucket.bucket_arn,
                                f"{self.s3_analytics_bucket.bucket_arn}/*"
                            ]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.DYNAMODB_READ,
                            resources=["*"]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.SNS_PUBLISH,
                            resources=[self.sns_failed_topic.topic_arn]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.LOGS_WRITE,
                            resources=["*"]
                        )
                    ]
                )
            }
        )
        self.glue_job_role = self.builder.build_role(glue_job_role_config)
        
        # Role para Glue Crawler
        glue_crawler_role_config = RoleConfig(
            role_name=f"{self.PROJECT_CONFIG.app_config['business_process']}-crawler",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSLakeFormationDataAdmin")
            ]
        )
        self.glue_crawler_role = self.builder.build_role(glue_crawler_role_config)
        
        ##############################################################################
        # Role específico para State Machine Base
        ##############################################################################
        sf_base_role_config = RoleConfig(
            role_name=f"{self.PROJECT_CONFIG.app_config['business_process']}-sf-base",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            inline_policies={
                "BaseExecutionPolicy": iam.PolicyDocument(
                    statements=[
                        # Permisos de Glue
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "glue:StartJobRun",
                                "glue:GetJobRun", 
                                "glue:BatchStopJobRun",
                                "glue:GetJobRuns"
                            ],
                            resources=["*"]
                        ),
                        # Permisos de Lambda
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.LAMBDA_INVOKE,
                            resources=["*"]
                        ),
                        # Permisos de SNS
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.SNS_PUBLISH,
                            resources=[
                                self.sns_failed_topic.topic_arn,
                                self.sns_success_topic.topic_arn
                            ]
                        )
                    ]
                )
            }
        )
        self.step_function_base_role = self.builder.build_role(sf_base_role_config)
        
        # Role para Step Functions
        sf_role_config = RoleConfig(
            role_name=f"{self.PROJECT_CONFIG.app_config['business_process']}-sf",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            inline_policies={
                "StepFunctionExecutionPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.join_permissions(
                                PolicyUtils.GLUE_START_JOB, 
                                PolicyUtils.CRAWLER_START_JOB),
                            resources=["*"]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.LAMBDA_INVOKE,
                            resources=["*"]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.STEP_FUNCTIONS_START_EXECUTION,
                            resources=["*"]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.SNS_PUBLISH,
                            resources=[
                                self.sns_failed_topic.topic_arn,
                                self.sns_success_topic.topic_arn
                            ]
                        )
                    ]
                )
            }
        )
        self.step_function_role = self.builder.build_role(sf_role_config)
        
        # Role para Lambda
        lambda_role_config = RoleConfig(
            role_name=f"{self.PROJECT_CONFIG.app_config['business_process']}-fn",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ],
            inline_policies={
                "GlueAccessPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.join_permissions(
                                PolicyUtils.GLUE_START_JOB, 
                                PolicyUtils.CRAWLER_START_JOB),
                            resources=["*"]
                        )
                    ]
                )
            }
        )
        self.lambda_role = self.builder.build_role(lambda_role_config)
        
    def _deploy_assets_to_s3(self):
        """Deploy Glue scripts and Lambda layers to S3"""
        
        # Deploy Glue layer
        layer_config = S3DeploymentConfig(
            name=f"S3DeploymentLayer{self.PROJECT_CONFIG.app_config['business_process']}",
            sources=[s3_deployment.Source.asset(self.Paths.LOCAL_ARTIFACTS_GLUE_LAYER)],
            destination_bucket=self.s3_artifacts_bucket,
            destination_key_prefix=self.Paths.AWS_ARTIFACTS_GLUE_LAYER
        )
        self.builder.deploy_s3_bucket(layer_config)
        
        # Deploy CSV configs
        csv_config = S3DeploymentConfig(
            name=f"S3DeploymentCSV-{self.PROJECT_CONFIG.app_config['business_process']}", 
            sources=[s3_deployment.Source.asset(self.Paths.LOCAL_ARTIFACTS_GLUE_CSV)],
            destination_bucket=self.s3_artifacts_bucket,
            destination_key_prefix=self.Paths.AWS_ARTIFACTS_GLUE_CSV
        )
        self.builder.deploy_s3_bucket(csv_config)
        
        # Deploy Glue code
        code_config = S3DeploymentConfig(
            name=f"S3DeploymentCode-{self.PROJECT_CONFIG.app_config['business_process']}",
            sources=[s3_deployment.Source.asset(self.Paths.LOCAL_ARTIFACTS_GLUE_CODE)],
            destination_bucket=self.s3_artifacts_bucket,
            destination_key_prefix=self.Paths.AWS_ARTIFACTS_GLUE_CODE
        )
        self.builder.deploy_s3_bucket(code_config)
        
    def _create_lambda_functions(self):
        """Crear funciones Lambda usando aje-cdk-libs"""
        
        # Lambda para validar ejecución de jobs
        get_data_config = LambdaConfig(
            function_name=f"analytics-{self.PROJECT_CONFIG.app_config['business_process']}-get-data",
            handler="get_data/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE_ANALYTICS}",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=512,
            timeout=Duration.minutes(10),
            role=self.lambda_role
        )
        self.lambda_get_data = self.builder.build_lambda_function(get_data_config)
        
        # Lambda para manejar crawlers
        crawler_config = LambdaConfig(
            function_name=f"analytics-{self.PROJECT_CONFIG.app_config['business_process']}-crawler",
            handler="crawler/lambda_function.lambda_handler", 
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE_ANALYTICS}",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=512,
            timeout=Duration.minutes(10),
            role=self.lambda_role
        )
        self.lambda_crawler = self.builder.build_lambda_function(crawler_config)
        
    def _create_glue_connection(self):
        """Crear conexión Glue para Redshift (opcional)"""
        enable_redshift = os.getenv("ENABLE_REDSHIFT", "false").lower() == "true"
        
        if enable_redshift:
            # Obtener parámetros de conexión desde variables de entorno
            redshift_jdbc = os.getenv("GLUE_CONN_REDSHIFT_JDBC")
            redshift_user = os.getenv("GLUE_CONN_REDSHIFT_USER") 
            redshift_pass = os.getenv("GLUE_CONN_REDSHIFT_PASS")
            redshift_sg = os.getenv("GLUE_CONN_REDSHIFT_SG")
            redshift_subnet = os.getenv("GLUE_CONN_REDSHIFT_SUBNET")
            redshift_az = os.getenv("GLUE_CONN_REDSHIFT_AVAILABILITY_ZONE")
            
            if all([redshift_jdbc, redshift_user, redshift_pass, redshift_sg, redshift_subnet, redshift_az]):
                self.glue_redshift_connection = glue.CfnConnection(
                    self, "GlueRedshiftConnection",
                    catalog_id=self.account,
                    connection_input=glue.CfnConnection.ConnectionInputProperty(
                        name=f"{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-redshift-connection",
                        connection_properties={
                            "JDBC_CONNECTION_URL": redshift_jdbc,
                            "USERNAME": redshift_user,
                            "PASSWORD": redshift_pass,
                        },
                        connection_type="JDBC",
                        physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                            availability_zone=redshift_az,
                            security_group_id_list=[redshift_sg],
                            subnet_id=redshift_subnet,
                        )
                    )
                )
                self.builder.tag_resource(self.glue_redshift_connection, 
                                        f"glue-redshift-connection", "AWS Glue")
            else:
                print("Redshift connection parameters incomplete")
                self.glue_redshift_connection = None
        else:
            self.glue_redshift_connection = None

    def _create_glue_jobs(self):
        """Crear Glue jobs usando aje-cdk-libs"""
        
        # Cargar configuración de jobs desde CSV
        jobs_config = self._load_jobs_configuration()
        
        # Argumentos base para todos los jobs
        base_args = self._build_base_job_arguments()
        
        # Crear jobs por capa
        for layer, jobs_data in jobs_config.items():
            if not jobs_data:
                continue
                
            self._create_jobs_for_layer(layer, jobs_data, base_args)
            
    def _load_jobs_configuration(self) -> dict:
        """Cargar configuración de jobs desde archivos CSV"""
        
        jobs_by_layer = {}
        
        file_layer_mapping = {
            'domain.csv': 'domain'
        }
        
        for filename, layer_name in file_layer_mapping.items():
            csv_path = f"{self.Paths.LOCAL_ARTIFACTS_CONFIGURE_CSV}/{filename}"
            
            try:
                df = pd.read_csv(csv_path, delimiter=';')
                if not df.empty:
                    jobs_data = df.to_dict('records')
                    jobs_by_layer[layer_name] = jobs_data
                    self._process_job_relations(jobs_data, layer_name)
                    print(f"Loaded {len(jobs_data)} jobs for layer '{layer_name}'")
                else:
                    jobs_by_layer[layer_name] = []
            except Exception as e:
                print(f"Error loading {filename}: {str(e)}")
                jobs_by_layer[layer_name] = []
                
        return jobs_by_layer
        
    def _process_job_relations(self, jobs_data: list, layer_name: str):
        """Procesar relaciones de jobs para Step Functions"""
        
        for job_data in jobs_data:
            process_id = str(job_data.get('process_id', '10'))
            
            if process_id not in self.relation_process:
                self.relation_process[process_id] = []
                
            self.relation_process[process_id].append({
                'table': job_data['procedure'],
                'periods': job_data.get('periods', 2),
                'layer': layer_name
            })
            
    def _build_base_job_arguments(self) -> dict:
        """Construir argumentos base para jobs de Glue"""
        
        base_args = {
            '--extra-py-files': f's3://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_GLUE_LAYER}/common_jobs_functions.py',
            '--S3_PATH_STG': f"s3://{self.s3_stage_bucket.bucket_name}/{self.PROJECT_CONFIG.app_config['team']}/",
            '--S3_PATH_ANALYTICS': f"s3://{self.s3_analytics_bucket.bucket_name}/{self.PROJECT_CONFIG.app_config['team']}/",
            '--S3_PATH_EXTERNAL': f"s3://{self.s3_external_bucket.bucket_name}/{self.PROJECT_CONFIG.app_config['team']}/",
            '--S3_PATH_ARTIFACTS': f"s3://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_GLUE}/",
            '--S3_PATH_ARTIFACTS_CSV': f"s3://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_GLUE_CSV}",
            '--S3_PATH_ARTIFACTS_CONFIG': f"s3://{self.s3_artifacts_bucket.bucket_name}/",
            '--PROJECT_NAME': self.PROJECT_CONFIG.app_config["team"],
            '--TEAM': self.PROJECT_CONFIG.app_config['team'],
            '--BUSINESS_PROCESS': self.PROJECT_CONFIG.app_config['business_process'],
            '--REGION_NAME': self.PROJECT_CONFIG.region_name,
            '--DYNAMODB_DATABASE_NAME': self.dynamodb_credentials_table.table_name,
            '--DYNAMODB_COLUMNS_NAME': self.dynamodb_columns_table.table_name,
            '--DYNAMODB_LOGS_TABLE': self.dynamodb_logs_table.table_name,
            '--COD_PAIS': 'PE',
            '--INSTANCIAS': 'PE',
            '--ERROR_TOPIC_ARN': self.sns_failed_topic.topic_arn,
            '--datalake-formats': "delta",
            '--enable-continuous-log-filter': "true",
            '--LOAD_PROCESS': '10',
            '--ORIGIN': 'AJE'
        }
        
        # Agregar relaciones de proceso
        for key, value in self.relation_process.items():
            base_args[f'--P{key}'] = json.dumps(value)
            
        return base_args
        
    def _create_jobs_for_layer(self, layer: str, jobs_data: list, base_args: dict):
        """Crear jobs de Glue para una capa específica"""
        
        # Inicializar estructuras para la capa
        if layer not in self.glue_jobs:
            self.glue_jobs[layer] = {}
        if layer not in self.cr_targets:
            self.cr_targets[layer] = []
        if layer not in self.state_machine_order_list:
            self.state_machine_order_list[layer] = {}
            
        for job_data in jobs_data:
            procedure_name = job_data['procedure']
            
            # Crear configuración del job usando aje-cdk-libs
            job_config = GlueJobConfig(
                job_name=f"{layer}_{self.datasource}_{procedure_name}",
                executable=glue_alpha.JobExecutable.python_etl(
                    glue_version=self._get_glue_version(job_data.get('glue_version', '4')),
                    python_version=glue_alpha.PythonVersion.THREE,
                    script=glue_alpha.Code.from_bucket(
                        self.s3_artifacts_bucket,
                        f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE}/{layer}/{procedure_name}.py"
                    )
                ),
                default_arguments={
                    **base_args,
                    '--PROCESS_NAME': f"{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-{layer}-{procedure_name}-job",
                    '--FLOW_NAME': layer,
                    '--PERIODS': str(job_data.get('periods', 2))
                },
                worker_type=self._get_worker_type(job_data.get('worker_type', 'G.1X')),
                worker_count=job_data.get('num_workers', 2),
                continuous_logging=glue_alpha.ContinuousLoggingProps(enabled=True),
                timeout=Duration.hours(1),
                max_concurrent_runs=100,
                role=self.glue_job_role
            )
            
            # Crear job usando ResourceBuilder
            glue_job = self.builder.build_glue_job(job_config)
            self.glue_jobs[layer][procedure_name] = glue_job
            
            # Agregar target para crawler
            s3_target_path = f"{self.PROJECT_CONFIG.app_config['team']}/{self.PROJECT_CONFIG.app_config['business_process']}/{layer}/{procedure_name}/"
            self.cr_targets[layer].append(
                cdk.aws_glue.CfnCrawler.DeltaTargetProperty(
                    create_native_delta_table=False,
                    delta_tables=[f"s3://{self.s3_analytics_bucket.bucket_name}/{s3_target_path}"],
                    write_manifest=True
                )
            )
            
            # Organizar para Step Functions
            exe_order = job_data.get('exe_order', 0)
            if exe_order > 0:
                self._add_to_execution_order(layer, job_data, glue_job.job_name)
                
            print(f"Created Glue job: {glue_job.job_name}")

    def _add_to_execution_order(self, layer: str, job_data: dict, job_name: str):
        """Agregar job al orden de ejecución de Step Functions"""
        
        exe_order = job_data.get('exe_order', 0)
        order_key = str(exe_order)
        
        if order_key not in self.state_machine_order_list[layer]:
            self.state_machine_order_list[layer][order_key] = []
            
        self.state_machine_order_list[layer][order_key].append({
            'table': job_data['procedure'],
            'process_id': str(job_data.get('process_id', '10')),
            'job_name': job_name
        })

    def _create_glue_databases_and_crawlers(self):
        """Crear bases de datos y crawlers de Glue"""
        
        self.databases_layers = {}
        self.crawlers_layers = {}
        
        for layer_key, targets in self.cr_targets.items():
            if not targets:
                continue
                
            # Crear database
            database_name = f"{self.PROJECT_CONFIG.app_config['team']}_{self.PROJECT_CONFIG.app_config['business_process']}_{layer_key}"
            
            database = cdk.aws_glue.CfnDatabase(
                self, f"Database{layer_key.title()}",
                catalog_id=self.account,
                database_input=cdk.aws_glue.CfnDatabase.DatabaseInputProperty(
                    name=database_name
                )
            )
            self.databases_layers[layer_key] = database
            
            # Crear crawler
            crawler_name = f"{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-{layer_key}-crawler"
            
            crawler = cdk.aws_glue.CfnCrawler(
                self, f"Crawler{layer_key.title()}",
                name=crawler_name,
                database_name=database_name,
                role=self.glue_crawler_role.role_arn,
                targets=cdk.aws_glue.CfnCrawler.TargetsProperty(delta_targets=targets),
                schema_change_policy=cdk.aws_glue.CfnCrawler.SchemaChangePolicyProperty(
                    update_behavior="UPDATE_IN_DATABASE",
                    delete_behavior="DEPRECATE_IN_DATABASE"
                )
            )
            self.crawlers_layers[layer_key] = crawler
            
            self.builder.tag_resource(database, f"database-{layer_key}", "AWS Glue")
            self.builder.tag_resource(crawler, f"crawler-{layer_key}", "AWS Glue")

    def _create_step_functions(self):
        """Crear Step Functions usando aje-cdk-libs"""
        
        # 1. PRIMERO: Crear State Machine base SIN referenciar otras State Machines
        base_definition = self._build_base_state_machine_definition()
        base_config = StepFunctionConfig(
            name=f"workflow_{self.PROJECT_CONFIG.app_config['business_process']}_base",
            definition_body=sfn.DefinitionBody.from_chainable(base_definition),
            role=self.step_function_base_role
        )
        
        self.state_machine_base = self.builder.build_step_function(base_config)

        # 2. SEGUNDO: Crear State Machines por capa
        for layer in self.state_machine_order_list.keys():
            if layer in self.crawlers_layers:
                # Crear definición SIN referenciar state_machine_base en la definición
                layer_definition = self._build_layer_state_machine_definition(
                    layer, 
                    self.crawlers_layers[layer].name
                )

                layer_config = StepFunctionConfig(
                    name=f"workflow_{self.PROJECT_CONFIG.app_config.get('business_process')}_{layer}",
                    definition_body=sfn.DefinitionBody.from_chainable(layer_definition),
                    role=self.step_function_role
                )

                layer_sf = self.builder.build_step_function(layer_config)
                setattr(self, f"state_machine_{layer}", layer_sf)
        
        # 3. TERCERO: Actualizar permisos del role DESPUÉS de crear las State Machines
        self._update_step_function_permissions()

    def _build_layer_state_machine_definition(self, layer: str, crawler_name: str):
        """Construir definición de Step Function SIN referenciar otras State Machines"""
        
        jobs_by_order = self.state_machine_order_list.get(layer, {})
        
        current_state = None
        initial_state = None
        
        # Procesar jobs por orden de ejecución
        for order in sorted(jobs_by_order.keys()):
            if int(order) <= 0:
                continue
                
            # Crear estado paralelo para este orden
            parallel_state = sfn.Parallel(
                self, f"{layer}_order_{order}",
                result_path="$.execution_results"
            )
            
            # Agregar cada job como rama paralela
            for job_config in jobs_by_order[order]:
                job_execution = tasks.StepFunctionsStartExecution(
                    self, f"{layer}_{job_config['table']}",
                    state_machine=self.state_machine_base,
                    input=sfn.TaskInput.from_object({
                        "exe_process_id.$": "$.exe_process_id",
                        "own_process_id": job_config['process_id'],
                        "job_name": job_config['job_name'],
                        "COD_PAIS.$": "$.COD_PAIS",
                        "INSTANCIAS.$": "$.INSTANCIAS",
                        "redshift": False
                    }),
                    integration_pattern=sfn.IntegrationPattern.RUN_JOB
                )
                parallel_state.branch(job_execution)
                
            # Conectar estados secuencialmente
            if current_state is None:
                initial_state = parallel_state
            else:
                current_state.next(parallel_state)
                
            current_state = parallel_state
        
        # Agregar crawler al final (mismo código que antes)
        if crawler_name and current_state:
            crawler_task = tasks.LambdaInvoke(
                self, f"run_crawler_{layer}",
                lambda_function=self.lambda_crawler,
                payload=sfn.TaskInput.from_object({
                    "CRAWLER_NAME": crawler_name
                }),
                result_path="$.crawler_result"
            )
            
            crawler_wait = sfn.Wait(
                self, f"wait_crawler_{layer}",
                time=sfn.WaitTime.duration(Duration.seconds(60))
            )
            
            crawler_choice = sfn.Choice(self, f"check_crawler_{layer}")
            
            start_crawler = tasks.CallAwsService(
                self, f"start_crawler_{layer}",
                service="glue",
                action="startCrawler",
                parameters={"Name": crawler_name},
                iam_resources=["*"]
            )
            
            crawler_task.next(
                crawler_choice
                .when(
                    sfn.Condition.boolean_equals("$.crawler_result.Payload.wait", True),
                    crawler_wait.next(crawler_task)
                )
                .otherwise(start_crawler)
            )
            
            current_state.next(crawler_task)
            
        return initial_state or sfn.Succeed(self, f"no_jobs_{layer}")

    def _update_step_function_permissions(self):
        """Actualizar permisos del role DESPUÉS de crear todas las State Machines"""
        
        # Crear política separada para evitar dependencias circulares
        policy_statements = []
        
        # Agregar permisos para cada State Machine creada
        for layer in self.state_machine_order_list.keys():
            if hasattr(self, f"state_machine_{layer}"):
                state_machine = getattr(self, f"state_machine_{layer}")
                policy_statements.append(
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "states:StartExecution",
                            "states:StopExecution", 
                            "states:DescribeExecution"
                        ],
                        resources=[state_machine.state_machine_arn]
                    )
                )
        
        # Crear política independiente
        if policy_statements:
            step_function_policy = iam.Policy(
                self, "StepFunctionCrossExecutionPolicy",
                statements=policy_statements
            )
            step_function_policy.attach_to_role(self.step_function_role)

    def _build_base_state_machine_definition(self):
        """Construir definición base de Step Function para validación de jobs"""
        
        # Task: Validar ejecución concurrente
        validate_execution = tasks.LambdaInvoke(
            self, "ValidateExecution",
            lambda_function=self.lambda_get_data,
            payload=sfn.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "own_process_id.$": "$.own_process_id", 
                "job_name.$": "$.job_name",
                "COD_PAIS.$": "$.COD_PAIS",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "redshift.$": "$.redshift"
            }),
            result_path="$.validation_result"
        )
        
        # Wait state para concurrencia
        wait_state = sfn.Wait(
            self, "WaitForConcurrency",
            time=sfn.WaitTime.duration(Duration.seconds(60))
        )
        
        # Task: Ejecutar job de Glue
        execute_glue_job = tasks.GlueStartJobRun(
            self, "ExecuteGlueJob",
            glue_job_name=sfn.JsonPath.string_at("$.job_name"),
            arguments=sfn.TaskInput.from_object({
                "--COD_PAIS.$": "$.COD_PAIS",
                "--INSTANCIAS.$": "$.INSTANCIAS"
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB
        )
        
        # Error notification
        error_notification = tasks.SnsPublish(
            self, "NotifyExecutionError",
            topic=self.sns_failed_topic,
            message=sfn.TaskInput.from_object({
                "error": "Job execution failed",
                "job_name.$": "$.job_name",
                "timestamp.$": "$$.State.EnteredTime"
            })
        )
        
        # Choice state para determinar acción
        execution_choice = sfn.Choice(self, "ExecutionChoice")
        
        # Construir flujo
        validate_execution.next(
            execution_choice
            .when(
                sfn.Condition.boolean_equals("$.validation_result.Payload.wait", True),
                wait_state.next(validate_execution)
            )
            .when(
                sfn.Condition.boolean_equals("$.validation_result.Payload.error", True),
                error_notification
            )
            .when(
                sfn.Condition.boolean_equals("$.validation_result.Payload.ready", True),
                execute_glue_job
            )
            .otherwise(sfn.Succeed(self, "SkipExecution"))
        )
        
        return validate_execution

    def _build_layer_state_machine(self, layer: str, crawler: cdk.aws_glue.CfnCrawler = None):
        """Crear Step Function para una capa específica"""
        layer_definition = self._build_layer_state_machine_definition(layer, crawler.name)
        layer_config = StepFunctionConfig(
            name=f"{self.PROJECT_CONFIG.app_config['business_process']}_analytics_{layer}",
            definition_body=sfn.DefinitionBody.from_chainable(layer_definition),
            role=self.step_function_role
        )
        
        layer_sm = self.builder.build_step_function(
            layer_config
        )

        setattr(self, f'state_machine_{layer}', layer_sm)
        
        return layer_sm

    def _build_orchestration_definition(self):
        """Construir definición de orquestación principal"""
        
        # Obtener referencias a las state machines por capa
        domain_sm = getattr(self, 'state_machine_domain', None)
        analytics_sm = getattr(self, 'state_machine_analytics', None)
        
        if domain_sm:
            # Ejecutar dominio primero
            execute_domain = tasks.StepFunctionsStartExecution(
                self, "ExecuteDomain",
                state_machine=domain_sm,
                input=sfn.TaskInput.from_object({
                    "exe_process_id.$": "$.exe_process_id",
                    "COD_PAIS.$": "$.COD_PAIS",
                    "INSTANCIAS.$": "$.INSTANCIAS"
                }),
                integration_pattern=sfn.IntegrationPattern.RUN_JOB
            )
            
            if analytics_sm:
                # Ejecutar analytics después de dominio
                execute_analytics = tasks.StepFunctionsStartExecution(
                    self, "ExecuteAnalytics",
                    state_machine=analytics_sm,
                    input=sfn.TaskInput.from_object({
                        "exe_process_id.$": "$.exe_process_id",
                        "COD_PAIS.$": "$.COD_PAIS",
                        "INSTANCIAS.$": "$.INSTANCIAS"
                    }),
                    integration_pattern=sfn.IntegrationPattern.RUN_JOB
                )
                
                # Notificación de éxito
                success_notification = tasks.SnsPublish(
                    self, "NotifyOrchestrationSuccess",
                    topic=self.sns_success_topic,
                    message=sfn.TaskInput.from_object({
                        "status": "SUCCESS",
                        "message": "Domain analytics pipeline completed successfully",
                        "timestamp.$": "$$.State.EnteredTime"
                    })
                )
                
                return execute_domain.next(execute_analytics).next(success_notification)
            else:
                # Solo dominio
                success_notification = tasks.SnsPublish(
                    self, "NotifyDomainSuccess",
                    topic=self.sns_success_topic,
                    message=sfn.TaskInput.from_object({
                        "status": "SUCCESS",
                        "message": "Domain processing completed successfully",
                        "timestamp.$": "$$.State.EnteredTime"
                    })
                )
                
                return execute_domain.next(success_notification)
        else:
            return sfn.Succeed(self, "NoWorkflowsDefined")

    def _get_glue_version(self, version_str: str) -> glue_alpha.GlueVersion:
        """Convertir string de versión a enum GlueVersion"""
        version_map = {
            '3': glue_alpha.GlueVersion.V3_0,
            '4': glue_alpha.GlueVersion.V4_0,
            '5': glue_alpha.GlueVersion.V4_0
        }
        return version_map.get(str(version_str), glue_alpha.GlueVersion.V4_0)

    def _get_worker_type(self, worker_str: str) -> glue_alpha.WorkerType:
        """Convertir string de worker a enum WorkerType"""
        worker_map = {
            'G.1X': glue_alpha.WorkerType.G_1_X,
            'G.2X': glue_alpha.WorkerType.G_2_X
        }
        return worker_map.get(worker_str, glue_alpha.WorkerType.G_1_X)