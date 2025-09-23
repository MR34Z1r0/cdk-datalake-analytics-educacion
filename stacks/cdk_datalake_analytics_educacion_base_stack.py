from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_glue_alpha as glue_alpha,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_s3_deployment as s3_deployment,
    aws_sns as sns
)
import aws_cdk as cdk
from constructs import Construct
from aje_cdk_libs.builders.resource_builder import ResourceBuilder
from aje_cdk_libs.models.configs import *
from aje_cdk_libs.constants.policies import PolicyUtils
from constants.paths import Paths
from constants.layers import Layers
import logging
import json
import os
import csv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CdkDatalakeAnalyticsEducacionBaseStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, project_config, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.PROJECT_CONFIG = project_config
        self.builder = ResourceBuilder(self, self.PROJECT_CONFIG)
        self.Paths = Paths(self.PROJECT_CONFIG.app_config)
        self.Layers = Layers(self.PROJECT_CONFIG.app_config, project_config.region_name, project_config.account_id)


        # Apply stack-level tags to all resources in this stack
        self._apply_stack_tags()

        # Crear recursos base en orden
        self._load_instances_configuration()
        self._import_existing_resources()
        self._create_iam_roles()
        self._deploy_assets_to_s3()
        self._create_lambda_functions()
        self._create_invoke_next_layer_lambda_function()
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
            
        logger.info("Applied stack-level tags for domain base stack")

    def _import_existing_resources(self):
        """Importar recursos S3, DynamoDB y SNS existentes"""

        # Buckets S3 usando aje-cdk-libs
        self.s3_artifacts_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["artifacts"])
        self.s3_external_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["external"])
        self.s3_stage_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["stage"])
        self.s3_analytics_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["analytics"])

        # Tablas DynamoDB usando aje-cdk-libs
        self.dynamodb_columns_table = self.builder.import_dynamodb_table(self.PROJECT_CONFIG.app_config["dynamodb_tables"]["columns-specifications"])
        self.dynamodb_configuration_table = self.builder.import_dynamodb_table(self.PROJECT_CONFIG.app_config["dynamodb_tables"]["configuration"])
        self.dynamodb_credentials_table = self.builder.import_dynamodb_table(self.PROJECT_CONFIG.app_config["dynamodb_tables"]["credentials"])
        self.dynamodb_logs_table = self.builder.import_dynamodb_table(self.PROJECT_CONFIG.app_config["dynamodb_tables"]["logs"])

        # Import SNS topics from analytics-core using CloudFormation imports
        self.sns_failed_topic = sns.Topic.from_topic_arn(self, "AnalyticsCoreSNSFailedTopic", cdk.Fn.import_value("AnalyticsCoreSNSFailedTopicArn"))
        self.sns_success_topic = sns.Topic.from_topic_arn(self, "AnalyticsCoreSNSSuccessTopic", cdk.Fn.import_value("AnalyticsCoreSNSSuccessTopicArn"))

        # Import analytics-core base Step Function using CloudFormation import
        self.analytics_core_base_step_function = sfn.StateMachine.from_state_machine_arn(self, "AnalyticsCoreBaseStepFunction", cdk.Fn.import_value("AnalyticsCoreStepFunctionBaseGlueArn"))

        # Import analytics-core domain crawler job name
        self.analytics_core_domain_crawler_job_name = cdk.Fn.import_value("AnalyticsCoreDomainCrawlerJobName")

        self.process_matrix_s3_path = cdk.Fn.import_value("AnalyticsCoreProcessMatrixS3Path")

        logger.info("Successfully imported analytics-core resources using CloudFormation exports")

    def _create_iam_roles(self):
        """Crear roles IAM usando aje-cdk-libs"""

        # Role para Glue Jobs usando RoleConfig
        glue_job_role_config = RoleConfig(
            role_name=f"{self.PROJECT_CONFIG.app_config['business_process']}-job",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")],
            inline_policies={
                "DataLakeAccessPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.join_permissions(PolicyUtils.S3_READ, PolicyUtils.S3_WRITE),
                            resources=[
                                self.s3_artifacts_bucket.bucket_arn,
                                f"{self.s3_artifacts_bucket.bucket_arn}/*",
                                self.s3_external_bucket.bucket_arn,
                                f"{self.s3_external_bucket.bucket_arn}/*",
                                self.s3_stage_bucket.bucket_arn,
                                f"{self.s3_stage_bucket.bucket_arn}/*",
                                self.s3_analytics_bucket.bucket_arn,
                                f"{self.s3_analytics_bucket.bucket_arn}/*",
                            ],
                        ),
                        iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=PolicyUtils.DYNAMODB_READ, resources=["*"]),
                        iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=PolicyUtils.DYNAMODB_WRITE, resources=["*"]),
                        iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=PolicyUtils.SNS_PUBLISH, resources=[self.sns_failed_topic.topic_arn]),
                        iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=PolicyUtils.LOGS_WRITE, resources=["*"]),
                    ]
                )
            },
        )
        self.glue_job_role = self.builder.build_role(glue_job_role_config)

        # Role para Step Functions
        sf_role_config = RoleConfig(
            role_name=f"{self.PROJECT_CONFIG.app_config['business_process']}-sf",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            inline_policies={
                "StepFunctionExecutionPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=PolicyUtils.join_permissions(PolicyUtils.GLUE_START_JOB, PolicyUtils.CRAWLER_START_JOB), resources=["*"]),
                        iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=PolicyUtils.LAMBDA_INVOKE, resources=["*"]),
                        iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=PolicyUtils.STEP_FUNCTIONS_START_EXECUTION, resources=["*"]),
                        iam.PolicyStatement(effect=iam.Effect.ALLOW, actions=PolicyUtils.SNS_PUBLISH, resources=[self.sns_failed_topic.topic_arn, self.sns_success_topic.topic_arn]),
                    ]
                )
            },
        )
        self.step_function_role = self.builder.build_role(sf_role_config)

        # Role para Lambda
        lambda_role_config = RoleConfig(
            role_name=f"{self.PROJECT_CONFIG.app_config['business_process']}-fn",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
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
                ),
                "StepFunctionAccessPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "states:StartExecution",
                                "states:DescribeExecution", 
                                "states:DescribeStateMachine",
                                "states:StopExecution"
                            ],
                            resources=["*"]
                        )
                    ]
                ),
                "SNSPublishPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["sns:Publish"],
                            resources=[
                                self.sns_failed_topic.topic_arn,
                                self.sns_success_topic.topic_arn
                            ]
                        )
                    ]
                ),
                "S3ReadAccessPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.S3_READ,
                            resources=[
                                self.s3_artifacts_bucket.bucket_arn,
                                f"{self.s3_artifacts_bucket.bucket_arn}/*"
                            ]
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
            destination_key_prefix=self.Paths.AWS_ARTIFACTS_GLUE_LAYER,
        )
        self.builder.deploy_s3_bucket(layer_config)

        # Deploy CSV configs
        csv_config = S3DeploymentConfig(
            name=f"S3DeploymentCSV-{self.PROJECT_CONFIG.app_config['business_process']}",
            sources=[s3_deployment.Source.asset(self.Paths.LOCAL_ARTIFACTS_CONFIGURE_CSV)],
            destination_bucket=self.s3_artifacts_bucket,
            destination_key_prefix=self.Paths.AWS_ARTIFACTS_CONFIGURE_CSV,
        )
        self.builder.deploy_s3_bucket(csv_config)

        # Deploy Glue code
        code_config = S3DeploymentConfig(
            name=f"S3DeploymentCode-{self.PROJECT_CONFIG.app_config['business_process']}",
            sources=[s3_deployment.Source.asset(self.Paths.LOCAL_ARTIFACTS_GLUE_CODE)],
            destination_bucket=self.s3_artifacts_bucket,
            destination_key_prefix=self.Paths.AWS_ARTIFACTS_GLUE_CODE,
        )
        self.builder.deploy_s3_bucket(code_config)

    def _create_lambda_functions(self):
        """Crear funciones Lambda usando aje-cdk-libs"""

        # Lambda para validar ejecución de jobs
        get_data_config = LambdaConfig(
            function_name=f"analytics-{self.PROJECT_CONFIG.app_config['business_process']}-get-data",
            handler="get_data/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE_ANALYTICS}",
            runtime=_lambda.Runtime("python3.13"),
            memory_size=512,
            timeout=Duration.minutes(10),
            role=self.lambda_role,
        )
        self.lambda_get_data = self.builder.build_lambda_function(get_data_config)

    def _create_invoke_next_layer_lambda_function(self):
        """Create the lambda function that invokes the next layer step function (comercial/cadena/etc)"""
        
        lambda_tags = self._create_job_tags("NextLayerLambda")
        
        lambda_config = LambdaConfig(
            function_name=f"{self.PROJECT_CONFIG.app_config['business_process']}_invoke_next_layer_sf",
            handler="lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE_ANALYTICS}/invoke_next_layer_step_function",
            timeout=Duration.minutes(15),
            runtime=_lambda.Runtime("python3.13"),
            role=self.lambda_role,
            environment={
                'DATALAKE_AWS_REGION': Stack.of(self).region,
                'DATALAKE_AWS_ACCOUNT_ID': Stack.of(self).account,
                'DATALAKE_ENVIRONMENT': self.PROJECT_CONFIG.environment.value,
                'PROCESS_MATRIX_S3_PATH': self.process_matrix_s3_path,
            },
        )
        
        self.invoke_next_layer_lambda_function = self.builder.build_lambda_function(lambda_config)
        
        # Add tags to the lambda function
        for key, value in lambda_tags.items():
            cdk.Tags.of(self.invoke_next_layer_lambda_function).add(key, value)

    def _create_job_tags(self, job_type, endpoint=None):
        """Generate resource tags"""
        tags = {
            'DataSource': self.PROJECT_CONFIG.app_config.get('business_process', ''),
            'Process': 'Analytic',
            'Environment': self.PROJECT_CONFIG.environment.value,
            'SubProcess': job_type
        }
        return tags

    def _create_outputs(self):
        """Crear outputs para group stacks"""

        # S3 Buckets
        cdk.CfnOutput(self, "S3ArtifactsBucketName", value=self.s3_artifacts_bucket.bucket_name)
        cdk.CfnOutput(self, "S3ExternalBucketName", value=self.s3_external_bucket.bucket_name)
        cdk.CfnOutput(self, "S3StageBucketName", value=self.s3_stage_bucket.bucket_name)
        cdk.CfnOutput(self, "S3AnalyticsBucketName", value=self.s3_analytics_bucket.bucket_name)

        # DynamoDB Tables
        cdk.CfnOutput(self, "DynamoColumnsTableName", value=self.dynamodb_columns_table.table_name)
        cdk.CfnOutput(self, "DynamoConfigurationTableName", value=self.dynamodb_configuration_table.table_name)
        cdk.CfnOutput(self, "DynamoCredentialsTableName", value=self.dynamodb_credentials_table.table_name)
        cdk.CfnOutput(self, "DynamoLogsTableName", value=self.dynamodb_logs_table.table_name)

        # SNS Topics
        cdk.CfnOutput(self, "SnsFailedTopicArn", value=self.sns_failed_topic.topic_arn)
        cdk.CfnOutput(self, "SnsSuccessTopicArn", value=self.sns_success_topic.topic_arn)

        # IAM Roles
        cdk.CfnOutput(self, "GlueJobRoleArn", value=self.glue_job_role.role_arn)
        cdk.CfnOutput(self, "StepFunctionRoleArn", value=self.step_function_role.role_arn)
        cdk.CfnOutput(self, "LambdaRoleArn", value=self.lambda_role.role_arn)

        # Lambda Functions
        cdk.CfnOutput(self, "LambdaGetDataArn", value=self.lambda_get_data.function_arn)
        cdk.CfnOutput(self, "InvokeNextLayerLambdaFunctionArn", value=self.invoke_next_layer_lambda_function.function_arn, description="ARN of the lambda function that invokes next layer step function")
        cdk.CfnOutput(self, "InvokeNextLayerLambdaFunctionName", value=self.invoke_next_layer_lambda_function.function_name, description="Name of the lambda function that invokes next layer step function")

        # Analytics Core imports
        cdk.CfnOutput(self, "AnalyticsCoreBaseStepFunctionArn", value=self.analytics_core_base_step_function.state_machine_arn)
        cdk.CfnOutput(self, "AnalyticsCoreDomainCrawlerJobName", value=self.analytics_core_domain_crawler_job_name)

    def _load_instances_configuration(self):
        """Load and filter instances configuration from CSV"""
        
        # Load instances from local CSV file
        instances_csv_path = f"{self.Paths.LOCAL_ARTIFACTS_CONFIGURE_CSV}/instances.csv"
        
        self.active_instances = []
        
        try:
            # Get current environment as string, handling both string and enum cases
            if hasattr(self.PROJECT_CONFIG.environment, 'value'):
                current_env = self.PROJECT_CONFIG.environment.value.upper()
            else:
                current_env = str(self.PROJECT_CONFIG.environment).upper()
            logger.info(f"Loading instances for environment: {current_env}")
            
            with open(instances_csv_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=';')
                all_instances = []
                for row in reader:
                    all_instances.append(row)
                    # Filter by active status and current environment
                    csv_env = row.get('environment', '').upper()
                    csv_status = row.get('status', '').lower()
                    csv_instance = row.get('instance', '').strip()
                    
                    environment_match = csv_env == current_env
                    status_active = csv_status == 'a'
                    
                    logger.info(f"Processing instance: {csv_instance}, env: '{csv_env}' (current: '{current_env}'), status: '{csv_status}'")
                    logger.info(f"  Environment match: {environment_match}, Status active: {status_active}")
                    
                    if status_active and environment_match:
                        self.active_instances.append({
                            'instance': csv_instance,
                            'countries': row.get('countries', '').strip(),
                            'environment': row.get('environment', '').strip()
                        })
                        logger.info(f"  -> Added active instance: {csv_instance}")
                        
            logger.info(f"Loaded {len(self.active_instances)} active instances for environment {current_env}")
            for instance in self.active_instances:
                logger.info(f"  Active Instance: {instance['instance']}, Countries: {instance['countries']}")
                
        except Exception as e:
            logger.error(f"Error loading instances configuration: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            self.active_instances = []

    def get_active_instances(self):
        """Get list of active instances for current environment"""
        return [inst['instance'] for inst in self.active_instances]
    
    def get_countries_for_instance(self, instance_name):
        """Get list of countries for a specific instance"""
        for inst in self.active_instances:
            if inst['instance'] == instance_name:
                return [c.strip() for c in inst['countries'].split(',') if c.strip()]
        return []
