import os
import aws_cdk as cdk
#from stacks.cdk_datalake_analytics_comercial import CdkDatalakeAnaliticsComercialStack
from stacks.cdk_datalake_analytics_stack import CdkDatalakeAnalyticsStack
from stacks.cdk_datalake_redshift_stack import CdkDatalakeRedshiftStack
from aje_cdk_libs.constants.environments import Environments
from aje_cdk_libs.constants.project_config import ProjectConfig
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

# Stack para Analytics (Glue Jobs de procesamiento)
analytics_stack = CdkDatalakeAnalyticsStack(
    app,
    "CdkDatalakeAnalyticsStack",
    project_config,
    env=cdk.Environment(
        account=project_config.account_id,
        region=project_config.region_name
    )
)

# Stack para Redshift (opcional, habilitado por parámetro)
enable_redshift = os.getenv("ENABLE_REDSHIFT", "false").lower() == "true"

if enable_redshift:
    redshift_stack = CdkDatalakeRedshiftStack(
        app,
        "CdkDatalakeRedshiftStack", 
        project_config,
        analytics_resources={
            "glue_connection": analytics_stack.glue_redshift_connection,
            "lambda_get_data": analytics_stack.lambda_get_data,
            "sns_failed_topic": analytics_stack.sns_failed_topic,
            "sns_success_topic": analytics_stack.sns_success_topic,
            "glue_job_role": analytics_stack.glue_job_role,
            "step_function_role": analytics_stack.step_function_role,
            "state_machine_base": analytics_stack.state_machine_base
        },
        env=cdk.Environment(
            account=project_config.account_id,
            region=project_config.region_name
        )
    )
    
    # Dependencia entre stacks
    redshift_stack.add_dependency(analytics_stack)

app.synth()