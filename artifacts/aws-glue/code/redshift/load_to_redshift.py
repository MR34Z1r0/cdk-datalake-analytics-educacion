import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, ENVIRONMENT

spark_controller = SPARK_CONTROLLER()
data_paths_instance = data_paths()

args = getResolvedOptions(
    sys.argv,
    [
        "CATALOG_CONNECTION",
        "REGION_NAME",
        "ERROR_TOPIC_ARN",
        "PROCESS_NAME",
        "LOAD_PROCESS",
        "ORIGIN",
    ],
)

CATALOG_CONNECTION = args["CATALOG_CONNECTION"]
ERROR_TOPIC_ARN = args["ERROR_TOPIC_ARN"]
LOAD_PROCESS = args["LOAD_PROCESS"]

# e.g. ["10"] -> into list ["10"]
LOAD_PROCESS_LIST = LOAD_PROCESS.split(",")

TABLE_LIST = []
try:
    for process in LOAD_PROCESS_LIST:
        TABLE_LIST.append(json.loads(getResolvedOptions(sys.argv, [f"P{process}"])[f"P{process}"]))
    print(TABLE_LIST)
except Exception as e:
    logger.error(e) 
    raise

try:
    cod_pais = spark_controller.get_cod_pais_list_from_credentials()
    url, properties = spark_controller.get_catalog_connection_redshift(CATALOG_CONNECTION)
    for tables_list_details in TABLE_LIST:
        for table_details in tables_list_details:
            try:
                table_name = table_details["table"]
                table_periods = table_details["periods"]
                table_layer = table_details["layer"]
                periods_filter=[]
                print(f"Table: {table_name}, Periods: {table_periods}")
                if "comercial" not in table_layer:
                    continue

                #Get table PATH
                table_path = data_paths_instance.getDataPath(table_layer)

                df = spark_controller.read_table(table_path,table_details["table"])
                df = df.filter(col("id_pais").isin(cod_pais))
                if not (table_name.startswith("m_") or table_name.startswith("dim_")):
                    periods_filter = spark_controller.get_periods(periods=table_periods)
                    df = df.filter(col("id_periodo").isin(periods_filter))
                redshift_table_name = f"{table_layer}_analytics_{ENVIRONMENT.lower()}.{table_name}"
                print(redshift_table_name)
                spark_controller.load_to_redshift(df, properties, url, redshift_table_name, cod_pais, periods_filter)
            except Exception as e:
                logger.error(f"Error processing table {table_name}: {str(e)}")
                spark_controller.logger.send_error_redshift_message(
                    ERROR_TOPIC_ARN,
                    table_name,
                    str(e)
                )

except Exception as e:
    logger.error(f"Failed to write to Redshift: {str(e)}")
    raise
