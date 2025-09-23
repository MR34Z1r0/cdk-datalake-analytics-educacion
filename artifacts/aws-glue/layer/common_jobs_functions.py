import logging
import os
import boto3
import datetime as dt
import sys
import pytz
from io import StringIO
import csv
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType, TimestampType, FloatType

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGGING", logging.INFO))

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_PATH_STG",
        "S3_PATH_ANALYTICS",
        "S3_PATH_EXTERNAL",
        "S3_PATH_ARTIFACTS",
        "S3_PATH_ARTIFACTS_CSV",
        "S3_PATH_ARTIFACTS_CONFIG",
        "S3_PATH_STAGE_CSV",
        "TEAM",
        "BUSINESS_PROCESS",
        "REGION_NAME", 
        "INSTANCE",
        "DYNAMODB_LOGS_TABLE",
        "ERROR_TOPIC_ARN",
        "PROJECT_NAME",
        "ENVIRONMENT",
        "PROCESS_NAME",
        "FLOW_NAME",
        "PERIODS",
        "TABLE_NAME",
    ],
)

JOB_NAME = args["JOB_NAME"]
S3_PATH_STG = args["S3_PATH_STG"]
S3_PATH_ANALYTICS = args["S3_PATH_ANALYTICS"]
S3_PATH_EXTERNAL = args["S3_PATH_EXTERNAL"]
S3_PATH_ARTIFACTS = args["S3_PATH_ARTIFACTS"]
S3_PATH_ARTIFACTS_CSV = args["S3_PATH_ARTIFACTS_CSV"]
S3_PATH_ARTIFACTS_CONFIG = args["S3_PATH_ARTIFACTS_CONFIG"]
S3_PATH_STAGE_CSV = args["S3_PATH_STAGE_CSV"]
TEAM = args["TEAM"]
BUSINESS_PROCESS = args["BUSINESS_PROCESS"]
REGION_NAME = args["REGION_NAME"]
INSTANCE = args["INSTANCE"]
DYNAMODB_LOGS_TABLE = args["DYNAMODB_LOGS_TABLE"]
ERROR_TOPIC_ARN = args["ERROR_TOPIC_ARN"]
PROJECT_NAME = args["PROJECT_NAME"]
ENVIRONMENT = args["ENVIRONMENT"]
PROCESS_NAME = args["PROCESS_NAME"]
FLOW_NAME = args["FLOW_NAME"]
PERIODS = int(args["PERIODS"])
TABLE_NAME = args["TABLE_NAME"]

TZ_LIMA = pytz.timezone("America/Lima")
NOW_LIMA = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)

logger.info(f"project name: {PROJECT_NAME} | flow name:  {FLOW_NAME} | process name: {PROCESS_NAME}")
logger.info(f"instance: {INSTANCE} | periods: {PERIODS} | table name: {TABLE_NAME}")

DOMAIN_LAYER = 'domain'
EDUCACION_LAYER = 'educacion'
STAGE_LAYER_UPEU = 'upeu' # UPEU data source
STAGE_LAYER_SALESFORCE = 'salesforce'

# Determine load type based on table name
if TABLE_NAME.lower().startswith("m"):
    LOAD_TYPE = "full"
    logger.info(f"Table {TABLE_NAME} starts with 'm' - using full load type")
elif TABLE_NAME.lower().startswith("t"):
    LOAD_TYPE = f"incremental; loading {PERIODS} periods"
    logger.info(f"Table {TABLE_NAME} starts with 't' - using incremental load type, loading {PERIODS} periods")
else:
    LOAD_TYPE = "default"
    logger.info(f"Table {TABLE_NAME} does not start with 'm' or 't' - using default load type")

INSTANCES = [x.upper() for x in INSTANCE.split(",")]

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
dynamodb_resource = boto3.resource('dynamodb')

class data_paths:
    DOMAIN = f'{S3_PATH_ANALYTICS}{DOMAIN_LAYER}/'
    EDUCACION = f'{S3_PATH_ANALYTICS}{EDUCACION_LAYER}/'
    UPEU = f'{S3_PATH_STG}{STAGE_LAYER_UPEU}/'
    SALESFORCE = f'{S3_PATH_STG}{STAGE_LAYER_SALESFORCE}/public/'
    EXTERNAL = f'{S3_PATH_EXTERNAL}'
    ARTIFACTS_CSV = S3_PATH_ARTIFACTS_CSV
    ARTIFACTS_GLUE_CSV = f'{S3_PATH_ARTIFACTS}csv/'

    def getDataPath(self, layer):
        if layer.upper() == 'DOMAIN':
            return self.DOMAIN
        elif layer.upper() == 'UPEU':
            return self.UPEU
        elif layer.upper() == 'EDUCACION':
            return self.EDUCACION
        else:
            raise ValueError(f'Layer {layer} not found')

class STATUS:
    IN_PROGRESS = 0
    LANDING_SUCCEEDED = 1
    RAW_SUCCEEDED = 2
    STAGE_SUCCEEDED = 3
    LANDING_FAILED = 4
    RAW_FAILED = 5
    STAGE_FAILED = 6
    WARNING = 2

class SPARK_CONTROLLER:
    def __init__(self) -> None:
        self.spark = (
            SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
            .getOrCreate()
        )

        self.logger = LOGGING_UTILS()

    def _load_csv_from_s3(self, s3_path):
        """Load CSV file from S3 and return as list of dictionaries"""
        try: 
            bucket = s3_path.split('/')[2]
            key = '/'.join(s3_path.split('/')[3:])
            
            logger.info(f"Loading CSV from S3: bucket={bucket}, key={key}")
            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('latin-1')
            
            csv_data = []
            reader = csv.DictReader(StringIO(content), delimiter=';')
            for row in reader:
                csv_data.append(row)
            
            return csv_data

        except Exception as e:
            error_msg = f'Failed to load CSV from S3 path {s3_path}: {str(e)}'
            logger.error(error_msg)

            LOGGING_UTILS.log_and_exit_on_failure(status='FAILED', message=error_msg, context={'s3_path': s3_path, 'error_type': type(e).__name__, 'function': '_load_csv_from_s3'})

    def get_cod_pais_list_from_credentials(self):
        try:
            schema_path = f"{S3_PATH_ARTIFACTS_CONFIG}/instances.csv"
            logger.info(f"Obteniendo lista de paises desde: {schema_path}")

            instances_data = self._load_csv_from_s3(schema_path)
            instances_set = set()

            for item in instances_data:
                try:
                    if item.get("status", "").lower() == "a" and item.get("environment", "").upper() == ENVIRONMENT.upper():
                        instance = item.get("instance", "").strip()
                        countries = item.get("countries", "").strip()

                        if instance in INSTANCES:
                            country_list = [c.strip() for c in countries.split(",") if c.strip()]
                            instances_set.update(country_list)
                except Exception as row_error:
                    logger.warning(f"Error processing credentials row {item}: {str(row_error)}")
                    continue

            result_list = list(instances_set)
            logger.info(f"Successfully obtained {len(result_list)} countries: {result_list}")
            return result_list

        except Exception as e:
            error_msg = f"Failed to get country list from credentials: {str(e)}"
            logger.error(error_msg)

            log_record = LOGGING_UTILS.create_log_record(
                status="WARNING",
                message=error_msg,
                context={
                    "config_path": f"{S3_PATH_ARTIFACTS_CONFIG}/instances.csv",
                    "environment": ENVIRONMENT,
                    "instances": INSTANCES,
                    "error_type": type(e).__name__,
                    "function": "get_cod_pais_list_from_credentials",
                },
            )
            LOGGING_UTILS.add_log_to_dynamodb(log_record)

            logger.warning("Returning empty country list as fallback")
            return []

    def _create_empty_dataframe_from_csv(self, team, source, table_name):
        """
        Crea un DataFrame vacío usando el esquema definido en un archivo CSV en S3.
        """
        try:
            # Use stage layer path for columns.csv, not domain layer path
            schema_path = f"{S3_PATH_STAGE_CSV}columns.csv"
            logger.info(f"Cargando esquema desde: {schema_path}")

            columns_data = self._load_csv_from_s3(schema_path)

            filtered_columns = [
                col for col in columns_data
                if col.get("TABLE_NAME", "").upper() == table_name.upper()
            ]

            if not filtered_columns:
                error_msg = f"No se encontraron columnas para la tabla: {table_name}"
                logger.warning(error_msg)

                # Log as warning since this might be expected for some tables
                log_record = LOGGING_UTILS.create_log_record(
                    status="WARNING",
                    message=error_msg,
                    context={
                        "table_name": table_name,
                        "team": team,
                        "source": source,
                        "schema_path": schema_path,
                        "function": "_create_empty_dataframe_from_csv",
                    },
                )
                LOGGING_UTILS.add_log_to_dynamodb(log_record)
                raise ValueError(error_msg)

            # Ordenar por COLUMN_ID si existe
            try:
                if "COLUMN_ID" in filtered_columns[0]:
                    filtered_columns.sort(key=lambda x: int(x.get("COLUMN_ID", 0)))
            except (ValueError, TypeError) as sort_error:
                logger.warning(f"Error sorting columns by COLUMN_ID for {table_name}: {str(sort_error)}")
                # Continue without sorting

            # Mapeo de tipos
            type_mapping = {
                'string': StringType(),
                'int': IntegerType(),
                'integer': IntegerType(),
                'double': DoubleType(),
                'float': FloatType(),
                'boolean': BooleanType(),
                'date': DateType(),
                'timestamp': TimestampType()
            }

            fields = []
            for col_spec in filtered_columns:
                try:
                    column_name = col_spec.get("COLUMN_NAME")
                    data_type = col_spec.get("NEW_DATA_TYPE", "string").lower()
                    spark_type = type_mapping.get(data_type, StringType())
                    fields.append(StructField(column_name, spark_type, True))
                except Exception as field_error:
                    logger.warning(f"Error processing column {col_spec} for table {table_name}: {str(field_error)}")
                    continue

            schema = StructType(fields)
            logger.info(f"Schema de tabla {table_name}: {[f.name + ':' + f.dataType.simpleString() for f in fields]}")

            empty_df = self.spark.createDataFrame([], schema)
            logger.info(f"Successfully created empty DataFrame for {table_name} with {len(fields)} columns")
            return empty_df

        except Exception as e:
            error_msg = f"Error al construir DataFrame vacío desde CSV para {table_name}: {str(e)}"
            logger.error(error_msg)

            # Use log_and_exit_on_failure for critical schema errors
            LOGGING_UTILS.log_and_exit_on_failure(
                status="FAILED",
                message=error_msg,
                context={
                    "table_name": table_name,
                    "team": team,
                    "source": source,
                    "error_type": type(e).__name__,
                    "function": "_create_empty_dataframe_from_csv",
                },
            )
    def get_now_lima_datetime(self):
        return NOW_LIMA

    def read_table(self, path, table_name, have_principal=False, schema=False):
        try:
            logger.info(f"Starting read_table operation for table: {table_name}")
            logger.info(f"Parameters - path: {path}, have_principal: {have_principal}, schema: {bool(schema)}")

            s3_path = f"{path}{table_name}/"

            if path == data_paths.EXTERNAL or path == data_paths.ARTIFACTS_CSV:
                logger.info(f"Reading from EXTERNAL/ARTIFACTS_CSV path for table {table_name}")
                if schema:
                    logger.info(f"Using provided schema for CSV reading; path: {s3_path}")
                    df = self.spark.read.format("csv").options(delimiter=",", header=False).schema(schema).load(s3_path)
                else:
                    logger.info(f"Using default CSV options (delimiter=';', header=true); path: {s3_path}")
                    df = self.spark.read.format("csv").option("sep", ";").option("header", "true").load(s3_path)

            elif path == data_paths.BIGMAGIC:
                # Use domain-specific configuration path for credentials.csv
                schema_path = f"{S3_PATH_STAGE_CSV}credentials.csv"
                logger.info(f"Loading credentials schema from: {schema_path}")
                credentials_data = self._load_csv_from_s3(schema_path)
                items = []
                for item in credentials_data:
                    if item.get("INSTANCE").upper() in INSTANCES and item.get("ENV").upper() == ENVIRONMENT.upper():
                        if have_principal:
                            if not bool(item.get("IS_PRINCIPAL", False)):
                                continue
                        items.append(item["ENDPOINT_NAME"])
                logger.info(f"Found {len(items)} matching instances: {items}")

                df_list = []
                table_exists_somewhere = False

                for carpeta in items:
                    try:
                        carpeta_path = f"{path}{carpeta}/{table_name}/"
                        logger.info(f"Attempting to read from path: {carpeta_path}")
                        # Verificar si existe la tabla usando DeltaTable
                        if DeltaTable.isDeltaTable(self.spark, carpeta_path):
                            df_tmp = self.spark.read.format("delta").load(carpeta_path)
                            df_list.append(df_tmp)
                            table_exists_somewhere = True
                        else:
                            logger.warning(f"Path {carpeta_path} is not a valid Delta table")
                    except Exception as e:
                        logger.warning(f"Failed to read table {table_name} from {carpeta}: {str(e)}")

                        # Log detailed error for troubleshooting
                        log_record = LOGGING_UTILS.create_log_record(
                            status="WARNING",
                            message=f"Failed to read table {table_name} from instance {carpeta}: {str(e)}",
                            context={"table_name": table_name, "instance": carpeta, "path": f"{path}{carpeta}/{table_name}/", "error_type": type(e).__name__, "function": "read_table"},
                        )
                        LOGGING_UTILS.add_log_to_dynamodb(log_record)
                        continue

                # Si no se encontró ninguna tabla, crear DataFrame vacío con schema de DynamoDB
                if not table_exists_somewhere:
                    logger.warning(f"Table {table_name} not found. Creating empty DataFrame with schema from stage CSV config file.")

                    # Log this as warning since it might indicate missing data
                    log_record = LOGGING_UTILS.create_log_record(
                        status="WARNING",
                        message=f"Table {table_name} not found in any instance. Creating empty DataFrame.",
                        context={
                            "table_name": table_name,
                            "searched_instances": items,
                            "environment": ENVIRONMENT,
                            "function": "read_table",
                        },
                    )
                    LOGGING_UTILS.add_log_to_dynamodb(log_record)

                    df = self._create_empty_dataframe_from_csv(TEAM, STAGE_LAYER_BIGMAGIC, table_name)
                    logger.info(f"Created empty DataFrame for {table_name} with schema from CSV")
                else:
                    # Unir todos los DataFrames en uno solo si hay más de una carpeta
                    if len(df_list) == 1:
                        df = df_list[0]
                        logger.info(f"Using single DataFrame from one instance for table {table_name}")
                    else:
                        df = df_list[0].unionByName(*df_list[1:])
                        logger.info(f"Merged {len(df_list)} DataFrames into single DataFrame for table {table_name}")

            elif path == data_paths.SALESFORCE:
                logger.info(f"Reading from SALESFORCE path for table {table_name}")

                try:
                    logger.info(f"Reading Salesforce table {table_name} from {s3_path}")

                    if DeltaTable.isDeltaTable(self.spark, s3_path):
                        df = self.spark.read.format("delta").load(s3_path)
                        logger.info(f"Successfully read Salesforce table {table_name}")
                    else:
                        error_msg = f"Path {s3_path} is not a valid Delta table"
                        logger.error(error_msg)

                        # Log failure to DynamoDB
                        log_record = LOGGING_UTILS.create_log_record(
                            status="WARNING",
                            message=f"Salesforce table path validation failed for {table_name}: {error_msg}",
                            context={
                                "table_name": table_name,
                                "s3_path": s3_path,
                                "error_type": "InvalidDeltaTable",
                                "function": "read_table_salesforce_validation",
                            },
                        )
                        LOGGING_UTILS.add_log_to_dynamodb(log_record)

                        raise Exception(error_msg)

                except Exception as e:
                    logger.error(f"Failed to read Salesforce table {table_name}: {str(e)}")

                    # Log critical error to DynamoDB
                    log_record = LOGGING_UTILS.create_log_record(
                        status="WARNING",
                        message=f"Failed to read Salesforce table {table_name}, creating empty DataFrame",
                        context={
                            "table_name": table_name,
                            "s3_path": s3_path,
                            "error": str(e),
                            "error_type": type(e).__name__,
                            "function": "read_table_salesforce",
                        },
                    )
                    LOGGING_UTILS.add_log_to_dynamodb(log_record)

                    try:
                        df = self._create_empty_dataframe_from_csv(TEAM, "salesforce", table_name)
                        logger.warning(f"Created empty DataFrame for Salesforce table {table_name}")
                    except Exception as csv_error:
                        logger.error(f"Could not create empty DataFrame for {table_name}: {str(csv_error)}")
                        df = self.spark.createDataFrame([], StructType([]))
                        logger.warning(f"Created completely empty DataFrame as last resort for {table_name}")

            else:
                logger.info(f"Reading from Delta table {table_name} at path {s3_path}")
                df = self.spark.read.format("delta").load(s3_path)

            logger.info(f"read_table operation completed successfully for {table_name}")
            return df

        except Exception as e:
            error_msg = f"Critical error in read_table for {table_name}: {str(e)}"
            logger.error(error_msg)

            # Log critical failure and exit
            LOGGING_UTILS.log_and_exit_on_failure(
                status="FAILED",
                message=error_msg,
                context={
                    "table_name": table_name,
                    "path": path,
                    "have_principal": have_principal,
                    "schema_provided": bool(schema),
                    "error_type": type(e).__name__,
                    "function": "read_table",
                },
            )

    def upsert(self, df, path, table_name, id_columns, partition_by: list = []):
        try:
            logger.info(f"Starting upsert operation for table {table_name}")
            logger.info(f"ID columns: {id_columns}, Partition columns: {partition_by}")

            if self.table_exists(path, table_name):
                logger.info(f"Table {table_name} exists - performing update/merge")
                self.update_table(df, path, table_name, id_columns)
            else:
                logger.info(f"Table {table_name} does not exist - performing initial write")
                self.write_table(df, path, table_name, partition_by)

            logger.info(f"Successfully completed upsert operation for table {table_name}")

        except Exception as e:
            error_msg = f"Failed to upsert table {table_name}: {str(e)}"
            logger.error(error_msg)

            # Use log_and_exit_on_failure to properly log and exit job as success
            LOGGING_UTILS.log_and_exit_on_failure(
                status="FAILED",
                message=error_msg,
                context={
                    "table_name": table_name,
                    "path": path,
                    "id_columns": id_columns,
                    "partition_by": partition_by,
                    "error_type": type(e).__name__,
                    "function": "upsert",
                },
            )

    def update_table(self, update_records, path, table_name, update_columns_ids):
        try:
            table_path = f"{path}{table_name}/"
            logger.info(f"Starting merge operation for table {table_name} at path {table_path}")

            expression = ""
            for column in update_columns_ids:
                expression += f"a.{column} = b.{column} and "
            expression = expression[:-4]

            logger.info(f"Merge condition: {expression}")

            deltaTable = DeltaTable.forPath(self.spark, table_path)
            deltaTable.alias("a")\
                .merge(source=update_records.alias("b"), condition=expression) \
                .whenMatchedUpdateAll()\
                .whenNotMatchedInsertAll()\
                .execute()

            # Optimize table
            logger.info(f"Performing vacuum and manifest generation for {table_name}")
            deltaTable.vacuum(100)
            deltaTable.generate("symlink_format_manifest")

            logger.info(f"Successfully completed merge operation for table {table_name}")

        except Exception as e:
            error_msg = f"Failed to update table {table_name}: {str(e)}"
            logger.error(error_msg)

            # Use log_and_exit_on_failure to properly log and exit job as success
            LOGGING_UTILS.log_and_exit_on_failure(
                status="FAILED",
                message=error_msg,
                context={
                    "table_name": table_name,
                    "table_path": f"{path}{table_name}/",
                    "update_columns_ids": update_columns_ids,
                    "merge_condition": expression if "expression" in locals() else "not_built",
                    "error_type": type(e).__name__,
                    "function": "update_table",
                },
            )

    def write_table(self, df, path, table_name, partition_by: list = []):
        try:

            s3_path = f"{path}{table_name}/"
            logger.info(f"Writing to S3 path: {s3_path}")

            if len(partition_by) == 0:
                logger.info(f"Writing table {table_name} without partitioning")
                df.write.format("delta").mode("overwrite").save(s3_path)
            else:
                logger.info(f"Writing table {table_name} with partitioning by: {partition_by}")
                df.write.partitionBy(*partition_by).format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic").save(s3_path)
            deltaTable = DeltaTable.forPath(self.spark, s3_path)
            deltaTable.vacuum(20)
            deltaTable.generate("symlink_format_manifest")
            logger.info(f"Successfully completed write_table operation for {table_name}")
        except Exception as e:
            error_msg = f"Failed to write table {table_name}: {str(e)}"
            logger.error(error_msg)

            # Use log_and_exit_on_failure for comprehensive error logging
            LOGGING_UTILS.log_and_exit_on_failure(
                status="FAILED",
                message=error_msg,
                context={
                    "table_name": table_name,
                    "s3_path": f"{path}{table_name}/",
                    "path": path,
                    "partition_by": partition_by,
                    "has_partitioning": len(partition_by) > 0,
                    "error_type": type(e).__name__,
                    "function": "write_table",
                },
            )

    ###########################################################################################################

    def read_spark_table(self, path, table_name, update_records, update_columns):
        try:
            update_expr =  ""
            for column in update_columns:
                update_expr += f"a.{column} = b.{column} and "
            update_expr = update_expr[:-4]
            s3_path = f"{path}{table_name}/"
            if DeltaTable.isDeltaTable(self.spark, s3_path):
                dt = DeltaTable.forPath(self.spark, s3_path)
                dt.alias("a")\
                    .merge(source = update_records.alias("b"), condition = update_expr) \
                    .whenMatchedUpdateAll()\
                    .execute()
        except Exception as e:
            logger.error(e)
    
    def insert_into_table(self, df, path, table_name, partition_by : list = []):
        try:
            s3_path = f"{path}{table_name}/"
            if len(partition_by) == 0:
                df.write.format("delta").mode("append").save(s3_path)
            else:
                df.write.partitionBy(*partition_by).format("delta").mode("append").save(s3_path)
        except Exception as e:
            logger.error(str(e))
            self.logger.send_error_message(ERROR_TOPIC_ARN,f"Writing table {table_name}", str(e))
            raise e

    def table_exists(self, path, table_name):
        try:
            table_path = f"{path}{table_name}/"
            logger.debug(f"Checking if table exists at path: {table_path}")

            exists = DeltaTable.isDeltaTable(self.spark, table_path)
            logger.debug(f"Table {table_name} exists: {exists}")
            return exists

        except Exception as e:
            logger.warning(f"Error checking if table {table_name} exists at {path}: {str(e)}")

            # Log as warning since this is used for flow control
            log_record = LOGGING_UTILS.create_log_record(
                status="WARNING",
                message=f"Error checking table existence for {table_name}: {str(e)}",
                context={
                    "table_name": table_name,
                    "path": path,
                    "error_type": type(e).__name__,
                    "function": "table_exists"
                },
            )
            LOGGING_UTILS.add_log_to_dynamodb(log_record)

            # Return False as safe default - table will be created if needed
            return False

    def get_previous_period(self, date : str = NOW_LIMA.strftime("%Y%m")):
        year = int(date[:4])
        month = int(date[4:])
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1
        return f"{year}{month:02d}"

    def get_periods(self, periods : int = PERIODS, date : dt.datetime = NOW_LIMA):
        periods_list = []
        current_period = date.strftime("%Y%m")

        periods_list.append(current_period)
        extra_periods = periods - 1
        for i in range(extra_periods):
            current_period = self.get_previous_period(current_period)
            periods_list.append(current_period)
        
        return periods_list
    
    def get_dates_filter(self, periods):
        dates_filter = []
        dates_magic = []
        periods_filter = self.get_periods(periods)
        #Get the last day of previous month
        for period in periods_filter:
            year = int(period[:4])
            month = int(period[4:])
            #get next period
            if month == 12:
                month_next = 1
                year_next = year + 1
            else:
                month_next = month + 1
                year_next = year
            #get last day of current period
            last_day = (dt.date(year_next, month_next, 1) - dt.timedelta(days=1)).day
            #Get remaining days of current period e.g. last_day = 2023-01-31 then get dates from 2023-01-01 to 2023-01-31
            for i in range(1, last_day + 1):
                date = dt.date(year, month, i)
                dates_filter.append(date)

            #date = dt.date(year, month, last_day)
            #dates_filter.append(date)

        for date_normal in dates_filter:
            #data type int

            #Receive timestamp column: column_name_deltalake, transform into an integer column: column_name_target
            #integer column is a number of days from a pivot date: 2008-01-01 | days 733042
            #pesudo code from integer to date:
            #pivot_date = datetime(2008,1,1)
            #move_date = date_magic - 733042
            #real_date = pivot_date + timedelta(days=move_date)

            #pseudo code from timestamp to integer:
            #pivot_date = datetime(2008,1,1)
            #real_date = date_magic - pivot_date
            #move_date = real_date.days
            #integer_date = move_date + 733042
            
            date_magic = (date_normal - dt.date(2008, 1, 1)).days + 733042
            dates_magic.append(date_magic)

        return dates_filter, dates_magic, periods_filter

    def get_catalog_connection_redshift(self, catalog_connection):
        # Get the connection
        glue_client = boto3.client("glue", region_name=REGION_NAME)
        try:
            # Fetch the connection details
            response = glue_client.get_connection(Name=catalog_connection)
            connection_properties = response["Connection"]["ConnectionProperties"]

            # Extract the credentials
            username = connection_properties.get("USERNAME")
            password = connection_properties.get("PASSWORD")
            jdbc_url = connection_properties.get("JDBC_CONNECTION_URL")

            redshift_properties = {
                "user": username,
                "password": password,
                "driver": "com.amazon.redshift.jdbc42.Driver",
            }

            return jdbc_url, redshift_properties
        except Exception as e:
            print(f"Error fetching connection details: {e}")
            raise

    def load_to_redshift(self, df, properties, url, redshift_table_name, pais_filter=[], periodos_filter=[]):
        try:
            #Use preactions to delete the data from the table
            preactions = None
            if len(pais_filter) > 0 and len(periodos_filter) > 0:
                if len(pais_filter) == 1:
                    tuple_pais_filter = f"('{pais_filter[0]}')"
                else:
                    tuple_pais_filter = f"{tuple(pais_filter)}"
                if len(periodos_filter) == 1:
                    tuple_periodos_filter = f"('{periodos_filter[0]}')"
                else:
                    tuple_periodos_filter = f"{tuple(periodos_filter)}"
                preactions = f"DELETE FROM {redshift_table_name} WHERE id_pais IN {tuple_pais_filter} AND id_periodo IN {tuple_periodos_filter};"
            elif len(pais_filter) > 0:
                if len(pais_filter) == 1:
                    tuple_pais_filter = f"('{pais_filter[0]}')"
                else:
                    tuple_pais_filter = f"{tuple(pais_filter)}"
                preactions = f"DELETE FROM {redshift_table_name} WHERE id_pais IN {tuple_pais_filter};"
            # Execute the preactions query manually
            if preactions:
                print(f"Validate existence of table {redshift_table_name}")
                try:
                    # Validate existence of table using Spark DataFrame
                    query = f"(SELECT 1 FROM {redshift_table_name} LIMIT 1)"
                    df_redshift = self.spark.read.format("jdbc").options(
                        url=url,
                        user=properties["user"],
                        password=properties["password"],
                        dbtable=query,
                        driver="com.amazon.redshift.jdbc42.Driver"
                    ).load()

                except Exception as e:
                    logger.error(f"Table {redshift_table_name} does not exist. Error: {str(e)}")
                    preactions = 'select 1;'
                # If the table does not exist, set preactions to a dummy query

                print(f"Executing preactions query: {preactions}")
                glue_context = GlueContext(SparkContext.getOrCreate())
                connection_options = {
                    "url": url,
                    "user": properties["user"],
                    "password": properties["password"],
                    "preactions": preactions,
                    "dbtable": redshift_table_name,
                    "redshiftTmpDir": f"{S3_PATH_ARTIFACTS}temp/"
                }

                # Write the data to the Redshift table using Glue DynamicFrame
                dynamic_frame = DynamicFrame.fromDF(df, glue_context, "dynamic_frame")
                glue_context.write_dynamic_frame.from_options(
                    frame=dynamic_frame,
                    connection_type="redshift",
                    connection_options=connection_options
                )
        except Exception as e:
            error_msg = f"Failed to write to Redshift table {redshift_table_name}: {str(e)}"
            logger.error(error_msg)

            # Use log_and_exit_on_failure for comprehensive error logging
            LOGGING_UTILS.log_and_exit_on_failure(
                status="FAILED",
                message=error_msg,
                context={
                    "redshift_table_name": redshift_table_name,
                    "jdbc_url": url,
                    "pais_filter": pais_filter,
                    "periodos_filter": periodos_filter,
                    "has_pais_filter": len(pais_filter) > 0,
                    "has_periodo_filter": len(periodos_filter) > 0,
                    "preactions": connection_options.get("preactions", "None") if "connection_options" in locals() else "Not_set",
                    "error_type": type(e).__name__,
                    "function": "load_to_redshift",
                },
            )

    def load_to_redshift_stage(self, df, properties, url, redshift_table_name, compania_filters=[], compania_column='cod_compania', fechas_filter=[], fecha_column='fecha'):
        try:
            # Use preactions to delete the data from the table
            preactions = None
            if len(compania_filters) > 0 and len(fechas_filter) > 0:
                if len(compania_filters) == 1:
                    tuple_compania_filter = f"('{compania_filters[0]}')"
                else:
                    tuple_compania_filter = f"{tuple(compania_filters)}"
                if len(fechas_filter) == 1:
                    tuple_fechas_filter = f"('{fechas_filter[0]}')"
                else:
                    tuple_fechas_filter = f"{tuple(fechas_filter)}"
                preactions = f"DELETE FROM {redshift_table_name} WHERE {compania_column} IN {tuple_compania_filter} AND {fecha_column} IN {tuple_fechas_filter};"
            elif len(compania_filters) > 0:
                if len(compania_filters) == 1:
                    tuple_compania_filter = f"('{compania_filters[0]}')"
                else:
                    tuple_compania_filter = f"{tuple(compania_filters)}"
                preactions = f"DELETE FROM {redshift_table_name} WHERE {compania_column} IN {tuple_compania_filter};"
            else:
                #preactions = f"DELETE FROM {redshift_table_name} WHERE 1=1;"
                preactions = f"select 1;"
            # Execute the preactions query manually
            if preactions:
                print(f"Validate existence of table {redshift_table_name}")
                try:
                    # Validate existence of table using Spark DataFrame
                    query = f"(SELECT 1 FROM {redshift_table_name} LIMIT 1)"
                    df_redshift = self.spark.read.format("jdbc").options(
                        url=url,
                        user=properties["user"],
                        password=properties["password"],
                        dbtable=query,
                        driver="com.amazon.redshift.jdbc42.Driver"
                    ).load()

                except Exception as e:
                    logger.error(f"Table {redshift_table_name} does not exist. Error: {str(e)}")
                    preactions = 'select 1;'
                # If the table does not exist, set preactions to a dummy query

                print(f"Executing preactions query: {preactions}")
                glue_context = GlueContext(SparkContext.getOrCreate())
                connection_options = {
                    "url": url,
                    "user": properties["user"],
                    "password": properties["password"],
                    "preactions": preactions,
                    "dbtable": redshift_table_name,
                    "redshiftTmpDir": f"{S3_PATH_ARTIFACTS}temp/"
                }

                # Write the data to the Redshift table using Glue DynamicFrame
                dynamic_frame = DynamicFrame.fromDF(df, glue_context, "dynamic_frame")
                glue_context.write_dynamic_frame.from_options(
                    frame=dynamic_frame,
                    connection_type="redshift",
                    connection_options=connection_options
                )
        except Exception as e:
            error_msg = f"Failed to write to Redshift stage table {redshift_table_name}: {str(e)}"
            logger.error(error_msg)

            # Use log_and_exit_on_failure for comprehensive error logging
            LOGGING_UTILS.log_and_exit_on_failure(
                status="FAILED",
                message=error_msg,
                context={
                    "redshift_table_name": redshift_table_name,
                    "jdbc_url": url,
                    "compania_filters": compania_filters,
                    "fechas_filter": fechas_filter,
                    "compania_column": compania_column,
                    "fecha_column": fecha_column,
                    "has_compania_filter": len(compania_filters) > 0,
                    "has_fecha_filter": len(fechas_filter) > 0,
                    "preactions": connection_options.get("preactions", "None") if "connection_options" in locals() else "Not_set",
                    "error_type": type(e).__name__,
                    "function": "load_to_redshift_stage",
                },
            )


class LOGGING_UTILS:
    def send_error_redshift_message(self, topic_arn, table_name, error):
        message = f"Failed table: {table_name} \nStep: load to redshift \nLog ERROR \n{error}"

        sns_client.publish(
            TopicArn=topic_arn,
            Message=message
        )

    @staticmethod
    def create_log_record(status, message, context=""):
        """Create a standardized log record for DynamoDB logging with size limits"""
        log_context = {}

        # DynamoDB item size limit is 400KB, let's keep context under 300KB for safety
        MAX_CONTEXT_SIZE = 300 * 1024  # 300KB in bytes

        # Helper function to truncate data structures
        def truncate_data(data, max_length=1000):
            """Truncate strings and limit list/dict sizes"""
            if isinstance(data, str):
                if len(data) > max_length:
                    return data[:max_length] + "...[TRUNCATED]"
                return data
            elif isinstance(data, dict):
                truncated = {}
                for k, v in list(data.items())[:10]:  # Limit dict to 10 items
                    truncated[k] = truncate_data(v, 500)  # Limit nested strings to 500 chars
                if len(data) > 10:
                    truncated["_truncated_items"] = f"...and {len(data) - 10} more items"
                return truncated
            elif isinstance(data, list):
                truncated = [truncate_data(item, 200) for item in data[:5]]  # Limit to 5 items, 200 chars each
                if len(data) > 5:
                    truncated.append(f"...and {len(data) - 5} more items")
                return truncated
            else:
                return str(data)[:500] if data else data

        # Add custom context if provided (with size limits)
        if context:
            if isinstance(context, str):
                log_context["custom_message"] = truncate_data(context, 2000)  # Limit to 2KB
            elif isinstance(context, dict):
                truncated_context = truncate_data(context)
                log_context.update(truncated_context)

        # Add timestamp for when this log was created
        log_context["log_created_at"] = NOW_LIMA.strftime("%Y-%m-%d %H:%M:%S")

        # Check total context size and truncate if necessary
        context_json = json.dumps(log_context, default=str)
        if len(context_json.encode("utf-8")) > MAX_CONTEXT_SIZE:
            logger.warning(f"Log context size ({len(context_json.encode('utf-8'))} bytes) exceeds limit, truncating...")

            # Remove execution context first if it exists
            if "execution" in log_context:
                del log_context["execution"]
                log_context["execution_removed"] = "Execution context removed due to size limits"

            # If still too large, truncate custom message
            context_json = json.dumps(log_context, default=str)
            if len(context_json.encode("utf-8")) > MAX_CONTEXT_SIZE:
                if "custom_message" in log_context:
                    log_context["custom_message"] = str(log_context["custom_message"])[:1000] + "...[TRUNCATED_FOR_SIZE]"

                # Last resort: keep only essential fields
                context_json = json.dumps(log_context, default=str)
                if len(context_json.encode("utf-8")) > MAX_CONTEXT_SIZE:
                    log_context = {
                        "log_created_at": log_context.get("log_created_at"),
                        "size_limit_applied": "Context truncated due to DynamoDB size limits",
                        "original_size_bytes": len(context_json.encode("utf-8")),
                    }

        # Limit message size as well
        truncated_message = message[:2000] + "...[TRUNCATED]" if len(message) > 2000 else message

        return {
            "PROCESS_ID": f"{TEAM}-{STAGE_LAYER_BIGMAGIC}-{FLOW_NAME}",
            "DATE_SYSTEM": dt.datetime.now(pytz.utc).astimezone(TZ_LIMA).strftime("%Y%m%d_%H%M%S"),
            "RESOURCE_NAME": JOB_NAME,
            "RESOURCE_TYPE": "glue_job",
            "STATUS": status,  # SUCCESS, FAILED, WARNING, STARTED
            "MESSAGE": truncated_message,
            "PROCESS_TYPE": LOAD_TYPE,
            "CONTEXT": log_context,
            "TEAM": TEAM,
            "DATASOURCE": STAGE_LAYER_BIGMAGIC,
            "TABLE_NAME": TABLE_NAME,
            "ENVIRONMENT": ENVIRONMENT,
        }

    @staticmethod
    def add_log_to_dynamodb(record):
        """Safely log to DynamoDB with error handling to prevent blocking"""

        try:
            if DYNAMODB_LOGS_TABLE:
                dynamo_table = dynamodb_resource.Table(DYNAMODB_LOGS_TABLE)
                dynamo_table.put_item(Item=record)
                logger.info(f"Logged record PROCESS_ID: {record.get('PROCESS_ID')}, DATE_SYSTEM: {record.get('DATE_SYSTEM')} to DynamoDB with status: {record.get('STATUS')}")

                if record.get("STATUS") == "FAILED":
                    logger.info("FAILED message logged to DynamoDB - job will exit as success to prevent double notifications")

                    try:
                        # Limit message length to prevent SNS size issues
                        message_text = str(record.get("MESSAGE", ""))
                        truncated_message = message_text[:800] + "..." if len(message_text) > 800 else message_text

                        record_summary = {
                            "PROCESS_ID": record.get("PROCESS_ID"),
                            "DATE_SYSTEM": record.get("DATE_SYSTEM"),
                            "STATUS": record.get("STATUS"),
                            "MESSAGE": truncated_message,
                            "TABLE_NAME": record.get("TABLE_NAME"),
                            "TEAM": record.get("TEAM"),
                        }
                        record_json = json.dumps(record_summary)

                        notification_message = f"""Process Status: {record.get('STATUS')}
                            Table: {record.get("TABLE_NAME")}
                            Message: {truncated_message}

                            DynamoDB Log Details (for lookup):
                            {record_json}

                            Search in DynamoDB using:
                            - PROCESS_ID: {record.get('PROCESS_ID')}
                            - DATE_SYSTEM: {record.get('DATE_SYSTEM')}

                            Full details available in CloudWatch logs."""

                        LOGGING_UTILS.send_error_message(ERROR_TOPIC_ARN, notification_message)
                    except Exception as sns_error:
                        logger.warning(f"Failed to send SNS notification for logged record: {sns_error}")

        except Exception as e:
            # Log to CloudWatch but don't fail the entire process
            logger.warning(f"Failed to log to DynamoDB: {e}")

            # Send SNS notification only if the original record status was FAILED
            try:
                if record.get("STATUS") in ["FAILED"]:
                    # Set global flag even if DynamoDB logging failed
                    logger.info("FAILED message attempted to log to DynamoDB (but failed) - job will still exit as success to prevent double notifications")

                    # Create a minimal record summary to avoid message size issues
                    record_summary = {
                        "PROCESS_ID": record.get("PROCESS_ID"),
                        "DATE_SYSTEM": record.get("DATE_SYSTEM"),
                        "STATUS": record.get("STATUS"),
                        "MESSAGE": str(record.get("MESSAGE", ""))[:500] + "..." if len(str(record.get("MESSAGE", ""))) > 500 else record.get("MESSAGE", ""),  # Limit message length
                        "TABLE_NAME": record.get("TABLE_NAME"),
                        "TEAM": record.get("TEAM"),
                        "DATASOURCE": record.get("DATASOURCE"),
                        "ERROR_DETAILS": str(e)[:200] + "..." if len(str(e)) > 200 else str(e),  # Limit error details
                    }
                    record_json = json.dumps(record_summary, indent=2)

                    notification_message = f"""DynamoDB Logging Failed for table: {record.get("TABLE_NAME")}
                        Error: {str(e)[:300]}{"..." if len(str(e)) > 300 else ""}
                        Record Summary:
                        {record_json}
                        Full details available in CloudWatch logs."""

                    LOGGING_UTILS.send_error_message(ERROR_TOPIC_ARN, notification_message)
                else:
                    logger.warning(f"Failed to log non-critical record to DynamoDB. Record status: {record.get('STATUS')}")
            except Exception as sns_error:
                logger.error(f"Failed to send SNS notification for DynamoDB logging error: {sns_error}")

    @staticmethod
    def send_error_message(topic_arn, notification_message):
        """Safely send SNS notification with error handling to prevent blocking"""
        try:
            if topic_arn:
                # SNS message size limit is 256KB, but let's be safe and limit to 200KB
                MAX_MESSAGE_SIZE = 200 * 1024  # 200KB in bytes

                # If message is too long, truncate it
                if len(notification_message.encode("utf-8")) > MAX_MESSAGE_SIZE:
                    # Calculate how much we need to truncate
                    truncate_note = "\n\n[MESSAGE TRUNCATED DUE TO SIZE LIMIT - Check CloudWatch logs for full details]"
                    available_size = MAX_MESSAGE_SIZE - len(truncate_note.encode("utf-8"))

                    # Truncate the message
                    truncated_message = notification_message.encode("utf-8")[:available_size].decode("utf-8", errors="ignore")
                    notification_message = truncated_message + truncate_note

                    logger.warning(f"SNS message truncated from {len(notification_message.encode('utf-8'))} bytes to {MAX_MESSAGE_SIZE} bytes")

                sns_client.publish(TopicArn=topic_arn, Message=notification_message)
                logger.debug("Successfully sent SNS notification")
        except Exception as e:
            # Log to CloudWatch but don't fail the entire process
            logger.warning(f"Failed to send SNS notification: {e}")

    @staticmethod
    def log_and_exit_on_failure(status, message, context=""):
        """Log a failure message to DynamoDB and exit the job as success to prevent double notifications"""

        # Create and log the failure record
        log_record = LOGGING_UTILS.create_log_record(status=status, message=message, context=context)
        LOGGING_UTILS.add_log_to_dynamodb(log_record)

        # Exit as success to prevent state machine from triggering another notification
        logger.info(f"Logged {status} message to DynamoDB. Exiting job as SUCCESS to prevent double notifications.")
        logger.info("The error has been properly logged and notification sent. Check DynamoDB logs for error details.")
        raise JobCompletionException(f"Job completed after logging {status}")


class JobCompletionException(Exception):
    """
    Custom exception to signal that the job should complete successfully
    after logging appropriate messages to DynamoDB.
    """

    def __init__(self, message="Job completed with logged failures"):
        self.message = message
        super().__init__(message)

    def __str__(self):
        return self.message
