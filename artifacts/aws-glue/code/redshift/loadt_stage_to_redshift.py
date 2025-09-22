import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, when, date_format, round, trim, to_date, substring, lower, to_timestamp, row_number, max, sum, datediff
from pyspark.sql.window import Window
import json
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS

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
PROCESS_NAME = args["PROCESS_NAME"]
REGION_NAME = args["REGION_NAME"]
LOAD_PROCESS = args["LOAD_PROCESS"]
ORIGIN = args["ORIGIN"]

# e.g. ["10"] -> into list ["10"]
LOAD_PROCESS_LIST = LOAD_PROCESS.split(",")
cod_pais = COD_PAIS.split(",")
def transform_default(df, column_name_deltalake, column_name_target, column_type_target, column_literal):
    if column_literal is not None and column_literal != "":
        if column_literal.lower() == "null":
            column_value = lit(None)
        else:
            column_value = lit(column_literal)
    else:
        column_value = col(column_name_deltalake)

    if column_type_target == "varchar varying" or column_type_target == "character varying" or column_type_target == "text" or column_type_target == "character" or column_type_target == "string" or column_type_target == "varchar":
        df = df.withColumn(column_name_target, column_value.cast("string"))
    elif column_type_target == "integer":
        df = df.withColumn(column_name_target, column_value.cast("int"))
    #Handle if comes with precision: e.g. numeric(16,4)
    elif column_type_target.startswith("numeric"):
        #Get precision and scale from column_type_target
        if column_type_target.find("(") != -1 and column_type_target.find(")") != -1:
            precision = column_type_target[column_type_target.find("(")+1:column_type_target.find(")")].split(",")[0]
            scale = column_type_target[column_type_target.find("(")+1:column_type_target.find(")")].split(",")[1]
            #Set precision and scale to decimal
            df = df.withColumn(column_name_target, column_value.cast(f"decimal({precision},{scale})"))
        else:
            #Set default precision and scale to decimal
            df = df.withColumn(column_name_target, column_value.cast("decimal(38,12)"))
    elif column_type_target == "smallint":
        df = df.withColumn(column_name_target, column_value.cast("int"))
    elif column_type_target == "bigint":
        df = df.withColumn(column_name_target, column_value.cast("bigint"))
    elif column_type_target == "boolean":
        df = df.withColumn(column_name_target, column_value.cast("boolean"))
    elif column_type_target == "double precision":
        df = df.withColumn(column_name_target, column_value.cast("double"))
    else :
        df = None
    
    #if column_name_deltalake is not None and column_name_deltalake != "":
    #    df = df.drop(column_name_deltalake)

    return df

def transform_convert_fecha_bigmagic(df, column_name_deltalake, column_name_target, column_type_target, column_literal):
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

    df = df.withColumn(column_name_target, datediff(to_date(col(column_name_deltalake), "yyyy-MM-dd"), lit("2008-01-01").cast("date")) + 733042)

    return df

def transform_convert_hora_bigmagic(df, column_name_deltalake, column_name_target, column_type_target, column_literal):
    #Receive timestamp column: column_name_deltalake, transform into an varchar column: column_name_target
    #varcahr column is a text that concats hours, minutes and seconds: 2023-10-01 12:00:00 = 120000

    df = df.withColumn(column_name_target, ((substring(col(column_name_deltalake), 12, 2).cast("int") * 10000) + (substring(col(column_name_deltalake), 15, 2).cast("int") * 100) + substring(col(column_name_deltalake), 18, 2)).cast("string"))

    return df

def transform_get_date(df, column_name_deltalake, column_name_target, column_type_target, column_literal):
    #Get current date and set it to column_name_target
    current_date = spark_controller.get_now_lima_datetime().strftime("%Y-%m-%d")

    if column_type_target == "varchar varying" or column_type_target == "character varying" or column_type_target == "text" or column_type_target == "character" or column_type_target == "string" or column_type_target == "varchar":
        #Format YYYYMMDD
        df = df.withColumn(column_name_target, date_format(lit(current_date), "yyyyMMdd"))
    elif column_type_target == "integer" or column_type_target == "int" or column_type_target == "smallint" or column_type_target == "bigint":
        #Format YYYYMMDD
        df = df.withColumn(column_name_target, date_format(lit(current_date), "yyyyMMdd").cast("int"))
    elif column_type_target == "date":
        #Format YYYYMMDD
        df = df.withColumn(column_name_target, to_date(lit(current_date), "yyyyMMdd"))
    elif column_type_target == "timestamp":
        #Format YYYYMMDD
        df = df.withColumn(column_name_target, to_timestamp(lit(current_date), "yyyyMMdd"))

    return df

def transform_column(df, column_name_deltalake, column_name_target, column_type_target, column_literal, column_function):
    if column_function == "default":
        df = transform_default(df, column_name_deltalake, column_name_target, column_type_target, column_literal)
    elif column_function == "convert_fecha_bigmagic":
        
        df = transform_convert_fecha_bigmagic(df, column_name_deltalake, column_name_target, column_type_target, column_literal)
        
    elif column_function == "convert_hora_bigmagic":
        df = transform_convert_hora_bigmagic(df, column_name_deltalake, column_name_target, column_type_target, column_literal)
    elif column_function == "get_date":
        #get today date 
        df = transform_get_date(df, column_name_deltalake, column_name_target, column_type_target, column_literal)

    return df
    


TABLE_LIST = []
#get tables to read from s3 delta lake
try:
    #Read csv file with tables to load
    df_csv_tables = spark_controller.read_table(data_paths.ARTIFACTS_CSV, "stage_tables_to_load.csv")
    #filter by PROCESS_ID
    df_csv_tables = df_csv_tables.filter(col("PROCESS_ID").isin(LOAD_PROCESS_LIST) & col("PAIS").isin(cod_pais))
    df_csv_columns = spark_controller.read_table(data_paths.ARTIFACTS_CSV, "stage_columns_to_load.csv")
    for row in df_csv_tables.collect():
        table_info = {}
        table_country = row["PAIS"]
        table_name = row["TABLA_STAGE"]
        table_schema = row["ESQUEMA"]
        table_target = row["TABLA_REDSHIFT"]
        table_periods = row["PERIODOS"]
        table_type = row["TIPO_TABLA"]
        table_is_principal = row["IS_PRINCIPAL"]
        #table columns configuration

        table_info["deltalake"] = table_name
        table_info["country"] = table_country
        table_info["schema"] = table_schema
        table_info["target"] = table_target
        table_info["periods"] = table_periods
        table_info["type"] = table_type
        table_info["is_principal"] = table_is_principal
        print(f"Table: {table_name}, Country: {table_country}, Schema: {table_schema}, Target: {table_target}, Periods: {table_periods}, Type: {table_type}, IS_PRINCIPAL: {table_is_principal}")
        table_info["columns"] = []
        for row_columns in df_csv_columns.filter((col("TABLA_STAGE") == table_name) & (col("PAIS") == table_country)).collect():
            column_name = row_columns["COLUMNA"]
            column_type = row_columns["TIPO_DATO_DESTINO"]
            column_target = row_columns["COLUMNA_DESTINO"] + "_redshifttarget"
            column_literal = row_columns["LITERAL_DESTINO"]
            column_function = row_columns["FUNCION"]
            #column_obligatorio = row_columns["ES_OBLIGATORIO"]
            column_filtro_fecha = row_columns["ES_FILTRO_FECHA"]
            column_info = {}
            column_info["delta_column"] = column_name
            column_info["target_column"] = column_target
            column_info["target_type"] = column_type
            column_info["literal"] = column_literal
            column_info["function"] = column_function
            #column_info["obligatorio"] = column_obligatorio
            column_info["es_filtro_fecha"] = column_filtro_fecha
            table_info["columns"].append(column_info)
        TABLE_LIST.append(table_info)
            
except Exception as e:
    logger.error(e) 
    raise

try:
    #get connection to redshift
    url, properties = spark_controller.get_catalog_connection_redshift(CATALOG_CONNECTION)
    #iterate over tables to load
    for table_to_load in TABLE_LIST:
        #Read table from delta lake
        try:
            #Read table from delta lake
            is_principal = False
            if table_to_load["is_principal"] == "SI":
                is_principal = True
            df_deltalake = spark_controller.read_table(data_paths.BIG_BAGIC, table_to_load["deltalake"].lower(), cod_pais=[table_to_load["country"]], have_principal=is_principal)
            print("esquema inicial")
            df_deltalake.printSchema()
            print(f"Table: {table_to_load['deltalake']}, Country: {table_to_load['country']}, IS_PRINCIPAL: {is_principal}")
        except Exception as e:
            logger.error(f"Error reading table {table_to_load['deltalake']}: {str(e)}")
            spark_controller.logger.send_error_redshift_message(
                ERROR_TOPIC_ARN,
                table_to_load["deltalake"],
                "Error Stage to bigmagic load - read table\n\n"+str(e)
            )
            df_deltalake = None
        #If df_deltalake is None, continue to next table
        if df_deltalake is None:
            continue
        column_fecha_filter = None
        #Prepare columns to load
        columns_ok = True
        for column in table_to_load["columns"]:
            try:
                column_name_target = column["target_column"]
                column_name_deltalake = column["delta_column"]
                column_type_target = column["target_type"]
                column_literal = column["literal"]
                column_function = column["function"]
                #column_obligatorio = column["obligatorio"]
                column_filtro_fecha = column["es_filtro_fecha"]
                if column_function is None or column_function == "":
                    column_function = "default"
                    print(f"Column function is None or empty, setting to default")
                else:
                    column_function = column_function.lower()
                    print(f"Column function: {column_function}")
                if column_filtro_fecha is not None and (column_filtro_fecha.lower() == "si"):
                    column_fecha_filter = column_name_target
                    print(f"Column filter fecha: {column_fecha_filter}")
                column_type_target = column_type_target.lower()
                print(f"Column name: {column_name_deltalake}, Column type: {column_type_target}, Column literal: {column_literal}, Column function: {column_function}")
                df_deltalake = transform_column(df_deltalake, column_name_deltalake, column_name_target, column_type_target, column_literal, column_function)
            except Exception as e:
                logger.error(f"Error processing columns for table {table_to_load['deltalake']}: {str(e)}")
                spark_controller.logger.send_error_redshift_message(
                    ERROR_TOPIC_ARN,
                    table_to_load["deltalake"],
                    "Error Stage to bigmagic load - tranform column\n\n"+str(e)
                )
                columns_ok = False
                break

        if not columns_ok:
            continue

        #Load table to redshift
        try:
            print(df_deltalake.count())
            table_name_target = table_to_load["target"]
            table_schema_target = table_to_load["schema"]
            table_periods_target = table_to_load["periods"]
            table_periods_target = int(table_periods_target) if table_periods_target != "" and table_periods_target is not None else 0
            dates_filter=[]
            dates_magic=[]
            print(f"Table: {table_name_target}, Schema: {table_schema_target}, Periods: {table_periods_target}")
            #Periods filter
            if table_to_load["type"] == "T":
                dates_filter, dates_magic,periods_filter = spark_controller.get_dates_filter(periods=int(table_periods_target))
                print(f"Dates filter: {dates_filter}\n\n Dates magic: {dates_magic} \n\n Periods filter: {periods_filter}")
                df_deltalake = df_deltalake.filter(col(column_fecha_filter).isin(dates_magic))
            #Compania filter
            compania_filters = []
            compania_filters = df_deltalake.select("id_compania").distinct().collect() #e.g. [Row(id_compania='10'), Row(id_compania='20')]
            print(f"Compania filters raw: {compania_filters}")
            #Keep only values
            compania_filters = [row["id_compania"] for row in compania_filters] #e.g. ['10', '20']
            print(f"Compania filters: {compania_filters}")

            redshift_table_name = f"{table_schema_target}.{table_name_target}"
            print(redshift_table_name)
            print ("Esquema antes de insertar a redshift")
            df_deltalake.printSchema()
            #Drop columns that dont have suffix _redshifttarget
            df_deltalake = df_deltalake.select([col for col in df_deltalake.columns if col.endswith("_redshifttarget")])
            #Rename columns to remove suffix _redshifttarget
            df_deltalake = df_deltalake.select([col(col_name).alias(col_name.replace("_redshifttarget", "")) for col_name in df_deltalake.columns])

            spark_controller.load_to_redshift_stage(df_deltalake, properties, url, redshift_table_name, compania_filters = compania_filters, fechas_filter = dates_magic, fecha_column = column_fecha_filter.replace("_redshifttarget", ""))
        except Exception as e:
            logger.error(f"Error processing table {table_name_target}: {str(e)}")
            spark_controller.logger.send_error_redshift_message(
                ERROR_TOPIC_ARN,
                table_name_target,
                str(e)
            )
        
        

except Exception as e:
    logger.error(f"Failed to write to Redshift: {str(e)}")
    raise
