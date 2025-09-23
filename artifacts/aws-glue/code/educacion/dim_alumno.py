import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date, broadcast
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_alumno"

# Configuraciones de optimización
spark_controller.spark.conf.set("spark.sql.adaptive.enabled", "true")
spark_controller.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark_controller.spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
spark_controller.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark_controller.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

try:
    logger.info("Reading tables from UPEU (Stage layer)")
    
    df_ma_contrato = spark_controller.read_table(data_paths.UPEU, "ma_contrato")
    df_personas = spark_controller.read_table(data_paths.UPEU, "personas")
    
    logger.info("Tables loaded successfully from UPEU")
except Exception as e:
    logger.error(f"Error reading tables from UPEU: {e}")
    raise ValueError(f"Error reading tables from UPEU: {e}")

try:
    logger.info("Starting transformations - creating dim_alumno from ma_contrato and personas")
    
    # Usar selectExpr para mejor rendimiento con funciones SQL nativas
    df_dim_alumno = (
        df_ma_contrato.alias("c")
        .join(df_personas.alias("p"), col("c.alum_id") == col("p.id"), "inner")
        .selectExpr(
            "CAST(c.alum_id AS STRING) as id_alumno",
            "CAST(p.apellidopaterno AS STRING) as apellido_paterno",
            "CAST(p.apellidomaterno AS STRING) as apellido_materno", 
            "CAST(p.nombres AS STRING) as nombre",
            "CAST(CONCAT_WS(' ', p.apellidopaterno, p.apellidomaterno, p.nombres) AS STRING) as nombre_completo",
            "CAST(p.codmodular AS STRING) as codigo_estudiante"
        )
        .distinct()
    )

    id_columns = ["id_alumno"]
    partition_columns_array = []
    
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_alumno, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing df_dim_alumno: {e}")
    raise ValueError(f"Error processing df_dim_alumno: {e}")