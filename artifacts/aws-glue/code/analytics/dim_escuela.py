import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_escuela"

try:
    # Lectura directa desde STAGE (UPEU) - equivalente a temp.GeoReferencias
    logger.info("Reading tables from UPEU (Stage layer)")
    
    # Tabla principal GeoReferencias (equivalente a temp.GeoReferencias)
    df_geo_referencias = spark_controller.read_table(data_paths.UPEU, "geo_referencias")
    
    logger.info("Tables loaded successfully from UPEU")
except Exception as e:
    logger.error(f"Error reading tables from UPEU: {e}")
    raise ValueError(f"Error reading tables from UPEU: {e}")

try:
    # Aplicar la lógica del MERGE SQL - crear el dataset origen
    logger.info("Starting transformations - creating dim_escuela from geo_referencias")
    
    df_dim_escuela = (
        df_geo_referencias
        .select(
            col("id").cast(IntegerType()).alias("id_escuela"),
            col("name").cast(StringType()).alias("nomb_escuela"),
            col("entidadid").cast(IntegerType()).alias("id_facultad")
        )
        .distinct()  # Aplicar distinct para asegurar unicidad
    )

    # Escribir a analytics usando el método upsert (equivalente al MERGE)
    # El upsert maneja automáticamente both matched/not matched scenarios
    id_columns = ["id_escuela"]  # Clave para el merge
    partition_columns_array = []  # Sin partición específica, puedes agregar si necesitas
    
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_escuela, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing dim_escuela: {e}")
    raise ValueError(f"Error processing dim_escuela: {e}")