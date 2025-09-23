import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_programa_educativo"

try:
    # Lectura directa desde STAGE (UPEU) - equivalente a temp.ProgramasEducativo
    logger.info("Reading tables from UPEU (Stage layer)")
    
    # Tabla principal ProgramasEducativo (equivalente a temp.ProgramasEducativo)
    df_programaseducativo = spark_controller.read_table(data_paths.UPEU, "programaseducativo")
    
    logger.info("Tables loaded successfully from UPEU")
except Exception as e:
    logger.error(f"Error reading tables from UPEU: {e}")
    raise ValueError(f"Error reading tables from UPEU: {e}")

try:
    # Aplicar la lógica del MERGE SQL - crear el dataset origen
    logger.info("Starting transformations - creating dim_programa_educativo from programaseducativo")
    
    df_dim_programa_educativo = (
        df_programaseducativo
        .select(
            col("id").cast(IntegerType()).alias("id_programa_educativo"),
            col("nombre").cast(StringType()).alias("nomb_programa_educativo"),
            col("entidadid").cast(IntegerType()).alias("id_universidad")
        )
        .distinct()  # Aplicar distinct para asegurar unicidad
    )

    # Escribir a analytics usando el método upsert (equivalente al MERGE)
    # El upsert maneja automáticamente both matched/not matched scenarios
    id_columns = ["id_programa_educativo"]  # Clave para el merge
    partition_columns_array = []  # Sin partición específica, puedes agregar si necesitas
    
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_programa_educativo, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing dim_programa_educativo: {e}")
    raise ValueError(f"Error processing dim_programa_educativo: {e}")