import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_alumno"

try:
    # Lectura directa desde STAGE (BIGMAGIC) - equivalente a las tablas temp
    logger.info("Reading tables from BIGMAGIC (Stage layer)")
    
    # Tablas principales para alumno (equivalentes a temp.ma_contrato y temp.personas)
    df_ma_contrato = spark_controller.read_table(data_paths.BIGMAGIC, "ma_contrato")
    df_personas = spark_controller.read_table(data_paths.BIGMAGIC, "personas")
    
    logger.info("Tables loaded successfully from BIGMAGIC")
except Exception as e:
    logger.error(f"Error reading tables from BIGMAGIC: {e}")
    raise ValueError(f"Error reading tables from BIGMAGIC: {e}")

try:
    # Aplicar la lógica del MERGE SQL - crear el dataset origen
    logger.info("Starting transformations - creating dim_alumno from ma_contrato and personas")
    
    df_dim_alumno = (
        df_ma_contrato.alias("c")
        .join(
            df_personas.alias("p"), 
            col("c.alum_id") == col("p.id"), 
            "inner"
        )
        .select(
            col("c.alum_id").cast(StringType()).alias("id_alumno"),
            col("p.apellidopaterno").cast(StringType()).alias("apellido_paterno"),
            col("p.apellidomaterno").cast(StringType()).alias("apellido_materno"),
            col("p.nombres").cast(StringType()).alias("nombre"),
            concat_ws(" ", 
                col("p.apellidopaterno"), 
                col("p.apellidomaterno"), 
                col("p.nombres")
            ).cast(StringType()).alias("nombre_completo"),
            col("p.codmodular").cast(StringType()).alias("codigo_estudiante")
        )
        .distinct()  # Aplicar distinct como en el SQL original
    )

    # Escribir a analytics usando el método upsert (equivalente al MERGE)
    # El upsert maneja automáticamente both matched/not matched scenarios
    id_columns = ["id_alumno"]  # Clave para el merge
    partition_columns_array = []  # Sin partición específica, puedes agregar si necesitas
    
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_alumno, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing df_dim_alumno: {e}")
    raise ValueError(f"Error processing df_dim_alumno: {e}")