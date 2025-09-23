import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_anio_academico"

try:
    # Lectura directa desde STAGE (UPEU) - equivalente a temp.AnioAcademico
    logger.info("Reading tables from UPEU (Stage layer)")
    
    # Tabla principal AnioAcademico (equivalente a temp.AnioAcademico)
    df_anio_academico = spark_controller.read_table(data_paths.UPEU, "anio_academico")
    
    logger.info("Tables loaded successfully from UPEU")
except Exception as e:
    logger.error(f"Error reading tables from UPEU: {e}")
    raise ValueError(f"Error reading tables from UPEU: {e}")

try:
    # Aplicar la lógica del MERGE SQL - crear el dataset origen
    logger.info("Starting transformations - creating dim_anio_academico from anio_academico")
    
    df_dim_anio_academico = (
        df_anio_academico.alias("p")
        .select(
            col("p.id").cast(IntegerType()).alias("id_anio_academico"),
            col("p.nombre").cast(StringType()).alias("nomb_anio_academico"),
            col("p.fechainicio").cast(DateType()).alias("fecha_inicio"),
            col("p.fechafin").cast(DateType()).alias("fecha_fin"),
            col("p.georeferenciaid").cast(IntegerType()).alias("id_escuela"),
            col("p.empresaid").cast(IntegerType()).alias("id_facultad"),
            col("p.anio").cast(IntegerType()).alias("anio")
        )
        .distinct()  # Aplicar distinct para asegurar unicidad
    )

    # Escribir a analytics usando el método upsert (equivalente al MERGE)
    # El upsert maneja automáticamente both matched/not matched scenarios
    id_columns = ["id_anio_academico"]  # Clave para el merge
    partition_columns_array = []  # Sin partición específica, puedes agregar si necesitas
    
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_anio_academico, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing dim_anio_academico: {e}")
    raise ValueError(f"Error processing dim_anio_academico: {e}")