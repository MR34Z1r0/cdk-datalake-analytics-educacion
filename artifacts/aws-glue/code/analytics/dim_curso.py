import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_curso"

try:
    # Lectura directa desde STAGE (UPEU) - equivalente a temp.Cursos y temp.Tipos
    logger.info("Reading tables from UPEU (Stage layer)")
    
    # Tabla principal Cursos (equivalente a temp.Cursos)
    df_cursos = spark_controller.read_table(data_paths.UPEU, "cursos")
    
    # Tabla Tipos (equivalente a temp.Tipos)
    df_tipos = spark_controller.read_table(data_paths.UPEU, "tipos")
    
    logger.info("Tables loaded successfully from UPEU")
except Exception as e:
    logger.error(f"Error reading tables from UPEU: {e}")
    raise ValueError(f"Error reading tables from UPEU: {e}")

try:
    # Aplicar la lógica del MERGE SQL - crear el dataset origen con JOIN
    logger.info("Starting transformations - creating dim_curso from cursos and tipos")
    
    df_dim_curso = (
        df_cursos.alias("c")
        .join(
            df_tipos.alias("t"),
            col("c.tipocursoid") == col("t.id"),
            "inner"
        )
        .select(
            col("c.id").cast(IntegerType()).alias("id_curso"),
            col("c.nombre").cast(StringType()).alias("nomb_curso"),
            col("c.entidadid").cast(IntegerType()).alias("id_universidad"),
            col("t.nombre").cast(StringType()).alias("tipo_curso")
        )
        .distinct()  # Aplicar distinct para asegurar unicidad
    )

    # Escribir a analytics usando el método upsert (equivalente al MERGE)
    # El upsert maneja automáticamente both matched/not matched scenarios
    id_columns = ["id_curso"]  # Clave para el merge
    partition_columns_array = []  # Sin partición específica, puedes agregar si necesitas
    
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_curso, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing dim_curso: {e}")
    raise ValueError(f"Error processing dim_curso: {e}")