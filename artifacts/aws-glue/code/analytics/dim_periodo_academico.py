import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_periodo_academico"

try:
    # Lectura directa desde STAGE (UPEU) - equivalente a temp.T_GC_MAE_CALENDARIO_PERIODO y temp.Tipos
    logger.info("Reading tables from UPEU (Stage layer)")
    
    # Tabla principal T_GC_MAE_CALENDARIO_PERIODO (equivalente a temp.T_GC_MAE_CALENDARIO_PERIODO)
    df_t_gc_mae_calendario_periodo = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_calendario_periodo")
    
    # Tabla Tipos (equivalente a temp.Tipos)
    df_tipos = spark_controller.read_table(data_paths.UPEU, "tipos")
    
    logger.info("Tables loaded successfully from UPEU")
except Exception as e:
    logger.error(f"Error reading tables from UPEU: {e}")
    raise ValueError(f"Error reading tables from UPEU: {e}")

try:
    # Aplicar la lógica del MERGE SQL - crear el dataset origen con JOIN
    logger.info("Starting transformations - creating dim_periodo_academico from t_gc_mae_calendario_periodo and tipos")
    
    df_dim_periodo_academico = (
        df_t_gc_mae_calendario_periodo.alias("p")
        .join(
            df_tipos.alias("pt"),
            col("p.tipoid") == col("pt.id"),
            "inner"
        )
        .select(
            col("p.calendarioperiodoid").cast(IntegerType()).alias("id_periodo_academico"),
            col("p.calendarioacademicoid").cast(IntegerType()).alias("id_calendario_academico"),
            col("pt.nombre").cast(StringType()).alias("nomb_periodo_academico"),
            col("p.fechainicio").cast(DateType()).alias("fecha_inicio"),
            col("p.fechafin").cast(DateType()).alias("fecha_fin")
        )
        .distinct()  # Aplicar distinct para asegurar unicidad
    )

    # Escribir a analytics usando el método upsert (equivalente al MERGE)
    # El upsert maneja automáticamente both matched/not matched scenarios
    id_columns = ["id_periodo_academico"]  # Clave para el merge
    partition_columns_array = []  # Sin partición específica, puedes agregar si necesitas
    
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_periodo_academico, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing dim_periodo_academico: {e}")
    raise ValueError(f"Error processing dim_periodo_academico: {e}")