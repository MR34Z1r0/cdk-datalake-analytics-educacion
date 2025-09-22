import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, TimestampType

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_vendedor"

try:
    # Leer directamente desde stage/bigmagic en lugar de domain
    # Basado en el código de m_responsable_comercial.py del dominio
    df_m_vendedor_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_vendedor")
    df_m_persona_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_persona")
    df_m_compania_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_pais_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal=True)
    
    logger.info("Dataframes load successfully from stage")
except Exception as e:
    logger.error(f"Error reading tables from stage: {e}")
    raise ValueError(f"Error reading tables from stage: {e}")

try:
    logger.info("Starting creation of dim_vendedor from stage")
    
    # Recrear la lógica de m_responsable_comercial directamente para analytics
    df_dim_vendedor = (
        df_m_vendedor_stage.alias("mv")
        .join(
            df_m_persona_stage.alias("mpe"),
            (col("mv.cod_vendedor") == col("mpe.cod_persona")) & (col("mv.cod_compania") == col("mpe.cod_compania")),
            "inner",
        )
        .join(
            df_m_compania_stage.alias("mc"),
            col("mv.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(
            df_m_pais_stage.alias("mp"), 
            col("mc.cod_pais") == col("mp.cod_pais"), 
            "inner"
        )
        .select(
            # Crear id_vendedor igual que id_responsable_comercial en domain
            concat_ws("|",
                trim(col("mv.cod_compania")),
                trim(col("mv.cod_vendedor"))
            ).cast(StringType()).alias("id_vendedor"),
            
            col("mp.id_pais").cast(StringType()),
            
            # cod_vendedor igual que cod_responsable_comercial en domain
            trim(col("mv.cod_vendedor")).cast(StringType()).alias("cod_vendedor"),
            
            # nombre_vendedor igual que nomb_responsable_comercial en domain
            coalesce(col("mpe.nomb_persona"), lit("")).cast(StringType()).alias("nombre_vendedor"),
            
            # Campos adicionales que pueden ser útiles
            col("mv.cod_tipo_vendedor").cast(StringType()).alias("cod_tipo_vendedor"),
            #coalesce(col("mv.estado"), lit("A")).cast(StringType()).alias("estado"),
            coalesce(lit("A")).cast(StringType()).alias("estado"),
            # Campos de auditoría
            current_date().cast(TimestampType()).alias("fecha_creacion"),
            current_date().cast(TimestampType()).alias("fecha_modificacion")
        )
        .distinct()
    )

    id_columns = ["id_vendedor"]
    partition_columns_array = ["id_pais"]
    
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_vendedor, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")     
    
except Exception as e:
    logger.error(f"Error processing df_dim_vendedor: {e}")
    raise ValueError(f"Error processing df_dim_vendedor: {e}")