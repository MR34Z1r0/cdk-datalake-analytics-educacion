import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat_ws, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_sucursal"

try:
    # Leer directamente desde stage/bigmagic en lugar de domain
    df_m_sucursal_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_sucursal")
    df_m_compania_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_pais_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais")
    
    logger.info("Dataframes load successfully from stage")
except Exception as e:
    logger.error(f"Error reading tables from stage: {e}")
    raise ValueError(f"Error reading tables from stage: {e}")

try:
    logger.info("Starting creation of df_dim_sucursal from stage")
    
    # Crear df_m_compania con join a m_pais para obtener id_pais
    df_m_compania = (
        df_m_compania_stage.alias("mc")
        .join(
            df_m_pais_stage.alias("mp"),
            col("mp.cod_pais") == col("mc.cod_pais"),
            "inner"
        )
        .select(
            col("mp.id_pais"),
            col("mc.cod_compania")
        )
    )
    
    # Crear dimensión sucursal desde stage
    df_dim_sucursal = (
        df_m_sucursal_stage.alias("ms")
        .join(
            df_m_compania.alias("mc"),
            col("mc.cod_compania") == col("ms.cod_compania"),
            "inner"
        )
        .select(
            # Crear id_sucursal combinando cod_compania + cod_sucursal
            concat_ws('|', col("ms.cod_compania"), col("ms.cod_sucursal")).alias('id_sucursal'),
            col('mc.id_pais').cast("string"),
            col('ms.cod_compania').cast("string"),
            col('ms.cod_sucursal').cast("string"),
            coalesce(col('ms.desc_sucursal'), lit("")).cast("string").alias('desc_sucursal'),
            lit('A').cast("string").alias('estado')
        )
        .distinct()
    )

    id_columns = ["id_sucursal"]
    partition_columns_array = ["id_pais"]
    
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_sucursal, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing df_dim_sucursal: {e}")
    raise ValueError(f"Error processing df_dim_sucursal: {e}")