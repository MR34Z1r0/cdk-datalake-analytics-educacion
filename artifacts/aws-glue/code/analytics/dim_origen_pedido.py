import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_origen_pedido"

try:
    # Leer directamente desde stage/bigmagic en lugar de domain
    df_m_origen_pedido_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_origen_pedido")
    df_m_compania_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_pais_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais")
    
    logger.info("Dataframes load successfully from stage")
except Exception as e:
    logger.error(f"Error reading tables from stage: {e}")
    raise ValueError(f"Error reading tables from stage: {e}")

try:
    logger.info("Starting creation of df_dim_origen_pedido from stage")
    
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
    
    # Crear dimensión origen_pedido desde stage
    df_dim_origen_pedido = (
        df_m_origen_pedido_stage.alias("mop")
        .join(
            df_m_compania.alias("mc"),
            col("mc.cod_compania") == col("mop.cod_compania"),
            "inner"
        )
        .select(
            # Crear id_origen_pedido combinando cod_compania + cod_origen_pedido
            concat(col("mop.cod_compania"), lit("|"), col("mop.cod_origen_pedido")).alias('id_origen_pedido'),
            col('mc.id_pais').cast("string"),
            col('mop.cod_origen_pedido').cast("string"),
            coalesce(col('mop.desc_origen_pedido'), lit("")).cast("string").alias('desc_origen_pedido')
        )
        .distinct()
    )

    id_columns = ["id_origen_pedido"]
    partition_columns_array = ["id_pais"]
    
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_origen_pedido, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing df_dim_origen_pedido: {e}")
    raise ValueError(f"Error processing df_dim_origen_pedido: {e}")