import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_lista_precio" 

try:
    # Leer directamente desde stage/bigmagic en lugar de domain
    df_m_lista_precio_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_lista_precio")
    df_m_compania_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_pais_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais")
    
    logger.info("Dataframes load successfully from stage")
except Exception as e:
    logger.error(f"Error reading tables from stage: {e}")
    raise ValueError(f"Error reading tables from stage: {e}")

try:
    logger.info("Starting creation of df_dim_lista_precio from stage")
    
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
    
    # Crear dimensión lista_precio desde stage
    df_dim_lista_precio = (
        df_m_lista_precio_stage.alias("mlp")
        .join(
            df_m_compania.alias("mc"),
            col("mc.cod_compania") == col("mlp.cod_compania"),
            "inner"
        )
        .select(
            # Crear id_lista_precio combinando cod_compania + cod_lista_precio
            concat(col("mlp.cod_compania"), lit("|"), col("mlp.cod_lista_precio")).alias('id_lista_precio'),
            col('mc.id_pais').cast("string"),
            col('mlp.cod_lista_precio').cast("string"),
            coalesce(col('mlp.desc_lista_precio'), lit("")).cast("string").alias('desc_lista_precio')
        )
        .distinct()
    )

    id_columns = ["id_lista_precio"]
    partition_columns_array = ["id_pais"]
    
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_lista_precio, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")    
    
except Exception as e:
    logger.error(f"Error processing df_dim_lista_precio: {e}")
    raise ValueError(f"Error processing df_dim_lista_precio: {e}")