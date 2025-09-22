import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_pais"

try:
    # Lectura directa desde STAGE (BIGMAGIC) - integrando lógica de domain
    logger.info("Reading tables from BIGMAGIC (Stage layer)")
    m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal=True)
    
    logger.info("Tables loaded successfully from BIGMAGIC")
except Exception as e:
    logger.error(f"Error reading tables from BIGMAGIC: {e}")
    raise ValueError(f"Error reading tables from BIGMAGIC: {e}")

try:
    # Aplicar transformaciones que estaban en m_pais.py (domain)
    logger.info("Starting domain transformations - filtering countries with active companies")
    
    # Obtener lista de países que tienen compañías activas
    list_cod_pais = m_compania.select("cod_pais").rdd.flatMap(lambda x: x).collect()
    logger.info(f"Found {len(list_cod_pais)} active countries: {list_cod_pais}")

    # Filtrar países activos (equivalente a m_pais de domain)
    df_m_pais_transformed = (
        m_pais
        .where(col("cod_pais").isin(list_cod_pais))
        .select(
            col("id_pais").cast(StringType()),
            col("cod_pais").cast(StringType()),
            col("desc_pais").cast(StringType()),
            col("continente").cast(StringType()).alias("desc_continente")
        )
    )

    # Aplicar transformaciones específicas de analytics (las que ya tenías)
    logger.info("Starting analytics transformations - creating df_dim_pais")
    df_dim_pais = (
        df_m_pais_transformed
        .select(
            col('id_pais').cast("string"),
            col('cod_pais').cast("string"),
            col('desc_pais').cast("string"),
            lit(None).alias('desc_pais_comercial').cast("string"),  # Campo que se agregaba en analytics
            col('desc_continente').cast("string")
        )
    )

    # Escribir a analytics
    id_columns = ["id_pais"]
    partition_columns_array = ["id_pais"]
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_pais, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing df_dim_pais: {e}")
    raise ValueError(f"Error processing df_dim_pais: {e}")