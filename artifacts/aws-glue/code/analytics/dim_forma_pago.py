import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_forma_pago"

try:
    # Lectura directa desde STAGE (BIGMAGIC) - integrando lógica de domain
    logger.info("Reading tables from BIGMAGIC (Stage layer)")
    df_m_forma_pago_raw = spark_controller.read_table(data_paths.BIGMAGIC, "m_forma_pago")
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal=True)
    
    logger.info("Tables loaded successfully from BIGMAGIC")
except Exception as e:
    logger.error(f"Error reading tables from BIGMAGIC: {e}")
    raise ValueError(f"Error reading tables from BIGMAGIC: {e}")

try:
    # Aplicar transformaciones que estaban en m_forma_pago.py (domain)
    logger.info("Starting domain transformations - processing forma pago")
    df_m_forma_pago = (
        df_m_forma_pago_raw.alias("mfp")
        .join(df_m_compania.alias("mc"), col("mfp.cod_compania") == col("mc.cod_compania"), "inner")
        .join(df_m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            concat_ws("|",
                trim(col("mfp.cod_compania")),
                trim(col("mfp.cod_forma_pago").cast("string")),
            ).alias("id_forma_pago"),
            col("mp.id_pais").alias("id_pais"),
            col("mfp.cod_forma_pago").cast("string").alias("cod_forma_pago"),
            col("mfp.desc_forma_pago").cast("string").alias("nomb_forma_pago"),
            lit("A").cast("string").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    # Aplicar transformaciones específicas de analytics (las que ya tenías)
    logger.info("Starting analytics transformations - creating dim_forma_pago")
    df_dim_forma_pago = (
        df_m_forma_pago
        .select(
            col('id_forma_pago').cast("string"),
            col('id_pais').cast("string"),
            col('cod_forma_pago').cast("string"),
            col('nomb_forma_pago').cast("string").alias('desc_forma_pago')
        )
    )

    # Escribir a analytics
    id_columns = ["id_forma_pago"]
    partition_columns_array = ["id_pais"]
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_forma_pago, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing df_dim_forma_pago: {e}")
    raise ValueError(f"Error processing df_dim_forma_pago: {e}")