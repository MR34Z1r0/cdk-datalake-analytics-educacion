import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, upper, lit, cast, current_date, concat, concat_ws, trim
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_clasificacion_cliente"

try:
    # Lectura directa desde STAGE (BIGMAGIC) - integrando lógica de domain
    logger.info("Reading tables from BIGMAGIC (Stage layer)")
    i_relacion_consumo = spark_controller.read_table(data_paths.BIGMAGIC, "i_relacion_consumo")
    m_canal_visibilidad = spark_controller.read_table(data_paths.BIGMAGIC, "m_canal")
    m_subgiro_visibilidad = spark_controller.read_table(data_paths.BIGMAGIC, "m_subgiro")
    m_giro_visibilidad = spark_controller.read_table(data_paths.BIGMAGIC, "m_giro")
    m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal=True)
    
    logger.info("Tables loaded successfully from BIGMAGIC")
except Exception as e:
    logger.error(f"Error reading tables from BIGMAGIC: {e}")
    raise ValueError(f"Error reading tables from BIGMAGIC: {e}")

try:
    # Aplicar transformaciones que estaban en m_clasificacion_cliente.py (domain)
    logger.info("Starting domain transformations - creating df_subgiro")
    
    df_subgiro = (
        i_relacion_consumo.alias("irc")
        .join(m_compania.alias("mc"), col("mc.cod_compania") == col("irc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .join(
            m_subgiro_visibilidad.alias("mgv"),
            (col("mgv.cod_subgiro") == col("irc.cod_subgiro")) & (col("mgv.cod_compania") == col("irc.cod_compania")),
            "inner",
        )
        .select(
            col("mp.id_pais").alias("id_pais"), 
            concat_ws("|", 
                      trim(col("irc.cod_compania")), 
                      lit("SG"),
                      trim(col("irc.cod_subgiro"))
                      ).alias("id_clasificacion_cliente"),
            concat_ws("|", 
                      trim(col("irc.cod_compania")), 
                      lit("GR"), 
                      trim(col("irc.cod_giro")),
                      trim(col("irc.cod_canal")),
                      ).alias("id_clasificacion_cliente_padre"),
            col("irc.cod_subgiro").alias("cod_clasificacion_cliente"),
            col("mgv.desc_subgiro").alias("nomb_clasificacion_cliente"),
            lit("Subgiro").alias("cod_tipo_clasificacion_cliente"),
            col("mgv.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    logger.info("Starting domain transformations - creating df_giro")
    
    df_giro = (
        i_relacion_consumo.alias("irc")
        .join(m_compania.alias("mc"), col("mc.cod_compania") == col("irc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .join(
            m_giro_visibilidad.alias("mgv"),
            (col("mgv.cod_giro") == col("irc.cod_giro")) & (col("mgv.cod_compania") == col("irc.cod_compania")),
            "inner",
        )
        .select(
            col("mp.id_pais").alias("id_pais"), 
            concat_ws("|", 
                      trim(col("irc.cod_compania")), 
                      lit("GR"), 
                      trim(col("irc.cod_giro")),
                      trim(col("irc.cod_canal")),
                      ).alias("id_clasificacion_cliente"),
            concat_ws("|", 
                      trim(col("irc.cod_compania")), 
                      lit("CN"), 
                      trim(col("irc.cod_canal"))
                      ).alias("id_clasificacion_cliente_padre"),
            col("irc.cod_giro").alias("cod_clasificacion_cliente"),
            col("mgv.desc_giro").alias("nomb_clasificacion_cliente"),
            lit("Giro").alias("cod_tipo_clasificacion_cliente"),
            col("mgv.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    logger.info("Starting domain transformations - creating df_canal")
    
    df_canal = (
        i_relacion_consumo.alias("irc")
        .join(m_compania.alias("mc"), col("mc.cod_compania") == col("irc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .join(
            m_canal_visibilidad.alias("mcv"),
            (col("mcv.cod_canal") == col("irc.cod_canal")) & (col("mcv.cod_compania") == col("irc.cod_compania")),
            "inner",
        )
        .select(
            col("mp.id_pais").alias("id_pais"),
            concat_ws("|",
                      trim(col("irc.cod_compania")), 
                      lit("CN"), 
                      trim(col("irc.cod_canal"))
                      ).alias("id_clasificacion_cliente"),
            lit(None).alias("id_clasificacion_cliente_padre"),
            col("irc.cod_canal").alias("cod_clasificacion_cliente"),
            col("mcv.desc_canal").alias("nomb_clasificacion_cliente"),
            lit("Canal").alias("cod_tipo_clasificacion_cliente"),
            col("mcv.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    # Aplicar distinct y unión como estaba en domain
    logger.info("Applying distinct and union operations")
    df_subgiro = df_subgiro.distinct()
    df_giro = df_giro.distinct()
    df_canal = df_canal.distinct()

    # Crear la tabla unificada de clasificación de cliente (equivalente a m_clasificacion_cliente de domain)
    logger.info("Creating unified m_clasificacion_cliente table")
    df_m_clasificacion_cliente = (
        df_subgiro.union(df_giro).union(df_canal)
        .select(
            col("id_pais").cast(StringType()).alias("id_pais"),
            col("id_clasificacion_cliente").cast(StringType()).alias("id_clasificacion_cliente"),
            col("id_clasificacion_cliente_padre").cast(StringType()).alias("id_clasificacion_cliente_padre"),
            col("cod_clasificacion_cliente").cast(StringType()).alias("cod_clasificacion_cliente"),
            col("nomb_clasificacion_cliente").cast(StringType()).alias("nomb_clasificacion_cliente"),
            col("cod_tipo_clasificacion_cliente").cast(StringType()).alias("cod_tipo_clasificacion_cliente"),
            col("estado").cast(StringType()).alias("estado"),
            col("fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("fecha_modificacion").cast(DateType()).alias("fecha_modificacion"),
        )
    ).cache()

    # Aplicar transformaciones específicas de analytics (las que ya tenías)
    logger.info("Starting analytics transformations - creating df_m_clasificacion_cliente_subgiro")
    df_m_clasificacion_cliente_subgiro = (
        df_m_clasificacion_cliente.alias("mcc")
        .where(
            (upper(col('cod_tipo_clasificacion_cliente')) == 'SUBGIRO')
        )
        .select(
            col('mcc.id_pais'),
            col('mcc.id_clasificacion_cliente').alias('id_subgiro'),
            col('mcc.id_clasificacion_cliente_padre').alias('id_giro'),
            col('mcc.cod_clasificacion_cliente').alias('cod_subgiro'),
            col('mcc.nomb_clasificacion_cliente').alias('desc_subgiro')
        )
    )
    
    logger.info("Starting analytics transformations - creating df_m_clasificacion_cliente_giro")
    df_m_clasificacion_cliente_giro = (
        df_m_clasificacion_cliente.alias("mcc")
        .where(
            (upper(col('cod_tipo_clasificacion_cliente')) == 'GIRO')
        )
        .select(
            col('mcc.id_pais'),
            col('mcc.id_clasificacion_cliente').alias('id_giro'),
            col('mcc.id_clasificacion_cliente_padre').alias('id_canal'),
            col('mcc.cod_clasificacion_cliente').alias('cod_giro'),
            col('mcc.nomb_clasificacion_cliente').alias('desc_giro')
        )
    )
     
    logger.info("Starting analytics transformations - creating df_m_clasificacion_cliente_canal")
    df_m_clasificacion_cliente_canal = (
        df_m_clasificacion_cliente.alias("mcc")
        .where(
            (upper(col('cod_tipo_clasificacion_cliente')) == 'CANAL')
        )
        .select(
            col('mcc.id_pais'),
            col('mcc.id_clasificacion_cliente').alias('id_canal'),
            col('mcc.cod_clasificacion_cliente').alias('cod_canal'),
            col('mcc.nomb_clasificacion_cliente').alias('desc_canal')
        )
    )
    
    # Crear la dimensión final
    logger.info("Creating final dimension - df_dim_clasificacion_cliente")
    df_dim_clasificacion_cliente = (
        df_m_clasificacion_cliente_subgiro.alias("su")
        .join(
            df_m_clasificacion_cliente_giro.alias("gi"),
            (col("gi.id_giro") == col("su.id_giro")),
            "left"
        )
        .join(
            df_m_clasificacion_cliente_canal.alias("ca"),
            (col("ca.id_canal") == col("gi.id_canal")),
            "left",
        )
        .select(
            col('su.id_subgiro').cast("string").alias('id_clasificacion_cliente'),
            col('su.id_pais').cast("string").alias('id_pais'),
            col('su.cod_subgiro').cast("string").alias('cod_subgiro'),
            col('su.desc_subgiro').cast("string").alias('desc_subgiro'),
            lit(None).cast("string").alias('cod_ocasion_consumo'),
            lit(None).cast("string").alias('desc_ocasion_consumo'),
            col('gi.cod_giro').cast("string").alias('cod_giro'),
            col('gi.desc_giro').cast("string").alias('desc_giro'),
            col('ca.cod_canal').cast("string").alias('cod_canal'),
            col('ca.desc_canal').cast("string").alias('desc_canal')
        )
    )

    # Escribir a analytics
    column_keys = ["id_clasificacion_cliente"]
    partition_keys = ["id_pais"]
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_clasificacion_cliente, data_paths.ANALYTICS, target_table_name, column_keys, partition_keys)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing df_dim_clasificacion_cliente: {e}")
    raise ValueError(f"Error processing df_dim_clasificacion_cliente: {e}")