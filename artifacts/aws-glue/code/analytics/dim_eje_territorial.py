import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, split, concat_ws, current_date
from pyspark.sql.types import StringType, DateType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_eje_territorial"

try:
    # Lectura directa desde STAGE (BIGMAGIC) - integrando lógica de domain
    logger.info("Reading tables from BIGMAGIC (Stage layer)")
    df_m_distrito = spark_controller.read_table(data_paths.BIGMAGIC, "m_ng3")
    df_m_provincia = spark_controller.read_table(data_paths.BIGMAGIC, "m_ng2")
    df_m_departamento = spark_controller.read_table(data_paths.BIGMAGIC, "m_ng1")
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal=True)
    
    logger.info("Tables loaded successfully from BIGMAGIC")
except Exception as e:
    logger.error(f"Error reading tables from BIGMAGIC: {e}")
    raise ValueError(f"Error reading tables from BIGMAGIC: {e}")

try:
    # Aplicar transformaciones que estaban en m_eje_territorial.py (domain)
    logger.info("Starting domain transformations - creating NG4 level")
    
    # Crear NG4 (Distritos/Zonas Postales)
    df_ng4 = (
        df_m_distrito.alias("di")
        .join(df_m_pais.alias("p"), col("di.id_pais") == col("p.cod_pais"), "inner")
        .where(col("di.cod_zona_postal").isNotNull())
        .select(
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(col("di.cod_zona_postal")),
            ).alias("id_eje_territorial"),
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(col("di.cod_zona_postal")),
                lit("NG3"),
            ).alias("id_eje_territorial_padre"),
            col("p.id_pais").alias("id_pais"),
            trim(coalesce(col("di.cod_zona_postal"), lit("0"))).alias("cod_eje_territorial"),
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(coalesce(col("di.cod_zona_postal"), lit("0"))),
            ).alias("cod_eje_territorial_ref"),
            col("di.desc_ng3").alias("nomb_eje_territorial"),
            lit("NG4").alias("cod_tipo_eje_territorial"),
            lit("A").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
            row_number()
            .over(
                Window.partitionBy(
                    col("di.id_pais"), coalesce(col("di.cod_zona_postal"), lit("0"))
                ).orderBy(col("di.cod_ng3").desc())
            )
            .alias("orden"),
        )
    )

    # Aplicar filtro de orden para NG4
    df_ng4 = df_ng4.where(col("orden") == 1).select(
        col("id_eje_territorial"),
        col("id_eje_territorial_padre"),
        col("id_pais"),
        col("cod_eje_territorial"),
        col("cod_eje_territorial_ref"),
        col("nomb_eje_territorial"),
        col("cod_tipo_eje_territorial"),
        col("estado"),
        col("fecha_creacion"),
        col("fecha_modificacion"),
    )

    logger.info("Starting domain transformations - creating NG3 level")
    
    # Crear NG3 (Distritos)
    df_ng3 = (
        df_m_distrito.alias("di")
        .join(df_m_pais.alias("p"), col("di.id_pais") == col("p.cod_pais"), "inner")
        .where(col("di.cod_zona_postal").isNotNull())
        .select(
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(col("di.cod_zona_postal")),
                lit("NG3"),
            ).alias("id_eje_territorial"),
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(col("di.cod_ng1")),
                trim(col("di.cod_ng2")),
            ).alias("id_eje_territorial_padre"),
            col("p.id_pais").alias("id_pais"),
            trim(coalesce(col("di.cod_zona_postal"), lit("0"))).alias("cod_eje_territorial"),
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(coalesce(col("di.cod_zona_postal"), lit("0"))),
            ).alias("cod_eje_territorial_ref"),
            col("di.desc_ng3").alias("nomb_eje_territorial"),
            lit("NG3").alias("cod_tipo_eje_territorial"),
            lit("A").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
            row_number()
            .over(
                Window.partitionBy(
                    col("di.id_pais"), coalesce(col("di.cod_zona_postal"), lit("0"))
                ).orderBy(col("di.cod_ng3").desc())
            )
            .alias("orden"),
        )
    )

    # Aplicar filtro de orden para NG3
    df_ng3 = df_ng3.where(col("orden") == 1).select(
        col("id_eje_territorial"),
        col("id_eje_territorial_padre"),
        col("id_pais"),
        col("cod_eje_territorial"),
        col("cod_eje_territorial_ref"),
        col("nomb_eje_territorial"),
        col("cod_tipo_eje_territorial"),
        col("estado"),
        col("fecha_creacion"),
        col("fecha_modificacion"),
    )

    logger.info("Starting domain transformations - creating NG2 level")
    
    # Crear NG2 (Provincias)
    df_ng2 = (
        df_m_provincia.alias("pr")
        .join(df_m_pais.alias("p"), col("pr.id_pais") == col("p.cod_pais"), "inner")
        .where(col("pr.cod_ng2").isNotNull())
        .select(
            concat_ws("|",
                trim(col("p.id_pais")),
                trim((col("pr.cod_ng1"))),
                trim((col("pr.cod_ng2")))
                ).alias("id_eje_territorial"),
            concat_ws("|",
                      trim(col("p.id_pais")),
                      trim(col("pr.cod_ng1"))
                      ).alias("id_eje_territorial_padre"),
            col("p.id_pais").alias("id_pais"),
            trim(coalesce(col("pr.cod_ng2"), lit("0"))).alias("cod_eje_territorial"),
            lit(None).alias("cod_eje_territorial_ref"),
            col("pr.desc_ng2").alias("nomb_eje_territorial"),
            lit("NG2").alias("cod_tipo_eje_territorial"),
            lit("A").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    logger.info("Starting domain transformations - creating NG1 level")
    
    # Crear NG1 (Departamentos)
    df_ng1 = (
        df_m_departamento.alias("de")
        .join(df_m_pais.alias("p"), col("de.id_pais") == col("p.cod_pais"), "inner")
        .where(col("de.cod_ng1").isNotNull())
        .select(
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(coalesce(col("de.cod_ng1"), lit("0"))),
            ).alias("id_eje_territorial"),
            lit(None).alias("id_eje_territorial_padre"),
            col("p.id_pais").alias("id_pais"),
            trim(coalesce(col("de.cod_ng1"), lit("0"))).alias("cod_eje_territorial"),
            lit(None).alias("cod_eje_territorial_ref"),
            col("de.desc_ng1").alias("nomb_eje_territorial"),
            lit("NG1").alias("cod_tipo_eje_territorial"),
            lit("A").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    # Unir todos los niveles (equivalente a m_eje_territorial de domain)
    logger.info("Creating unified m_eje_territorial table")
    df_m_eje_territorial = df_ng4.union(df_ng3.union(df_ng2.union(df_ng1))).distinct()

    df_m_eje_territorial = (df_m_eje_territorial
        .select(
            col("id_eje_territorial").cast(StringType()),
            col("id_eje_territorial_padre").cast(StringType()),
            col("id_pais").cast(StringType()),
            col("cod_eje_territorial").cast(StringType()),
            col("cod_eje_territorial_ref").cast(StringType()),
            col("nomb_eje_territorial").cast(StringType()),
            col("cod_tipo_eje_territorial").cast(StringType()),
            col("estado").cast(StringType()),
            col("fecha_creacion").cast(DateType()),
            col("fecha_modificacion").cast(DateType())
        )
    ).cache()

    # Aplicar transformaciones específicas de analytics (las que ya tenías)
    logger.info("Starting analytics transformations - creating dimension levels")
    
    # Separar por niveles para la dimensión final
    df_dim_eje_territorial_ng4 = (
        df_m_eje_territorial.alias("di")
        .where(col("di.cod_tipo_eje_territorial") == 'NG4')
        .select(
            col('di.id_eje_territorial'),
            col('di.id_eje_territorial_padre'),
            col('di.id_pais'),
            col('di.cod_eje_territorial'),
            col('di.cod_eje_territorial_ref'),
            col('di.nomb_eje_territorial'),
            col('di.cod_tipo_eje_territorial')
        )
    ) 

    df_dim_eje_territorial_ng3 = (
        df_m_eje_territorial.alias("di")
        .where(col("di.cod_tipo_eje_territorial") == 'NG3')
        .select(
            col('di.id_eje_territorial'),
            col('di.id_eje_territorial_padre'),
            col('di.id_pais'),
            col('di.cod_eje_territorial'),
            col('di.cod_eje_territorial_ref'),
            col('di.nomb_eje_territorial'),
            col('di.cod_tipo_eje_territorial')
        )
    )
    
    df_dim_eje_territorial_ng2 = (
        df_m_eje_territorial.alias("pr")
        .where(col("pr.cod_tipo_eje_territorial") == 'NG2')
        .select(
            col('pr.id_eje_territorial'),
            col('pr.id_eje_territorial_padre'),
            col('pr.id_pais'),
            col('pr.cod_eje_territorial'),
            col('pr.nomb_eje_territorial'),
            col('pr.cod_tipo_eje_territorial')
        )
    )
    
    df_dim_eje_territorial_ng1 = (
        df_m_eje_territorial.alias("de")
        .where(col("de.cod_tipo_eje_territorial") == 'NG1')
        .select(
            col('de.id_eje_territorial'),
            col('de.id_eje_territorial_padre'),
            col('de.id_pais'),
            col('de.cod_eje_territorial'),
            col('de.nomb_eje_territorial'),
            col('de.cod_tipo_eje_territorial')
        )
    )

    # Crear la dimensión final con jerarquía completa
    logger.info("Creating final dimension with complete hierarchy")
    df_dim_eje_territorial = (
        df_dim_eje_territorial_ng4.alias("ng4")
        .join(
            df_dim_eje_territorial_ng3.alias("ng3"),
            (col("ng3.id_eje_territorial") == col("ng4.id_eje_territorial_padre")),
            "inner",
        )
        .join(
            df_dim_eje_territorial_ng2.alias("ng2"),
            (col("ng2.id_eje_territorial") == col("ng3.id_eje_territorial_padre")),
            "inner",
        )
        .join(
            df_dim_eje_territorial_ng1.alias("ng1"),
            (col("ng1.id_eje_territorial") == col("ng2.id_eje_territorial_padre")),
            "inner",
        )
        .join(
            df_m_pais.alias("mp"),
            (col("ng4.id_pais") == col("mp.id_pais")),
            "inner",
        )
        .select(
            col('ng4.id_eje_territorial').cast("string").alias('id_eje_territorial'),
            col('mp.id_pais').cast("string"),
            col('mp.cod_pais').cast("string"),
            col('mp.desc_pais').cast("string"),
            col('ng1.cod_eje_territorial').cast("string").alias('cod_ng1'),
            col('ng1.nomb_eje_territorial').cast("string").alias('desc_ng1'),
            col('ng2.cod_eje_territorial').cast("string").alias('cod_ng2'),
            col('ng2.nomb_eje_territorial').cast("string").alias('desc_ng2'),
            col('ng3.cod_eje_territorial').cast("string").alias('cod_ng3'),
            col('ng3.nomb_eje_territorial').cast("string").alias('desc_ng3'),
            col('ng4.cod_eje_territorial').cast("string").alias('cod_ng4'),
            col('ng4.nomb_eje_territorial').cast("string").alias('desc_ng4'),
            split(col("ng4.cod_eje_territorial_ref"), '\\|').getItem(1).cast("string").alias("zona_postal"),
        )
    )

    # Escribir a analytics
    id_columns = ["id_eje_territorial"]
    partition_columns_array = ["id_pais"]
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_eje_territorial, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing df_dim_eje_territorial: {e}")
    raise ValueError(f"Error processing df_dim_eje_territorial: {e}")