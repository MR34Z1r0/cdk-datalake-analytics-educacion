import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date
from pyspark.sql.types import StringType, DateType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_estructura_comercial"

try:
    # Lectura directa desde STAGE (BIGMAGIC) - integrando lógica de domain
    logger.info("Reading tables from BIGMAGIC (Stage layer)")
    
    # Tablas base para estructura comercial
    df_m_region = spark_controller.read_table(data_paths.BIGMAGIC, "m_region")
    df_m_subregion = spark_controller.read_table(data_paths.BIGMAGIC, "m_subregion")
    df_m_centro_distribucion = spark_controller.read_table(data_paths.BIGMAGIC, "m_division")
    df_m_zona_distribucion = spark_controller.read_table(data_paths.BIGMAGIC, "m_zona")
    df_m_ruta = spark_controller.read_table(data_paths.BIGMAGIC, "m_ruta")
    
    # Tablas complementarias
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal=True)
    
    # Tablas para módulo
    df_m_modulo_raw = spark_controller.read_table(data_paths.BIGMAGIC, "m_modulo")
    df_m_asignacion_modulo = spark_controller.read_table(data_paths.BIGMAGIC, "m_asignacion_modulo")
    
    logger.info("Tables loaded successfully from BIGMAGIC")
except Exception as e:
    logger.error(f"Error reading tables from BIGMAGIC: {e}")
    raise ValueError(f"Error reading tables from BIGMAGIC: {e}")

try:
    # PASO 1: Aplicar transformaciones que estaban en m_estructura_comercial.py (domain)
    logger.info("Starting domain transformations - creating estructura comercial hierarchy")
    
    # Crear estructura comercial - Region
    df_estructura_comercial_region = (
        df_m_region.alias("mrd")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mrd.cod_pais"), "inner")
        .select(
            concat_ws("|",
                col("mp.id_pais"),
                trim(col("mrd.cod_region")).cast("string")
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            lit(None).alias("id_sucursal"),
            lit(None).alias("id_estructura_comercial_padre"),
            lit(None).alias("id_responsable_comercial"),
            trim(col("mrd.cod_region").cast("string")).alias("cod_estructura_comercial"),
            col("mrd.desc_region").alias("nomb_estructura_comercial"),
            lit("Región").alias("cod_tipo_estructura_comercial"),
            col("mrd.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    # Crear estructura comercial - Subregion
    df_estructura_comercial_subregion = (
        df_m_subregion.alias("msr")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("msr.cod_pais"), "inner")
        .select(
            concat_ws("|",
                col("mp.id_pais"),
                trim(col("msr.cod_region").cast("string")),
                trim(col("msr.cod_subregion").cast("string")),
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            lit(None).alias("id_sucursal"),
            concat_ws("|",
                col("mp.id_pais"),
                trim(col("msr.cod_region").cast("string"))
            ).alias("id_estructura_comercial_padre"),
            lit(None).alias("id_responsable_comercial"),
            trim(col("cod_subregion").cast("string")).alias("cod_estructura_comercial"),
            col("msr.desc_subregion").alias("nomb_estructura_comercial"),
            lit("Subregión").alias("cod_tipo_estructura_comercial"),
            col("msr.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    # Obtener zonas distribucion distinct
    df_m_zona_distribucion_distinct = df_m_zona_distribucion.select(
        col("cod_compania"),
        col("cod_sucursal"),
        col("cod_centro_distribucion"),
        col("cod_subregion"),
        col("cod_region"),
    ).distinct()

    # Crear estructura comercial - Division
    df_estructura_comercial_division = (
        df_m_centro_distribucion.alias("mrd")
        .join(
            df_m_zona_distribucion_distinct.alias("mzd"),
            (col("mrd.cod_compania") == col("mzd.cod_compania")) & (col("mrd.cod_division") == col("mzd.cod_centro_distribucion")),
            "inner",
        )
        .join(
            df_m_compania.alias("mc"),
            col("mrd.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .select(
            concat_ws("|",
                trim(col("mrd.cod_compania")), 
                trim(col("mzd.cod_sucursal")), 
                col("mzd.cod_region"), 
                col("mzd.cod_subregion"), 
                col("mrd.cod_division").cast("string"),
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                trim(col("cod_sucursal"))
            ).alias("id_sucursal"),
            concat_ws("|",
                col("mp.id_pais"), 
                trim(col("mzd.cod_region").cast("string")), 
                trim(col("mzd.cod_subregion").cast("string")),
            ).alias("id_estructura_comercial_padre"),
            concat_ws("|",
                trim(col("mrd.cod_compania")), 
                trim(col("cod_jefe_venta").cast("string")),
            ).alias("id_responsable_comercial"),
            trim(col("mrd.cod_division").cast("string")).alias("cod_estructura_comercial"),
            col("mrd.desc_division").alias("nomb_estructura_comercial"),
            lit("División").alias("cod_tipo_estructura_comercial"),
            col("mrd.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    # Crear estructura comercial - Zona
    df_estructura_comercial_zona = (
        df_m_zona_distribucion.alias("mrd")
        .join(
            df_m_compania.alias("mc"),
            col("mrd.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .select(
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                trim(col("cod_sucursal")),
                trim(col("cod_zona").cast("string")),
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            concat_ws("|",
                trim(col("mrd.cod_compania")), 
                trim(col("cod_sucursal"))
            ).alias("id_sucursal"),
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                trim(col("cod_sucursal")),
                col("mrd.cod_region"),
                col("mrd.cod_subregion"),
                trim(col("cod_centro_distribucion").cast("string")),
            ).alias("id_estructura_comercial_padre"),
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                col("cod_supervisor").cast("string"),
            ).alias("id_responsable_comercial"),
            col("cod_zona").cast("string").alias("cod_estructura_comercial"),
            col("mrd.desc_zona").alias("nomb_estructura_comercial"),
            lit("Zona").alias("cod_tipo_estructura_comercial"),
            col("mrd.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    # Crear estructura comercial - Ruta
    df_estructura_comercial_ruta = (
        df_m_ruta.alias("mrd")
        .join(
            df_m_compania.alias("mc"),
            col("mrd.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .select(
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                trim(col("cod_sucursal")),
                trim(col("cod_fuerza_venta").cast("string")),
                trim(col("cod_ruta").cast("string")),
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                trim(col("cod_sucursal"))
            ).alias("id_sucursal"),
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                trim(col("cod_sucursal")),
                trim(col("cod_zona").cast("string")),
            ).alias("id_estructura_comercial_padre"),
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                trim(col("cod_vendedor").cast("string")),
            ).alias("id_responsable_comercial"),
            col("cod_ruta").cast("string").alias("cod_estructura_comercial"),
            col("desc_ruta").alias("nomb_estructura_comercial"),
            lit("Ruta").alias("cod_tipo_estructura_comercial"),
            col("mrd.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    # Unir todas las estructuras comerciales
    logger.info("Unifying estructura comercial hierarchy")
    df_m_estructura_comercial = (
        df_estructura_comercial_region
        .union(df_estructura_comercial_subregion)
        .union(df_estructura_comercial_division)
        .union(df_estructura_comercial_zona)
        .union(df_estructura_comercial_ruta)
        .distinct()
    ).cache()

    # PASO 4: Procesar m_modulo (domain) 
    logger.info("Processing modulo from BIGMAGIC")
    # Crear tabla temporal con asignación módulo filtrada
    df_tmp_modulo = (
        df_m_modulo_raw.alias("mm")
        .join(
            df_m_asignacion_modulo.alias("mam"),
            (col("mm.cod_compania") == col("mam.cod_compania"))
            & (col("mm.cod_sucursal") == col("mam.cod_sucursal"))
            & (col("mm.cod_fuerza_venta") == col("mam.cod_fuerza_venta"))
            & (col("mm.cod_modulo") == col("mam.cod_modulo")),
            "inner",
        )
        .join(df_m_compania.alias("mc"), col("mm.cod_compania") == col("mc.cod_compania"), "inner")
        .join(df_m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            concat_ws("|",
                trim(col("mm.cod_compania")),
                trim(col("mm.cod_sucursal")),
                trim(col("mm.cod_fuerza_venta").cast("string")),
                trim(col("mm.cod_modulo")),
            ).alias("id_modulo"),
            col("mp.id_pais").alias("id_pais"),
            concat_ws("|",
                trim(col("mm.cod_compania")),
                trim(col("mm.cod_sucursal"))
            ).alias("id_sucursal"),
            concat_ws("|",
                trim(col("mm.cod_compania")),
                trim(col("mm.cod_sucursal")),
                trim(col("mm.cod_fuerza_venta").cast("string")),
                trim(col("mm.cod_ruta").cast("string")),
            ).alias("id_estructura_comercial"),
            col("mm.cod_modulo").cast("string").alias("cod_modulo"),
            col("mm.desc_modulo").cast("string").alias("desc_modulo"),
            lit(None).cast("string").alias("desc_fuerza_venta"),
            lit(None).cast("string").alias("periodo_visita"),
            lit("A").cast("string").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion")
        )
    )

    df_m_modulo = df_tmp_modulo.cache()

    # PASO 5: Aplicar transformaciones específicas de analytics (las que ya tenías)
    logger.info("Starting analytics transformations - creating dimension structure")
    
    df_m_modulo_select = (
        df_m_modulo.alias("mm")
        .join(
            df_m_pais.alias("mp"),
            (col("mm.id_pais") == col("mp.id_pais")),
            "left",
        )
        .join(
            df_m_estructura_comercial.alias("mec_ruta"),
            (col("mm.id_estructura_comercial") == col("mec_ruta.id_estructura_comercial")),
            "left",
        ) 
        .join(
            df_m_estructura_comercial.alias("mec_zona"),
            (col("mec_ruta.id_estructura_comercial_padre") == col("mec_zona.id_estructura_comercial")),
            "left",
        )
        .join(
            df_m_estructura_comercial.alias("mec_division"),
            (col("mec_zona.id_estructura_comercial_padre") == col("mec_division.id_estructura_comercial")),
            "left",
        )
        .join(
            df_m_estructura_comercial.alias("mec_subregion"),
            (col("mec_division.id_estructura_comercial_padre") == col("mec_subregion.id_estructura_comercial")),
            "left",
        )
        .join(
            df_m_estructura_comercial.alias("mec_region"),
            (col("mec_subregion.id_estructura_comercial_padre") == col("mec_region.id_estructura_comercial")),
            "left",
        )
        .select(
            col('mm.id_modulo').alias('id_estructura_comercial'),
            col('mm.id_pais'),
            lit(None).alias('cod_fuerza_venta'), 
            col('mp.cod_pais'),
            col('mec_region.cod_estructura_comercial').alias('cod_region'),
            col('mec_subregion.cod_estructura_comercial').alias('cod_subregion'),
            col('mec_division.cod_estructura_comercial').alias('cod_division'),
            col('mec_zona.cod_estructura_comercial').alias('cod_zona'),
            col('mec_ruta.cod_estructura_comercial').alias('cod_ruta'),
            col('mm.cod_modulo'),
            lit(None).alias('desc_fuerza_venta'),
            col('mec_region.nomb_estructura_comercial').alias('desc_region'),
            col('mec_subregion.nomb_estructura_comercial').alias('desc_subregion'),
            col('mec_division.nomb_estructura_comercial').alias('desc_division'),
            col('mec_zona.nomb_estructura_comercial').alias('desc_zona'),
            col('mec_ruta.nomb_estructura_comercial').alias('desc_ruta'),
            col('mm.desc_modulo').alias('desc_modulo'),
        )
    )

    # Crear la dimensión final
    logger.info("Creating final dimension - df_dim_estructura_comercial")
    df_dim_estructura_comercial = (
        df_m_modulo_select
        .select(
            col("id_estructura_comercial").cast("string"),
            col("id_pais").cast("string"),
            col("cod_fuerza_venta").cast("string"),
            col("cod_pais").cast("string"),
            col("cod_region").cast("string"),
            col("cod_subregion").cast("string"),
            col("cod_division").cast("string"),
            col("cod_zona").cast("string"),
            col("cod_ruta").cast("string"),
            col("cod_modulo").cast("string"),
            col("desc_fuerza_venta").cast("string"),
            col("desc_region").cast("string"),
            col("desc_subregion").cast("string"),
            col("desc_division").cast("string"),
            col("desc_zona").cast("string"),
            col("desc_ruta").cast("string"),
            col("desc_modulo").cast("string"),            
        )   
    )

    window_spec = Window.partitionBy("id_estructura_comercial").orderBy(df_dim_estructura_comercial["desc_modulo"].desc_nulls_last())

    df_dim_estructura_comercial = df_dim_estructura_comercial.withColumn(
        "row_num", row_number().over(window_spec)
    ).filter("row_num = 1").drop("row_num")

    # Escribir a analytics
    id_columns = ["id_estructura_comercial"]
    partition_columns_array = ["id_pais"]
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_estructura_comercial, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing df_dim_estructura_comercial: {e}")
    raise ValueError(f"Error processing df_dim_estructura_comercial: {e}")