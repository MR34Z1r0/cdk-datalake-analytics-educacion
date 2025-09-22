import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_cliente"

try:
    # Lectura directa desde STAGE (BIGMAGIC) - integrando lógica de domain
    logger.info("Reading tables from BIGMAGIC (Stage layer)")
    
    # Tablas principales para cliente (m_cliente.py domain)
    df_m_cliente_raw = spark_controller.read_table(data_paths.BIGMAGIC, "m_cliente")
    df_m_estructura_cliente = spark_controller.read_table(data_paths.BIGMAGIC, "m_asignacion_modulo")
    df_m_tipo_cliente = spark_controller.read_table(data_paths.BIGMAGIC, "m_tipo_cliente")
    df_m_cuenta_clave = spark_controller.read_table(data_paths.BIGMAGIC, "m_cuenta_clave")
    df_m_canal = spark_controller.read_table(data_paths.BIGMAGIC, "m_canal")
    df_m_giro = spark_controller.read_table(data_paths.BIGMAGIC, "m_giro")
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal=True)
    
    # Tablas para eje territorial (m_eje_territorial.py domain)
    df_m_distrito = spark_controller.read_table(data_paths.BIGMAGIC, "m_ng3")
    df_m_provincia = spark_controller.read_table(data_paths.BIGMAGIC, "m_ng2")
    df_m_departamento = spark_controller.read_table(data_paths.BIGMAGIC, "m_ng1")
    
    # Tablas para módulo (m_modulo.py domain)
    df_m_modulo_raw = spark_controller.read_table(data_paths.BIGMAGIC, "m_modulo")
    df_m_asignacion_modulo_raw = spark_controller.read_table(data_paths.BIGMAGIC, "m_asignacion_modulo")
        
    logger.info("Tables loaded successfully from BIGMAGIC")
except Exception as e:
    logger.error(f"Error reading tables from BIGMAGIC: {e}")
    raise ValueError(f"Error reading tables from BIGMAGIC: {e}")

try:
    # PASO 1: Aplicar transformaciones que estaban en m_cliente.py (domain)
    logger.info("Starting domain transformations - creating m_cliente")
    
    # Crear tabla temporal con estructura de cliente
    df_tmp_estructura_cliente = (
        df_m_cliente_raw.alias("mcl")
        .join(
            df_m_estructura_cliente.alias("mecl"),
            (col("mcl.cod_compania") == col("mecl.cod_compania"))
            & (col("mcl.cod_cliente") == col("mecl.cod_cliente"))
            & (
                col("mcl.cod_sucursal")
                == when(
                    (col("mcl.cod_sucursal") == "00"), col("mcl.cod_sucursal")
                ).otherwise(col("mecl.cod_sucursal"))
            ),
            "inner",
        )
        .select(
            col("mcl.cod_compania"),
            col("mcl.cod_cliente"),
            col("mecl.coord_x"),
            col("mecl.coord_y"),
            row_number()
            .over(
                Window.partitionBy(
                    "mcl.cod_compania", "mcl.cod_sucursal", "mcl.cod_cliente"
                ).orderBy(col("mecl.cod_fuerza_venta").asc())
            )
            .alias("orden"),
        )
    )
    
    # Crear m_cliente transformado (domain)
    df_m_cliente = (
        df_m_cliente_raw.alias("mc")
        .join(
            df_m_tipo_cliente.alias("tc"),
            (col("mc.cod_compania") == col("tc.cod_compania"))
            & (col("mc.cod_cliente") == col("tc.cod_cliente"))
            & (lower(col("tc.tipo_cliente")).isin(["a", "v", "t"])),
            "left",
        )
        .join(
            df_m_cuenta_clave.alias("cc"),
            (col("mc.cod_compania") == col("cc.cod_compania"))
            & (col("mc.cod_cuenta_clave") == col("cc.cod_cuenta_clave")),
            "left",
        )
        .join(
            df_m_canal.alias("c"),
            (col("c.cod_compania") == col("mc.cod_compania"))
            & (col("c.cod_canal") == col("mc.cod_canal")),
            "left",
        )
        .join(
            df_m_giro.alias("g"),
            (col("g.cod_compania") == col("mc.cod_compania"))
            & (col("g.cod_giro") == col("mc.cod_giro")),
            "left",
        )
        .join(
            df_tmp_estructura_cliente.alias("mecl"),
            (col("mc.cod_compania") == col("mecl.cod_compania"))
            & (col("mc.cod_cliente") == col("mecl.cod_cliente"))
            & (col("mecl.orden") == 1),
            "left",
        )
        .join(
            df_m_compania.alias("mco"),
            (col("mco.cod_compania") == col("mc.cod_compania")),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mco.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_cliente")),
            ).cast(StringType()).alias("id_cliente"),
            lit(None).cast(StringType()).alias("id_cliente_ref"),
            lit(None).cast(StringType()).alias("id_cliente_ref2"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_sucursal")),
            ).cast(StringType()).alias("id_sucursal"),
            when(
                (col("mc.cod_zona_postal").isNull())
                | (col("mc.cod_zona_postal") == ""),
                lit(None),
            )
            .otherwise(
                concat(
                    trim(col("mp.id_pais")),
                    lit("|"),
                    trim(coalesce(col("mc.cod_zona_postal"), lit("0"))),
                )
            )
            .cast(StringType()).alias("id_eje_territorial"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                lit("SG"),
                lit("|"),
                trim(col("mc.cod_subgiro")),
            ).cast(StringType()).alias("id_clasificacion_cliente"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_lista_precio")),
            ).cast(StringType()).alias("id_lista_precio"), 
            col("mc.cod_cliente").cast(StringType()).alias("cod_cliente"),
            col("mc.nomb_cliente").cast(StringType()).alias("nomb_cliente"),
            col("cc.cod_cuenta_clave").cast(StringType()).alias("cod_cuenta_clave"),
            col("cc.descripcion").cast(StringType()).alias("nomb_cuenta_clave"),
            lit(None).cast(StringType()).alias("cod_segmento"),
            lit(None).cast(StringType()).alias("desc_subsegmento"),
            lit(None).cast(StringType()).alias("cod_cliente_ref"),
            lit(None).cast(StringType()).alias("cod_cliente_ref2"),
            lit(None).cast(StringType()).alias("cod_cliente_ref3"),
            lit(None).cast(StringType()).alias("cod_cliente_ref4"),
            col("tc.tipo_cliente").cast(StringType()).alias("cod_tipo_cliente"),
            col("c.desc_canal").cast(StringType()).alias("desc_canal_local"),
            col("g.desc_giro").cast(StringType()).alias("desc_giro_local"),
            col("mc.direccion").cast(StringType()).alias("direccion"),
            col("mc.nro_documento_identidad").cast(StringType()).alias("nro_documento"),
            col("mc.cod_cliente_principal").cast(StringType()).alias("cod_cliente_principal"),
            lit(None).cast(StringType()).alias("cod_cliente_transferencia"),
            col("mecl.coord_x").cast(StringType()).alias("coord_x"),
            col("mecl.coord_y").cast(StringType()).alias("coord_y"),                
            col("mc.fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            lit(None).cast(DateType()).alias("fecha_baja"),
            col("mc.es_activo").cast(StringType()).alias("estado")
        )
    ).cache()

    # PASO 2: Crear m_eje_territorial (similar a dim_eje_territorial pero simplificado)
    logger.info("Creating simplified m_eje_territorial for cliente")
    
    # Crear NG4 (Zonas Postales) - simplificado
    df_ng4 = (
        df_m_distrito.alias("di")
        .join(df_m_pais.alias("p"), col("di.id_pais") == col("p.cod_pais"), "inner")
        .where(col("di.cod_zona_postal").isNotNull())
        .select(
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(col("di.cod_zona_postal")),
            ).alias("id_eje_territorial"),
            col("p.id_pais").alias("id_pais"),
            trim(coalesce(col("di.cod_zona_postal"), lit("0"))).alias("cod_eje_territorial"),
            lit("NG4").alias("cod_tipo_eje_territorial"),
        )
    )

    df_m_eje_territorial = df_ng4.cache()

    # PASO 3: Crear m_asignacion_modulo y m_modulo (simplificado)
    logger.info("Creating simplified m_asignacion_modulo and m_modulo for cliente")
     
    
    # Procesar módulo simplificado
    df_m_modulo = (
        df_m_modulo_raw.alias("mm")
        .join(
            df_m_asignacion_modulo_raw.alias("mam"),
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
            col("mm.fecha_creacion").cast(DateType()).alias("fecha_creacion")
        )
    ).cache()
    
    # Procesar asignación módulo
    df_m_asignacion_modulo = (
        df_m_asignacion_modulo_raw.alias("mam")
        .join(df_m_compania.alias("mc"), col("mam.cod_compania") == col("mc.cod_compania"), "inner")
        .join(df_m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            concat_ws("|",
                trim(col("mam.cod_compania")),
                trim(col("mam.cod_cliente")),
            ).alias("id_cliente"),
            concat_ws("|",
                trim(col("mam.cod_compania")),
                trim(col("mam.cod_sucursal")),
                trim(col("mam.cod_fuerza_venta").cast("string")),
                trim(col("mam.cod_modulo")),
            ).alias("id_modulo"),
            lit(None).cast(StringType()).alias("frecuencia_visita"),
            lit(None).cast(StringType()).alias("periodo_visita"),
            lit(1).cast(IntegerType()).alias("es_activo"),
            lit(0).cast(IntegerType()).alias("es_eliminado")
        )
    ).cache()

    # PASO 4: Aplicar transformaciones específicas de analytics (las que ya tenías)
    logger.info("Starting analytics transformations - creating asignacion modulo filter")
    
    df_m_asignacion_modulo_filter = (
        df_m_asignacion_modulo.alias("mam")
        .filter((col("es_activo") == 1) & (col("es_eliminado") == 0))
        .join(
            df_m_modulo.alias("mm"),
            col("mm.id_modulo") == col("mam.id_modulo"),
            "left",
        )
        .select(
            col("mam.id_cliente"),
            col("mm.id_modulo"),
            col("mam.frecuencia_visita"),
            col("mam.periodo_visita"),
            col("mm.fecha_creacion"),
        )
        .select(
            row_number()
            .over(
                Window.partitionBy("mam.id_cliente").orderBy(col("mm.fecha_creacion").desc())
            )
            .alias("orden"),
            col("mam.id_cliente"),
            col("mm.id_modulo"),
            col("mam.frecuencia_visita"),
            col("mam.periodo_visita")
        )
    )
    
    logger.info("Starting analytics transformations - creating df_m_cliente_select")
    df_m_cliente_select = (
        df_m_cliente.alias("mc")
        .join(
            df_m_asignacion_modulo_filter.alias("dc"),
            (col("mc.id_cliente") == col("dc.id_cliente")) & (col("dc.orden") == 1),
            "left",
        )
        .join(
            df_m_eje_territorial.alias("met"),
            (col("mc.id_eje_territorial") == col("met.id_eje_territorial")),
            "left",
        )
        .select(
            col("mc.id_cliente"),
            col("mc.id_pais"),
            col("mc.id_sucursal"),
            col("dc.id_modulo").alias("id_estructura_comercial"),
            col("mc.id_clasificacion_cliente"),
            col("mc.id_eje_territorial"),
            col("mc.id_lista_precio"),
            col("mc.cod_cliente"),
            col("mc.nomb_cliente"),
            col("mc.cod_segmento"),
            col("mc.desc_subsegmento"),
            col("mc.cod_cliente_ref"),
            col("mc.cod_cliente_ref2"),
            col("mc.cod_cliente_ref3"),
            col("mc.cod_cliente_ref4"), 
            col("mc.cod_tipo_cliente"),
            col("mc.cod_cuenta_clave"),
            col("mc.nomb_cuenta_clave"),
            col("mc.desc_canal_local"),
            col("mc.desc_giro_local"),
            col("mc.direccion"),
            col("mc.nro_documento"),
            col("mc.cod_cliente_principal"),
            col("mc.cod_cliente_transferencia"),
            col("met.cod_eje_territorial"),
            col("mc.coord_x").alias("coordx"),
            col("mc.coord_y").alias("coordy"),
            col("mc.fecha_creacion"),
            col("mc.fecha_baja"),
            col("mc.estado")
        )
    )

    # Crear la dimensión final
    logger.info("Creating final dimension - df_dim_cliente")
    df_dim_cliente = (
        df_m_cliente_select
        .select(
            col("id_cliente").cast("string"),
            col("id_pais").cast("string"),
            col("id_sucursal").cast("string"),
            col("id_estructura_comercial").cast("string"),
            col("id_clasificacion_cliente").cast("string"),
            col("id_eje_territorial").cast("string"),
            col("id_lista_precio").cast("string"),
            col("cod_cliente").cast("string"),
            col("nomb_cliente").cast("string"),
            col("cod_segmento").cast("string"),
            col("desc_subsegmento").cast("string"),
            col("cod_cliente_ref").cast("string"),
            col("cod_cliente_ref2").cast("string"),
            col("cod_cliente_ref3").cast("string"),
            col("cod_cliente_ref4").cast("string"),
            col("cod_tipo_cliente").cast("string"),
            col("cod_cuenta_clave").cast("string"),
            col("nomb_cuenta_clave").cast("string"),
            col("desc_canal_local").cast("string"),
            col("desc_giro_local").cast("string"),
            col("direccion").cast("string"),
            col("nro_documento").cast("string"),
            col("cod_cliente_principal").cast("string"),
            col("cod_cliente_transferencia").cast("string"),
            col("cod_eje_territorial").cast("string"),
            col("coordx").cast("string"),
            col("coordy").cast("string"),
            col("fecha_creacion").cast("timestamp"),
            col("fecha_baja").cast("timestamp"),
            col("estado").cast("string")
        )
    )

    # Escribir a analytics
    id_columns = ["id_cliente"]
    partition_columns_array = ["id_pais"]
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_cliente, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing df_dim_cliente: {e}")
    raise ValueError(f"Error processing df_dim_cliente: {e}")