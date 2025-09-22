import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date
from pyspark.sql.types import StringType, IntegerType, DecimalType, TimestampType, DateType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_producto"

try:
    # Lectura directa desde STAGE (BIGMAGIC) - integrando lógica de domain
    logger.info("Reading tables from BIGMAGIC (Stage layer)")
    
    # Tabla principal de artículo
    m_articulo = spark_controller.read_table(data_paths.BIGMAGIC, "m_articulo")
    
    # Tablas de referencia para construir la dimensión completa
    m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal=True)
    m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    m_linea = spark_controller.read_table(data_paths.BIGMAGIC, "m_linea")
    m_familia = spark_controller.read_table(data_paths.BIGMAGIC, "m_familia")
    m_subfamilia = spark_controller.read_table(data_paths.BIGMAGIC, "m_subfamilia")
    m_marca = spark_controller.read_table(data_paths.BIGMAGIC, "m_marca")
    m_presentacion = spark_controller.read_table(data_paths.BIGMAGIC, "m_presentacion")
    m_formato = spark_controller.read_table(data_paths.BIGMAGIC, "m_formato")
    m_sabor = spark_controller.read_table(data_paths.BIGMAGIC, "m_sabor")
    m_categoria = spark_controller.read_table(data_paths.BIGMAGIC, "m_categoria")
    m_tipo_envase = spark_controller.read_table(data_paths.BIGMAGIC, "m_tipo_envase")
    
    logger.info("Tables loaded successfully from BIGMAGIC")
except Exception as e:
    logger.error(f"Error reading tables from BIGMAGIC: {e}")
    raise ValueError(f"Error reading tables from BIGMAGIC: {e}")

try:
    # Aplicar transformaciones que estaban en m_articulo.py (domain)
    logger.info("Starting domain transformations - creating m_articulo")
    
    df_m_articulo = (
        m_articulo.alias("ma")
        .join(m_compania.alias("mc"), col("ma.cod_compania") == col("mc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "left")
        .join(
            m_linea.alias("ml"),
            (col("ma.cod_compania") == col("ml.cod_compania"))
            & (col("ma.cod_linea") == col("ml.cod_linea")),
            "left",
        )
        .join(
            m_familia.alias("mf"),
            (col("ma.cod_compania") == col("mf.cod_compania"))
            & (col("ma.cod_familia") == col("mf.cod_familia")),
            "left",
        )
        .join(
            m_subfamilia.alias("msf"),
            (col("ma.cod_compania") == col("msf.cod_compania"))
            & (col("ma.cod_familia") == col("msf.cod_familia"))
            & (col("ma.cod_subfamilia") == col("msf.cod_subfamilia")),
            "left",
        )
        .join(
            m_marca.alias("mm"),
            (col("ma.cod_compania") == col("mm.cod_compania"))
            & (col("ma.cod_marca") == col("mm.cod_marca")),
            "left",
        )
        .join(
            m_presentacion.alias("mp_pres"),
            (col("ma.cod_compania") == col("mp_pres.cod_compania"))
            & (col("ma.cod_presentacion") == col("mp_pres.cod_presentacion")),
            "left",
        )
        .join(
            m_formato.alias("mfo"),
            (col("ma.cod_compania") == col("mfo.cod_compania"))
            & (col("ma.cod_formato") == col("mfo.cod_formato")),
            "left",
        )
        .join(
            m_sabor.alias("msab"),
            (col("ma.cod_compania") == col("msab.cod_compania"))
            & (col("ma.cod_sabor") == col("msab.cod_sabor")),
            "left",
        )
        .join(
            m_categoria.alias("mcat"),
            (col("ma.cod_compania") == col("mcat.cod_compania"))
            & (col("ma.cod_categoria") == col("mcat.cod_categoria")),
            "left",
        )
        .join(
            m_tipo_envase.alias("mte"),
            (col("ma.cod_compania") == col("mte.cod_compania"))
            & (col("ma.cod_tipo_envase") == col("mte.cod_tipo_envase")),
            "left",
        )
        .select(
            # IDs principales
            concat_ws("|",
                trim(col("ma.cod_compania")),
                trim(col("ma.cod_articulo").cast("string")),
            ).alias("id_articulo"),
            col("mp.id_pais").alias("id_pais"),
            
            # Códigos y descripciones principales
            col("ma.cod_articulo").cast(StringType()).alias("cod_articulo"),
            col("ma.desc_articulo").cast(StringType()).alias("desc_articulo"),
            col("ma.desc_articulo_corp").cast(StringType()).alias("desc_articulo_corp"),
            
            # Categorización del producto
            col("ma.cod_categoria").cast(StringType()).alias("cod_categoria"),
            coalesce(col("mcat.desc_categoria"), lit("N/A")).cast(StringType()).alias("desc_categoria"),
            
            col("ma.cod_marca").cast(StringType()).alias("cod_marca"),
            coalesce(col("mm.desc_marca"), lit("N/A")).cast(StringType()).alias("desc_marca"),
            
            col("ma.cod_presentacion").cast(StringType()).alias("cod_presentacion"),
            coalesce(col("mp_pres.desc_presentacion"), lit("N/A")).cast(StringType()).alias("desc_presentacion"),
            
            col("ma.cod_formato").cast(StringType()).alias("cod_formato"),
            coalesce(col("mfo.desc_formato"), lit("N/A")).cast(StringType()).alias("desc_formato"),
            
            col("ma.cod_sabor").cast(StringType()).alias("cod_sabor"),
            coalesce(col("msab.desc_sabor"), lit("N/A")).cast(StringType()).alias("desc_sabor"),
            
            col("ma.cod_tipo_envase").cast(StringType()).alias("cod_tipo_envase"),
            coalesce(col("mte.desc_tipo_envase"), lit("N/A")).cast(StringType()).alias("desc_tipo_envase"),
            
            # Jerarquía de producto
            col("ma.cod_linea").cast(StringType()).alias("cod_linea"),
            coalesce(col("ml.desc_linea"), lit("N/A")).cast(StringType()).alias("desc_linea"),
            
            col("ma.cod_familia").cast(StringType()).alias("cod_familia"),
            coalesce(col("mf.desc_familia"), lit("N/A")).cast(StringType()).alias("desc_familia"),
            
            col("ma.cod_subfamilia").cast(StringType()).alias("cod_subfamilia"),
            coalesce(col("msf.desc_subfamilia"), lit("N/A")).cast(StringType()).alias("desc_subfamilia"),
            
            # Unidad de negocio (hardcodeado como en el analytics original)
            col("ma.cod_unidad_negocio").cast(StringType()).alias("cod_unidad_negocio"),
            when(col("ma.cod_unidad_negocio") == "001", "Bebidas Gaseosas")
            .when(col("ma.cod_unidad_negocio") == "002", "Cervezas")
            .when(col("ma.cod_unidad_negocio") == "003", "Aguas")
            .when(col("ma.cod_unidad_negocio") == "004", "Jugos")
            .otherwise("Otros").cast(StringType()).alias("desc_unidad_negocio"),
            
            # Unidades de manejo y volumen
            col("ma.unidad_manejo").cast(StringType()).alias("cod_unidad_manejo"),
            col("ma.unidad_volumen").cast(DecimalType(38,12)).alias("cod_unidad_volumen"),
            col("ma.cant_unidad_paquete").cast(DecimalType(38,12)).alias("cant_unidad_paquete"),
            col("ma.cant_unidad_volumen").cast(DecimalType(38,12)).alias("cant_unidad_volumen"),
            
            # Estado y fechas
            col("ma.es_activo").cast(IntegerType()).alias("es_activo"),
            col("ma.fecha_creacion").cast(TimestampType()).alias("fecha_creacion"),
            col("ma.fecha_modificacion").cast(TimestampType()).alias("fecha_modificacion")
        )
    ).cache()

    # Aplicar transformaciones específicas de analytics (las que ya tenías)
    logger.info("Starting analytics transformations - creating dim_producto")
    
    df_dim_producto = (
        df_m_articulo
        .select(
            col('id_articulo').cast("string").alias('id_producto'),
            col('id_pais').cast("string"),
            col('cod_articulo').cast("string").alias('cod_producto'),
            col('desc_articulo').cast("string").alias('desc_producto'),
            col('desc_articulo_corp').cast("string"),
            col('cod_categoria').cast("string"),
            col('desc_categoria').cast("string"),
            col('cod_marca').cast("string"),
            col('desc_marca').cast("string"),
            col('cod_presentacion').cast("string"),
            col('desc_presentacion').cast("string"),
            col('cod_formato').cast("string"),
            col('desc_formato').cast("string"),
            col('cod_sabor').cast("string"),
            col('desc_sabor').cast("string"),
            col('cod_tipo_envase').cast("string"),
            col('desc_tipo_envase').cast("string"),
            col('cod_linea').cast("string"),
            col('desc_linea').cast("string"),
            col('cod_familia').cast("string"),
            col('desc_familia').cast("string"),
            col('cod_subfamilia').cast("string"),
            col('desc_subfamilia').cast("string"),
            col('cod_unidad_negocio').cast("string"),
            col('desc_unidad_negocio').cast("string"),
            col('cod_unidad_manejo').cast("string").alias('cod_unidad_paquete'),
            col('cod_unidad_volumen').cast("numeric(38,12)").alias('cod_unidad_volumen'),
            col('cant_unidad_paquete').cast("numeric(38,12)").alias('cant_unidad_paquete'),
            col('cant_unidad_volumen').cast("numeric(38,12)").alias('cant_unidad_volumen'),
            col('es_activo').cast("int").alias('es_activo')
        )
    )

    # Escribir a analytics
    id_columns = ["id_producto"]
    partition_columns_array = ["id_pais"]
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_producto, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array) 
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing df_dim_producto: {e}")
    raise ValueError(f"Error processing df_dim_producto: {e}")