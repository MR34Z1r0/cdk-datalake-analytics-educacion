import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import (
    col, concat, concat_ws, lit, coalesce, when, trim, row_number, current_date, 
    upper, date_format, round, to_date, substring, lower, to_timestamp, max, sum,
    cast
)
from pyspark.sql.types import StringType, TimestampType, DateType, DecimalType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()

target_table_name = "fact_venta_detalle"

try:
    PERIODOS = spark_controller.get_periods()
    logger.info(f"Periods: {PERIODOS}")
    
    # Tablas maestras de stage
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal=True)
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_parametro = spark_controller.read_table(data_paths.BIGMAGIC, "m_parametro")
    df_m_procedimiento = spark_controller.read_table(data_paths.BIGMAGIC, "m_procedimiento")
    df_m_articulo = spark_controller.read_table(data_paths.BIGMAGIC, "m_articulo")
    df_m_zona_distribucion = spark_controller.read_table(data_paths.BIGMAGIC, "m_zona")
    df_m_centro_distribucion = spark_controller.read_table(data_paths.BIGMAGIC, "m_division")
    df_m_region = spark_controller.read_table(data_paths.BIGMAGIC, "m_region")
    df_m_subregion = spark_controller.read_table(data_paths.BIGMAGIC, "m_subregion")
    df_m_tipo_cambio = spark_controller.read_table(data_paths.BIGMAGIC, "m_tipo_cambio")
    df_m_operacion = spark_controller.read_table(data_paths.BIGMAGIC, "m_operacion")
    df_m_licencia = spark_controller.read_table(data_paths.BIGMAGIC, "m_licencia")
    df_i_relacion_proced_venta = spark_controller.read_table(data_paths.BIGMAGIC, "i_relacion_proced_venta")
    
    # Tablas transaccionales desde stage
    df_t_historico_venta = spark_controller.read_table(data_paths.BIGMAGIC, "t_documento_venta")
    df_t_historico_venta_detalle = spark_controller.read_table(data_paths.BIGMAGIC, "t_documento_venta_detalle")
    df_t_historico_pedido = spark_controller.read_table(data_paths.BIGMAGIC, "t_documento_pedido")
    df_t_licencia_cuota = spark_controller.read_table(data_paths.BIGMAGIC, "t_licencia_cuota")

    logger.info("Dataframes loaded successfully from BIGMAGIC stage")
    
except Exception as e:
    logger.error(f"Error reading tables from stage: {e}")
    raise ValueError(f"Error reading tables from stage: {e}")

try:
    logger.info("Applying period filters")
    df_t_historico_venta = df_t_historico_venta.filter(date_format(col("fecha_emision"), "yyyyMM").isin(PERIODOS))
    df_t_historico_venta_detalle = df_t_historico_venta_detalle.filter(date_format(col("fecha_emision"), "yyyyMM").isin(PERIODOS))
    df_t_historico_pedido = df_t_historico_pedido.filter(date_format(col("fecha_pedido"), "yyyyMM").isin(PERIODOS))
    
except Exception as e:
    logger.error(f"Error applying period filters: {e}")
    raise ValueError(f"Error applying period filters: {e}")

try:
    logger.info("Starting combined transformations for fact_venta_detalle")
    
    # ======================= PREPARACIÓN DE COMPAÑÍA =======================
    logger.info("Creating company dimension")
    df_m_compania_prep = (
        df_m_compania.alias("mc")
        .join(
            df_m_parametro.alias("mpar"),
            col("mpar.id_compania") == col("mc.id_compania"),
            "left",
        )
        .join(
            df_m_pais.alias("mp"),
            col("mp.cod_pais") == col("mc.cod_pais"),
        )
        .select(
            col("mp.id_pais"), 
            col("mc.cod_compania").alias("id_compania"), 
            col("mc.cod_compania"), 
            col("mc.cod_pais"), 
            col("mpar.cod_moneda_mn").alias("moneda_mn")
        )
    ).cache()

    # ======================= PREPARACIÓN DE TIPO VENTA =======================
    logger.info("Creating tipo_venta dimension")
    df_m_tipo_venta_prep = (
        df_i_relacion_proced_venta.alias("irpv")
        .join(
            df_m_procedimiento.alias("d"),
            (col("irpv.cod_compania") == col("d.cod_compania"))
            & (col("irpv.cod_procedimiento_venta") == col("d.cod_procedimiento")),
            "inner",
        )
        .join(
            df_m_compania_prep.alias("mc"),
            col("irpv.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .select(
            concat(
                trim(col("irpv.cod_compania")),
                lit("|"),
                trim(col("d.cod_procedimiento")),
            ).cast(StringType()).alias("id_tipo_venta"),
            col("mc.id_pais").cast(StringType()).alias("id_pais"),
            col("d.cod_procedimiento").cast(StringType()).alias("cod_tipo_venta"),
            coalesce(col("d.desc_procedimiento"), lit("ninguno")).cast(StringType()).alias("nomb_tipo_venta"),
            col("d.cod_tipo_operacion").cast(StringType()).alias("cod_tipo_operacion"),
        )
    )

    # ======================= PREPARACIÓN DE ARTÍCULOS =======================
    logger.info("Creating articulo filter")
    df_m_articulo_filter = (
        df_m_articulo
        .select(
            concat_ws("|", col("cod_compania"), col("cod_articulo")).alias("id_producto"),
            col("cant_unidad_paquete"),
            col("cant_paquete_caja"),
            col("cant_unidad_volumen")
        )
    )

    # ======================= PREPARACIÓN DE OPERACIONES =======================
    logger.info("Creating operacion filter")
    df_m_operacion_filter = (
        df_m_operacion
        .select(
            concat_ws("|", 
                     col("cod_compania"), 
                     col("cod_documento_transaccion"), 
                     col("cod_procedimiento"), 
                     col("cod_operacion")).alias("id_operacion"),
            col("cod_tipo_operacion")
        )
    )

    # ======================= PREPARACIÓN DE LICENCIAS =======================
    logger.info("Creating licencia cuota filter")
    df_t_licencia_cuota_filter = (
        df_t_licencia_cuota.alias("tlc")
        .join(
            df_m_licencia.alias("ml"),
            (col("tlc.cod_compania") == col("ml.cod_compania")) &
            (col("tlc.cod_licencia") == col("ml.cod_licencia")), 
            "left")
        .select(
            concat_ws("|", 
                      col("tlc.cod_compania"), 
                      col("tlc.cod_sucursal"), 
                      col("tlc.cod_almacen"), 
                      col("tlc.cod_documento_venta"), 
                      col("tlc.nro_documento_venta"), 
                      col("tlc.cod_articulo"),
                      col("tlc.cod_operacion"), 
                      col("tlc.cod_unidad_venta")
                      ).alias("id_documento_venta_detalle"),
            concat_ws("|", 
                      col("tlc.cod_compania"), 
                      col("tlc.cod_licencia")
                      ).alias("id_licencia"),
            concat_ws("|", 
                      col("ml.cod_compania"), 
                      col("ml.cod_persona")
                      ).alias("id_socio"),    
            concat_ws("|", 
                      col("ml.cod_compania"), 
                      col("ml.cod_persona"), 
                      col("ml.cod_direccion")
                      ).alias("id_establecimiento")
        )
    )

    # ======================= PROCESAMIENTO DE VENTA CABECERA =======================
    logger.info("Processing venta cabecera")
    df_t_venta_filter = (
        df_t_historico_venta.alias("tp")
        .filter(
            (~col("tp.cod_documento_venta").isin(["CMD", "RMD"]))
            & (coalesce(col("tp.flg_facglob"), lit("F")) == "F")
            & (coalesce(col("tp.flg_refact"), lit("F")) == "F")
        )
        .join(
            df_m_compania_prep.alias("mc"), 
            (col("tp.cod_compania") == col("mc.cod_compania")),
            "inner"
        )
        .join(
            df_m_zona_distribucion.alias("mzo"),
            (col("mzo.cod_compania") == col("tp.cod_compania"))
            & (col("mzo.cod_sucursal") == col("tp.cod_sucursal"))
            & (col("mzo.cod_zona") == col("tp.cod_zona")),
            "left"
        )
        .join(
            df_m_centro_distribucion.alias("mcd"),
            (col("mcd.cod_division") == col("mzo.cod_zona"))
            & (col("mcd.cod_compania") == col("mzo.cod_compania")),
            "left",
        )
        .join(
            df_m_region.alias("mr"),
            (col("mr.cod_pais") == col("mc.cod_pais"))
            & (col("mr.cod_region") == col("mzo.cod_region")),
            "left",
        )
        .join(
            df_m_subregion.alias("msr"),
            (col("msr.cod_pais") == col("mc.cod_pais"))
            & (col("msr.cod_region") == col("mzo.cod_region"))
            & (col("msr.cod_subregion") == col("mzo.cod_subregion")),
            "left",
        )
        .join(
            df_m_tipo_cambio.alias("mtc"),
            (col("mtc.fecha") == col("tp.fecha_emision"))
            & (col("mtc.cod_compania") == col("mc.cod_compania"))
            & (col("mtc.cod_moneda") == col("mc.moneda_mn")),
            "left",
        )
        .select(
            col("mc.id_pais"),
            date_format(col("tp.fecha_emision"), "yyyyMM").alias("id_periodo"),
            concat_ws("|", 
                      col("tp.cod_compania"), 
                      col("tp.cod_sucursal"),
                      col("tp.cod_almacen"), 
                      col("tp.cod_documento_venta"), 
                      col("tp.nro_documento_venta")).alias("id_venta"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal")).alias("id_sucursal"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_cliente")).alias("id_cliente"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_forma_pago")).alias("id_forma_pago"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_lista_precio")).alias("id_lista_precio"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_procedimiento")).alias("id_tipo_venta"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal"), col("tp.cod_documento_pedido"), col("tp.nro_documento_pedido")).alias("id_pedido"),
            col("tp.cod_compania"),
            col("tp.cod_documento_venta"),
            col("tp.cod_procedimiento"),
            col("tp.fecha_emision"),
            col("tp.fecha_liquidacion"),
            col("tp.fecha_pedido"),
            coalesce(
                when(col("tp.cod_moneda") == col("mc.moneda_mn"), 1)
                .otherwise(col("mtc.tc_venta")), 
                col("tp.tipo_cambio_mn")
            ).alias("tipo_cambio_mn"),
            coalesce(
                when((col("tp.cod_moneda") == "DOL") | (col("tp.cod_moneda") == "USD"), 1)
                .otherwise(col("mtc.tc_venta")), 
                when(col("tp.tipo_cambio_me") == 0, 1)
                .otherwise(col("tp.tipo_cambio_me"))
            ).alias("tipo_cambio_me"),
            when(col("tp.cod_estado_comprobante") == "002", 1).otherwise(0).alias("es_eliminado")
        )
    )

    # ======================= PROCESAMIENTO DE VENTA DETALLE =======================
    logger.info("Processing venta detalle")
    df_t_venta_detalle_filter = (
        df_t_historico_venta_detalle
        .select(
            concat_ws("|", 
                      col("cod_compania"), 
                      col("cod_sucursal"), 
                      col("cod_almacen"), 
                      col("cod_documento_transaccion"), 
                      col("nro_comprobante_venta")).alias("id_venta"),
            concat_ws("|", 
                      col("cod_compania"), 
                      col("cod_articulo")).alias("id_producto"),
            concat_ws("|", 
                      col("cod_compania"), 
                      col("cod_documento_transaccion"), 
                      col("cod_procedimiento"), 
                      col("cod_operacion")).alias("id_operacion"),
            col("id_documento_venta_detalle"),
            col("cod_compania"),
            col("cod_operacion"),
            col("cant_paquete"),
            col("cant_unidad"),
            col("imp_valorizado"),
            col("imp_cobrar"),
            col("imp_descuento"),
            col("imp_descuento_sinimp"),
            col("precio_paquete"),
            col("imp_isc"),
            col("imp_igv"),
            col("imp_im3"),
            col("imp_im4"),
            col("imp_im5"),
            col("imp_im6"),            
            col("fecha_creacion"),
            col("fecha_modificacion"),
            lit(0).alias("es_eliminado")
        )
    )

    # ======================= PROCESAMIENTO DE PEDIDO =======================
    logger.info("Processing pedido")
    df_t_pedido_filter = (
        df_t_historico_pedido.alias("tp")
        .filter(col("tp.cod_documento_pedido").isin(["200", "300"]))
        .join(
            df_m_compania_prep.alias("mc"), 
            (col("tp.cod_compania") == col("mc.cod_compania")),
            "inner"
        )
        .select(
            col("mc.id_pais"),
            date_format(col("fecha_pedido"), "yyyyMM").alias("id_periodo"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal"), col("tp.cod_documento_pedido"), col("tp.nro_documento_pedido")).alias("id_pedido"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_vendedor")).alias("id_vendedor"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal"), col("cod_fuerza_venta")).alias("id_fuerza_venta"),
            col("tp.fecha_pedido"),
            col("tp.cod_zona_distribucion").alias("cod_zona"),
            col("tp.cod_ruta_distribucion").alias("cod_ruta"),
            lit(None).alias("cod_modulo")
        )
    )

    # ======================= COMBINACIÓN FINAL PARA FACT TABLE =======================
    logger.info("Creating final fact_venta_detalle")
    
    df_t_venta_detalle_combined = (
        df_t_venta_detalle_filter.alias("tvd")
        .join(
            df_t_venta_filter.alias("tv"),
            col("tv.id_venta") == col("tvd.id_venta"),
            "inner"
        )
        .join(
            df_m_articulo_filter.alias("ma"),
            col("tvd.id_producto") == col("ma.id_producto"),
            "inner",
        )
        .join(
            df_m_operacion_filter.alias("mo"),
            col("tvd.id_operacion") == col("mo.id_operacion"),
            "inner",
        )   
        .join(
            df_t_licencia_cuota_filter.alias("tl"),
            col("tvd.id_documento_venta_detalle") == col("tl.id_documento_venta_detalle"),
            "left",
        )       
        .select(
            col("tv.id_pais"),
            col("tv.id_periodo"),
            col("tv.id_venta"),
            col("tvd.id_producto"),
            col("tl.id_licencia"),
            col("tl.id_socio"),
            col("tl.id_establecimiento"),
            when(col("tv.cod_documento_venta") == "NCC", -1).otherwise(1).alias("factor"),
            col("mo.cod_tipo_operacion"),
            col("tv.tipo_cambio_mn"),
            col("tv.tipo_cambio_me"),     
            col("tv.fecha_emision"),  
            col("tv.fecha_liquidacion"),      
            col("tv.fecha_pedido"),  
            col("tvd.cant_paquete"),
            col("tvd.cant_unidad"),
            col("ma.cant_unidad_paquete"),
            col("ma.cant_paquete_caja"),
            col("ma.cant_unidad_volumen"),
            col("tvd.imp_valorizado"),
            col("tvd.imp_cobrar"),
            col("tvd.imp_descuento"),
            col("tvd.imp_descuento_sinimp"),
            col("tvd.precio_paquete"),
            lit(0).alias("imp_sugerido"),
            lit(0).alias("imp_ventafull"),
            col("tvd.imp_isc"),
            col("tvd.imp_igv"),
            col("tvd.imp_im3"),
            col("tvd.imp_im4"),
            col("tvd.imp_im5"),
            col("tvd.imp_im6"),    
            col("tvd.fecha_creacion"),
            col("tvd.fecha_modificacion"),
            col("tvd.es_eliminado")            
        )
    )
 
    # Agregación final similar al dominio original
    df_t_venta_detalle_agg = (
        df_t_venta_detalle_combined
        .groupby(
            col("id_venta"),
            col("id_producto"),
            col("id_licencia"),
            col("id_socio"),
            col("id_establecimiento")
        )
        .agg(
            max(col("id_pais")).alias('id_pais'),
            max(col("id_periodo")).alias('id_periodo'),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * 
                col("factor") * 
                (col("cant_paquete") + (col("cant_unidad") / col("cant_unidad_paquete"))) * 
                (col("cant_paquete_caja"))
            ).alias("cant_caja_fisica_ven"),
            sum(
                (when(upper(col("cod_tipo_operacion")) == "PRO", 1).otherwise(0)) * 
                col("factor") * 
                (col("cant_paquete") + (col("cant_unidad") / col("cant_unidad_paquete"))) * 
                (col("cant_paquete_caja"))
            ).alias("cant_caja_fisica_pro"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * 
                col("factor") * 
                (col("cant_paquete") * col("cant_unidad_paquete") + col("cant_unidad")) * 
                (col("cant_unidad_volumen"))
            ).alias("cant_caja_volumen_ven"),
            sum(
                (when(upper(col("cod_tipo_operacion")) == "PRO", 1).otherwise(0)) * 
                col("factor") * 
                (col("cant_paquete") * col("cant_unidad_paquete") + col("cant_unidad")) * 
                (col("cant_unidad_volumen"))
            ).alias("cant_caja_volumen_pro"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * 
                col("factor") * col("imp_valorizado") * col("tipo_cambio_mn")
            ).alias("imp_valorizado_ven_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) == "PRO", 1).otherwise(0)) * 
                col("factor") * col("imp_valorizado") * col("tipo_cambio_mn")
            ).alias("imp_valorizado_pro_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * 
                col("factor") * col("imp_valorizado") * col("tipo_cambio_me")
            ).alias("imp_valorizado_ven_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) == "PRO", 1).otherwise(0)) * 
                col("factor") * col("imp_valorizado") * col("tipo_cambio_me")
            ).alias("imp_valorizado_pro_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * 
                col("factor") * col("imp_cobrar") * col("tipo_cambio_mn")
            ).alias("imp_neto_vta_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * 
                col("factor") * col("imp_cobrar") * col("tipo_cambio_me")
            ).alias("imp_neto_vta_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * 
                col("factor") * (col("imp_cobrar") + col("imp_descuento")) * col("tipo_cambio_mn")
            ).alias("imp_bruto_vta_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * 
                col("factor") * (col("imp_cobrar") + col("imp_descuento")) * col("tipo_cambio_me")
            ).alias("imp_bruto_vta_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * 
                col("factor") * col("imp_descuento") * col("tipo_cambio_mn")
            ).alias("imp_descuento_vta_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * 
                col("factor") * col("imp_descuento") * col("tipo_cambio_me")
            ).alias("imp_descuento_vta_me"),
            sum(
                col("factor") * col("imp_isc") * col("tipo_cambio_mn")
            ).alias("imp_impuesto1_mn"),
            sum(
                col("factor") * col("imp_isc") * col("tipo_cambio_me")
            ).alias("imp_impuesto1_me"),
            sum(
                col("factor") * col("imp_igv") * col("tipo_cambio_mn")
            ).alias("imp_impuesto2_mn"),
            sum(
                col("factor") * col("imp_igv") * col("tipo_cambio_me")
            ).alias("imp_impuesto2_me"),
            sum(
                col("factor") * col("imp_im3") * col("tipo_cambio_mn")
            ).alias("imp_impuesto3_mn"),
            sum(
                col("factor") * col("imp_im3") * col("tipo_cambio_me")
            ).alias("imp_impuesto3_me"),
            sum(
                col("factor") * col("imp_im4") * col("tipo_cambio_mn")
            ).alias("imp_impuesto4_mn"),
            sum(
                col("factor") * col("imp_im4") * col("tipo_cambio_me")
            ).alias("imp_impuesto4_me"),
            sum(
                col("factor") * col("imp_im5") * col("tipo_cambio_mn")
            ).alias("imp_impuesto5_mn"),
            sum(
                col("factor") * col("imp_im5") * col("tipo_cambio_me")
            ).alias("imp_impuesto5_me"),
            sum(
                col("factor") * col("imp_im6") * col("tipo_cambio_mn")
            ).alias("imp_impuesto6_mn"),
            sum(
                col("factor") * col("imp_im6") * col("tipo_cambio_me")
            ).alias("imp_impuesto6_me"),
            max(col("fecha_creacion")).alias("fecha_creacion"),
            max(col("fecha_modificacion")).alias("fecha_modificacion"),
            max(col("es_eliminado")).alias("es_eliminado")
        )
    )

    # ======================= APLICAR LÓGICA FINAL DE ANALYTICS =======================
    logger.info("Applying final analytics logic")
    
    df_fact_venta_detalle = (
        df_t_venta_detalle_agg.alias("tvd")
        .join(
            df_t_venta_filter.alias("tv"),
            (col("tvd.id_venta") == col("tv.id_venta")),
            "inner",
        )
        .join(
            df_m_tipo_venta_prep.alias("mtv"),
            (col("tv.id_tipo_venta") == col("mtv.id_tipo_venta"))
            & (upper(col('mtv.cod_tipo_operacion')).isin(['VEN', 'EXP','OBS'])),
            "inner",
        )
        .join(
            df_t_pedido_filter.alias("tp"),
            (col("tv.id_pedido") == col("tp.id_pedido")),
            "left",
        ) 
        .where((col('tv.es_eliminado') == 0))
        .select(
            # Claves de dimensión
            col('tv.id_pais').cast("string"),
            col('tv.id_periodo').cast("string"),
            col('tv.id_sucursal').cast("string"),
            col('tv.id_cliente').cast("string"),
            col('tvd.id_producto').cast("string"),
            col('tvd.id_licencia').cast("string"),
            col('tvd.id_socio').cast("string"),
            col('tvd.id_establecimiento').cast("string"),
            col('tv.id_forma_pago').cast("string"),
            col('tv.id_lista_precio').cast("string"),
            col('tv.id_tipo_venta').cast("string"),
            coalesce(col('tp.id_vendedor'), lit("")).cast("string").alias('id_vendedor'),
            coalesce(col('tp.id_fuerza_venta'), lit("")).cast("string").alias('id_fuerza_venta'),
            col('tv.fecha_emision').cast("date"),
            col('tv.fecha_liquidacion').cast("date"),
            coalesce(col('tp.fecha_pedido'), col('tv.fecha_pedido')).cast("date").alias('fecha_pedido'),
            coalesce(col('tp.cod_zona'), lit("")).cast("string").alias('cod_zona'),
            coalesce(col('tp.cod_ruta'), lit("")).cast("string").alias('cod_ruta'),
            coalesce(col('tp.cod_modulo'), lit("")).cast("string").alias('cod_modulo'),
            
            # Métricas de cantidad
            col("tvd.cant_caja_fisica_ven").cast("numeric(38,12)").alias("cant_caja_fisica_ven"),
            col("tvd.cant_caja_fisica_pro").cast("numeric(38,12)").alias("cant_caja_fisica_pro"),
            col("tvd.cant_caja_volumen_ven").cast("numeric(38,12)").alias("cant_caja_volumen_ven"),
            col("tvd.cant_caja_volumen_pro").cast("numeric(38,12)").alias("cant_caja_volumen_pro"),
            
            # Métricas de importes
            col("tvd.imp_valorizado_ven_mn").cast("numeric(38,12)").alias("imp_valorizado_ven_mn"),
            col("tvd.imp_valorizado_ven_me").cast("numeric(38,12)").alias("imp_valorizado_ven_me"),
            col("tvd.imp_valorizado_pro_mn").cast("numeric(38,12)").alias("imp_valorizado_pro_mn"),
            col("tvd.imp_valorizado_pro_me").cast("numeric(38,12)").alias("imp_valorizado_pro_me"),
            col("tvd.imp_neto_vta_mn").cast("numeric(38,12)").alias("imp_neto_vta_mn"),
            col("tvd.imp_neto_vta_me").cast("numeric(38,12)").alias("imp_neto_vta_me"),
            col("tvd.imp_bruto_vta_mn").cast("numeric(38,12)").alias("imp_bruto_vta_mn"),
            col("tvd.imp_bruto_vta_me").cast("numeric(38,12)").alias("imp_bruto_vta_me"),
            col("tvd.imp_descuento_vta_mn").cast("numeric(38,12)").alias("imp_descuento_vta_mn"),
            col("tvd.imp_descuento_vta_me").cast("numeric(38,12)").alias("imp_descuento_vta_me"),
            lit(0).cast("numeric(38,12)").alias("imp_sugerido_mn"),
            lit(0).cast("numeric(38,12)").alias("imp_sugerido_me"),
            lit(0).cast("numeric(38,12)").alias("imp_full_vta_mn"),
            lit(0).cast("numeric(38,12)").alias("imp_full_vta_me"),
            col("tvd.imp_impuesto1_mn").cast("numeric(38,12)").alias("imp_impuesto1_mn"),
            col("tvd.imp_impuesto1_me").cast("numeric(38,12)").alias("imp_impuesto1_me"),
            col("tvd.imp_impuesto2_mn").cast("numeric(38,12)").alias("imp_impuesto2_mn"),
            col("tvd.imp_impuesto2_me").cast("numeric(38,12)").alias("imp_impuesto2_me"),
            col("tvd.imp_impuesto3_mn").cast("numeric(38,12)").alias("imp_impuesto3_mn"),
            col("tvd.imp_impuesto3_me").cast("numeric(38,12)").alias("imp_impuesto3_me"),
            col("tvd.imp_impuesto4_mn").cast("numeric(38,12)").alias("imp_impuesto4_mn"),
            col("tvd.imp_impuesto4_me").cast("numeric(38,12)").alias("imp_impuesto4_me"),
            col("tvd.imp_impuesto5_mn").cast("numeric(38,12)").alias("imp_impuesto5_mn"),
            col("tvd.imp_impuesto5_me").cast("numeric(38,12)").alias("imp_impuesto5_me"),
            col("tvd.imp_impuesto6_mn").cast("numeric(38,12)").alias("imp_impuesto6_mn"),
            col("tvd.imp_impuesto6_me").cast("numeric(38,12)").alias("imp_impuesto6_me"),
        )
    )

    # ======================= ESCRIBIR A ANALYTICS =======================
    logger.info(f"Writing fact_venta_detalle to analytics layer")
    
    # Configuración para particionado
    partition_columns_array = ["id_pais", "id_periodo"]
    
    # Escribir directamente a analytics usando write_table (similar al original)
    spark_controller.write_table(
        df_fact_venta_detalle, 
        data_paths.ANALYTICS, 
        target_table_name, 
        partition_columns_array
    )
    
    logger.info(f"fact_venta_detalle successfully written to analytics")

except Exception as e:
    logger.error(f"Error processing fact_venta_detalle from stage to analytics: {e}")
    raise ValueError(f"Error processing fact_venta_detalle from stage to analytics: {e}")