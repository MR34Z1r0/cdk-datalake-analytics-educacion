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

target_table_name = "fact_venta_cliente_historico"

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
    
    # m_compania con parámetros y país
    df_m_compania_enriched = (
        df_m_compania.alias("mc")
        .join(
            df_m_parametro.alias("mpar"),
            col("mpar.id_compania") == col("mc.id_compania"),
            "left"
        )
        .join(
            df_m_pais.alias("mp"),
            col("mp.cod_pais") == col("mc.cod_pais")
        )
        .select(
            col("mc.cod_compania"),
            col("mc.desc_compania"),
            col("mp.cod_pais"),
            col("mp.desc_pais"),
            col("mpar.moneda_mn"),
            concat_ws("|", col("mp.cod_pais"), col("mc.cod_compania")).alias("id_compania"),
            col("mp.cod_pais").alias("id_pais")
        )
    )
    
    # m_tipo_cambio filtrado
    df_m_tipo_cambio_filtered = (
        df_m_tipo_cambio
        .filter(date_format(col("fecha_tipo_cambio"), "yyyyMM").isin(PERIODOS))
        .select(
            col("cod_compania"),
            col("fecha_tipo_cambio"),
            col("tc_venta")
        )
    )
    
    # m_articulo con línea
    df_m_articulo_enriched = (
        df_m_articulo.alias("ma")
        .join(
            df_m_linea.alias("ml"),
            (col("ma.cod_compania") == col("ml.cod_compania")) &
            (col("ma.cod_linea") == col("ml.cod_linea")),
            "left"
        )
        .select(
            concat_ws("|", col("ma.cod_compania"), col("ma.cod_articulo")).alias("id_producto"),
            col("ma.cod_compania"),
            col("ma.cod_articulo"),
            col("ma.desc_articulo"),
            col("ma.cant_unidad_paquete"),
            col("ma.cant_paquete_caja"),
            col("ma.cant_unidad_volumen"),
            col("ml.desc_linea")
        )
    )
    
    # m_operacion
    df_m_operacion_filtered = (
        df_m_operacion
        .select(
            concat_ws("|", col("cod_compania"), col("cod_documento_transaccion"), 
                     col("cod_procedimiento"), col("cod_operacion")).alias("id_operacion"),
            col("cod_compania"),
            col("cod_operacion"),
            col("cod_tipo_operacion")
        )
    )
    
    # === PREPARACIÓN DE TABLA DE VENTAS ===
    logger.info("Processing sales header (t_documento_venta)")
    
    df_venta_processed = (
        df_t_documento_venta.alias("tv")
        .join(
            df_m_compania_enriched.alias("mc"),
            col("tv.cod_compania") == col("mc.cod_compania")
        )
        .join(
            df_m_tipo_cambio_filtered.alias("mtc"),
            (col("tv.cod_compania") == col("mtc.cod_compania")) &
            (col("tv.fecha_liquidacion") == col("mtc.fecha_tipo_cambio")),
            "left"
        )
        .select(
            # IDs para joins
            concat_ws("|", col("tv.cod_compania"), col("tv.cod_sucursal"), 
                     col("tv.cod_almacen"), col("tv.cod_documento_venta"), 
                     col("tv.nro_documento_venta")).alias("id_venta"),
            concat_ws("|", col("tv.cod_compania"), col("tv.cod_cliente")).alias("id_cliente"),
            concat_ws("|", col("tv.cod_compania"), col("tv.cod_sucursal")).alias("id_sucursal"),
            concat_ws("|", col("tv.cod_compania"), col("tv.cod_forma_pago")).alias("id_forma_pago"),
            concat_ws("|", col("tv.cod_compania"), col("tv.cod_lista_precio")).alias("id_lista_precio"),
            concat_ws("|", col("tv.cod_compania"), col("tv.cod_documento_venta")).alias("id_tipo_venta"),
            
            # Campos base
            col("mc.id_pais"),
            date_format(col("tv.fecha_liquidacion"), "yyyyMM").alias("id_periodo"),
            col("tv.cod_compania"),
            col("tv.fecha_liquidacion"),
            col("tv.fecha_emision"),
            col("tv.fecha_pedido"),
            col("tv.cod_estado_comprobante"),
            col("tv.cod_moneda"),
            col("tv.tipo_cambio_mn"),
            col("tv.tipo_cambio_me"),
            
            # Tipos de cambio calculados
            coalesce(
                when(col("tv.cod_moneda") == col("mc.moneda_mn"), 1)
                .otherwise(col("mtc.tc_venta")), 
                col("tv.tipo_cambio_mn")
            ).alias("tipo_cambio_mn_calc"),
            coalesce(
                when((col("tv.cod_moneda") == "DOL") | (col("tv.cod_moneda") == "USD"), 1)
                .otherwise(col("mtc.tc_venta")), 
                when(col("tv.tipo_cambio_me") == 0, 1)
                .otherwise(col("tv.tipo_cambio_me"))
            ).alias("tipo_cambio_me_calc"),
            
            # Flags
            when(col("tv.cod_estado_comprobante") == "002", 1).otherwise(0).alias("es_anulado"),
            when(col("tv.cod_estado_comprobante") == "002", 1).otherwise(0).alias("es_eliminado")
        )
    )
    
    # === PREPARACIÓN DE TABLA DE LICENCIAS/CUOTAS ===
    logger.info("Processing license quota table")
    
    df_licencia_cuota_processed = (
        df_t_licencia_cuota.alias("tlc")
        .join(
            df_m_licencia.alias("ml"),
            (col("tlc.cod_compania") == col("ml.cod_compania")) &
            (col("tlc.cod_licencia") == col("ml.cod_licencia")),
            "left"
        )
        .select(
            concat_ws("|", col("tlc.cod_compania"), col("tlc.cod_sucursal"), 
                     col("tlc.cod_almacen"), col("tlc.cod_documento_venta"), 
                     col("tlc.nro_documento_venta"), col("tlc.cod_articulo"),
                     col("tlc.cod_operacion"), col("tlc.cod_unidad_venta")).alias("id_documento_venta_detalle"),
            concat_ws("|", col("tlc.cod_compania"), col("tlc.cod_licencia")).alias("id_licencia"),
            concat_ws("|", col("ml.cod_compania"), col("ml.cod_persona")).alias("id_socio"),
            concat_ws("|", col("ml.cod_compania"), col("ml.cod_persona"), 
                     col("ml.cod_direccion")).alias("id_establecimiento")
        )
    )
    
    # === PREPARACIÓN DE TABLA DE DETALLE DE VENTAS ===
    logger.info("Processing sales detail (t_documento_venta_detalle)")
    
    df_venta_detalle_processed = (
        df_t_documento_venta_detalle.alias("tvd")
        .select(
            # IDs para joins
            concat_ws("|", col("tvd.cod_compania"), col("tvd.cod_sucursal"), 
                     col("tvd.cod_almacen"), col("tvd.cod_documento_transaccion"), 
                     col("tvd.nro_comprobante_venta")).alias("id_venta"),
            concat_ws("|", col("tvd.cod_compania"), col("tvd.cod_articulo")).alias("id_producto"),
            concat_ws("|", col("tvd.cod_compania"), col("tvd.cod_documento_transaccion"), 
                     col("tvd.cod_procedimiento"), col("tvd.cod_operacion")).alias("id_operacion"),
            col("tvd.id_documento_venta_detalle"),
            
            # Campos de cantidad e importes
            col("tvd.cod_compania"),
            col("tvd.cod_operacion"),
            col("tvd.cant_paquete"),
            col("tvd.cant_unidad"),
            col("tvd.imp_valorizado"),
            col("tvd.imp_cobrar"),
            col("tvd.imp_descuento"),
            col("tvd.imp_descuento_sinimp"),
            col("tvd.precio_paquete"),
            col("tvd.imp_isc"),
            col("tvd.imp_igv"),
            col("tvd.imp_im3"),
            col("tvd.imp_im4"),
            col("tvd.imp_im5"),
            col("tvd.imp_im6"),
            col("tvd.fecha_creacion"),
            col("tvd.fecha_modificacion")
        )
    )
    
    # === UNIÓN PRINCIPAL PARA CREAR FACT TABLE ===
    logger.info("Creating main fact table join")
    
    df_fact_base = (
        df_venta_detalle_processed.alias("tvd")
        .join(
            df_venta_processed.alias("tv"),
            col("tvd.id_venta") == col("tv.id_venta"),
            "inner"
        )
        .join(
            df_m_articulo_enriched.alias("ma"),
            col("tvd.id_producto") == col("ma.id_producto"),
            "inner"
        )
        .join(
            df_m_operacion_filtered.alias("mo"),
            col("tvd.id_operacion") == col("mo.id_operacion"),
            "inner"
        )
        .join(
            df_licencia_cuota_processed.alias("tl"),
            col("tvd.id_documento_venta_detalle") == col("tl.id_documento_venta_detalle"),
            "left"
        )
        .join(
            df_m_tipo_venta.alias("mtv"),
            col("tv.id_tipo_venta") == concat_ws("|", col("mtv.cod_compania"), col("mtv.cod_tipo_venta")),
            "inner"
        )
        .where(
            (col("tv.es_eliminado") == 0) &
            (upper(col("mtv.cod_tipo_operacion")).isin(['VEN', 'EXP']))
        )
    )
    
    # === CÁLCULOS FINALES Y AGRUPACIÓN ===
    logger.info("Performing final calculations and aggregations")
    
    # Factor para notas de crédito
    df_fact_calculated = df_fact_base.select(
        # Dimensiones para agrupación
        col("tv.id_sucursal"),
        col("tv.id_cliente"),
        col("tvd.id_producto"),
        coalesce(col("tl.id_licencia"), lit("")).alias("id_licencia"),
        coalesce(col("tl.id_socio"), lit("")).alias("id_socio"),
        coalesce(col("tl.id_establecimiento"), lit("")).alias("id_establecimiento"),
        col("tv.id_forma_pago"),
        col("tv.id_lista_precio"),
        col("tv.id_periodo"),
        col("tv.id_pais"),
        
        # Factor para cálculos (notas de crédito = -1)
        when(col("tv.cod_documento_venta") == "NCC", -1).otherwise(1).alias("factor"),
        
        # Campos para cálculos
        col("mo.cod_tipo_operacion"),
        col("tv.tipo_cambio_mn_calc").alias("tipo_cambio_mn"),
        col("tv.tipo_cambio_me_calc").alias("tipo_cambio_me"),
        col("tvd.cant_paquete"),
        col("tvd.cant_unidad"),
        col("ma.cant_unidad_paquete"),
        col("ma.cant_paquete_caja"),
        col("ma.cant_unidad_volumen"),
        col("tvd.imp_valorizado"),
        col("tvd.imp_cobrar"),
        col("tvd.imp_descuento"),
        col("tvd.precio_paquete"),
        col("tvd.imp_isc"),
        col("tvd.imp_igv")
    )
    
    # Agrupación final para crear el fact table
    df_fact_venta_cliente_historico = (
        df_fact_calculated
        .groupby(
            col("id_sucursal"),
            col("id_cliente"),
            col("id_producto"),
            col("id_licencia"),
            col("id_socio"),
            col("id_establecimiento"),
            col("id_forma_pago"),
            col("id_lista_precio"),
            col("id_periodo"),
            col("id_pais")
        )
        .agg(
            # Cantidades físicas
            sum(
                col("factor") * 
                (col("cant_paquete") + (col("cant_unidad") / coalesce(col("cant_unidad_paquete"), lit(1)))) * 
                coalesce(col("cant_paquete_caja"), lit(1))
            ).alias("cant_caja_fisica"),
            
            # Cantidades volumétricas
            sum(
                col("factor") * 
                (col("cant_paquete") * coalesce(col("cant_unidad_paquete"), lit(1)) + col("cant_unidad")) * 
                coalesce(col("cant_unidad_volumen"), lit(1))
            ).alias("cant_caja_volumen"),
            
            # Importes en moneda nacional
            sum(col("factor") * col("imp_valorizado") * col("tipo_cambio_mn")).alias("imp_valorizado_mn"),
            sum(col("factor") * col("imp_cobrar") * col("tipo_cambio_mn")).alias("imp_cobrar_mn"),
            sum(col("factor") * col("imp_descuento") * col("tipo_cambio_mn")).alias("imp_descuento_mn"),
            sum(col("factor") * col("precio_paquete") * col("tipo_cambio_mn")).alias("imp_precio_mn"),
            sum(col("factor") * col("imp_isc") * col("tipo_cambio_mn")).alias("imp_isc_mn"),
            sum(col("factor") * col("imp_igv") * col("tipo_cambio_mn")).alias("imp_igv_mn"),
            
            # Importes en moneda extranjera  
            sum(col("factor") * col("imp_valorizado") * col("tipo_cambio_me")).alias("imp_valorizado_me"),
            sum(col("factor") * col("imp_cobrar") * col("tipo_cambio_me")).alias("imp_cobrar_me"),
            sum(col("factor") * col("imp_descuento") * col("tipo_cambio_me")).alias("imp_descuento_me"),
            sum(col("factor") * col("precio_paquete") * col("tipo_cambio_me")).alias("imp_precio_me"),
            sum(col("factor") * col("imp_isc") * col("tipo_cambio_me")).alias("imp_isc_me"),
            sum(col("factor") * col("imp_igv") * col("tipo_cambio_me")).alias("imp_igv_me")
        )
    )
    
    logger.info("Final fact table created successfully")
    
    # === ESCRITURA DEL RESULTADO ===
    logger.info(f"Writing result to analytics layer: {target_table_name}")
    
    spark_controller.write_table(
        df_fact_venta_cliente_historico,
        data_paths.ANALYTICS,
        target_table_name,
        "overwrite"
    )
    
    logger.info(f"Job completed successfully for {target_table_name}")
    
except Exception as e:
    logger.error(f"Error in transformation process: {e}")
    raise ValueError(f"Error in transformation process: {e}")