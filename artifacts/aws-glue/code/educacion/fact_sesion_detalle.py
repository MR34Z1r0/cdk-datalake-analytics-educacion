import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import (
    col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date,
    sum, count, countDistinct, isnull, desc, asc, max, expr
)
from pyspark.sql.types import StringType, DateType, IntegerType, DecimalType, TimestampType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "fact_sesion_detalle"

try:
    # Lectura directa desde STAGE (UPEU) - todas las tablas necesarias
    logger.info("Reading tables from UPEU (Stage layer)")
    
    # Tablas principales de evaluación
    df_t_rn_mae_rubro_evaluacion_proceso = spark_controller.read_table(data_paths.UPEU, "t_rn_mae_rubro_evaluacion_proceso")
    df_t_rn_mov_evaluacion_proceso = spark_controller.read_table(data_paths.UPEU, "t_rn_mov_evaluacion_proceso")
    
    # Tablas de actividades y tareas
    df_t_gc_mae_actividad_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_actividad_evento")
    df_t_gc_mae_tarea_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_tarea_evento")
    df_t_gc_mov_estado_entrega_tarea = spark_controller.read_table(data_paths.UPEU, "t_gc_mov_estado_entrega_tarea")
    
    # Tablas de instrumentos y evidencias
    df_t_gie_mae_instrumento_evaluacion = spark_controller.read_table(data_paths.UPEU, "t_gie_mae_instrumento_evaluacion")
    df_t_gc_mae_producto_aprendizaje_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_producto_aprendizaje_evento")
    df_t_gc_rel_producto_evento_referencia = spark_controller.read_table(data_paths.UPEU, "t_gc_rel_producto_evento_referencia")
    
    # Tablas de recursos
    df_t_gc_rel_recurso_evento_referencia = spark_controller.read_table(data_paths.UPEU, "t_gc_rel_recurso_evento_referencia")
    df_t_gc_mae_recurso_didatico_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_recurso_didatico_evento")
    
    # Tablas principales de sesiones
    df_t_gc_mae_sesion_aprendizaje_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_sesion_aprendizaje_evento")
    df_t_gc_mae_unidad_aprendizaje_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_unidad_aprendizaje_evento")
    df_t_gc_mae_silabo_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_silabo_evento")
    
    # Tablas de carga académica
    df_ca_carga_cursos = spark_controller.read_table(data_paths.UPEU, "ca_carga_cursos")
    df_ca_carga_academica = spark_controller.read_table(data_paths.UPEU, "ca_carga_academica")
    df_planestudios = spark_controller.read_table(data_paths.UPEU, "planestudios")
    df_programaseducativo = spark_controller.read_table(data_paths.UPEU, "programaseducativo")
    df_plancursos = spark_controller.read_table(data_paths.UPEU, "plancursos")
    
    # Dimensiones ya existentes en analytics
    df_dim_docente = spark_controller.read_table(data_paths.ANALYTICS, "dim_docente")
    df_dim_grupo = spark_controller.read_table(data_paths.ANALYTICS, "dim_grupo")
    df_dim_programa_educativo = spark_controller.read_table(data_paths.ANALYTICS, "dim_programa_educativo")
    df_dim_periodo = spark_controller.read_table(data_paths.ANALYTICS, "dim_periodo")
    df_dim_anio_academico = spark_controller.read_table(data_paths.ANALYTICS, "dim_anio_academico")
    df_dim_curso = spark_controller.read_table(data_paths.ANALYTICS, "dim_curso")
    
    logger.info("Tables loaded successfully from UPEU and Analytics")
except Exception as e:
    logger.error(f"Error reading tables from UPEU: {e}")
    raise ValueError(f"Error reading tables from UPEU: {e}")

try:
    # PASO 1: Crear tablas temporales equivalentes a los #temp de SQL
    logger.info("Creating temporary tables - temp_sesion_evaluacion")
    
    # #temp_sesion_evaluacion
    df_temp_sesion_evaluacion = (
        df_t_rn_mae_rubro_evaluacion_proceso
        .where(col("sesionaprendizajeid").isNotNull())
        .select(
            col("unidadaprendizajeid").cast(IntegerType()).alias("id_unidad_aprendizaje"),
            col("calendarioperiodoid").cast(IntegerType()).alias("id_periodo_academico")
        )
        .distinct()
    )
    
    # #evaluaciones
    logger.info("Creating temporary tables - evaluaciones")
    df_evaluaciones = (
        df_t_rn_mae_rubro_evaluacion_proceso
        .where(
            (col("tiporubroid").cast(IntegerType()) == 471) &
            (col("estadoid").cast(IntegerType()) != 280) &
            (col("promedio").cast(DecimalType(4,2)) > 0)
        )
        .select(col("sesionaprendizajeid").cast(IntegerType()).alias("sesionaprendizajeid"))
        .distinct()
    )
    
    # #evaluacionespublicadas
    logger.info("Creating temporary tables - evaluacionespublicadas")
    df_evaluacionespublicadas = (
        df_t_rn_mae_rubro_evaluacion_proceso.alias("rubrop")
        .join(
            df_t_rn_mov_evaluacion_proceso.alias("notas"),
            col("rubrop.rubroevalprocesoid") == col("notas.rubroevalprocesoid"),
            "inner"
        )
        .where(
            (col("rubrop.tiporubroid").cast(IntegerType()) == 471) &
            (col("rubrop.estadoid").cast(IntegerType()) != 280) &
            (col("rubrop.promedio").cast(DecimalType(4,2)) > 0) &
            (col("notas.publicado").cast(IntegerType()) == 1) &
            (col("notas.nota").cast(DecimalType(4,2)) > 0)
        )
        .select(col("rubrop.sesionaprendizajeid").cast(IntegerType()).alias("sesionaprendizajeid"))
        .distinct()
    )
    
    # #actividades
    logger.info("Creating temporary tables - actividades")
    df_actividades = (
        df_t_gc_mae_actividad_evento
        .select(col("sesionaprendizajeid").cast(IntegerType()).alias("sesionaprendizajeid"))
        .distinct()
    )
    
    # #tareas
    logger.info("Creating temporary tables - tareas")
    df_tareas = (
        df_t_gc_mae_tarea_evento
        .where(col("estadoid").cast(IntegerType()) == 264)
        .groupBy(col("sesionaprendizajeid").cast(IntegerType()).alias("sesionaprendizajeid"))
        .agg(count(lit(1)).alias("totaltareas"))
    )
    
    # #tareasentregadas
    logger.info("Creating temporary tables - tareasentregadas")
    df_tareasentregadas = (
        df_t_gc_mae_tarea_evento.alias("tareas")
        .join(
            df_t_gc_mov_estado_entrega_tarea.alias("estadotrabajos"),
            col("tareas.tareaid") == col("estadotrabajos.tareaid"),
            "inner"
        )
        .where(col("tareas.estadoid").cast(IntegerType()) == 264)
        .groupBy(col("tareas.sesionaprendizajeid").cast(IntegerType()).alias("sesionaprendizajeid"))
        .agg(
            sum(when(col("estadotrabajos.entregado").cast(IntegerType()) == 1, 1).otherwise(0))
            .alias("tiene_tareas_entregadas")
        )
    )
    
    # #instrumentos
    logger.info("Creating temporary tables - instrumentos")
    df_instrumentos = (
        df_t_gie_mae_instrumento_evaluacion
        .where(
            (col("estado").cast(IntegerType()) == 1) &
            (col("publicado").cast(IntegerType()) == 1) &
            (col("corporativa").cast(IntegerType()) == 0)
        )
        .groupBy(col("sesionid").cast(IntegerType()).alias("sesionid"))
        .agg(count(lit(1)).alias("totalinstrumentos"))
    )
    
    # #evidencias
    logger.info("Creating temporary tables - evidencias")
    df_evidencias = (
        df_t_gc_mae_producto_aprendizaje_evento.alias("producto")
        .join(
            df_t_gc_rel_producto_evento_referencia.alias("referencia"),
            col("producto.productoaprendizajeid") == col("referencia.productoaprendizajeid"),
            "inner"
        )
        .where(col("producto.estado").cast(IntegerType()) == 1)
        .groupBy(col("referencia.sesionaprendizajeid").cast(IntegerType()).alias("sesionaprendizajeid"))
        .agg(count(lit(1)).alias("totalevidencias"))
    )
    
    # PASO 2: Crear tabla de recursos (#recursos) - más compleja con UNION
    logger.info("Creating temporary tables - recursos (complex union)")
    
    # Primera parte del UNION - recursos por actividad
    df_recursos_actividad = (
        df_t_gc_rel_recurso_evento_referencia.alias("refactiv")
        .join(
            df_t_gc_mae_recurso_didatico_evento.alias("recurso"),
            col("refactiv.recursodidacticoid") == col("recurso.recursodidacticoid"),
            "inner"
        )
        .join(
            df_t_gc_mae_actividad_evento.alias("actividad"),
            col("refactiv.actividadaprendizajeid") == col("actividad.actividadaprendizajeid"),
            "inner"
        )
        .where(
            (col("recurso.estado").cast(IntegerType()) == 1) &
            (coalesce(col("actividad.rolid"), lit(6)) == 6)
        )
        .groupBy(col("actividad.sesionaprendizajeid").cast(IntegerType()).alias("sesionaprendizajeid"))
        .agg(
            sum(when(col("refactiv.tiporecursoid").cast(IntegerType()) == 589, 1).otherwise(0)).alias("cant_motivacion"),
            sum(when(col("refactiv.tiporecursoid").cast(IntegerType()).isin([590, 593, 656]), 1).otherwise(0)).alias("cant_guia_teorica"),
            sum(when(col("refactiv.tiporecursoid").cast(IntegerType()).isin([591, 593, 656, 670]), 1).otherwise(0)).alias("cant_guia_practica"),
            sum(when(col("refactiv.tiporecursoid").cast(IntegerType()).isin([592, 656, 670]), 1).otherwise(0)).alias("cant_guia_aprendizaje_autonomo"),
            sum(when(col("refactiv.tiporecursoid").isNull(), 1).otherwise(0)).alias("cant_sin_clasificacion"),
            sum(when(col("recurso.tipoid").cast(IntegerType()) == 380, 1).otherwise(0)).alias("cant_enlaces"),
            sum(when(col("recurso.tipoid").cast(IntegerType()) == 379, 1).otherwise(0)).alias("cant_videos"),
            sum(when(col("recurso.tipoid").cast(IntegerType()).isin([397,398,399,400,401,402,403,625,626,627,628,653]), 1).otherwise(0)).alias("cant_archivos"),
            count(lit(1)).alias("cant_recursos")
        )
    )
    
    # Segunda parte del UNION - recursos por sesión
    df_recursos_sesion = (
        df_t_gc_rel_recurso_evento_referencia.alias("refses")
        .join(
            df_t_gc_mae_recurso_didatico_evento.alias("recurso"),
            col("refses.recursodidacticoid") == col("recurso.recursodidacticoid"),
            "inner"
        )
        .where(col("recurso.estado").cast(IntegerType()) == 1)
        .groupBy(col("refses.sesionaprendizajeid").cast(IntegerType()).alias("sesionaprendizajeid"))
        .agg(
            sum(when(col("refses.tiporecursoid").cast(IntegerType()) == 589, 1).otherwise(0)).alias("cant_motivacion"),
            sum(when(col("refses.tiporecursoid").cast(IntegerType()).isin([590, 593, 656]), 1).otherwise(0)).alias("cant_guia_teorica"),
            sum(when(col("refses.tiporecursoid").cast(IntegerType()).isin([591, 593, 656, 670]), 1).otherwise(0)).alias("cant_guia_practica"),
            sum(when(col("refses.tiporecursoid").cast(IntegerType()).isin([592, 656, 670]), 1).otherwise(0)).alias("cant_guia_aprendizaje_autonomo"),
            sum(when(col("refses.tiporecursoid").isNull(), 1).otherwise(0)).alias("cant_sin_clasificacion"),
            sum(when(col("recurso.tipoid").cast(IntegerType()) == 380, 1).otherwise(0)).alias("cant_enlaces"),
            sum(when(col("recurso.tipoid").cast(IntegerType()) == 379, 1).otherwise(0)).alias("cant_videos"),
            sum(when(col("recurso.tipoid").cast(IntegerType()).isin([397,398,399,400,401,402,403,625,626,627,628,653]), 1).otherwise(0)).alias("cant_archivos"),
            count(lit(1)).alias("cant_recursos")
        )
    )
    
    # UNION de recursos y agrupación final
    df_recursos = (
        df_recursos_actividad.union(df_recursos_sesion)
        .groupBy("sesionaprendizajeid")
        .agg(
            sum("cant_motivacion").alias("cant_motivacion"),
            sum("cant_guia_teorica").alias("cant_guia_teorica"),
            sum("cant_guia_practica").alias("cant_guia_practica"),
            sum("cant_guia_aprendizaje_autonomo").alias("cant_guia_aprendizaje_autonomo"),
            sum("cant_sin_clasificacion").alias("cant_sin_clasificacion"),
            sum("cant_enlaces").alias("cant_enlaces"),
            sum("cant_videos").alias("cant_videos"),
            sum("cant_archivos").alias("cant_archivos"),
            sum("cant_recursos").alias("cant_recursos")
        )
    )
    
    # #sesiontea
    logger.info("Creating temporary tables - sesiontea")
    df_sesiontea = (
        df_t_rn_mae_rubro_evaluacion_proceso.alias("rubrop")
        .join(
            df_t_rn_mov_evaluacion_proceso.alias("notas"),
            col("rubrop.rubroevalprocesoid") == col("notas.rubroevalprocesoid"),
            "inner"
        )
        .join(
            df_t_gc_mae_tarea_evento.alias("tarea"),
            col("rubrop.tareaid") == col("tarea.tareaid"),
            "inner"
        )
        .join(
            df_t_gc_mov_estado_entrega_tarea.alias("estadotrabajos"),
            col("tarea.tareaid") == col("estadotrabajos.tareaid"),
            "inner"
        )
        .where(
            (col("rubrop.tiporubroid").cast(IntegerType()) == 471) &
            (col("rubrop.estadoid").cast(IntegerType()) != 280) &
            (col("rubrop.promedio").cast(DecimalType(4,2)) > 0) &
            (col("rubrop.sesionaprendizajeid").isNotNull()) &
            (col("notas.nota").cast(DecimalType(4,2)) > 0) &
            (col("tarea.estadoid").cast(IntegerType()) == 264) &
            (col("estadotrabajos.entregado").cast(IntegerType()) == 1)
        )
        .groupBy(col("rubrop.sesionaprendizajeid").cast(IntegerType()).alias("sesionaprendizajeid"))
        .agg(countDistinct(col("tarea.tareaid")).alias("sesion_tea"))
    )
    
    # PASO 3: Consulta principal - fact_sesion_detalle
    logger.info("Creating main query - fact_sesion_detalle")
    
    df_fact_sesion_detalle = (
        df_t_gc_mae_sesion_aprendizaje_evento.alias("msa")
        .join(
            df_t_gc_mae_unidad_aprendizaje_evento.alias("mua"),
            col("msa.unidadaprendizajeid") == col("mua.unidadaprendizajeid"),
            "inner"
        )
        .join(
            df_t_gc_mae_silabo_evento.alias("mse"),
            col("mua.silaboeventoid") == col("mse.silaboeventoid"),
            "inner"
        )
        .join(
            df_temp_sesion_evaluacion.alias("tsse"),
            col("msa.unidadaprendizajeid") == col("tsse.id_unidad_aprendizaje"),
            "left"
        )
        .join(
            df_ca_carga_cursos.alias("mcc"),
            col("mcc.id") == col("mse.cargacursoid"),
            "left"
        )
        .join(
            df_ca_carga_academica.alias("mca"),
            col("mcc.idcargaacademica") == col("mca.id"),
            "left"
        )
        .join(
            df_dim_docente.alias("ddo"),
            col("mcc.idempleado") == col("ddo.id_docente"),
            "left"
        )
        .join(
            df_dim_grupo.alias("ddg"),
            col("mca.idgrupo") == col("ddg.id_grupo"),
            "left"
        )
        .join(
            df_planestudios.alias("mpe"),
            col("mca.idplanestudio") == col("mpe.id"),
            "left"
        )
        .join(
            df_programaseducativo.alias("dpe"),
            col("mpe.programaid") == col("dpe.id"),
            "left"
        )
        .join(
            df_dim_programa_educativo.alias("mna"),
            col("dpe.nivelacademicoid") == col("mna.id_programa_educativo"),
            "left"
        )
        .join(
            df_dim_periodo.alias("dps"),
            col("mca.idperiodoacad") == col("dps.id_periodo"),
            "left"
        )
        .join(
            df_dim_anio_academico.alias("daa"),
            col("mca.idanioacademico") == col("daa.id_anio_academico"),
            "left"
        )
        .join(
            df_plancursos.alias("mpc"),
            (col("mcc.idplancursos") == col("mpc.id")) & (col("mpc.planestudioid") == col("mca.idplanestudio")),
            "left"
        )
        .join(
            df_dim_curso.alias("dcu"),
            col("mpc.cursoid") == col("dcu.id_curso"),
            "left"
        )
        # CORRECCIÓN: Usar los nombres de columna correctos sin alias inexistentes
        .join(df_tareas.alias("tar"), 
              col("msa.sesionaprendizajeid").cast(IntegerType()) == col("tar.sesionaprendizajeid"), "left")
        .join(df_tareasentregadas.alias("tent"), 
              col("msa.sesionaprendizajeid").cast(IntegerType()) == col("tent.sesionaprendizajeid"), "left")
        .join(df_instrumentos.alias("inst"), 
              col("msa.sesionaprendizajeid").cast(IntegerType()) == col("inst.sesionid"), "left")
        .join(df_evidencias.alias("evid"), 
              col("msa.sesionaprendizajeid").cast(IntegerType()) == col("evid.sesionaprendizajeid"), "left")
        .join(df_recursos.alias("rec"), 
              col("msa.sesionaprendizajeid").cast(IntegerType()) == col("rec.sesionaprendizajeid"), "left")
        .join(df_sesiontea.alias("sest"), 
              col("msa.sesionaprendizajeid").cast(IntegerType()) == col("sest.sesionaprendizajeid"), "left")
        # LEFT JOINs para las condiciones EXISTS del SELECT
        .join(df_evaluaciones.alias("evaluaciones"), 
              col("msa.sesionaprendizajeid").cast(IntegerType()) == col("evaluaciones.sesionaprendizajeid"), "left")
        .join(df_evaluacionespublicadas.alias("evaluacionespublicadas"), 
              col("msa.sesionaprendizajeid").cast(IntegerType()) == col("evaluacionespublicadas.sesionaprendizajeid"), "left")
        .join(df_actividades.alias("actividades"), 
              col("msa.sesionaprendizajeid").cast(IntegerType()) == col("actividades.sesionaprendizajeid"), "left")
        .where(
            (col("msa.estadoid").cast(IntegerType()) != 299) &
            (col("mua.estadoid").cast(IntegerType()) != 295) &
            (coalesce(col("msa.rolid").cast(IntegerType()), lit(6)) == 6)
        )
        .select(
            col("mca.idanioacademico").cast(IntegerType()).alias("id_anio_academico"),
            col("mse.georeferenciaid").cast(IntegerType()).alias("id_escuela"),
            col("dpe.id").cast(IntegerType()).alias("id_programa_educativo"),
            col("tsse.id_periodo_academico"),
            col("dps.id_periodo"),
            col("mse.silaboeventoid").cast(IntegerType()).alias("id_silabo"),
            col("msa.unidadaprendizajeid").cast(IntegerType()).alias("id_unidad_aprendizaje"),
            col("msa.sesionaprendizajeid").cast(IntegerType()).alias("id_sesion_aprendizaje"),
            col("mpc.cursoid").cast(IntegerType()).alias("id_curso"),
            col("mca.idgrupo").cast(IntegerType()).alias("id_grupo"),
            col("ddo.id_docente"),
            col("msa.titulo").alias("titulo"),
            coalesce(col("msa.fechaejecucion"), lit("")).alias("fecha_ejecucion"),
            col("mua.nrounidad").cast(IntegerType()).alias("nro_unidad"),
            col("msa.nrosesion").cast(IntegerType()).alias("nro_sesion"),
            when(col("msa.fechaejecucion").isNotNull(), 1).otherwise(0).alias("es_programado"),
            when(col("msa.estadoverificacionid").cast(IntegerType()).isin([328, 332, 329]), 1).otherwise(0).alias("es_entregado"),
            when(col("msa.estadoverificacionid").cast(IntegerType()) == 329, 1).otherwise(0).alias("es_certificado"),
            when(col("msa.estadoejecucionid").cast(IntegerType()) == 317, 1).otherwise(0).alias("es_hecho"),
            when(col("evaluaciones.sesionaprendizajeid").isNotNull(), 1).otherwise(0).alias("es_evaluado"),
            when(col("msa.estadoid").cast(IntegerType()) == 297, 1).otherwise(0).alias("es_publicado"),
            coalesce(col("rec.cant_motivacion"), lit(0)).alias("cant_motivacion"),
            coalesce(col("rec.cant_guia_teorica"), lit(0)).alias("cant_guia_teorica"),
            coalesce(col("rec.cant_guia_practica"), lit(0)).alias("cant_guia_practica"),
            coalesce(col("rec.cant_guia_aprendizaje_autonomo"), lit(0)).alias("cant_guia_aprendizaje_autonomo"),
            coalesce(col("rec.cant_sin_clasificacion"), lit(0)).alias("cant_sin_clasificacion"),
            coalesce(col("rec.cant_enlaces"), lit(0)).alias("cant_enlaces"),
            coalesce(col("rec.cant_videos"), lit(0)).alias("cant_videos"),
            coalesce(col("rec.cant_archivos"), lit(0)).alias("cant_archivos"),
            coalesce(col("msa.ignorarmonitoreo").cast(IntegerType()), lit(0)).alias("es_ignorar_monitoreo"),
            col("msa.etiquetaid").cast(IntegerType()).alias("id_etiqueta"),
            when(col("evaluacionespublicadas.sesionaprendizajeid").isNotNull(), 1).otherwise(0).alias("es_eval_publicado"),
            when(col("actividades.sesionaprendizajeid").isNotNull(), 1).otherwise(0).alias("tiene_actividades"),
            coalesce(col("tar.totaltareas"), lit(0)).alias("tiene_tareas"),
            coalesce(col("tent.tiene_tareas_entregadas"), lit(0)).alias("tiene_tareas_entregadas"),
            coalesce(col("inst.totalinstrumentos"), lit(0)).alias("tiene_instrumentos"),
            coalesce(col("evid.totalevidencias"), lit(0)).alias("tiene_evidenc_planf"),
            coalesce(col("rec.cant_recursos"), lit(0)).alias("cant_recursos"),
            coalesce(col("sest.sesion_tea"), lit(0)).alias("sesion_tea")
        )
        .orderBy(col("mua.nrounidad"), col("msa.nrosesion"))
    )

    # Escribir a analytics usando overwrite (equivalente a TRUNCATE + INSERT)
    # Como es una tabla de hechos, usamos write_table con overwrite en lugar de upsert
    partition_columns_array = []  # Sin partición según estándar de fact tables
    
    logger.info(f"Starting overwrite of {target_table_name}")
    spark_controller.write_table(df_fact_sesion_detalle, data_paths.ANALYTICS, target_table_name, partition_columns_array)
    logger.info(f"Overwrite de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing fact_sesion_detalle: {e}")
    raise ValueError(f"Error processing fact_sesion_detalle: {e}")