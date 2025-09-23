import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import (
    col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date,
    sum, count, countDistinct, isnull, desc, asc, max, expr, broadcast
)
from pyspark.sql.types import StringType, DateType, IntegerType, DecimalType, TimestampType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "fact_unidad_detalle"

# CONFIGURACIONES DE OPTIMIZACIÓN
logger.info("Setting Spark optimization configurations")
spark = spark_controller.spark

# Configuraciones de optimización para mejor rendimiento
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

# Configuración para joins automáticos tipo broadcast
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "100MB")

# Optimización para UDFs (futuras mejoras)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

try:
    # PASO 1: LECTURA DE TABLAS CON ESTRATEGIA DE CACHEO
    logger.info("Reading tables from UPEU (Stage layer)")
    
    # === TABLAS PRINCIPALES (MÁS GRANDES) ===
    # Estas son las tablas principales que se reutilizan múltiples veces
    df_t_rn_mae_rubro_evaluacion_proceso = spark_controller.read_table(data_paths.UPEU, "t_rn_mae_rubro_evaluacion_proceso")
    df_t_rn_mov_evaluacion_proceso = spark_controller.read_table(data_paths.UPEU, "t_rn_mov_evaluacion_proceso")
    df_t_gc_mae_tarea_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_tarea_evento")
    df_t_gc_mae_unidad_aprendizaje_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_unidad_aprendizaje_evento")
    
    # Cachear las tablas que se usan múltiples veces en agregaciones
    logger.info("Caching frequently used tables")
    df_t_rn_mae_rubro_evaluacion_proceso.cache()
    df_t_gc_mae_tarea_evento.cache()
    
    # === TABLAS DE DIMENSIONES (PEQUEÑAS - CANDIDATAS A BROADCAST) ===
    df_dim_docente = spark_controller.read_table(data_paths.ANALYTICS, "dim_docente")
    df_dim_programa_educativo = spark_controller.read_table(data_paths.ANALYTICS, "dim_programa_educativo")
    df_dim_periodo = spark_controller.read_table(data_paths.ANALYTICS, "dim_periodo")
    df_dim_anio_academico = spark_controller.read_table(data_paths.ANALYTICS, "dim_anio_academico")
    df_dim_curso = spark_controller.read_table(data_paths.ANALYTICS, "dim_curso")
    
    # === TABLAS DE REFERENCIA ===
    df_t_gc_mov_estado_entrega_tarea = spark_controller.read_table(data_paths.UPEU, "t_gc_mov_estado_entrega_tarea")
    df_t_gc_rel_recurso_evento_referencia = spark_controller.read_table(data_paths.UPEU, "t_gc_rel_recurso_evento_referencia")
    df_t_gc_mae_recurso_didatico_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_recurso_didatico_evento")
    df_t_gc_mae_competencia = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_competencia")
    
    # === TABLAS DE ESTRUCTURA ACADÉMICA ===
    df_planestudios = spark_controller.read_table(data_paths.UPEU, "planestudios")
    df_plancursos = spark_controller.read_table(data_paths.UPEU, "plancursos")
    df_t_gc_mae_silabo_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_silabo_evento")
    df_ca_carga_cursos = spark_controller.read_table(data_paths.UPEU, "ca_carga_cursos")
    df_ca_carga_academica = spark_controller.read_table(data_paths.UPEU, "ca_carga_academica")
    df_ca_carga_academica_calendario_periodo = spark_controller.read_table(data_paths.UPEU, "ca_carga_academica_calendario_periodo")
    df_programaseducativo = spark_controller.read_table(data_paths.UPEU, "programaseducativo")

    logger.info("Tables loaded successfully from UPEU")

    # PASO 2: CREAR TABLAS TEMPORALES OPTIMIZADAS
    logger.info("Creating optimized temporary tables")
    
    # === EvaluacionesUnidad (Corregida para coincidir con SQL original) ===
    logger.info("Creating temporary tables - EvaluacionesUnidad")
    df_evaluaciones_unidad = (
        df_t_rn_mae_rubro_evaluacion_proceso.alias("RubroP")
        .join(df_t_rn_mov_evaluacion_proceso.alias("Notas"),
              col("RubroP.rubroevalprocesoid") == col("Notas.rubroevalprocesoid"), "left")
        .where(
            (col("RubroP.tiporubroid").cast(IntegerType()) == 471) &
            col("RubroP.sesionaprendizajeid").isNull()
        )
        .groupBy(col("RubroP.unidadaprendizajeid").cast(IntegerType()).alias("unidadaprendizajeid"))
        .agg(
            max(when(
                (col("RubroP.estadoid").cast(IntegerType()) != 280) &
                (col("RubroP.promedio").cast(DecimalType(4,2)) > 0), 1
            ).otherwise(0)).alias("es_evaluado"),
            max(when(
                (col("RubroP.estadoid").cast(IntegerType()) != 280) &
                (col("RubroP.promedio").cast(DecimalType(4,2)) > 0) &
                (col("Notas.publicado").cast(IntegerType()) == 1) &
                (col("Notas.nota").cast(DecimalType(4,2)) > 0), 1
            ).otherwise(0)).alias("es_eval_publicado")
        )
    )
    
    # === TareasUnidad (Optimizada) ===
    logger.info("Creating temporary tables - TareasUnidad")
    df_tareas_unidad = (
        df_t_gc_mae_tarea_evento
        .where(
            (col("estadoid").cast(IntegerType()) == 264) &
            col("sesionaprendizajeid").isNull()
        )
        .groupBy(col("unidadaprendizajeid").cast(IntegerType()).alias("unidadaprendizajeid"))
        .agg(count(lit(1)).alias("tiene_tareas"))
    )
    
    # === TareasEntregadasUnidad (Optimizada) ===
    logger.info("Creating temporary tables - TareasEntregadasUnidad")
    df_tareas_entregadas_unidad = (
        df_t_gc_mae_tarea_evento.alias("Tareas")
        .join(df_t_gc_mov_estado_entrega_tarea.alias("EstadoTrabajos"),
              col("Tareas.tareaid") == col("EstadoTrabajos.tareaid"), "inner")
        .where(
            (col("Tareas.estadoid").cast(IntegerType()) == 264) &
            col("Tareas.sesionaprendizajeid").isNull()
        )
        .groupBy(col("Tareas.unidadaprendizajeid").cast(IntegerType()).alias("unidadaprendizajeid"))
        .agg(sum(when(col("EstadoTrabajos.entregado").cast(IntegerType()) == 1, 1).otherwise(0)).alias("tiene_tareas_entregadas"))
    )
    
    # === RecursosUnidad (Optimizada) ===
    logger.info("Creating temporary tables - RecursosUnidad")
    df_recursos_unidad = (
        df_t_gc_rel_recurso_evento_referencia.alias("RefUnd")
        .join(df_t_gc_mae_recurso_didatico_evento.alias("Recurso"),
              col("RefUnd.recursodidacticoid") == col("Recurso.recursodidacticoid"), "inner")
        .where(col("Recurso.estado").cast(IntegerType()) == 1)
        .groupBy(col("RefUnd.unidadaprendizajeid").cast(IntegerType()).alias("unidadaprendizajeid"))
        .agg(
            sum(when(col("Recurso.tipoid").cast(IntegerType()) == 380, 1).otherwise(0)).alias("cant_enlaces"),
            sum(when(col("Recurso.tipoid").cast(IntegerType()) == 379, 1).otherwise(0)).alias("cant_videos"),
            sum(when(col("Recurso.tipoid").cast(IntegerType())
                    .isin([397,398,399,400,401,402,403,625,626,627,628,653]), 1).otherwise(0)).alias("cant_archivos"),
            count(lit(1)).alias("cant_recursos")
        )
    )
    
    # === UnidadTEA (Optimizada) ===
    logger.info("Creating temporary tables - UnidadTEA")
    df_unidad_tea = (
        df_t_rn_mae_rubro_evaluacion_proceso.alias("RubroP")
        .join(df_t_rn_mov_evaluacion_proceso.alias("Notas"),
              col("RubroP.rubroevalprocesoid") == col("Notas.rubroevalprocesoid"), "inner")
        .join(df_t_gc_mae_tarea_evento.alias("Tarea"),
              col("RubroP.tareaid") == col("Tarea.tareaid"), "inner")
        .join(df_t_gc_mov_estado_entrega_tarea.alias("EstadoTrabajos"),
              col("Tarea.tareaid") == col("EstadoTrabajos.tareaid"), "inner")
        .where(
            (col("RubroP.tiporubroid").cast(IntegerType()) == 471) &
            (col("RubroP.estadoid").cast(IntegerType()) != 280) &
            (col("RubroP.promedio").cast(DecimalType(4,2)) > 0) &
            col("RubroP.sesionaprendizajeid").isNull() &
            (col("Notas.nota").cast(DecimalType(4,2)) > 0) &
            (col("Tarea.estadoid").cast(IntegerType()) == 264) &
            (col("EstadoTrabajos.entregado").cast(IntegerType()) == 1)
        )
        .groupBy(col("RubroP.unidadaprendizajeid").cast(IntegerType()).alias("unidadaprendizajeid"))
        .agg(when(count(lit(1)) > 0, 1).otherwise(0).alias("unidad_tea"))
    )
    
    # === EvalTransversal (Optimizada) ===
    logger.info("Creating temporary tables - EvalTransversal")
    df_eval_transversal = (
        df_t_rn_mae_rubro_evaluacion_proceso.alias("RubroP")
        .join(df_t_gc_mae_competencia.alias("Capacidad"),
              col("RubroP.competenciaid") == col("Capacidad.competenciaid"), "inner")
        .join(df_t_gc_mae_competencia.alias("Competencia"),
              col("Capacidad.parentid") == col("Competencia.competenciaid"), "inner")
        .where(
            (col("Competencia.tipocompetenciaid").cast(IntegerType()) == 348) &
            (col("RubroP.estadoid").cast(IntegerType()) != 280) &
            col("RubroP.sesionaprendizajeid").isNull()
        )
        .groupBy(col("RubroP.unidadaprendizajeid").cast(IntegerType()).alias("unidadaprendizajeid"))
        .agg(when(count(lit(1)) > 0, 1).otherwise(0).alias("tiene_eval_transversal"))
    )
    
    # PASO 3: CONSULTA PRINCIPAL OPTIMIZADA
    logger.info("Creating optimized main query - fact_unidad_detalle")
    
    # Usar broadcast en dimensiones pequeñas para optimizar joins
    df_fact_unidad_detalle = (
        df_planestudios.alias("PlanEstudios")
        .join(df_plancursos.alias("PlanCursos"),
              col("PlanEstudios.id") == col("PlanCursos.planestudioid"), "inner")
        .join(df_t_gc_mae_silabo_evento.alias("mse"),
              col("PlanCursos.id") == col("mse.plancursoid"), "inner")
        .join(df_t_gc_mae_unidad_aprendizaje_evento.alias("und"),
              col("mse.silaboeventoid") == col("und.silaboeventoid"), "inner")
        .join(df_ca_carga_cursos.alias("mcc"),
              col("mcc.id") == col("mse.cargacursoid"), "inner")
        .join(broadcast(df_dim_docente).alias("ddo"),  # Broadcast para tabla de dimensión
              col("mcc.idempleado") == col("ddo.id_docente"), "inner")
        .join(df_ca_carga_academica.alias("mca"),
              col("mcc.idcargaacademica") == col("mca.id"), "inner")
        .join(df_planestudios.alias("mpe"),
              col("mca.idplanestudio") == col("mpe.id"), "inner")
        .join(df_plancursos.alias("mpc"),
              (col("mcc.idplancursos") == col("mpc.id")) & 
              (col("mpc.planestudioid") == col("mca.idplanestudio")), "inner")
        .join(df_programaseducativo.alias("dpe"),
              col("mpe.programaid") == col("dpe.id"), "inner")
        .join(broadcast(df_dim_programa_educativo).alias("mna"),  # Broadcast
              col("dpe.nivelacademicoid") == col("mna.id_programa_educativo"), "inner")
        .join(broadcast(df_dim_periodo).alias("dps"),  # Broadcast
              col("mca.idperiodoacad") == col("dps.id_periodo"), "inner")
        .join(broadcast(df_dim_anio_academico).alias("daa"),  # Broadcast
              col("mca.idanioacademico") == col("daa.id_anio_academico"), "inner")
        .join(broadcast(df_dim_curso).alias("dcu"),  # Broadcast
              col("mpc.cursoid") == col("dcu.id_curso"), "inner")
        .join(df_ca_carga_academica_calendario_periodo.alias("map"),
              col("mca.id") == col("map.cargaacademicaid"), "inner")
        # LEFT JOINS para métricas agregadas
        .join(df_evaluaciones_unidad.alias("eund"),
              col("und.unidadaprendizajeid") == col("eund.unidadaprendizajeid"), "left")
        .join(df_tareas_unidad.alias("tund"),
              col("und.unidadaprendizajeid") == col("tund.unidadaprendizajeid"), "left")
        .join(df_tareas_entregadas_unidad.alias("teund"),
              col("und.unidadaprendizajeid") == col("teund.unidadaprendizajeid"), "left")
        .join(df_recursos_unidad.alias("rund"),
              col("und.unidadaprendizajeid") == col("rund.unidadaprendizajeid"), "left")
        .join(df_unidad_tea.alias("undt"),
              col("und.unidadaprendizajeid") == col("undt.unidadaprendizajeid"), "left")
        .join(df_eval_transversal.alias("etr"),
              col("und.unidadaprendizajeid") == col("etr.unidadaprendizajeid"), "left")
        .where(col("und.estadoid") != 295)
        .selectExpr(
            "CAST(mca.idanioacademico AS INT) as id_anio_academico",
            "CAST(mse.georeferenciaid AS INT) as id_escuela", 
            "CAST(dpe.id AS INT) as id_programa_educativo",
            "CAST(map.calendarioperiodoid AS INT) as id_periodo_academico",
            "dps.id_periodo",
            "CAST(mpc.cursoid AS INT) as id_curso",
            "ddo.id_docente",
            "CAST(mca.idgrupo AS INT) as id_grupo",
            "CAST(mse.silaboeventoid AS INT) as id_silabo",
            "CAST(und.unidadaprendizajeid AS INT) as id_unidad_aprendizaje",
            "CAST(und.nrounidad AS INT) as nro_unidad",
            "und.titulo as titulo",
            "COALESCE(eund.es_evaluado, 0) as es_evaluado",
            "COALESCE(eund.es_eval_publicado, 0) as es_eval_publicado",
            "COALESCE(tund.tiene_tareas, 0) as tiene_tareas",
            "COALESCE(teund.tiene_tareas_entregadas, 0) as tiene_tareas_entregadas",
            "COALESCE(rund.cant_enlaces, 0) as cant_enlaces",
            "COALESCE(rund.cant_videos, 0) as cant_videos",
            "COALESCE(rund.cant_archivos, 0) as cant_archivos",
            "COALESCE(rund.cant_recursos, 0) as cant_recursos",
            "COALESCE(undt.unidad_tea, 0) as unidad_tea",
            "COALESCE(etr.tiene_eval_transversal, 0) as tiene_eval_transversal"
        )
        .orderBy(col("und.nrounidad"))
    )

    # PASO 4: ESCRIBIR RESULTADO
    # Como es una tabla de hechos, usamos write_table con overwrite en lugar de upsert
    partition_columns_array = []  # Sin partición según estándar de fact tables
    
    logger.info(f"Starting overwrite of {target_table_name} (sin partición)")
    spark_controller.write_table(df_fact_unidad_detalle, data_paths.ANALYTICS, target_table_name, partition_columns_array)
    logger.info(f"Overwrite de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing fact_unidad_detalle: {e}")
    raise ValueError(f"Error processing fact_unidad_detalle: {e}")
    
finally:
    # LIMPIEZA: Liberar cache de las tablas cacheadas
    logger.info("Cleaning up cached tables")
    try:
        df_t_rn_mae_rubro_evaluacion_proceso.unpersist()
        df_t_gc_mae_tarea_evento.unpersist()
    except:
        pass  # Ignora errores si las tablas ya fueron liberadas