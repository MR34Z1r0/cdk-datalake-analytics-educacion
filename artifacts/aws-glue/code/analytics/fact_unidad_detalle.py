import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import (
    col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date,
    sum, count, countDistinct, isnull, isNull, desc, asc, max, expr
)
from pyspark.sql.types import StringType, DateType, IntegerType, DecimalType, TimestampType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "fact_unidad_detalle"

try:
    # Lectura directa desde STAGE (UPEU) - todas las tablas necesarias
    logger.info("Reading tables from UPEU (Stage layer)")
    
    # Tablas principales de evaluación y rúbricas
    df_t_rn_mae_rubro_evaluacion_proceso = spark_controller.read_table(data_paths.UPEU, "t_rn_mae_rubro_evaluacion_proceso")
    df_t_rn_mov_evaluacion_proceso = spark_controller.read_table(data_paths.UPEU, "t_rn_mov_evaluacion_proceso")
    
    # Tablas de tareas
    df_t_gc_mae_tarea_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_tarea_evento")
    df_t_gc_mov_estado_entrega_tarea = spark_controller.read_table(data_paths.UPEU, "t_gc_mov_estado_entrega_tarea")
    
    # Tablas de recursos
    df_t_gc_rel_recurso_evento_referencia = spark_controller.read_table(data_paths.UPEU, "t_gc_rel_recurso_evento_referencia")
    df_t_gc_mae_recurso_didatico_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_recurso_didatico_evento")
    
    # Tablas de competencias
    df_t_gc_mae_competencia = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_competencia")
    
    # Tablas principales de unidades y sílabos
    df_planestudios = spark_controller.read_table(data_paths.UPEU, "planestudios")
    df_plancursos = spark_controller.read_table(data_paths.UPEU, "plancursos")
    df_t_gc_mae_silabo_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_silabo_evento")
    df_t_gc_mae_unidad_aprendizaje_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_unidad_aprendizaje_evento")
    
    # Tablas de carga académica
    df_ca_carga_cursos = spark_controller.read_table(data_paths.UPEU, "ca_carga_cursos")
    df_ca_carga_academica = spark_controller.read_table(data_paths.UPEU, "ca_carga_academica")
    df_ca_carga_academica_calendario_periodo = spark_controller.read_table(data_paths.UPEU, "ca_carga_academica_calendario_periodo")
    df_programaseducativo = spark_controller.read_table(data_paths.UPEU, "programaseducativo")
    
    # Dimensiones ya existentes en analytics
    df_dim_docente = spark_controller.read_table(data_paths.ANALYTICS, "dim_docente")
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
    logger.info("Creating temporary tables - EvaluacionesUnidad")
    
    # #EvaluacionesUnidad
    df_evaluaciones_unidad = (
        df_t_rn_mae_rubro_evaluacion_proceso.alias("RubroP")
        .join(
            df_t_rn_mov_evaluacion_proceso.alias("Notas"),
            col("RubroP.rubroevalprocesoid") == col("Notas.rubroevalprocesoid"),
            "left"
        )
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
    
    # #TareasUnidad
    logger.info("Creating temporary tables - TareasUnidad")
    df_tareas_unidad = (
        df_t_gc_mae_tarea_evento.alias("Tareas")
        .where(
            (col("Tareas.estadoid").cast(IntegerType()) == 264) &
            col("Tareas.sesionaprendizajeid").isNull()
        )
        .groupBy(col("Tareas.unidadaprendizajeid").cast(IntegerType()).alias("unidadaprendizajeid"))
        .agg(count(lit(1)).alias("tiene_tareas"))
    )
    
    # #TareasEntregadasUnidad
    logger.info("Creating temporary tables - TareasEntregadasUnidad")
    df_tareas_entregadas_unidad = (
        df_t_gc_mae_tarea_evento.alias("Tareas")
        .join(
            df_t_gc_mov_estado_entrega_tarea.alias("EstadoTrabajos"),
            col("Tareas.tareaid") == col("EstadoTrabajos.tareaid"),
            "inner"
        )
        .where(
            (col("Tareas.estadoid").cast(IntegerType()) == 264) &
            col("Tareas.sesionaprendizajeid").isNull()
        )
        .groupBy(col("Tareas.unidadaprendizajeid").cast(IntegerType()).alias("unidadaprendizajeid"))
        .agg(
            sum(when(col("EstadoTrabajos.entregado").cast(IntegerType()) == 1, 1).otherwise(0))
            .alias("tiene_tareas_entregadas")
        )
    )
    
    # #RecursosUnidad
    logger.info("Creating temporary tables - RecursosUnidad")
    df_recursos_unidad = (
        df_t_gc_rel_recurso_evento_referencia.alias("RefUnd")
        .join(
            df_t_gc_mae_recurso_didatico_evento.alias("Recurso"),
            col("RefUnd.recursodidacticoid") == col("Recurso.recursodidacticoid"),
            "inner"
        )
        .where(col("Recurso.estado").cast(IntegerType()) == 1)
        .groupBy(col("RefUnd.unidadaprendizajeid").cast(IntegerType()).alias("unidadaprendizajeid"))
        .agg(
            sum(when(col("Recurso.tipoid").cast(IntegerType()) == 380, 1).otherwise(0)).alias("cant_enlaces"),
            sum(when(col("Recurso.tipoid").cast(IntegerType()) == 379, 1).otherwise(0)).alias("cant_videos"),
            sum(when(col("Recurso.tipoid").cast(IntegerType()).isin([397,398,399,400,401,402,403,625,626,627,628,653]), 1).otherwise(0)).alias("cant_archivos"),
            count(lit(1)).alias("cant_recursos")
        )
    )
    
    # #UnidadTEA
    logger.info("Creating temporary tables - UnidadTEA")
    df_unidad_tea = (
        df_t_rn_mae_rubro_evaluacion_proceso.alias("RubroP")
        .join(
            df_t_rn_mov_evaluacion_proceso.alias("Notas"),
            col("RubroP.rubroevalprocesoid") == col("Notas.rubroevalprocesoid"),
            "inner"
        )
        .join(
            df_t_gc_mae_tarea_evento.alias("Tarea"),
            col("RubroP.tareaid") == col("Tarea.tareaid"),
            "inner"
        )
        .join(
            df_t_gc_mov_estado_entrega_tarea.alias("EstadoTrabajos"),
            col("Tarea.tareaid") == col("EstadoTrabajos.tareaid"),
            "inner"
        )
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
        .agg(
            when(count(lit(1)) > 0, 1).otherwise(0).alias("unidad_tea")
        )
    )
    
    # #EvalTransversal
    logger.info("Creating temporary tables - EvalTransversal")
    df_eval_transversal = (
        df_t_rn_mae_rubro_evaluacion_proceso.alias("RubroP")
        .join(
            df_t_gc_mae_competencia.alias("Capacidad"),
            col("RubroP.competenciaid") == col("Capacidad.competenciaid"),
            "inner"
        )
        .join(
            df_t_gc_mae_competencia.alias("Competencia"),
            col("Capacidad.parentid") == col("Competencia.competenciaid"),
            "inner"
        )
        .where(
            (col("Competencia.tipocompetenciaid").cast(IntegerType()) == 348) &
            (col("RubroP.estadoid").cast(IntegerType()) != 280) &
            col("RubroP.sesionaprendizajeid").isNull()
        )
        .groupBy(col("RubroP.unidadaprendizajeid").cast(IntegerType()).alias("unidadaprendizajeid"))
        .agg(
            when(count(lit(1)) > 0, 1).otherwise(0).alias("tiene_eval_transversal")
        )
    )
    
    # PASO 2: Consulta principal - fact_unidad_detalle
    logger.info("Creating main query - fact_unidad_detalle")
    
    df_fact_unidad_detalle = (
        df_planestudios.alias("PlanEstudios")
        .join(
            df_plancursos.alias("PlanCursos"),
            col("PlanEstudios.id") == col("PlanCursos.planestudioid"),
            "inner"
        )
        .join(
            df_t_gc_mae_silabo_evento.alias("mse"),
            col("PlanCursos.id") == col("mse.plancursoid"),
            "inner"
        )
        .join(
            df_t_gc_mae_unidad_aprendizaje_evento.alias("und"),
            col("mse.silaboeventoid") == col("und.silaboeventoid"),
            "inner"
        )
        .join(
            df_ca_carga_cursos.alias("mcc"),
            col("mcc.id") == col("mse.cargacursoid"),
            "inner"
        )
        .join(
            df_dim_docente.alias("ddo"),
            col("mcc.idempleado") == col("ddo.id_docente"),
            "inner"
        )
        .join(
            df_ca_carga_academica.alias("mca"),
            col("mcc.idcargaacademica") == col("mca.id"),
            "inner"
        )
        .join(
            df_planestudios.alias("mpe"),
            col("mca.idplanestudio") == col("mpe.id"),
            "inner"
        )
        .join(
            df_plancursos.alias("mpc"),
            (col("mcc.idplancursos") == col("mpc.id")) & (col("mpc.planestudioid") == col("mca.idplanestudio")),
            "inner"
        )
        .join(
            df_programaseducativo.alias("dpe"),
            col("mpe.programaid") == col("dpe.id"),
            "inner"
        )
        .join(
            df_dim_programa_educativo.alias("mna"),
            col("dpe.nivelacademicoid") == col("mna.id_programa_educativo"),
            "inner"
        )
        .join(
            df_dim_periodo.alias("dps"),
            col("mca.idperiodoacad") == col("dps.id_periodo"),
            "inner"
        )
        .join(
            df_dim_anio_academico.alias("daa"),
            col("mca.idanioacademico") == col("daa.id_anio_academico"),
            "inner"
        )
        .join(
            df_dim_curso.alias("dcu"),
            col("mpc.cursoid") == col("dcu.id_curso"),
            "inner"
        )
        .join(
            df_ca_carga_academica_calendario_periodo.alias("map"),
            col("mca.id") == col("map.cargaacademicaid"),
            "inner"
        )
        .join(
            df_evaluaciones_unidad.alias("eund"),
            col("und.unidadaprendizajeid") == col("eund.unidadaprendizajeid"),
            "left"
        )
        .join(
            df_tareas_unidad.alias("tund"),
            col("und.unidadaprendizajeid") == col("tund.unidadaprendizajeid"),
            "left"
        )
        .join(
            df_tareas_entregadas_unidad.alias("teund"),
            col("und.unidadaprendizajeid") == col("teund.unidadaprendizajeid"),
            "left"
        )
        .join(
            df_recursos_unidad.alias("rund"),
            col("und.unidadaprendizajeid") == col("rund.unidadaprendizajeid"),
            "left"
        )
        .join(
            df_unidad_tea.alias("undt"),
            col("und.unidadaprendizajeid") == col("undt.unidadaprendizajeid"),
            "left"
        )
        .join(
            df_eval_transversal.alias("etr"),
            col("und.unidadaprendizajeid") == col("etr.unidadaprendizajeid"),
            "left"
        )
        .where(col("und.estadoid") != 295)
        .select(
            col("mca.idanioacademico").cast(IntegerType()).alias("id_anio_academico"),
            col("mse.georeferenciaid").cast(IntegerType()).alias("id_escuela"),
            col("dpe.id").cast(IntegerType()).alias("id_programa_educativo"),
            col("map.calendarioperiodoid").cast(IntegerType()).alias("id_periodo_academico"),
            col("dps.id_periodo"),
            col("mpc.cursoid").cast(IntegerType()).alias("id_curso"),
            col("ddo.id_docente"),
            col("mca.idgrupo").cast(IntegerType()).alias("id_grupo"),
            col("mse.silaboeventoid").cast(IntegerType()).alias("id_silabo"),
            col("und.unidadaprendizajeid").cast(IntegerType()).alias("id_unidad_aprendizaje"),
            col("und.nrounidad").cast(IntegerType()).alias("nro_unidad"),
            col("und.titulo").alias("titulo"),
            coalesce(col("eund.es_evaluado"), lit(0)).alias("es_evaluado"),
            coalesce(col("eund.es_eval_publicado"), lit(0)).alias("es_eval_publicado"),
            coalesce(col("tund.tiene_tareas"), lit(0)).alias("tiene_tareas"),
            coalesce(col("teund.tiene_tareas_entregadas"), lit(0)).alias("tiene_tareas_entregadas"),
            coalesce(col("rund.cant_enlaces"), lit(0)).alias("cant_enlaces"),
            coalesce(col("rund.cant_videos"), lit(0)).alias("cant_videos"),
            coalesce(col("rund.cant_archivos"), lit(0)).alias("cant_archivos"),
            coalesce(col("rund.cant_recursos"), lit(0)).alias("cant_recursos"),
            coalesce(col("undt.unidad_tea"), lit(0)).alias("unidad_tea"),
            coalesce(col("etr.tiene_eval_transversal"), lit(0)).alias("tiene_eval_transversal")
        )
        .orderBy(col("und.nrounidad"))
    )

    # Escribir a analytics usando overwrite (equivalente a TRUNCATE + INSERT)
    # Como es una tabla de hechos y usa TRUNCATE + INSERT, usamos overwrite
    partition_columns_array = ["id_anio_academico"]  # Partición por año académico
    
    logger.info(f"Starting overwrite of {target_table_name}")
    spark_controller.overwrite(df_fact_unidad_detalle, data_paths.ANALYTICS, target_table_name, partition_columns_array)
    logger.info(f"Overwrite de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing fact_unidad_detalle: {e}")
    raise ValueError(f"Error processing fact_unidad_detalle: {e}")