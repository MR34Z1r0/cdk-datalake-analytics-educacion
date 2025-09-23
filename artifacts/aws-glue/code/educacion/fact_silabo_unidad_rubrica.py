import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import (
    col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date,
    sum, count, countDistinct, isnull, desc, asc, max, expr
)
from pyspark.sql.types import StringType, DateType, IntegerType, DecimalType, TimestampType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "fact_silabo_unidad_rubrica"

try:
    # Lectura directa desde STAGE (UPEU) - todas las tablas necesarias
    logger.info("Reading tables from UPEU (Stage layer)")
    
    # Tablas principales de sílabo y unidades
    df_t_gc_mae_silabo_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_silabo_evento")
    df_t_gc_mae_unidad_aprendizaje_evento = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_unidad_aprendizaje_evento")
    df_t_gc_rel_unidad_apren_evento_tipo = spark_controller.read_table(data_paths.UPEU, "t_gc_rel_unidad_apren_evento_tipo")
    
    # Tablas de evaluación y rúbricas
    df_t_rn_mae_rubro_evaluacion_proceso = spark_controller.read_table(data_paths.UPEU, "t_rn_mae_rubro_evaluacion_proceso")
    
    # Tablas de carga académica
    df_ca_carga_academica = spark_controller.read_table(data_paths.UPEU, "ca_carga_academica")
    df_ca_carga_cursos = spark_controller.read_table(data_paths.UPEU, "ca_carga_cursos")
    df_ca_carga_academica_calendario_periodo = spark_controller.read_table(data_paths.UPEU, "ca_carga_academica_calendario_periodo")
    
    # Tablas de calendario
    df_t_gc_mae_calendario_periodo = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_calendario_periodo")
    df_t_gc_mae_calendario_academico = spark_controller.read_table(data_paths.UPEU, "t_gc_mae_calendario_academico")
    
    # Tablas de plan de estudios
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
    # PASO 1: Crear tabla temporal #temp_silabo_unidad
    logger.info("Creating temporary table - temp_silabo_unidad")
    
    df_temp_silabo_unidad = (
        df_t_gc_mae_silabo_evento.alias("SlbE")
        .join(
            df_t_gc_mae_unidad_aprendizaje_evento.alias("Und"),
            col("Und.silaboeventoid") == col("SlbE.silaboeventoid"),
            "inner"
        )
        .join(
            df_t_gc_rel_unidad_apren_evento_tipo.alias("Und_Tp"),
            col("Und.unidadaprendizajeid") == col("Und_Tp.unidadaprendizajeid"),
            "inner"
        )
        .where(col("Und.estadoid") != 295)
        .select(
            col("SlbE.silaboeventoid").cast(IntegerType()).alias("id_silabo"),
            col("Und.unidadaprendizajeid").cast(IntegerType()).alias("id_unidad_aprendizaje"),
            col("Und.nrounidad").cast(IntegerType()).alias("nro_unidad"),
            col("Und.titulo").alias("titulo")
        )
    )
    
    # PASO 2: Crear tabla temporal #temp_silabo
    logger.info("Creating temporary table - temp_silabo")
    
    df_temp_silabo = (
        df_ca_carga_academica.alias("mca")
        .join(
            df_ca_carga_cursos.alias("mcc"),
            col("mcc.idcargaacademica") == col("mca.id"),
            "inner"
        )
        .join(
            df_t_gc_mae_silabo_evento.alias("mse"),
            col("mcc.id") == col("mse.cargacursoid"),
            "inner"
        )
        .join(
            df_dim_docente.alias("ddo"),
            col("mcc.idempleado") == col("ddo.id_docente"),
            "inner"
        )
        .join(
            df_dim_grupo.alias("ddg"),
            col("mca.idgrupo") == col("ddg.id_grupo"),
            "inner"
        )
        .join(
            df_planestudios.alias("mpe"),
            col("mca.idplanestudio") == col("mpe.id"),
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
            df_plancursos.alias("mpc"),
            (col("mcc.idplancursos") == col("mpc.id")) & (col("mpc.planestudioid") == col("mca.idplanestudio")),
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
            df_t_gc_mae_calendario_periodo.alias("dpa"),
            col("dpa.calendarioperiodoid") == col("map.calendarioperiodoid"),
            "inner"
        )
        .join(
            df_t_gc_mae_calendario_academico.alias("mcac"),
            (col("dpa.calendarioacademicoid") == col("mcac.calendarioacademicoid")) & 
            (col("mca.idanioacademico") == col("mcac.anioacademicoid")),
            "inner"
        )
        .select(
            col("mse.georeferenciaid").cast(IntegerType()).alias("id_escuela"),
            col("mca.idanioacademico").cast(IntegerType()).alias("id_anio_academico"),
            col("dpe.id").cast(IntegerType()).alias("id_programa_educativo"),
            col("map.calendarioperiodoid").cast(IntegerType()).alias("id_periodo_academico"),
            col("dps.id_periodo"),
            col("mpc.cursoid").cast(IntegerType()).alias("id_curso"),
            col("ddo.id_docente"),
            col("mca.idgrupo").cast(IntegerType()).alias("id_grupo"),
            col("mse.silaboeventoid").cast(IntegerType()).alias("id_silabo")
        )
    )
    
    # PASO 3: Crear la consulta principal - fact_silabo_unidad_rubrica
    logger.info("Creating main query - fact_silabo_unidad_rubrica")
    
    df_fact_silabo_unidad_rubrica = (
        df_temp_silabo_unidad.alias("t")
        .join(
            df_t_rn_mae_rubro_evaluacion_proceso.alias("RubroP"),
            (col("t.id_unidad_aprendizaje") == col("RubroP.unidadaprendizajeid")) &
            (col("RubroP.tiporubroid") == 471) &
            ((col("RubroP.sesionaprendizajeid") == "") | col("RubroP.sesionaprendizajeid").isNull()) &
            (col("RubroP.estadoid") != 280),
            "left"
        )
        .join(
            df_temp_silabo.alias("ts"),
            col("ts.id_silabo") == col("t.id_silabo"),
            "left"
        )
        .select(
            col("ts.id_anio_academico"),
            col("ts.id_escuela"),
            col("ts.id_programa_educativo"),
            col("ts.id_periodo_academico"),
            col("ts.id_periodo"),
            col("ts.id_curso"),
            col("ts.id_docente"),
            col("ts.id_grupo"),
            col("t.id_silabo"),
            col("t.id_unidad_aprendizaje"),
            col("t.nro_unidad"),
            col("t.titulo"),
            col("RubroP.titulo").alias("titulo_rubrica"),
            col("RubroP.promedio").cast(DecimalType(10,4)).alias("promedio"),
            col("RubroP.desviacionestandar").cast(DecimalType(10,4)).alias("desviacion_standard"),
            when(col("RubroP.titulo").isNull(), 0).otherwise(1).alias("tiene_rubrica")
        )
    )

    # Escribir a analytics usando overwrite (equivalente a TRUNCATE + INSERT)
    # Como es una tabla de hechos, usamos overwrite en lugar de upsert
    partition_columns_array = []  # Sin partición según estándar de fact tables
    
    logger.info(f"Starting overwrite of {target_table_name}")
    spark_controller.write_table(df_fact_silabo_unidad_rubrica, data_paths.ANALYTICS, target_table_name, partition_columns_array)
    logger.info(f"Overwrite de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing fact_silabo_unidad_rubrica: {e}")
    raise ValueError(f"Error processing fact_silabo_unidad_rubrica: {e}")