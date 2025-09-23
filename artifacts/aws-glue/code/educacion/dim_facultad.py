import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_facultad"

try:
    # Lectura directa desde STAGE (UPEU) y ANALYTICS
    logger.info("Reading tables from UPEU (Stage layer) and Analytics")
    
    # Tabla principal Entidades (equivalente a temp.Entidades)
    df_entidades = spark_controller.read_table(data_paths.UPEU, "entidades")
    
    # Tabla Dim_Universidad desde analytics (ya existente)
    df_dim_universidad = spark_controller.read_table(data_paths.ANALYTICS, "dim_universidad")
    
    logger.info("Tables loaded successfully from UPEU and Analytics")
except Exception as e:
    logger.error(f"Error reading tables from UPEU and Analytics: {e}")
    raise ValueError(f"Error reading tables from UPEU and Analytics: {e}")

try:
    # Aplicar la lógica del SQL complejo - crear las tablas temporales equivalentes
    logger.info("Starting transformations - creating dim_facultad with hierarchical logic")
    
    # Paso 1: Crear #tmp_facultad_nivel1 (Filiales)
    df_tmp_facultad_nivel1 = (
        df_entidades.alias("p")
        .join(
            df_dim_universidad.alias("t"),
            col("p.parentid") == col("t.id_universidad"),
            "inner"
        )
        .select(
            col("p.id").cast(IntegerType()).alias("id_facultad"),
            col("p.nombre_rs").cast(StringType()).alias("nomb_facultad"),
            lit(None).cast(IntegerType()).alias("id_facultad_padre"),
            col("p.parentid").cast(IntegerType()).alias("id_universidad"),
            lit("Filial").cast(StringType()).alias("tipo"),
            col("p.fechaactualizacion").alias("fecha_modificacion")
        )
        .orderBy(col("id_facultad"))
    )
    
    # Paso 2: Crear #tmp_facultad_nivel2 (Facultades)
    df_tmp_facultad_nivel2 = (
        df_entidades.alias("p")
        .join(
            df_tmp_facultad_nivel1.alias("t"),
            col("p.parentid") == col("t.id_facultad"),
            "inner"
        )
        .select(
            col("p.id").cast(IntegerType()).alias("id_facultad"),
            col("p.nombre_rs").cast(StringType()).alias("nomb_facultad"),
            col("p.parentid").cast(IntegerType()).alias("id_facultad_padre"),
            col("t.id_universidad").cast(IntegerType()).alias("id_universidad"),
            lit("Facultad").cast(StringType()).alias("tipo"),
            col("p.fechaactualizacion").alias("fecha_modificacion")
        )
    )
    
    # Paso 3: Crear #Dim_Facultad (resultado final)
    df_dim_facultad = (
        df_tmp_facultad_nivel2.alias("nv2")
        .join(
            df_tmp_facultad_nivel1.alias("nv1"),
            col("nv2.id_facultad_padre") == col("nv1.id_facultad"),
            "inner"
        )
        .select(
            col("nv2.id_facultad"),
            col("nv2.nomb_facultad"),
            col("nv1.id_facultad").alias("id_filial"),
            col("nv1.nomb_facultad").alias("nomb_filial"),
            col("nv2.id_universidad")
        )
        .distinct()  # Aplicar distinct para asegurar unicidad
    )

    # Escribir a analytics usando el método upsert (equivalente al MERGE)
    # El upsert maneja automáticamente both matched/not matched scenarios
    id_columns = ["id_facultad"]  # Clave para el merge
    partition_columns_array = []  # Sin partición específica, puedes agregar si necesitas
    
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_facultad, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing dim_facultad: {e}")
    raise ValueError(f"Error processing dim_facultad: {e}")