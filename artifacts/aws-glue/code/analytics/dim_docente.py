import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, concat_ws, current_date
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_docente"

try:
    # Lectura directa desde STAGE (UPEU) - equivalente a temp.CA_MAE_EMPLEADO y temp.Personas
    logger.info("Reading tables from UPEU (Stage layer)")
    
    # Tabla principal CA_MAE_EMPLEADO (equivalente a temp.CA_MAE_EMPLEADO)
    df_ca_mae_empleado = spark_controller.read_table(data_paths.UPEU, "ca_mae_empleado")
    
    # Tabla Personas (equivalente a temp.Personas)
    df_personas = spark_controller.read_table(data_paths.UPEU, "personas")
    
    logger.info("Tables loaded successfully from UPEU")
except Exception as e:
    logger.error(f"Error reading tables from UPEU: {e}")
    raise ValueError(f"Error reading tables from UPEU: {e}")

try:
    # Aplicar la lógica del MERGE SQL - crear el dataset origen con JOIN
    logger.info("Starting transformations - creating dim_docente from ca_mae_empleado and personas")
    
    df_dim_docente = (
        df_ca_mae_empleado.alias("e")
        .join(
            df_personas.alias("p"),
            col("e.personaid") == col("p.id"),
            "inner"
        )
        .select(
            col("e.id").cast(IntegerType()).alias("id_docente"),
            col("p.apellidopaterno").cast(StringType()).alias("apellido_paterno"),
            col("p.apellidomaterno").cast(StringType()).alias("apellido_materno"),
            col("p.nombres").cast(StringType()).alias("nombre"),
            concat_ws(" ", 
                col("p.apellidopaterno"), 
                col("p.apellidomaterno"), 
                col("p.nombres")
            ).cast(StringType()).alias("nombre_completo")
        )
        .distinct()  # Aplicar distinct para asegurar unicidad
    )

    # Escribir a analytics usando el método upsert (equivalente al MERGE)
    # El upsert maneja automáticamente both matched/not matched scenarios
    id_columns = ["id_docente"]  # Clave para el merge
    partition_columns_array = []  # Sin partición específica, puedes agregar si necesitas
    
    logger.info(f"Starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_docente, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing dim_docente: {e}")
    raise ValueError(f"Error processing dim_docente: {e}")