import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number, current_date, upper
from pyspark.sql.types import StringType, DateType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_tipo_venta"

try:
    # Leer directamente desde stage/bigmagic en lugar de domain
    df_m_tipo_documento_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_tipo_documento")
    df_m_procedimiento_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_procedimiento")
    df_m_tipo_transaccion_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_tipo_transaccion")
    df_m_pais_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal=True)
    df_m_compania_stage = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    
    logger.info("Dataframes load successfully from stage")
except Exception as e:
    logger.error(f"Error reading tables from stage: {e}")
    raise ValueError(f"Error reading tables from stage: {e}")

try:
    logger.info("Starting creation of dim_tipo_venta from stage")
    
    # Filtrar tipo de transacción DCV (Documento Comercial de Venta)
    st_tipo_transaccion = df_m_tipo_transaccion_stage.where(
        col("cod_tipo_transaccion") == "DCV"
    ).select(col("cod_compania"), col("cod_documento_transaccion"))

    # Crear dimensión tipo_venta desde stage (misma lógica que en domain)
    df_dim_tipo_venta = (
        df_m_procedimiento_stage.alias("d")
        .join(
            df_m_tipo_documento_stage.alias("c"),
            (col("d.cod_compania") == col("c.cod_compania"))
            & (col("d.cod_documento_transaccion") == col("c.cod_tipo_documento")),
            "inner",
        )
        .join(
            df_m_compania_stage.alias("e"),
            col("d.cod_compania") == col("e.cod_compania"),
            "inner",
        )
        .join(
            df_m_pais_stage.alias("mp"), 
            col("mp.cod_pais") == col("e.cod_pais"), 
            "inner"
        )
        .join(
            st_tipo_transaccion.alias("tt"),
            (col("c.cod_compania") == col("tt.cod_compania"))
            & (col("c.cod_tipo_documento") == col("tt.cod_documento_transaccion")),
            "inner",
        )
        .select(
            # id_tipo_venta: cod_compania + cod_documento_transaccion + cod_procedimiento
            concat(
                trim(col("d.cod_compania")),
                lit("|"),
                trim(col("d.cod_documento_transaccion")),
                lit("|"),
                trim(col("d.cod_procedimiento")),
            ).cast(StringType()).alias("id_tipo_venta"),
            
            col("mp.id_pais").cast(StringType()),
            
            # Campos principales de la dimensión
            col("d.cod_procedimiento").cast(StringType()).alias("cod_tipo_venta"),
            coalesce(col("d.desc_procedimiento"), lit("")).cast(StringType()).alias("desc_tipo_venta"),
            col("d.cod_tipo_operacion").cast(StringType()).alias("cod_tipo_operacion"),
            
            # Campos adicionales útiles
            col("d.cod_compania").cast(StringType()),
            col("d.cod_documento_transaccion").cast(StringType()),
            coalesce(col("c.desc_tipo_documento"), lit("")).cast(StringType()).alias("desc_documento_transaccion"),
            
            # Campos de auditoría
            current_date().cast(DateType()).alias("fecha_creacion"),
            current_date().cast(DateType()).alias("fecha_modificacion")
        )
        .distinct()
    )

    id_columns = ["id_tipo_venta"]
    partition_columns_array = ["id_pais"]
    
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_tipo_venta, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
    
except Exception as e:
    logger.error(f"Error processing df_dim_tipo_venta: {e}")
    raise ValueError(f"Error processing df_dim_tipo_venta: {e}")