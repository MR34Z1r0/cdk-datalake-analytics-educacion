import boto3
import logging
from typing import Dict, Any

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client("glue")

# Estados que indican que un job está activo
ACTIVE_JOB_STATES = {"STARTING", "RUNNING", "STOPPING"}

def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Verifica si un job de Glue puede ejecutarse o debe esperar.
    """
    try:
        # Validar parámetros requeridos
        required_fields = ["exe_process_id", "own_process_id", "job_name", "COD_PAIS"]
        for field in required_fields:
            if field not in event:
                raise ValueError(f"Campo requerido faltante: {field}")
        
        # Control de procesos
        exe_process_id_list = event["exe_process_id"].split(",")
        should_skip = (
            str(event["own_process_id"]) not in exe_process_id_list 
            and not event.get("redshift", False)
        )
        
        if should_skip:
            return build_response(event, wait=False, ready=False)
        
        # Verificar jobs activos
        response = client.get_job_runs(
            JobName=event["job_name"], 
            MaxResults=50
        )
        
        # Buscar conflictos por país
        for run in response["JobRuns"]:
            if run["JobRunState"] not in ACTIVE_JOB_STATES:
                continue
                
            run_country = run.get("Arguments", {}).get("--COD_PAIS")
            if run_country == event["COD_PAIS"]:
                logger.info(f"Job activo encontrado para país {event['COD_PAIS']}")
                return build_response(event, wait=True)
        
        # No hay conflictos
        logger.info(f"No hay jobs activos para país {event['COD_PAIS']}")
        return build_response(event, wait=False, ready=True)
        
    except Exception as e:
        logger.error(f"Error en lambda_handler: {str(e)}")
        return {"ready": False, "error": True, "msg": str(e)}

def build_response(event: Dict[str, Any], wait: bool = False, ready: bool = False) -> Dict[str, Any]:
    """Construye la respuesta estándar."""
    return {
        "wait": wait,
        "error": False,
        "ready": ready,
        "table": event.get("table"),
        "job_name": event.get("job_name"),
        "INSTANCIAS": event.get("INSTANCIAS"),
        "COD_PAIS": event.get("COD_PAIS"),
        "ORIGIN": event.get("ORIGIN", "AJE"),
        "exe_process_id": event.get("exe_process_id"),
        "own_process_id": event.get("own_process_id"),
        "redshift": event.get("redshift"),
    }