import mlflow
from mlflow.tracing import set_databricks_monitoring_sql_warehouse_id


def configure(experiment_id: str, warehouse_id: str) -> None:
    if not experiment_id:
        raise ValueError("experiment_id is required")
    if not warehouse_id:
        raise ValueError("warehouse_id is required")
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(experiment_id=experiment_id)
    set_databricks_monitoring_sql_warehouse_id(sql_warehouse_id=warehouse_id, experiment_id=experiment_id)