from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from plugins.config import ChartableConfig, AppConfig
from plugins.utilities.clusters.chartable import get_chartable_configuration

from plugins.utilities.slack_service import success_alert, failure_alert

# ======================================
# CONFIGURATION for CHARTABLE
# ======================================
DAG_NAME = 'chartable_daily_ingest'

DB_NOTEBOOK_PATH = ChartableConfig.chartable_notebook

# ======================================
# DAG DECLARATION
# ======================================
default_args = {
    'owner': 'us_sliu',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(DAG_NAME,
          max_active_runs=1,
          schedule_interval='0 11 * * *',
          default_args=default_args,
          catchup=False)

# ======================================
# TASKS
# ======================================
notebook_task_params = {
    'notebook_task': {
        'notebook_path': DB_NOTEBOOK_PATH,
        'base_parameters': {
            'env': AppConfig.environment
        }
    },
    'new_cluster': get_chartable_configuration(instance_profile=ChartableConfig.instance_profile)
}

# Run Notebook Using Databricks API
ingest_chartable_data = DatabricksSubmitRunOperator(
    task_id='ingest_chartable',
    json=notebook_task_params,
    retries=1,
    on_success_callback=success_alert,
    on_failure_callback=failure_alert,
    dag=dag
)

# ======================================
# DEPENDENCIES
# ======================================
ingest_chartable_data
