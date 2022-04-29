from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from plugins.utilities.clusters.amazon_affiliates import get_cluster_config, get_cluster_libs
from plugins.config import AmazonAffiliatesConfig as config, AppConfig, VaultConfig
from plugins.utilities.slack_service import success_alert, failure_alert

# ======================================
# CONFIGURATION for AMAZON
# ======================================

ENV = AppConfig.environment

new_cluster = get_cluster_config(ENV)

notebook_task_params = {
    'libraries': get_cluster_libs(),
    'notebook_task': {
        'notebook_path': config.date_migration_notebook_name,
        'base_parameters': {
            "env": ENV,
            "token": VaultConfig.token
        }
    },
    'new_cluster': new_cluster
}

# # ======================================
# # DAG DECLARATION
# # ======================================

default_args = {
    'owner': 'us_ad2',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 15),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    dag_id='affiliate_data_migration',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# ======================================
# TASKS
# ======================================

# Run Notebook Using Databricks API on CurrentDate (Yesterday DS)
migrate_amazon_data = DatabricksSubmitRunOperator(
        task_id='migrate_amazon_data',
        dag=dag,
        json=notebook_task_params,
        on_failure_callback=failure_alert,
        on_success_callback=success_alert
)

migrate_amazon_data
