from datetime import datetime, timedelta
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
        'notebook_path': config.ingestion_notebook_name,
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
    'start_date': datetime(2020, 9, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(hours=3),
}

dag = DAG(
    dag_id='amazon_affiliate_ingest',
    default_args=default_args,
    schedule_interval="30 2 * * *",  # 7:30 pm PST / 2am UTC
    max_active_runs=10,
    catchup=False
)

# ======================================
# TASKS
# ======================================

# Run Notebook Using Databricks API on CurrentDate (Yesterday DS)
notebook_task_params['notebook_task']['base_parameters']['target_date'] = '{{ yesterday_ds }}'
ingest_amazon_data = DatabricksSubmitRunOperator(
        task_id='ingest_amazon',
        dag=dag,
        json=notebook_task_params,
        on_failure_callback=failure_alert,
        on_success_callback=success_alert
)

# Run Notebook Using Databricks API on CurrentDate(Yesterday DS) + 3 Days ago
notebook_task_params['notebook_task']['base_parameters']['target_date'] = '{{ macros.ds_add(yesterday_ds, -3) }}'
ingest_amazon_data_three_days_ago = DatabricksSubmitRunOperator(
        task_id='ingest_amazon_data_three_days_ago',
        dag=dag,
        json=notebook_task_params,
        on_failure_callback=failure_alert,
        on_success_callback=success_alert
)

# Process Missing Reports
notebook_task_params['notebook_task']['notebook_path'] = config.missing_report_notebook_name
notebook_task_params['notebook_task']['base_parameters']['target_date'] = '{{ yesterday_ds }}'
process_missing_reports = DatabricksSubmitRunOperator(
        task_id='process_missing_reports',
        dag=dag,
        json=notebook_task_params,
        on_failure_callback=failure_alert,
        on_success_callback=success_alert
)

ingest_amazon_data >> ingest_amazon_data_three_days_ago >> process_missing_reports
