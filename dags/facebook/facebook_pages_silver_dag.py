from functools import partial
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from plugins.utilities.clusters.facebook import get_marketing_campaign_cluster_config, get_marketing_campaign_lib
from plugins.config import AppConfig, FacebookPagesConfig
from plugins.utilities.databricks.DatabricksUtils import create_databricks_connection
from plugins.utilities.tardis_sensor import TardisDataStatusSensor
from dags.facebook.utils import update_tardis_status, success_alert, failure_alert

# ======================================
# CONFIGURATION for FACEBOOK
# ======================================

default_args = {'owner': 'us_ochoudha',
                'depends_on_past': False,
                'start_date': datetime(2021, 10, 19),
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5)
                }

# ======================================
# DAG and TASK DECLARATION
# ======================================

with DAG("facebook_pages_dag",
         schedule_interval='00 07 * * *',
         catchup=False,
         default_args=default_args,
         tags=['fivetran', 'native_connector', 'facebook'],
         max_active_runs=1,
         on_success_callback=success_alert
         ) as dag:

    # Dummy start task
    start_task = DummyOperator(
        task_id='start_task')

    # Dummy end task
    end_task = DummyOperator(
        task_id='end_task',
        trigger_rule='all_success')

    # Python operator to create databricks connection
    create_conn = PythonOperator(
        task_id='SetConnection',
        python_callable=create_databricks_connection)

    # Iterate over the connector list
    for connector in FacebookPagesConfig.pages_connectors:
        tardis_poller = TardisDataStatusSensor(
            task_id=f'tardis_poller_{connector}',
            sources=connector,
            start_logdate='{{ next_ds }}',
            status='Data Staged',
            poke_interval=300,
            timeout=3600,
            on_failure_callback=failure_alert
        )

        # Trigger the databricks notebook for silver processing
        silver_data_load = DatabricksSubmitRunOperator(
            task_id=f'silver_processing_{connector}',
            databricks_conn_id='databricks_default',
            json={
                'notebook_task': {
                    'notebook_path': FacebookPagesConfig.pages_silver_notebook,
                    'base_parameters': {
                        'connector': connector,
                        'load_date': '{{ next_ds }}',
                        'env': AppConfig.environment
                    }
                },
                'new_cluster': get_marketing_campaign_cluster_config(AppConfig.environment),
                'libraries': get_marketing_campaign_lib()
            },
            on_failure_callback=failure_alert,
            on_success_callback=partial(update_tardis_status,
                                        'Data',
                                        connector,
                                        'next_ds',
                                        'Data Complete',
                                        'Silver processing completed'),
            task_concurrency=1
        )

        start_task >> create_conn >> tardis_poller >> silver_data_load >> end_task
