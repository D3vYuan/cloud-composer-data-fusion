from datetime import datetime as dt
from datetime import timedelta
import json
import datetime
import logging
import ast

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryGetDataOperator
)
from airflow.providers.google.cloud.operators.datafusion import (
    CloudDataFusionStartPipelineOperator
)
from airflow.providers.google.cloud.sensors.datafusion import CloudDataFusionPipelineStateSensor
from airflow.providers.google.cloud.hooks.datafusion import PipelineStates
from airflow.utils.task_group import TaskGroup
logger = logging.getLogger(__name__)

PROJECT_ID = Variable.get("project_id")
BIGQUERY_DATASET_ID = Variable.get("bq_dataset_id")
BIGQUERY_CONFIGURATION_TABLE_NAME = Variable.get("bq_config_table")
BIGQUERY_OUTPUT_TABLE_NAME = Variable.get("bq_output_table")
BIGQUERY_LOCATION = Variable.get("bq_location")

CONFIGURATION_BUSINESS_UNIT_FIELD = "business_unit"
CONFIGURATION_DATA_SOURCE_FIELD = "data_source"
CONFIGURATION_IS_ENABLED_FIELD = "is_enabled"
CONFIGURATION_MAX_RESULTS = int(Variable.get("bq_config_max_results"))

DATA_FUSION_LOCATION = Variable.get("cdf_location")
DATA_FUSION_PIPELINE_NAME = Variable.get("cdf_pipeline_name")
DATA_FUSION_INSTANCE_NAME = Variable.get("cdf_instance")
DATA_FUSION_PIPELINE_TIMEOUT_SECONDS = int(Variable.get("cdf_pipeline_timeout_seconds"))

DAG_RUN_TIMEOUT_MINUTES = int(Variable.get("dag_timeout_minutes"))
DAG_RETRY_INTERVAL_MINUTES = int(Variable.get("dag_retry_interval_minutes"))
RUN_CONFIG_VARIABLE = "cdf_pipeline_run_arguments"

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    # 'start_date': dt.now(),
    'start_date': dt.now() - timedelta(days=1),
    # 'dagrun_timeout': datetime.timedelta(minutes=DAG_RUN_TIMEOUT_MINUTES),
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=DAG_RETRY_INTERVAL_MINUTES),
    'project_id': PROJECT_ID
}

def dict_string_to_json(dict_string):
    if not dict_string:
        return []
    
    try:
        json_list = ast.literal_eval(dict_string)
        return json.loads(json.dumps(json_list))
    except Exception as e:
        logger.error(f"{dict_string} convert to json fails due to {e}")
        return []

# Define the DAG
with DAG(dag_id='cdf-trigger-dag',
         default_args=default_dag_args,
         schedule_interval='@daily',
         tags=["datafusion", "bigquery"]) as dag:
    
    # Filter Runtime Arguments to Select Those That Are Active (is_enabled=true)
    @task
    def filter_arguments(configuration_results):

        if not configuration_results or len(configuration_results) <= 0:
            logger.error(f"No runtime arguments extracted from bigquery {BIGQUERY_DATASET_ID}.{BIGQUERY_CONFIGURATION_TABLE_NAME}")
            return []

        logger.info(f"{len(configuration_results)} runtime arguments extracted from bigquery {BIGQUERY_DATASET_ID}.{BIGQUERY_CONFIGURATION_TABLE_NAME}")
        logger.info(f"{type(configuration_results)} - {configuration_results}")
        
        # Argument is list of list
        # Example: [['hr', 'engineer', 'false'], ['hr', 'payroll', 'true'], ['marketing', 'leads', 'true']]
        active_configurations=[active_configuration for active_configuration in configuration_results if active_configuration[-1] == "true"]
        logger.info(f"Active: {active_configurations}")

        runtime_arguments = []
        for active_configuration in active_configurations:
            business_unit = active_configuration[0]
            data_source = active_configuration[1]
            active_configuration_dict = {
                CONFIGURATION_BUSINESS_UNIT_FIELD: business_unit,
                CONFIGURATION_DATA_SOURCE_FIELD: data_source
            }
            runtime_arguments.append(active_configuration_dict)
        Variable.set(RUN_CONFIG_VARIABLE, runtime_arguments)
        return runtime_arguments
        
    # Truncate the output table in BigQuery
    # Reference: https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html#:~:text=be%20Jinja%20templated.-,tests/system/providers/google/cloud/bigquery/example_bigquery_queries.py,-%5Bsource%5D
    truncate_table_task = BigQueryInsertJobOperator(task_id='truncate_table',
                                                    configuration={
                                                        "query": {
                                                            "query": f"TRUNCATE TABLE {BIGQUERY_DATASET_ID}.{BIGQUERY_OUTPUT_TABLE_NAME}",
                                                            "useLegacySql": False
                                                        }
                                                    })

    # Get Configuration from BigQuery
    # Reference: https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html#:~:text=for%20that%20row.-,tests/system/providers/google/cloud/bigquery/example_bigquery_queries.py,-%5Bsource%5D
    get_configuration_task = BigQueryGetDataOperator(
            task_id="get_configuration",
            dataset_id=BIGQUERY_DATASET_ID,
            table_id=BIGQUERY_CONFIGURATION_TABLE_NAME,
            max_results=CONFIGURATION_MAX_RESULTS,
            selected_fields=f"{CONFIGURATION_BUSINESS_UNIT_FIELD},{CONFIGURATION_DATA_SOURCE_FIELD},{CONFIGURATION_IS_ENABLED_FIELD}"
        )

    # Indicator to show the start of the pipeline
    start_task = EmptyOperator(
        task_id="start"
    )

    # Indicator to show the end of the pipeline
    end_task = EmptyOperator(
        task_id="end"
    )

    # Filter Active Configuration for Data Fusion Pipeline
    filter_task = filter_arguments(get_configuration_task.output)
    start_task >> truncate_table_task >> get_configuration_task  >> filter_task

    configuration_string = Variable.get(RUN_CONFIG_VARIABLE)
    configuration_json = dict_string_to_json(configuration_string)

    # Group Data Fusion Pipeline Jobs
    with TaskGroup(group_id="start_pipelines") as start_pipelines:
        for idx, configuration in enumerate(configuration_json):
            business_unit = configuration[CONFIGURATION_BUSINESS_UNIT_FIELD].lower()
            data_source = configuration[CONFIGURATION_DATA_SOURCE_FIELD].lower()

            # Start the Data Fusion Pipeline
            # [NOTE] success_states - to specify so that only "COMPLETED" is consider success for data fusion pipeline
            # otherwise, it will also consider "RUNNING" as a success state
            # [NOTE] with the success_states in place, the pipeline_timeout will be defaulted to 300 seconds
            # change this timeout according to the longest running job
            # Reference: https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/datafusion.html#:~:text=synchronous%20mode%3A%20CloudDataFusionStartPipelineOperator.-,tests/system/providers/google/cloud/datafusion/example_datafusion.py,-%5Bsource%5D
            pipeline_run = CloudDataFusionStartPipelineOperator(
                location=DATA_FUSION_LOCATION,
                pipeline_name=DATA_FUSION_PIPELINE_NAME,
                instance_name=DATA_FUSION_INSTANCE_NAME,
                asynchronous = False,
                task_id=f"start_pipeline_{business_unit}_{data_source}",
                runtime_args=configuration,
                success_states=[PipelineStates.COMPLETED],
                pipeline_timeout=DATA_FUSION_PIPELINE_TIMEOUT_SECONDS, #default is 300seconds
                retries=3,
                retry_delay=datetime.timedelta(minutes=DAG_RETRY_INTERVAL_MINUTES),
            )

            pipeline_run >> end_task

    filter_task >> start_pipelines
