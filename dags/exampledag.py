from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.models import Variable
# from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from cosmos.config import ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
from cosmos.constants import LoadMode
from cosmos.airflow.task_group import DbtTaskGroup
from pendulum import datetime
import os
import json
from pathlib import Path

GCP_CONN_ID = Variable.get("GCP_CONN_ID", default_var="bigquery-dbt-con") # Your BigQuery Airflow connection ID

# CONNECTION_ID = "bigquery-dbt-con"  
DATASET_NAME = "dbt-project"
PROJECT_ID = "ecstatic-night-457507-v7"
MODEL_TO_QUERY = "analyze_titanic_data"

# The path to the dbt project
# DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt"
# The path where Cosmos will find the dbt executable
DBT_EXECUTABLE_PATH = Path(f"{os.environ['AIRFLOW_HOME']}/dbt")


DBT_PROJECT_CONFIG_PATH = ProjectConfig(dbt_project_path = Path("/usr/local/airflow/dbt"))
DBT_PROJECT_PROFILE = ProfileConfig(profile_name="dbt_bigquery_data_pipeline",
                                    target_name="dev",
                                    profiles_yml_filepath = Path("/usr/local/airflow/dbt/profiles.yml")
                                )

default_args = {
    "owner": "airflow",
    "depends_on_past":False,
    "email_on_failure": False,
    "retries": 2
}

@dag(
    start_date=datetime(2025, 4, 26),
    schedule_interval=None,
    catchup=False,
    # params={"my_name": kevin},
)

def my_simple_dbt_dag_bigquery():

    dbt_test_raw = BashOperator(
        task_id="dbt_test_raw",
        bash_command="source /usr/local/airflow/dbt_venv/bin/activate && dbt test --select source:*",
        cwd="/usr/local/airflow/dbt/"
    )

    transform_data = DbtTaskGroup(
        group_id="dbt_dag_bigquery_transform",
        project_config=DBT_PROJECT_CONFIG_PATH,
        operator_args = {"install_deps": True},
        profile_config=DBT_PROJECT_PROFILE,
        # execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models'],
            dbt_executable_path="source /usr/local/airflow/dbt_venv/bin/activate && /usr/local/airflow/dbt"
        )
    )        

    # query_table = BigQueryOperator(
    #     task_id="query_table",
    #     gcp_conn_id=GCP_CONN_ID,
    #     sql=f"SELECT * FROM `{PROJECT_ID}.{DATASET_NAME}.{MODEL_TO_QUERY}`",
    #     use_legacy_sql=False,
    # )
    

    dbt_test_raw >> transform_data

my_simple_dbt_dag_bigquery()
