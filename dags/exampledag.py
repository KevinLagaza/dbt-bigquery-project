# from airflow.decorators import dag
from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pathlib import Path
from datetime import datetime, timedelta

# # from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
# from cosmos.config import ProjectConfig, ProfileConfig, RenderConfig
# from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
# from cosmos.constants import LoadMode
# from cosmos.airflow.task_group import DbtTaskGroup
# from pendulum import datetime
from datetime import datetime, timedelta
# import os
# import json

GCP_CONN_ID = Variable.get("GCP_CONN_ID", default_var="bigquery_con") # Your BigQuery Airflow connection ID
hook_bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
hook_gcs = GCSHook(gcp_conn_id=GCP_CONN_ID)
# client = hook.get_client()

# # CONNECTION_ID = "bigquery-dbt-con"  
# DATASET_NAME = "dbt-project"
# PROJECT_ID = "ecstatic-night-457507-v7"
# MODEL_TO_QUERY = "analyze_titanic_data"

# # The path to the dbt project
# # DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt"
# # The path where Cosmos will find the dbt executable
# DBT_EXECUTABLE_PATH = Path(f"{os.environ['AIRFLOW_HOME']}/dbt")


# DBT_PROJECT_CONFIG_PATH = ProjectConfig(dbt_project_path = Path("/usr/local/airflow/dbt"))
# DBT_PROJECT_PROFILE = ProfileConfig(profile_name="dbt_bigquery_data_pipeline",
#                                     target_name="dev",
#                                     profiles_yml_filepath = Path("/usr/local/airflow/dbt/profiles.yml")
#                                 )

# default_args = {
#     "owner": "airflow",
#     "depends_on_past":False,
#     "email_on_failure": False,
#     "retries": 2
# }

# @dag(
#     start_date=datetime(2025, 4, 26),
#     schedule_interval=None,
#     catchup=False,
#     # params={"my_name": kevin},
# )

# def my_simple_dbt_dag_bigquery():

#     dbt_test_raw = BashOperator(
#         task_id="dbt_test_raw",
#         bash_command="source /usr/local/airflow/dbt_venv/bin/activate && dbt test --select source:*",
#         cwd="/usr/local/airflow/dbt/"
#     )

#     transform_data = DbtTaskGroup(
#         group_id="dbt_dag_bigquery_transform",
#         project_config=DBT_PROJECT_CONFIG_PATH,
#         operator_args = {"install_deps": True},
#         profile_config=DBT_PROJECT_PROFILE,
#         # execution_config=execution_config,
#         render_config=RenderConfig(
#             load_method=LoadMode.DBT_LS,
#             select=['path:models'],
#             dbt_executable_path="source /usr/local/airflow/dbt_venv/bin/activate && /usr/local/airflow/dbt"
#         )
#     )        

#     # query_table = BigQueryOperator(
#     #     task_id="query_table",
#     #     gcp_conn_id=GCP_CONN_ID,
#     #     sql=f"SELECT * FROM `{PROJECT_ID}.{DATASET_NAME}.{MODEL_TO_QUERY}`",
#     #     use_legacy_sql=False,
#     # )
    

#     dbt_test_raw >> transform_data

# my_simple_dbt_dag_bigquery()

# ===================================================================================================================================

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.decorators import dag, task, task_group


datasets = ['bronze', 'silver', 'gold']

def read_sql_file(file_path:Path):
    with open(file_path, 'r') as file:
        return file.read()

@dag(
        start_date=datetime(2025, 5, 8),
        schedule=None,
        template_searchpath=['/usr/local/airflow/sql/']
    )
def create_bigquery_dwh_dag():
    
    @task_group
    def create_datasets():
        from airflow.exceptions import AirflowFailException
        success = True
        for dataset in datasets:
            try:
                print(f"Creation du dataset: {dataset}")
                BigQueryCreateEmptyDatasetOperator(
                    task_id=f'create_{dataset}',
                    dataset_id=f'{dataset}',
                    location='US',
                    project_id='ecstatic-night-457507-v7',
                    gcp_conn_id=GCP_CONN_ID,
                    exists_ok=True,  # Skip if dataset exists
                )
                # .execute(context={})
            except Exception as e:
                print(f"Echec de la creation du dataset {dataset}: {str(e)}")
                success = False
        if not success:
            raise AirflowFailException("Dataset creation failed")

    @task_group
    def load_data_into_bronze():
        """
            Chargement des données dans la zone Bronze
        """
        # for test
        # print(hook_gcs.list('demo_etl_data_ing_roubaix', prefix='crm/'))
        # print(hook_bq.get_client().get_table('ecstatic-night-457507-v7.bronze.crm_cust_info'))

        create_tables_in_bronze = BigQueryInsertJobOperator(
            task_id=f'create_initial_tables_in_bronze',
            configuration={
                "query": {
                    "query": read_sql_file(Path("/usr/local/airflow/sql/bronze/ddl_bronze_table.sql")),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        load_csv_into_bronze_crm_cust_info = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_bronze_crm_cust_info',
            bucket='demo_etl_data_ing_roubaix',
            source_objects=['crm/cust_info.csv'],
            source_format='CSV',
            destination_project_dataset_table="ecstatic-night-457507-v7.bronze.crm_cust_info",
            project_id='ecstatic-night-457507-v7',
            field_delimiter=',',
            autodetect=True,
            create_disposition='CREATE_IF_NEEDED', 
            impersonation_chain=["dbt-user-dev@ecstatic-night-457507-v7.iam.gserviceaccount.com"],
            skip_leading_rows=1,
            write_disposition='WRITE_EMPTY',
            gcp_conn_id=GCP_CONN_ID,           
        )

        load_csv_into_bronze_crm_prd_info = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_bronze_crm_prd_info',
            bucket='demo_etl_data_ing_roubaix',
            source_objects=['crm/prd_info.csv'],
            source_format='CSV',
            destination_project_dataset_table="ecstatic-night-457507-v7.bronze.crm_prd_info",
            field_delimiter=',',
            autodetect=True,
            create_disposition='CREATE_IF_NEEDED',
            impersonation_chain=["dbt-user-dev@ecstatic-night-457507-v7.iam.gserviceaccount.com"],
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=GCP_CONN_ID,
        )

        load_csv_into_bronze_crm_sales_details = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_bronze_crm_sales_details',
            bucket='demo_etl_data_ing_roubaix',
            source_objects=['crm/sales_details.csv'],
            source_format='CSV',
            destination_project_dataset_table="ecstatic-night-457507-v7.bronze.crm_sales_details",
            field_delimiter=',',
            autodetect=True,
            create_disposition='CREATE_IF_NEEDED',
            impersonation_chain=["dbt-user-dev@ecstatic-night-457507-v7.iam.gserviceaccount.com"],
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=GCP_CONN_ID,
        )

        load_csv_into_bronze_erp_px_cat_g1v2 = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_bronze_erp_px_cat_g1v2',
            bucket='demo_etl_data_ing_roubaix',
            source_objects=['erp/PX_CAT_G1V2.csv'],
            source_format='CSV',
            destination_project_dataset_table="ecstatic-night-457507-v7.bronze.erp_px_cat_g1v2",
            field_delimiter=',',
            autodetect=True,
            create_disposition='CREATE_IF_NEEDED',
            impersonation_chain=["dbt-user-dev@ecstatic-night-457507-v7.iam.gserviceaccount.com"],
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=GCP_CONN_ID,
            # schema_fields=[
            #     {'name': 'Year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            #     {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
            #     {'name': 'number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            #     {'name': 'result', 'type': 'INTEGER', 'mode': 'NULLABLE'}
            # ],
        )

        load_csv_into_bronze_erp_loc_a101 = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_bronze_erp_loc_a101',
            bucket='demo_etl_data_ing_roubaix',
            source_objects=['erp/LOC_A101.csv'],
            source_format='CSV',
            destination_project_dataset_table="ecstatic-night-457507-v7.bronze.erp_loc_a101",
            field_delimiter=',',
            create_disposition='CREATE_IF_NEEDED',
            allow_quoted_newlines=True,
            impersonation_chain=["dbt-user-dev@ecstatic-night-457507-v7.iam.gserviceaccount.com"],
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=GCP_CONN_ID,
            schema_fields=[
                {'name': 'cid', 'type': 'STRING'},
                {'name': 'cntry', 'type': 'STRING', 'mode': 'NULLABLE'},
            ],
        )

        load_csv_into_bronze_erp_cust_az12 = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_bronze_erp_cust_az12',
            bucket='demo_etl_data_ing_roubaix',
            source_objects=['erp/CUST_AZ12.csv'],
            source_format='CSV',
            destination_project_dataset_table="ecstatic-night-457507-v7.bronze.erp_cust_az12",
            field_delimiter = ',',
            autodetect=True,
            create_disposition='CREATE_IF_NEEDED',
            impersonation_chain=["dbt-user-dev@ecstatic-night-457507-v7.iam.gserviceaccount.com"],
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=GCP_CONN_ID,
            # schema_fields=[
            #     {'name': 'Year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            #     {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
            #     {'name': 'number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            #     {'name': 'result', 'type': 'INTEGER', 'mode': 'NULLABLE'}
            # ],
        )
    
        create_tables_in_bronze >> [load_csv_into_bronze_crm_cust_info, load_csv_into_bronze_crm_prd_info, load_csv_into_bronze_crm_sales_details, load_csv_into_bronze_erp_px_cat_g1v2, load_csv_into_bronze_erp_loc_a101, load_csv_into_bronze_erp_cust_az12]

    @task_group
    def load_data_into_silver():

        create_tables_in_silver = BigQueryInsertJobOperator(
            task_id=f'create_initial_tables_in_silver',
                configuration={
                "query": {
                    "query": read_sql_file(Path("/usr/local/airflow/sql/silver/ddl_silver_table.sql")),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        create_procedure_to_insert_data_in_silver = BigQueryInsertJobOperator(
            task_id=f'create_procedure_to_insert_data_in_silver',
                configuration={
                "query": {
                    "query": read_sql_file(Path("/usr/local/airflow/sql/silver/dml_silver_table.sql")),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        run_procedure_to_insert_data_in_silver = BigQueryInsertJobOperator(
            task_id='run_procedure_to_insert_data_in_silver',
            configuration={
                "query": {
                    "query": """
                        CALL `ecstatic-night-457507-v7.silver.load_silver`(
                        CURRENT_DATE()  -- Airflow execution date
                        );
                    """,
                    "useLegacySql": False
                }
            },
            # params={
            #     'project_id': PROJECT_ID,
            #     'dataset_id': DATASET_ID,
            #     'procedure_name': PROCEDURE_NAME
            # },
            gcp_conn_id=GCP_CONN_ID
        )
        create_tables_in_silver >> create_procedure_to_insert_data_in_silver >> run_procedure_to_insert_data_in_silver
    
    @task_group
    def load_data_into_gold():

        create_dim_tables_in_gold = BigQueryInsertJobOperator(
            task_id=f'create_dim_tables_in_gold',
                configuration={
                "query": {
                    "query": read_sql_file(Path("/usr/local/airflow/sql/gold/dml_gold_dimensions.sql")),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        create_facts_tables_in_gold = BigQueryInsertJobOperator(
            task_id=f'create_facts_tables_in_gold',
                configuration={
                "query": {
                    "query": read_sql_file(Path("/usr/local/airflow/sql/gold/dml_gold_facts.sql")),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        create_dim_tables_in_gold >> create_facts_tables_in_gold

    create_datasets() >> load_data_into_bronze() >> load_data_into_silver() >> load_data_into_gold()

dag = create_bigquery_dwh_dag()



        


    
